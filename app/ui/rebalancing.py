"""Ребалансировка только покупками: расширенная таблица до/после по всем подклассам."""
from collections import defaultdict

import pandas as pd
import streamlit as st

from app.db import (
    get_instrument_provider,
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    list_portfolio_blocks,
    list_positions_by_ticker,
    set_portfolio_blocked,
)
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import (
    get_app_quotes,
    is_crypto_ticker,
    normalize_quote_price_for_valuation,
    request_quotes_refresh,
)
from app.services.rebalancing import TickerPositionValue, compute_rebalance_plan

_NON_US_YF_SUFFIXES = {
    ".AS",
    ".AT",
    ".AX",
    ".BE",
    ".BK",
    ".BR",
    ".CO",
    ".DE",
    ".DU",
    ".F",
    ".HE",
    ".HK",
    ".IR",
    ".JK",
    ".JO",
    ".KQ",
    ".KS",
    ".L",
    ".LS",
    ".MC",
    ".ME",
    ".MI",
    ".MX",
    ".NS",
    ".NZ",
    ".OL",
    ".PA",
    ".PR",
    ".SA",
    ".SG",
    ".SI",
    ".SN",
    ".SR",
    ".SS",
    ".ST",
    ".SW",
    ".SZ",
    ".T",
    ".TA",
    ".TLV",
    ".TO",
    ".TSX",
    ".TW",
    ".VI",
    ".WA",
}


def _is_us_exchange_ticker(ticker: str) -> bool:
    up = (ticker or "").upper().strip()
    if not up:
        return False
    row = get_instrument_provider(up)
    provider = (row[0] if row else None) or ""
    if provider in ("moex_iss", "tbank", "coingecko"):
        return False
    if up.endswith("-EUR") or up.endswith("-RUB"):
        return False
    return not any(up.endswith(sfx) for sfx in _NON_US_YF_SUFFIXES)


def _storage_group(storage_name: str) -> str | None:
    n = (storage_name or "").strip().casefold()
    if not n:
        return None
    if "interactive brokers" in n or n == "ib" or "freedom finance" in n or n == "ff":
        return "Foreign Brokers"
    if (
        "тинько" in n
        or "т-банк" in n
        or "тбанк" in n
        or "t-bank" in n
        or "tbank" in n
        or n == "bcs"
        or "бкс" in n
    ):
        return "Russian Brokers"
    if any(
        x in n
        for x in (
            "wallet",
            "ledger",
            "metamask",
            "trust",
            "tangem",
            "binance",
            "bybit",
            "okx",
            "kucoin",
            "gate",
            "mexc",
            "crypto",
        )
    ):
        return "Crypto"
    return None


def _build_group_funding_plan(
    raw_totals: dict[str, float],
    input_amount: float,
    min_group_amount: float = 20_000.0,
    round_step: float = 1_000.0,
) -> tuple[dict[str, float], float, list[str]]:
    """
    Iterative post-processing for storage-group funding:
    1) absorb residual into groups,
    2) relocate too-small groups (< min_group_amount),
    3) round to round_step while preserving total when possible.
    Returns (group_totals, unsettled_cash, notes).
    """
    notes: list[str] = []
    groups = {
        k: float(raw_totals.get(k, 0.0))
        for k in ("Foreign Brokers", "Russian Brokers", "Crypto")
    }
    V = max(0.0, float(input_amount))
    if V <= 0:
        return groups, 0.0, notes

    def _primary_group() -> str:
        return (
            max(groups.keys(), key=lambda g: groups[g])
            if any(groups.values())
            else "Russian Brokers"
        )

    # Step 1: absorb residual/non-grouped amounts into primary group.
    allocated = sum(groups.values())
    residual = V - allocated
    if abs(residual) > 1e-9:
        pg = _primary_group()
        groups[pg] += residual
        notes.append(f"Остаток {residual:+.2f} добавлен в группу {pg}.")

    # Step 2: relocate tiny groups.
    changed = True
    while changed:
        changed = False
        for g in list(groups.keys()):
            amt = float(groups[g])
            if 0.0 < amt < float(min_group_amount):
                receivers = [x for x in groups.keys() if x != g]
                receiver = (
                    max(receivers, key=lambda x: groups[x]) if receivers else None
                )
                if receiver is None:
                    continue
                groups[receiver] += amt
                groups[g] = 0.0
                notes.append(
                    f"Группа {g} (< {int(min_group_amount)}): {amt:.2f} перенесено в {receiver}."
                )
                changed = True

    # Step 3: round groups to 1,000 RUB with iterative balancing.
    step = float(round_step)
    if step > 0:
        rounded = {g: float(step * round(groups[g] / step)) for g in groups.keys()}
        delta = float(V - sum(rounded.values()))
        # If V is not divisible by step, exact 0 unsettled is mathematically impossible.
        # We still reduce unsettled to |delta| < step by rebalancing in step chunks.
        iter_guard = 0
        while abs(delta) >= step - 1e-9 and iter_guard < 10000:
            iter_guard += 1
            if delta > 0:
                g = max(rounded.keys(), key=lambda x: groups[x])
                rounded[g] += step
                delta -= step
            else:
                candidates = [x for x in rounded.keys() if rounded[x] >= step]
                if not candidates:
                    break
                g = max(candidates, key=lambda x: rounded[x])
                rounded[g] -= step
                delta += step
        groups = rounded
        if abs(delta) > 0.01:
            notes.append(
                f"Точная сходимость до 0 невозможна из-за шага {int(step)} и суммы ввода; остаток {delta:+.2f}."
            )
        return groups, float(delta), notes

    return groups, 0.0, notes


def _persist_storage_blocks(rows: list[dict]) -> None:
    for row in rows:
        set_portfolio_blocked(
            str(row["Тикер"]).upper(),
            int(row["storage_id"]),
            bool(row["Блокировать"]),
        )


@st.dialog("Блокировка покупок по местам хранения")
def _render_blocked_tickers_dialog() -> None:
    current = list_portfolio_blocks(main_only=True)
    df = pd.DataFrame(
        [
            {
                "Тикер": r["ticker"],
                "Место хранения": r["storage_name"],
                "Блокировать": bool(r["blocked"]),
                "storage_id": int(r["storage_id"]),
            }
            for r in current
        ]
    )
    edited = st.data_editor(
        df,
        width="stretch",
        hide_index=True,
        disabled=["Тикер", "Место хранения", "storage_id"],
        column_config={
            "Тикер": st.column_config.TextColumn("Тикер"),
            "Место хранения": st.column_config.TextColumn("Место хранения"),
            "Блокировать": st.column_config.CheckboxColumn(
                "Блокировать", default=False
            ),
            "storage_id": st.column_config.NumberColumn("storage_id", disabled=True),
        },
        key="rebalance_blocked_dialog_table",
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Сохранить", type="primary", key="rebalance_blocked_dialog_save"):
            _persist_storage_blocks(edited.to_dict("records"))
            st.rerun()
    with c2:
        if st.button("Отмена", key="rebalance_blocked_dialog_cancel"):
            st.rerun()


def render_rebalancing():
    st.header(
        "Ребалансировка",
        help=(
            "Учитываются только инструменты с флагом main = 1. "
            "Сумма новых средств распределяется по недовложенным подклассам пропорционально "
            "пробелу до цели; внутри подкласса — по текущим позициям с котировкой. "
            "Продажи не используются."
        ),
    )

    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    process_messages_intro: list[str] = []
    process_messages_warnings: list[str] = []
    process_messages_checks: list[str] = []
    process_messages_residuals: list[str] = []
    warnings: list[str] = []

    control_col1, control_col2 = st.columns(2)
    with control_col1:
        V = st.number_input(
            f"Сумма к инвестированию ({display_ccy})",
            min_value=0.0,
            value=0.0,
            step=100.0,
            format="%.2f",
            key="rebalance_invest_amount",
        )
    with control_col2:
        st.write("")
        st.write("")
        if st.button(
            "Рассчитать покупки",
            type="primary",
            key="rebalance_compute",
            use_container_width=True,
        ):
            request_quotes_refresh()
            st.session_state["rebalance_last_V"] = float(V)

    run_v = float(st.session_state.get("rebalance_last_V", 0.0))

    positions = list_positions_by_ticker(main_only=True)
    if not positions:
        st.info("В основном портфеле нет позиций (`main = 1`).")
        return
    classes = list_asset_classes()
    subclasses = list_asset_subclasses()
    target_pct = {s.id: float(s.target_pct) for s in subclasses}
    sub_names = {s.id: s.name for s in subclasses}
    class_names = {c.id: c.name for c in classes}

    storage_block_rows = list_portfolio_blocks(main_only=True)
    blocked_current = {
        str(r["ticker"]).upper() for r in storage_block_rows if bool(r["blocked"])
    }
    if st.button(
        f"Настроить блокировки по местам ({len(blocked_current)} тик.)",
        key="rebalance_open_blocked_dialog",
        use_container_width=True,
    ):
        _render_blocked_tickers_dialog()
    blocked = {t.upper() for t in list_buy_blocked_tickers(main_only=True)}
    tickers = [p.ticker for p in positions]

    price_tickers = sorted({t.upper() for t in tickers} | {t.upper() for t in blocked})
    quotes = get_app_quotes(price_tickers) if price_tickers else {}

    rows: list[TickerPositionValue] = []
    for p in positions:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
            rows.append(
                TickerPositionValue(
                    ticker=p.ticker,
                    asset_subclass_id=p.asset_subclass_id,
                    value_display=float(value_disp),
                    price_display=float(price_disp),
                )
            )
        else:
            rows.append(
                TickerPositionValue(
                    ticker=p.ticker,
                    asset_subclass_id=p.asset_subclass_id,
                    value_display=None,
                    price_display=None,
                )
            )

    raw_target_sum = sum(target_pct.values())
    if abs(raw_target_sum - 100.0) > 0.05:
        process_messages_intro.append(
            f"Сумма целевых долей подклассов: {raw_target_sum:.3f}%. "
            "Для расчёта веса выполнена нормализация до 100%."
        )

    plan = compute_rebalance_plan(rows, target_pct, sub_names, run_v, blocked_tickers=blocked)

    process_messages_intro.append(
        f"После ввода {format_money(run_v, display_ccy)} целевая капитализация основного портфеля ~{format_money(plan.T, display_ccy)}."
    )

    if plan.unpriced_tickers:
        process_messages_warnings.append(
            "Без котировки (в расчёт не вошли): "
            + ", ".join(plan.unpriced_tickers)
            + "."
        )

    if plan.weights_were_normalized:
        process_messages_intro.append(
            "Целевые веса подклассов нормализованы, потому что исходная сумма долей отличалась от 100%."
        )

    if plan.unallocated:
        for u in plan.unallocated:
            warnings.append(
                f"**{u.subclass_name}**: нужно разместить **{format_money(u.budget, display_ccy)}** — "
                f"{u.reason}. Добавьте позицию с ценой или выберите тикер вручную."
            )

    current_by_sub = defaultdict(float)
    for r in rows:
        if r.value_display is not None:
            current_by_sub[r.asset_subclass_id] += float(r.value_display)

    total_before = float(plan.S)
    # "After" percentages should reflect only фактически размещенные покупки.
    total_after = float(plan.S + plan.total_implied_spend)
    unalloc_by_sub = {u.subclass_id: float(u.budget) for u in plan.unallocated}

    if run_v <= 0:
        process_messages_warnings.append(
            "Для расчёта укажите сумму больше 0 и нажмите «Рассчитать покупки»."
        )
    elif plan.total_gap <= 0:
        process_messages_warnings.append(
            "После увеличения капитала нет подклассов ниже цели; дополнительные покупки могут усилить текущий перекос."
        )

    if warnings:
        process_messages_warnings.extend(warnings)

    subclass_by_id = {s.id: s for s in subclasses}
    class_sort_by_id = {c.id: int(c.sort_order) for c in classes}

    storages_by_ticker_unblocked: dict[str, list[str]] = defaultdict(list)
    for r in storage_block_rows:
        t = str(r["ticker"]).upper()
        if bool(r["blocked"]):
            continue
        sn = str(r["storage_name"] or "").strip()
        if sn:
            storages_by_ticker_unblocked[t].append(sn)
    group_totals_raw = {"Foreign Brokers": 0.0, "Russian Brokers": 0.0, "Crypto": 0.0}
    unmapped_group_total = 0.0

    buy_entries = [
        {
            "ticker": b.ticker,
            "asset_subclass_id": int(b.asset_subclass_id),
            "subclass_name": b.subclass_name,
            "units": float(b.units),
            "implied_spend": float(b.implied_spend),
            "price_display": float(b.price_display),
        }
        for b in plan.suggested_buys
    ]
    buys_sorted = sorted(
        buy_entries,
        key=lambda x: (
            (
                class_sort_by_id.get(
                    subclass_by_id.get(x["asset_subclass_id"]).asset_class_id, 10**9
                )
                if subclass_by_id.get(x["asset_subclass_id"])
                else 10**9
            ),
            (
                int(subclass_by_id.get(x["asset_subclass_id"]).sort_order)
                if subclass_by_id.get(x["asset_subclass_id"])
                else 10**9
            ),
            0 if _is_us_exchange_ticker(x["ticker"]) else 1,
            x["ticker"],
        ),
    )
    for b in buys_sorted:
        ticker_unblocked_storages = storages_by_ticker_unblocked.get(
            str(b["ticker"]).upper(), []
        )
        if ticker_unblocked_storages:
            per_storage = float(b["implied_spend"]) / float(
                len(ticker_unblocked_storages)
            )
            for storage_name in ticker_unblocked_storages:
                grp = _storage_group(storage_name)
                if grp in group_totals_raw:
                    group_totals_raw[grp] += per_storage
                else:
                    unmapped_group_total += per_storage

    if unmapped_group_total > 0:
        # Treat unmapped storages as additional residual to be redistributed by planner.
        primary = (
            max(group_totals_raw.keys(), key=lambda g: group_totals_raw[g])
            if any(group_totals_raw.values())
            else "Russian Brokers"
        )
        group_totals_raw[primary] += float(unmapped_group_total)

    group_totals, unsettled_after_groups, plan_notes = _build_group_funding_plan(
        group_totals_raw,
        run_v,
        min_group_amount=20_000.0,
        round_step=1_000.0,
    )

    # If planner injected extra cash into groups, allocate that extra to instruments in these groups.
    extra_needed_by_group = {
        g: max(0.0, float(group_totals[g]) - float(group_totals_raw.get(g, 0.0)))
        for g in ("Foreign Brokers", "Russian Brokers", "Crypto")
    }
    if any(v > 0.01 for v in extra_needed_by_group.values()):
        entry_by_ticker = {str(e["ticker"]).upper(): e for e in buy_entries}
        # Extend candidates with held priced tickers (even if initially not in suggested buys).
        for r in rows:
            t_up = str(r.ticker).upper()
            if t_up in entry_by_ticker:
                continue
            if (
                r.price_display is None
                or float(r.price_display) <= 0
                or t_up in blocked
            ):
                continue
            entry_by_ticker[t_up] = {
                "ticker": r.ticker,
                "asset_subclass_id": int(r.asset_subclass_id),
                "subclass_name": sub_names.get(
                    int(r.asset_subclass_id), str(r.asset_subclass_id)
                ),
                "units": 0.0,
                "implied_spend": 0.0,
                "price_display": float(r.price_display),
            }
        for g, extra in extra_needed_by_group.items():
            rem = float(extra)
            if rem <= 0.01:
                continue
            group_candidates = []
            for t_up, e in entry_by_ticker.items():
                s_names = storages_by_ticker_unblocked.get(t_up, [])
                belongs = any(_storage_group(sn) == g for sn in s_names)
                if not belongs:
                    continue
                p = float(e["price_display"])
                if p <= 0:
                    continue
                group_candidates.append(e)
            if not group_candidates:
                continue
            crypto_candidates = [
                e for e in group_candidates if is_crypto_ticker(str(e["ticker"]))
            ]
            if crypto_candidates:
                target = max(crypto_candidates, key=lambda e: float(e["implied_spend"]))
                p = float(target["price_display"])
                add_units = max(0.0, round(rem / p, 8))
                add_spend = float(add_units) * p
                if add_units > 0:
                    target["units"] = float(target["units"]) + float(add_units)
                    target["implied_spend"] = float(target["implied_spend"]) + float(
                        add_spend
                    )
                    rem = max(0.0, rem - float(add_spend))
            if rem > 0.01:
                stock_candidates = [
                    e
                    for e in group_candidates
                    if not is_crypto_ticker(str(e["ticker"]))
                ]
                if stock_candidates:
                    cheapest = min(
                        stock_candidates, key=lambda e: float(e["price_display"])
                    )
                    cp = float(cheapest["price_display"])
                    extra_lots = int(rem // cp)
                    if extra_lots > 0:
                        add_spend = float(extra_lots) * cp
                        cheapest["units"] = float(cheapest["units"]) + float(extra_lots)
                        cheapest["implied_spend"] = float(
                            cheapest["implied_spend"]
                        ) + float(add_spend)

        buy_entries = [
            e
            for e in entry_by_ticker.values()
            if float(e["units"]) > 0 or float(e["implied_spend"]) > 0
        ]

    spend_by_sub = defaultdict(float)
    for b in buy_entries:
        spend_by_sub[int(b["asset_subclass_id"])] += float(b["implied_spend"])

    total_after = float(plan.S + sum(float(e["implied_spend"]) for e in buy_entries))
    subclass_meta_rows = []
    for s in sorted(subclasses, key=lambda x: (x.asset_class_id, x.sort_order)):
        cur_val = float(current_by_sub.get(s.id, 0.0))
        buy_val = float(spend_by_sub.get(s.id, 0.0))
        buy_budget_only = float(unalloc_by_sub.get(s.id, 0.0))
        post_val = cur_val + buy_val
        target = float(s.target_pct)
        before_pct = (cur_val / total_before * 100.0) if total_before > 0 else 0.0
        after_pct = (post_val / total_after * 100.0) if total_after > 0 else before_pct
        dev_before = before_pct - target
        dev_after = after_pct - target
        subclass_meta_rows.append(
            {
                "class_name": class_names.get(s.asset_class_id, "—"),
                "subclass_name": s.name,
                "target_pct": target,
                "dev_before": dev_before,
                "dev_after": dev_after,
                "buy_budget_only": buy_budget_only,
            }
        )
    meta_by_subclass_name = {r["subclass_name"]: r for r in subclass_meta_rows}
    table_rows = []
    buys_sorted = sorted(
        buy_entries,
        key=lambda x: (
            (
                class_sort_by_id.get(
                    subclass_by_id.get(x["asset_subclass_id"]).asset_class_id, 10**9
                )
                if subclass_by_id.get(x["asset_subclass_id"])
                else 10**9
            ),
            (
                int(subclass_by_id.get(x["asset_subclass_id"]).sort_order)
                if subclass_by_id.get(x["asset_subclass_id"])
                else 10**9
            ),
            0 if _is_us_exchange_ticker(str(x["ticker"])) else 1,
            str(x["ticker"]),
        ),
    )
    for b in buys_sorted:
        meta = meta_by_subclass_name.get(str(b["subclass_name"]), {})
        qty_disp = (
            int(b["units"])
            if b["units"] == int(b["units"])
            else round(float(b["units"]), 8)
        )
        table_rows.append(
            {
                "Класс": meta.get("class_name", "—"),
                "Подкласс": b["subclass_name"],
                "Цель, %": f"{float(meta.get('target_pct', 0.0)):.3f}",
                "Отклонение до, п.п.": f"{float(meta.get('dev_before', 0.0)):+.3f}",
                "Тикер": b["ticker"],
                "Количество к покупке": str(qty_disp),
                f"Купить на сумму ({display_ccy})": format_money(
                    float(b["implied_spend"]), display_ccy
                ),
                f"Не размещено ({display_ccy})": format_money(
                    float(meta.get("buy_budget_only", 0.0)), display_ccy
                ),
                "Отклонение после, п.п.": f"{float(meta.get('dev_after', 0.0)):+.3f}",
            }
        )

    # Keep strict rounded group plan authoritative (step = 1,000 RUB).
    # Realized per-instrument spends may deviate due lot/fraction constraints.
    realized_group_totals = {
        "Foreign Brokers": 0.0,
        "Russian Brokers": 0.0,
        "Crypto": 0.0,
    }
    for b in buy_entries:
        ticker_unblocked_storages = storages_by_ticker_unblocked.get(
            str(b["ticker"]).upper(), []
        )
        if not ticker_unblocked_storages:
            continue
        per_storage = float(b["implied_spend"]) / float(len(ticker_unblocked_storages))
        for storage_name in ticker_unblocked_storages:
            grp = _storage_group(storage_name)
            if grp in realized_group_totals:
                realized_group_totals[grp] += per_storage

    g1, g2, g3, g4 = st.columns(4)
    g1.metric(
        "Foreign Brokers", format_money(group_totals["Foreign Brokers"], display_ccy)
    )
    g2.metric(
        "Russian Brokers", format_money(group_totals["Russian Brokers"], display_ccy)
    )
    g3.metric("Crypto", format_money(group_totals["Crypto"], display_ccy))
    g4.metric(
        "Unsettled Cash", format_money(float(unsettled_after_groups), display_ccy)
    )
    grouped_plus_unsettled = (
        float(group_totals["Foreign Brokers"])
        + float(group_totals["Russian Brokers"])
        + float(group_totals["Crypto"])
        + float(unsettled_after_groups)
    )
    check_delta = float(run_v) - grouped_plus_unsettled
    if abs(check_delta) <= 0.01:
        process_messages_checks.append(
            "Проверка сумм успешна: группы + unsettled = "
            f"{format_money(grouped_plus_unsettled, display_ccy)} (ввод: {format_money(run_v, display_ccy)})."
        )
    else:
        process_messages_checks.append(
            "Проверка сумм не сошлась: группы + unsettled = "
            f"{format_money(grouped_plus_unsettled, display_ccy)}, "
            f"ввод = {format_money(run_v, display_ccy)}, "
            f"дельта = {format_money(check_delta, display_ccy)}. "
            f"Неразмеченные места хранения: {format_money(unmapped_group_total, display_ccy)}."
        )
    if plan_notes:
        process_messages_residuals.extend(plan_notes)
    realized_total = sum(float(v) for v in realized_group_totals.values())
    realized_delta = float(run_v) - float(realized_total)
    if abs(realized_delta) > 0.01:
        process_messages_residuals.append(
            "Фактическое исполнение по инструментам может отличаться от строгого плана групп "
            f"из-за лотности/дробности; дельта исполнения: {format_money(realized_delta, display_ccy)}."
        )
    realized_buy_total = sum(float(e["implied_spend"]) for e in buy_entries)
    realized_vs_input = float(run_v) - float(realized_buy_total)
    process_messages_checks.append(
        f"Фактические покупки после всех распределений: {format_money(realized_buy_total, display_ccy)}. "
        f"Дельта к вводу: {format_money(realized_vs_input, display_ccy)}."
    )
    process_messages_residuals.append(
        "Строгий остаток плана групп: "
        f"{format_money(float(unsettled_after_groups), display_ccy)}."
    )
    process_messages_residuals.append(
        "Остаток базового алгоритма (до распределения по группам): "
        f"{format_money(float(plan.residual_vs_V), display_ccy)}."
    )
    process_messages = (
        process_messages_intro
        + process_messages_warnings
        + process_messages_checks
        + process_messages_residuals
    )
    if process_messages:
        with st.popover("Инфо по расчету ребалансировки", width="stretch"):
            for msg in process_messages:
                st.markdown(f"- {msg}")
    if table_rows:
        st.dataframe(
            table_rows,
            width="stretch",
            height=700,
            hide_index=True,
            key="rebalance_full_table_df",
        )
    else:
        st.info("Нет доступных покупок по текущим условиям ребалансировки.")
