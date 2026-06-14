"""Ребалансировка только покупками: расширенная таблица до/после по всем подклассам."""
from collections import defaultdict

import pandas as pd
import streamlit as st

from app.db import (
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    list_portfolio_blocks,
    list_positions_by_ticker,
    set_portfolio_blocked,
)
from app.services.fx import convert_amount, format_money
from app.services.portfolio_order import build_portfolio_ticker_order
from app.services.price_currency import resolve_quote_currency
from app.services.prices import (
    get_app_quotes,
    is_crypto_ticker,
    normalize_quote_price_for_valuation,
    request_quotes_refresh,
)
from app.services.rebalancing import (
    TickerPositionValue,
    compute_rebalance_plan,
    compute_ticker_target_values,
)

_STORAGE_GROUPS = ("Foreign Brokers", "Russian Brokers", "Crypto")
_STORAGE_GROUP_LABELS = {
    "Foreign Brokers": "Зарубежные брокеры",
    "Russian Brokers": "Российские брокеры",
    "Crypto": "Крипто",
}
_REBALANCE_TABLE_COLUMNS = (
    "Подкласс",
    "Тикер",
    "Кол-во",
    "Сумма",
    "Цель",
    "Откл до",
    "Откл после",
)


def _sort_buy_entries(
    buy_entries: list[dict],
    *,
    ticker_order: dict[str, int],
) -> list[dict]:
    """Same row order as the main portfolio summary table (Tickers view)."""
    return sorted(
        buy_entries,
        key=lambda x: (
            ticker_order.get(str(x["ticker"]).upper(), 10**9),
            str(x["ticker"]).upper(),
        ),
    )


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


def _spend_by_storage_group(
    ticker: str,
    implied_spend: float,
    storages_by_ticker_unblocked: dict[str, list[str]],
) -> dict[str, float]:
    storage_names = storages_by_ticker_unblocked.get(str(ticker).upper(), [])
    if not storage_names or implied_spend <= 0:
        return {}
    per_storage = float(implied_spend) / float(len(storage_names))
    by_group: dict[str, float] = defaultdict(float)
    for storage_name in storage_names:
        grp = _storage_group(storage_name)
        if grp in _STORAGE_GROUPS:
            by_group[grp] += per_storage
    return dict(by_group)


def _format_signed_money(value: float, currency: str) -> str:
    sym = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(currency.upper(), currency)
    if value >= 0:
        return f"+{value:,.2f} {sym}"
    return f"{value:,.2f} {sym}"


def _format_buy_qty(ticker: str, units: float) -> str:
    if units == int(units) and not is_crypto_ticker(ticker):
        return str(int(units))
    if is_crypto_ticker(ticker):
        return str(round(float(units), 8))
    return str(int(units)) if units == int(units) else f"{float(units):.4f}"


def _ticker_deviation_cells(
    *,
    cur_value: float,
    post_value: float,
    target_before: float | None,
    target_after: float | None,
    total_before: float,
    total_after: float,
    display_ccy: str,
    percent_mode: bool,
) -> tuple[str, str, str]:
    if target_before is None or total_before <= 0:
        target_cell = "—"
        dev_before = "—"
    elif percent_mode:
        target_cell = f"{target_before / total_before * 100.0:.1f}%"
        dev_before = f"{(cur_value / total_before * 100.0 - target_before / total_before * 100.0):+.1f}"
    else:
        target_cell = f"{target_before / total_before * 100.0:.1f}%"
        dev_before = _format_signed_money(cur_value - target_before, display_ccy)

    if target_after is None or total_after <= 0:
        dev_after = "—"
    elif percent_mode:
        dev_after = f"{(post_value / total_after * 100.0 - target_after / total_after * 100.0):+.1f}"
    else:
        dev_after = _format_signed_money(post_value - target_after, display_ccy)

    return target_cell, dev_before, dev_after


def _build_rebalance_table_row(
    *,
    ticker: str,
    subclass_name: str,
    units: float,
    group_spend_display: float,
    quote_ccy: str,
    price_native: float,
    display_ccy: str,
    rub: float,
    eur: float,
    cur_value: float,
    total_buy: float,
    target_before: float | None,
    target_after: float | None,
    total_before: float,
    total_after: float,
    percent_mode: bool,
) -> dict:
    if price_native > 0:
        native_spend = float(units) * float(price_native)
    else:
        native_spend = convert_amount(
            float(group_spend_display), display_ccy, quote_ccy, rub, eur
        )
    post_value = float(cur_value) + float(total_buy)
    target_cell, dev_before, dev_after = _ticker_deviation_cells(
        cur_value=float(cur_value),
        post_value=post_value,
        target_before=target_before,
        target_after=target_after,
        total_before=total_before,
        total_after=total_after,
        display_ccy=display_ccy,
        percent_mode=percent_mode,
    )
    return {
        "Подкласс": subclass_name,
        "Тикер": ticker,
        "Кол-во": _format_buy_qty(ticker, units),
        "Сумма": format_money(native_spend, quote_ccy),
        "Цель": target_cell,
        "Откл до": dev_before,
        "Откл после": dev_after,
    }


def _build_group_table_rows(
    buy_entries: list[dict],
    *,
    group: str,
    ticker_order: dict[str, int],
    storages_by_ticker_unblocked: dict[str, list[str]],
    quote_ccy_by_ticker: dict[str, str],
    price_native_by_ticker: dict[str, float],
    value_by_ticker: dict[str, float],
    target_at_s: dict[str, float],
    target_at_t: dict[str, float],
    total_before: float,
    total_after: float,
    display_ccy: str,
    rub: float,
    eur: float,
    percent_mode: bool,
) -> list[dict]:
    rows: list[dict] = []
    for b in _sort_buy_entries(buy_entries, ticker_order=ticker_order):
        ticker = str(b["ticker"])
        t_up = ticker.upper()
        spend_by_group = _spend_by_storage_group(
            ticker, float(b["implied_spend"]), storages_by_ticker_unblocked
        )
        group_spend = float(spend_by_group.get(group, 0.0))
        if group_spend <= 1e-9:
            continue
        total_spend = float(b["implied_spend"])
        units = float(b["units"])
        if total_spend > 0 and abs(group_spend - total_spend) > 1e-9:
            units = units * (group_spend / total_spend)
        rows.append(
            _build_rebalance_table_row(
                ticker=ticker,
                subclass_name=str(b["subclass_name"]),
                units=units,
                group_spend_display=group_spend,
                quote_ccy=quote_ccy_by_ticker.get(t_up, display_ccy),
                price_native=float(price_native_by_ticker.get(t_up, 0.0)),
                display_ccy=display_ccy,
                rub=rub,
                eur=eur,
                cur_value=float(value_by_ticker.get(t_up, 0.0)),
                total_buy=total_spend,
                target_before=target_at_s.get(t_up),
                target_after=target_at_t.get(t_up),
                total_before=total_before,
                total_after=total_after,
                percent_mode=percent_mode,
            )
        )
    return rows


def _as_rebalance_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(_REBALANCE_TABLE_COLUMNS))
    return pd.DataFrame(rows)[list(_REBALANCE_TABLE_COLUMNS)]


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


def _render_rebalancing_body():
    st.header(
        "Ребалансировка",
        help=(
            "Учитываются только инструменты с флагом main = 1. "
            "Сумма новых средств распределяется по недовложенным подклассам пропорционально "
            "пробелу до цели; внутри подкласса — к целевым долям незаблокированных "
            "(заблокированным цель = текущая стоимость, остаток поровну между остальными). "
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
            width="stretch",
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

    storage_block_rows = list_portfolio_blocks(main_only=True)
    blocked_current = {
        str(r["ticker"]).upper() for r in storage_block_rows if bool(r["blocked"])
    }
    if st.button(
        f"Настроить блокировки по местам ({len(blocked_current)} тик.)",
        key="rebalance_open_blocked_dialog",
        width="stretch",
    ):
        _render_blocked_tickers_dialog()
    blocked = {t.upper() for t in list_buy_blocked_tickers(main_only=True)}
    tickers = [p.ticker for p in positions]

    price_tickers = sorted({t.upper() for t in tickers} | {t.upper() for t in blocked})
    quotes = get_app_quotes(price_tickers) if price_tickers else {}
    quote_ccy_by_ticker: dict[str, str] = {}
    price_native_by_ticker: dict[str, float] = {}

    rows: list[TickerPositionValue] = []
    for p in positions:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = resolve_quote_currency(p.ticker, q.currency if q else None)
        t_up = str(p.ticker).upper()
        quote_ccy_by_ticker[t_up] = quote_ccy
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_native_by_ticker[t_up] = float(price)
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

    if warnings:
        process_messages_warnings.extend(warnings)

    if run_v <= 0:
        process_messages_warnings.append(
            "Для расчёта укажите сумму больше 0 и нажмите «Рассчитать покупки»."
        )
    elif plan.total_gap <= 0:
        process_messages_warnings.append(
            "После увеличения капитала нет подклассов ниже цели; дополнительные покупки могут усилить текущий перекос."
        )

    subclass_by_id = {s.id: s for s in subclasses}
    class_sort_by_id = {c.id: int(c.sort_order) for c in classes}
    ticker_order = build_portfolio_ticker_order(
        [(r.ticker, int(r.asset_subclass_id)) for r in rows],
        subclass_by_id=subclass_by_id,
        class_sort_by_id=class_sort_by_id,
    )

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
    buys_sorted = _sort_buy_entries(buy_entries, ticker_order=ticker_order)
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

    total_before = float(plan.S)
    _, target_at_s = compute_ticker_target_values(rows, target_pct, blocked)
    _, target_at_t = compute_ticker_target_values(
        rows, target_pct, blocked, portfolio_total=float(plan.T)
    )
    value_by_ticker = {
        str(r.ticker).upper(): float(r.value_display or 0.0) for r in rows
    }

    total_after = float(plan.S + sum(float(e["implied_spend"]) for e in buy_entries))

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

    if run_v > 0:
        deviation_mode = st.segmented_control(
            "Отклонение",
            options=["Percent", "Absolute"],
            format_func=lambda x: "%" if x == "Percent" else "Абс.",
            default="Percent",
            key="rebalance_deviation_mode",
            width="content",
        )
        percent_mode = deviation_mode == "Percent"
        table_rows_by_group = {
            group: _build_group_table_rows(
                buy_entries,
                group=group,
                ticker_order=ticker_order,
                storages_by_ticker_unblocked=storages_by_ticker_unblocked,
                quote_ccy_by_ticker=quote_ccy_by_ticker,
                price_native_by_ticker=price_native_by_ticker,
                value_by_ticker=value_by_ticker,
                target_at_s=target_at_s,
                target_at_t=target_at_t,
                total_before=total_before,
                total_after=total_after,
                display_ccy=display_ccy,
                rub=rub,
                eur=eur,
                percent_mode=percent_mode,
            )
            for group in _STORAGE_GROUPS
        }
    else:
        deviation_mode = "Percent"
        table_rows_by_group = {group: [] for group in _STORAGE_GROUPS}

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

    if run_v <= 0:
        st.info("Укажите сумму больше 0 и нажмите «Рассчитать покупки».")
    elif not any(table_rows_by_group.values()):
        st.info("Нет доступных покупок по текущим условиям ребалансировки.")
    else:
        for group in _STORAGE_GROUPS:
            group_rows = table_rows_by_group[group]
            st.subheader(_STORAGE_GROUP_LABELS[group])
            if group_rows:
                st.dataframe(
                    _as_rebalance_dataframe(group_rows),
                    width="stretch",
                    hide_index=True,
                    column_order=list(_REBALANCE_TABLE_COLUMNS),
                    key=f"rebalance_{group.lower().replace(' ', '_')}_{deviation_mode}_df",
                )
            else:
                st.caption("Нет покупок для этой группы.")


def render_rebalancing():
    @st.fragment
    def _fragment():
        _render_rebalancing_body()

    _fragment()
