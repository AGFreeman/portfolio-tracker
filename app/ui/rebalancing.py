"""Ребалансировка только покупками: расширенная таблица до/после по всем подклассам."""
from collections import defaultdict

import pandas as pd
import streamlit as st

from app.db import (
    get_instrument_provider,
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    list_positions_by_ticker,
    set_ticker_buy_blocked,
)
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import get_quotes_cached
from app.services.rebalancing import TickerPositionValue, compute_rebalance_plan

REBALANCE_PRICES_TTL_SEC = 30


def _persist_blocked_tickers(all_tickers: set[str], selected_blocked: set[str]) -> None:
    existing = {t.upper() for t in list_buy_blocked_tickers()}
    target = {t.upper() for t in selected_blocked}
    for t in sorted({x.upper() for x in all_tickers} | existing | target):
        set_ticker_buy_blocked(t, t in target)


@st.dialog("Блокировка покупок тикеров")
def _render_blocked_tickers_dialog(all_tickers: list[str], blocked_default: list[str]) -> None:
    blocked_set = {t.upper() for t in blocked_default}
    df = pd.DataFrame(
        [{"Тикер": t, "Блокировать": t.upper() in blocked_set} for t in all_tickers]
    )
    edited = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        disabled=["Тикер"],
        column_config={
            "Тикер": st.column_config.TextColumn("Тикер"),
            "Блокировать": st.column_config.CheckboxColumn("Блокировать", default=False),
        },
        key="rebalance_blocked_dialog_table",
    )
    c1, c2 = st.columns(2)
    with c1:
        if st.button("Сохранить", type="primary", key="rebalance_blocked_dialog_save"):
            selected = {
                str(row["Тикер"]).upper()
                for _, row in edited.iterrows()
                if bool(row["Блокировать"])
            }
            _persist_blocked_tickers(set(all_tickers), selected)
            st.rerun()
    with c2:
        if st.button("Отмена", key="rebalance_blocked_dialog_cancel"):
            st.rerun()


def render_rebalancing():
    st.caption(
        "Введите сумму **новых** средств в валюте отображения. Расчёт: недовложенные подклассы "
        "получают долю ввода пропорционально **пробелу до целевой доли**; внутри подкласса — **пропорционально "
        "текущей стоимости** тикеров с котировкой. Продажи не учитываются."
    )

    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    warnings: list[str] = []

    V = st.number_input(
        f"Сумма к инвестированию ({display_ccy})",
        min_value=0.0,
        value=0.0,
        step=100.0,
        format="%.2f",
        key="rebalance_invest_amount",
    )

    if st.button("Рассчитать покупки", type="primary", key="rebalance_compute"):
        st.session_state["rebalance_last_V"] = float(V)

    run_v = float(st.session_state.get("rebalance_last_V", 0.0))

    positions = list_positions_by_ticker()
    classes = list_asset_classes()
    subclasses = list_asset_subclasses()
    target_pct = {s.id: float(s.target_pct) for s in subclasses}
    sub_names = {s.id: s.name for s in subclasses}
    class_names = {c.id: c.name for c in classes}

    tickers = [p.ticker for p in positions]
    blocked_current = set(list_buy_blocked_tickers())
    ticker_options = sorted({t.upper() for t in tickers} | {t.upper() for t in blocked_current})
    if st.button(
        f"Настроить блокировки тикеров ({len(blocked_current)})",
        key="rebalance_open_blocked_dialog",
    ):
        _render_blocked_tickers_dialog(ticker_options, sorted({t.upper() for t in blocked_current}))
    blocked = {t.upper() for t in list_buy_blocked_tickers()}

    provider_overrides = {}
    for t in tickers:
        prov = get_instrument_provider(t)
        if prov:
            provider_overrides[t] = prov
    quotes = (
        get_quotes_cached(
            tickers,
            cache_ttl_sec=REBALANCE_PRICES_TTL_SEC,
            provider_overrides=provider_overrides,
        )
        if tickers
        else {}
    )

    rows: list[TickerPositionValue] = []
    for p in positions:
        q = quotes.get(p.ticker)
        price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
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
        warnings.append(
            f"Сумма целевых долей подклассов **{raw_target_sum:.3f}%** — для расчёта веса **нормализовано** до 100%."
        )

    plan = compute_rebalance_plan(rows, target_pct, sub_names, run_v, blocked_tickers=blocked)

    st.caption(f"После ввода **{format_money(run_v, display_ccy)}** целевая капитализация **~{format_money(plan.T, display_ccy)}**.")

    if plan.unpriced_tickers:
        warnings.append(
            "Без котировки (в расчёте **не** участвуют): **"
            + "**, **".join(plan.unpriced_tickers)
            + "**."
        )

    if plan.weights_were_normalized:
        st.caption("Целевые веса подклассов нормализованы, т.к. сумма долей отличалась от 100%.")

    if plan.unallocated:
        for u in plan.unallocated:
            warnings.append(
                f"**{u.subclass_name}**: нужно разместить **{format_money(u.budget, display_ccy)}** — "
                f"{u.reason}. Добавьте позицию с ценой или выберите тикер вручную."
            )

    spend_by_sub = defaultdict(float)
    ticker_buys_by_sub = defaultdict(list)
    for b in plan.suggested_buys:
        spend_by_sub[b.asset_subclass_id] += float(b.implied_spend)
        qty_disp = int(b.units) if b.units == int(b.units) else round(float(b.units), 8)
        ticker_buys_by_sub[b.asset_subclass_id].append((b.ticker, str(qty_disp)))

    current_by_sub = defaultdict(float)
    for r in rows:
        if r.value_display is not None:
            current_by_sub[r.asset_subclass_id] += float(r.value_display)

    total_before = float(plan.S)
    total_after = float(plan.S + plan.total_implied_spend)
    unalloc_by_sub = {u.subclass_id: float(u.budget) for u in plan.unallocated}

    if run_v <= 0:
        st.info("Укажите сумму > 0 и нажмите **Рассчитать покупки**.")
    elif plan.total_gap <= 0:
        warnings.append(
            "Нет подклассов **ниже** целевой доли после увеличения капитала — дополнительные покупки могут усилить перекос."
        )

    if warnings:
        with st.popover(f"Предупреждения ({len(warnings)})", use_container_width=True):
            for msg in warnings:
                st.markdown(f"- {msg}")

    table_rows = []
    for s in sorted(subclasses, key=lambda x: (x.asset_class_id, x.sort_order)):
        cur_val = float(current_by_sub.get(s.id, 0.0))
        buy_val = float(spend_by_sub.get(s.id, 0.0))
        buy_budget_only = float(unalloc_by_sub.get(s.id, 0.0))
        buy_total = buy_val + buy_budget_only
        post_val = cur_val + buy_total
        target = float(s.target_pct)
        before_pct = (cur_val / total_before * 100.0) if total_before > 0 else 0.0
        after_pct = (post_val / total_after * 100.0) if total_after > 0 else before_pct
        dev_before = before_pct - target
        dev_after = after_pct - target
        pairs = ticker_buys_by_sub.get(s.id, [])
        tickers_col = ", ".join(t for t, _ in pairs) or "—"
        amounts_col = ", ".join(a for _, a in pairs) or "—"
        if not pairs and buy_budget_only > 0:
            tickers_col = "Нет доступного тикера"
            amounts_col = "—"
        table_rows.append(
            {
                "Класс": class_names.get(s.asset_class_id, "—"),
                "Подкласс": s.name,
                "Цель, %": f"{target:.3f}",
                "Отклонение до, п.п.": f"{dev_before:+.3f}",
                "Тикеры к покупке": tickers_col,
                "Количество к покупке": amounts_col,
                f"Купить на сумму ({display_ccy})": format_money(buy_total, display_ccy),
                "Отклонение после, п.п.": f"{dev_after:+.3f}",
            }
        )

    st.subheader("Ребалансировка по подклассам (все строки)")
    st.dataframe(table_rows, use_container_width=True, hide_index=True, key="rebalance_full_table_df")
    st.caption(
        f"Покупки после округления лотов: **{format_money(plan.total_implied_spend, display_ccy)}**. "
        f"Остаток кэша: **{format_money(plan.residual_vs_V, display_ccy)}**."
    )
