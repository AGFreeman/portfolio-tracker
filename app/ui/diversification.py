"""Portfolio diversification views: classes/subclasses/tickers/currency/storages."""
from collections import defaultdict

import altair as alt
import pandas as pd
import streamlit as st

from app.db import (
    list_asset_classes,
    list_asset_subclasses,
    list_positions,
    list_positions_by_ticker,
)
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency, infer_trading_currency
from app.services.prices import get_app_quotes, normalize_quote_price_for_valuation


def _build_context():
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    positions_ticker = list_positions_by_ticker()
    positions_storage = list_positions()
    classes = list_asset_classes()
    subclasses = list_asset_subclasses()

    class_by_id = {c.id: c for c in classes}
    subclass_by_id = {s.id: s for s in subclasses}

    tickers = sorted({p.ticker for p in positions_ticker})
    quotes = get_app_quotes(tickers) if tickers else {}

    value_by_ticker: dict[str, float] = {}
    subclass_by_ticker: dict[str, int] = {}
    trading_ccy_by_ticker: dict[str, str] = {}
    unpriced_tickers: set[str] = set()

    for p in positions_ticker:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        trading_ccy_by_ticker[p.ticker] = infer_trading_currency(p.ticker)
        subclass_by_ticker[p.ticker] = p.asset_subclass_id
        if price is None:
            unpriced_tickers.add(p.ticker)
            continue
        value_native = float(price) * float(p.amount)
        value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
        value_by_ticker[p.ticker] = float(value_disp)

    total_value = sum(value_by_ticker.values())

    qty_by_ticker = {p.ticker: float(p.amount) for p in positions_ticker}

    value_by_storage: dict[str, float] = defaultdict(float)
    for p in positions_storage:
        tval = value_by_ticker.get(p.ticker)
        if tval is None:
            continue
        total_qty = qty_by_ticker.get(p.ticker, 0.0)
        if total_qty <= 0:
            continue
        piece = tval * (float(p.amount) / float(total_qty))
        sname = (p.storage_name or "").strip() or "—"
        value_by_storage[sname] += piece

    return {
        "display_ccy": display_ccy,
        "class_by_id": class_by_id,
        "subclass_by_id": subclass_by_id,
        "value_by_ticker": value_by_ticker,
        "subclass_by_ticker": subclass_by_ticker,
        "trading_ccy_by_ticker": trading_ccy_by_ticker,
        "value_by_storage": value_by_storage,
        "total_value": total_value,
        "unpriced_tickers": sorted(unpriced_tickers),
    }


def _pct(part: float, total: float) -> float:
    return (part / total * 100.0) if total > 0 else 0.0


def _render_grouped_bar(df: pd.DataFrame, index_col: str, y_cols: list[str], key: str):
    if df.empty:
        st.info("Нет данных для графика.")
        return

    chart_df = df[[index_col] + y_cols].melt(
        id_vars=[index_col],
        value_vars=y_cols,
        var_name="Метрика",
        value_name="Значение",
    )
    chart_df = chart_df.dropna(subset=["Значение"])

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{index_col}:N", title=index_col),
            y=alt.Y("Значение:Q", title="%"),
            color=alt.Color("Метрика:N", title=""),
            xOffset=alt.XOffset("Метрика:N"),
            tooltip=[
                alt.Tooltip(f"{index_col}:N", title=index_col),
                alt.Tooltip("Метрика:N", title="Метрика"),
                alt.Tooltip("Значение:Q", title="Значение, %", format=".3f"),
            ],
        )
    )
    st.altair_chart(chart, width="stretch")

    with st.expander("Показать таблицу"):
        st.dataframe(df, width="stretch", hide_index=True, key=key)


def _render_by_tickers(ctx):
    st.caption("По тикерам: текущая доля. Цель для тикера показывается, если тикер один в своём подклассе.")

    total = ctx["total_value"]
    value_by_ticker = ctx["value_by_ticker"]
    subclass_by_ticker = ctx["subclass_by_ticker"]
    subclass_by_id = ctx["subclass_by_id"]

    tickers_in_subclass: dict[int, list[str]] = defaultdict(list)
    for t, sid in subclass_by_ticker.items():
        if t in value_by_ticker:
            tickers_in_subclass[sid].append(t)

    rows = []
    for t in sorted(value_by_ticker.keys()):
        val = float(value_by_ticker[t])
        sid = subclass_by_ticker.get(t)
        sub = subclass_by_id.get(sid) if sid is not None else None
        cur_pct = _pct(val, total)

        target_val = None
        delta_text = "—"
        if sub and len(tickers_in_subclass.get(sub.id, [])) == 1:
            target_pct = float(sub.target_pct)
            target_val = target_pct
            delta_text = f"{(cur_pct - target_pct):+.3f}"

        rows.append(
            {
                "Тикер": t,
                "Подкласс": sub.name if sub else "—",
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(val, ctx["display_ccy"]),
                "Текущая доля, %": round(cur_pct, 3),
                "Целевая доля, %": target_val,
                "Отклонение, п.п.": delta_text,
            }
        )
    df = pd.DataFrame(rows)
    _render_grouped_bar(
        df=df,
        index_col="Тикер",
        y_cols=["Текущая доля, %", "Целевая доля, %"],
        key="div_tickers_df",
    )


def _render_by_classes(ctx):
    total = ctx["total_value"]
    value_by_sub: dict[int, float] = defaultdict(float)
    for t, val in ctx["value_by_ticker"].items():
        sid = ctx["subclass_by_ticker"].get(t)
        if sid is not None:
            value_by_sub[sid] += float(val)

    value_by_class: dict[int, float] = defaultdict(float)
    for sid, sval in value_by_sub.items():
        sub = ctx["subclass_by_id"].get(sid)
        if sub:
            value_by_class[sub.asset_class_id] += sval

    rows = []
    for cid, cls in sorted(ctx["class_by_id"].items(), key=lambda x: x[1].sort_order):
        cur_val = float(value_by_class.get(cid, 0.0))
        cur_pct = _pct(cur_val, total)
        tgt = float(cls.target_pct)
        rows.append(
            {
                "Класс": cls.name,
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(cur_val, ctx["display_ccy"]),
                "Текущая доля, %": f"{cur_pct:.3f}",
                "Целевая доля, %": f"{tgt:.3f}",
                "Отклонение, п.п.": f"{(cur_pct - tgt):+.3f}",
            }
        )

    df = pd.DataFrame(rows)
    _render_grouped_bar(
        df=df,
        index_col="Класс",
        y_cols=["Текущая доля, %", "Целевая доля, %"],
        key="div_classes_df",
    )


def _render_by_subclasses(ctx):
    total = ctx["total_value"]
    value_by_sub: dict[int, float] = defaultdict(float)
    for t, val in ctx["value_by_ticker"].items():
        sid = ctx["subclass_by_ticker"].get(t)
        if sid is not None:
            value_by_sub[sid] += float(val)

    rows = []
    for sid, sub in sorted(ctx["subclass_by_id"].items(), key=lambda x: (x[1].asset_class_id, x[1].sort_order)):
        cls = ctx["class_by_id"].get(sub.asset_class_id)
        cur_val = float(value_by_sub.get(sid, 0.0))
        cur_pct = _pct(cur_val, total)
        tgt = float(sub.target_pct)
        rows.append(
            {
                "Класс": cls.name if cls else "—",
                "Подкласс": sub.name,
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(cur_val, ctx["display_ccy"]),
                "Текущая доля, %": f"{cur_pct:.3f}",
                "Целевая доля, %": f"{tgt:.3f}",
                "Отклонение, п.п.": f"{(cur_pct - tgt):+.3f}",
            }
        )

    df = pd.DataFrame(rows)
    _render_grouped_bar(
        df=df,
        index_col="Подкласс",
        y_cols=["Текущая доля, %", "Целевая доля, %"],
        key="div_subclasses_df",
    )


def _render_by_currency(ctx):
    st.caption("По торговой валюте тикера (фиксированные корзины: RUB, USD, EUR).")
    total = ctx["total_value"]

    value_by_ccy: dict[str, float] = {"RUB": 0.0, "USD": 0.0, "EUR": 0.0}
    for t, val in ctx["value_by_ticker"].items():
        ccy = ctx["trading_ccy_by_ticker"].get(t, "USD")
        if ccy not in value_by_ccy:
            ccy = "USD"
        value_by_ccy[ccy] += float(val)

    rows = []
    for ccy in ("RUB", "USD", "EUR"):
        val = float(value_by_ccy.get(ccy, 0.0))
        rows.append(
            {
                "Валюта": ccy,
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(val, ctx["display_ccy"]),
                "Текущая доля, %": round(_pct(val, total), 3),
            }
        )
    df = pd.DataFrame(rows)
    _render_grouped_bar(
        df=df,
        index_col="Валюта",
        y_cols=["Текущая доля, %"],
        key="div_currency_df",
    )

    map_rows = []
    for t in sorted(ctx["value_by_ticker"].keys()):
        val = float(ctx["value_by_ticker"].get(t, 0.0))
        map_rows.append(
            {
                "Тикер": t,
                "Торговая валюта": ctx["trading_ccy_by_ticker"].get(t, "USD"),
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(val, ctx["display_ccy"]),
                "Текущая доля, %": round(_pct(val, total), 3),
            }
        )
    st.caption("Проверка классификации: тикер -> торговая валюта.")
    st.dataframe(
        pd.DataFrame(map_rows),
        width="stretch",
        hide_index=True,
        key="div_currency_ticker_map_df",
    )


def _render_by_storage(ctx):
    total = ctx["total_value"]
    rows = []
    for sname, val in sorted(ctx["value_by_storage"].items(), key=lambda x: x[1], reverse=True):
        rows.append(
            {
                "Место хранения": sname,
                f"Текущая стоимость ({ctx['display_ccy']})": format_money(float(val), ctx["display_ccy"]),
                "Текущая доля, %": round(_pct(float(val), total), 3),
            }
        )
    df = pd.DataFrame(rows)
    _render_grouped_bar(
        df=df,
        index_col="Место хранения",
        y_cols=["Текущая доля, %"],
        key="div_storage_df",
    )


def render_diversification():
    ctx = _build_context()
    if ctx["total_value"] <= 0:
        st.info("Нет оценённых позиций для расчёта диверсификации.")
        return

    if ctx["unpriced_tickers"]:
        st.caption(
            "Без котировки (исключены из долей): **"
            + "**, **".join(ctx["unpriced_tickers"])
            + "**."
        )

    t1, t2, t3, t4, t5 = st.tabs(
        [
            "По тикерам",
            "По классам активов",
            "По подклассам",
            "По валюте активов",
            "По местам хранения",
        ]
    )
    with t1:
        _render_by_tickers(ctx)
    with t2:
        _render_by_classes(ctx)
    with t3:
        _render_by_subclasses(ctx)
    with t4:
        _render_by_currency(ctx)
    with t5:
        _render_by_storage(ctx)
