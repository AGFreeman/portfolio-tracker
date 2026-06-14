"""Read-only portfolio summary: Класс, Подкласс, Тикер, Цена, Количество, Стоимость (по тикеру, без разбивки по местам)."""
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

from app.db import (
    list_positions,
    list_positions_by_ticker,
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    get_instrument_provider,
    get_instrument_main_map,
)
from app.services.fx import convert_amount, format_money
from app.services.performance import (
    compute_current_portfolio_market_value,
    compute_main_group_returns,
)
from app.services.rebalancing import (
    TickerPositionValue,
    compute_ticker_target_values,
)
from app.services.price_currency import (
    bucket_diversification_currency,
    infer_quote_currency,
    resolve_quote_currency,
)
from app.services.prices import (
    get_app_quotes,
    get_quotes_cache_meta,
    is_crypto_ticker,
    normalize_quote_price_for_valuation,
)
from app.ui.live_refresh import live_quotes_run_every
from app.ui.performance import _get_portfolio_performance

_SUBCLASS_BY_ID = None

_RETURN_METRIC_OPTIONS = ("PNL", "MWR", "MWR_XIRR")
_RETURN_PERIOD_OPTIONS = ("ALL", "YTD", "1Y", "6M", "3M", "1M")
_RETURN_METRIC_LABELS = {
    "PNL": "P&L",
    "MWR": "MWR",
    "MWR_XIRR": "MWR (XIRR)",
}
_RETURN_PERIOD_LABELS = {
    "ALL": "Все время",
    "YTD": "YTD",
    "1Y": "1Y",
    "6M": "6M",
    "3M": "3M",
    "1M": "1M",
}

_NON_US_YF_SUFFIXES = {
    ".AS", ".AT", ".AX", ".BE", ".BK", ".BR", ".CO", ".DE", ".DU", ".F", ".HE",
    ".HK", ".IR", ".JK", ".JO", ".KQ", ".KS", ".L", ".LS", ".MC", ".ME", ".MI",
    ".MX", ".NS", ".NZ", ".OL", ".PA", ".PR", ".SA", ".SG", ".SI", ".SN", ".SR",
    ".SS", ".ST", ".SW", ".SZ", ".T", ".TA", ".TLV", ".TO", ".TSX", ".TW", ".VI",
    ".WA",
}


def _is_us_exchange_ticker(ticker: str) -> bool:
    """Heuristic: US-related = Yahoo ticker without explicit non-US suffix."""
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


def _subclass_by_id():
    global _SUBCLASS_BY_ID
    if _SUBCLASS_BY_ID is None:
        subclasses = list_asset_subclasses()
        _SUBCLASS_BY_ID = {s.id: s for s in subclasses}
    return _SUBCLASS_BY_ID


def _class_by_id():
    return {c.id: c for c in list_asset_classes()}


_DEVIATION_OK_STYLE = "color: #81c784; font-weight: 500"
_DEVIATION_BAD_STYLE = "color: #ef5350; font-weight: 500"
_RETURN_POS_STYLE = "color: #81c784; font-weight: 500"
_RETURN_NEG_STYLE = "color: #ef5350; font-weight: 500"
_BLOCKED_STYLE = "color: #9e9e9e; font-weight: 500"


def _fmt_return_pct(value: float) -> str:
    return f"{float(value) * 100.0:+.2f}%"


def _return_column_label(metric: str, period: str, *, pnl_display: str | None = None) -> str:
    metric_label = _RETURN_METRIC_LABELS.get(metric, metric)
    period_label = _RETURN_PERIOD_LABELS.get(period, period)
    if metric == "PNL":
        mode = ", абс." if pnl_display == "Absolute" else ", %"
        if period == "ALL":
            return f"P&L нереал.{mode} ({period_label})"
        return f"Δ P&L нереал.{mode} ({period_label})"
    return f"{metric_label} ({period_label})"


def _format_return_cell(
    metric: str,
    value: float | None,
    *,
    pnl_display: str | None = None,
    display_ccy: str | None = None,
) -> str:
    if value is None:
        return "—"
    if metric == "PNL" and pnl_display == "Absolute" and display_ccy:
        return _format_signed_money(float(value), display_ccy)
    return _fmt_return_pct(float(value))


def _return_row_key(row: dict, group_mode: str) -> str:
    index_col = _MAIN_GROUP_INDEX_COL[group_mode]
    if group_mode == "Tickers":
        return str(row.get("Тикер") or "—")
    return str(row.get(index_col) or "—")


def _format_signed_money(value: float, currency: str) -> str:
    sym = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(currency.upper(), currency)
    if value >= 0:
        return f"+{value:,.2f} {sym}"
    return f"{value:,.2f} {sym}"


def _format_qty_display(ticker: str, amount: float) -> str:
    if is_crypto_ticker(ticker):
        return str(amount)
    return str(int(round(amount)))


def _deviation_metrics(
    value: float | None, target: float | None
) -> tuple[float | None, float | None]:
    if value is None or target is None:
        return None, None
    abs_dev = float(value) - float(target)
    pct_dev = abs_dev / float(target) * 100.0 if float(target) > 0 else None
    return pct_dev, abs_dev


def _enrich_main_ticker_record(
    record: dict,
    target_by_ticker: dict[str, float],
    blocked_tickers: set[str],
) -> None:
    blocked = {t.upper() for t in blocked_tickers}
    ticker_up = str(record["ticker"]).upper()
    target_value = target_by_ticker.get(ticker_up)
    value_disp = record.get("value")
    pct_dev, abs_dev = _deviation_metrics(value_disp, target_value)
    record["target"] = target_value
    record["deviation_pct"] = pct_dev
    record["deviation_abs"] = abs_dev
    record["is_blocked"] = ticker_up in blocked


def _build_main_display_rows(
    rows: list[dict],
    deviations_pct: list[float | None],
    deviations_abs: list[float | None],
    percent_mode: bool,
    display_ccy: str,
) -> list[dict]:
    display_rows = []
    for i, row in enumerate(rows):
        out = {k: v for k, v in row.items() if not str(k).startswith("_")}
        if percent_mode:
            dev = deviations_pct[i]
            out["Отклонение от цели"] = f"{dev:+.1f}%" if dev is not None else "—"
        else:
            dev = deviations_abs[i]
            out["Отклонение от цели"] = (
                _format_signed_money(dev, display_ccy) if dev is not None else "—"
            )
        display_rows.append(out)
    return display_rows


def _build_main_returns_display_rows(
    rows: list[dict],
    return_column: str,
    *,
    return_metric: str,
    pnl_display: str | None,
    display_ccy: str,
) -> list[dict]:
    display_rows = []
    for row in rows:
        out = {k: v for k, v in row.items() if not str(k).startswith("_")}
        ret = row.get("_return_num")
        out[return_column] = _format_return_cell(
            return_metric,
            ret,
            pnl_display=pnl_display,
            display_ccy=display_ccy,
        )
        display_rows.append(out)
    return display_rows


_GROUP_MODES_WITHOUT_TARGETS = frozenset({"Currencies", "Storage"})

_MAIN_GROUP_INDEX_COL = {
    "Tickers": "Тикер",
    "Subclasses": "Подкласс",
    "Classes": "Класс",
    "Currencies": "Валюта",
    "Storage": "Место хранения",
}


def _value_only_row(display_ccy: str, value: float | None, **columns: str) -> dict:
    row = dict(columns)
    row["Стоимость"] = format_money(value, display_ccy) if value is not None else "—"
    row["_value_num"] = value
    row["_target_num"] = None
    return row


def _aggregated_row(
    display_ccy: str,
    value: float | None,
    target: float | None,
    *,
    column_mode: str = "targets",
    **columns: str,
) -> dict:
    pct_dev, abs_dev = _deviation_metrics(value, target)
    row = dict(columns)
    row["Стоимость"] = format_money(value, display_ccy) if value is not None else "—"
    if column_mode == "targets":
        row["Целевая стоимость"] = (
            format_money(target, display_ccy) if target is not None else "—"
        )
        row["_deviation_pct"] = pct_dev
        row["_deviation_abs"] = abs_dev
        row["_is_blocked"] = False
    row["_value_num"] = value
    row["_target_num"] = target if column_mode == "targets" else None
    return row


def _extract_row_metrics(rows: list[dict]) -> tuple[list[float | None], list[float | None], list[bool]]:
    return (
        [r.pop("_deviation_pct", None) for r in rows],
        [r.pop("_deviation_abs", None) for r in rows],
        [bool(r.pop("_is_blocked", False)) for r in rows],
    )


def _rows_by_subclasses(records: list[dict], display_ccy: str, *, column_mode: str) -> list[dict]:
    grouped: dict[int, dict] = {}
    for rec in records:
        sid = rec.get("subclass_id")
        if sid is None:
            continue
        bucket = grouped.setdefault(
            sid,
            {
                "class_name": rec["class_name"],
                "subclass_name": rec["subclass_name"],
                "class_sort": rec["class_sort"],
                "subclass_sort": rec["subclass_sort"],
                "value": 0.0,
                "target": 0.0,
                "has_value": False,
                "has_target": False,
            },
        )
        if rec.get("value") is not None:
            bucket["value"] += float(rec["value"])
            bucket["has_value"] = True
        if rec.get("target") is not None:
            bucket["target"] += float(rec["target"])
            bucket["has_target"] = True

    rows = []
    for sid in sorted(
        grouped.keys(),
        key=lambda x: (grouped[x]["class_sort"], grouped[x]["subclass_sort"]),
    ):
        g = grouped[sid]
        value = float(g["value"]) if g["has_value"] else None
        target = float(g["target"]) if g["has_target"] else None
        rows.append(
            _aggregated_row(
                display_ccy,
                value,
                target,
                column_mode=column_mode,
                Класс=g["class_name"],
                Подкласс=g["subclass_name"],
            )
        )
    return rows


def _rows_by_classes(records: list[dict], display_ccy: str, *, column_mode: str) -> list[dict]:
    grouped: dict[int, dict] = {}
    for rec in records:
        cid = rec.get("class_id")
        if cid is None:
            continue
        bucket = grouped.setdefault(
            cid,
            {
                "class_name": rec["class_name"],
                "class_sort": rec["class_sort"],
                "value": 0.0,
                "target": 0.0,
                "has_value": False,
                "has_target": False,
            },
        )
        if rec.get("value") is not None:
            bucket["value"] += float(rec["value"])
            bucket["has_value"] = True
        if rec.get("target") is not None:
            bucket["target"] += float(rec["target"])
            bucket["has_target"] = True

    rows = []
    for cid in sorted(grouped.keys(), key=lambda x: grouped[x]["class_sort"]):
        g = grouped[cid]
        value = float(g["value"]) if g["has_value"] else None
        target = float(g["target"]) if g["has_target"] else None
        rows.append(
            _aggregated_row(
                display_ccy,
                value,
                target,
                column_mode=column_mode,
                Класс=g["class_name"],
            )
        )
    return rows


def _rows_by_currencies(records: list[dict], display_ccy: str) -> list[dict]:
    grouped: dict[str, float] = {}
    for rec in records:
        if rec.get("value") is None:
            continue
        ccy = rec.get("currency_bucket") or "USD"
        grouped[ccy] = grouped.get(ccy, 0.0) + float(rec["value"])

    rows = []
    for ccy in ("RUB", "USD", "EUR"):
        if ccy not in grouped:
            continue
        rows.append(_value_only_row(display_ccy, grouped[ccy], Валюта=ccy))
    return rows


def _rows_by_storage(
    records: list[dict],
    storage_positions,
    qty_by_ticker: dict[str, float],
    display_ccy: str,
) -> list[dict]:
    record_by_ticker = {str(r["ticker"]).upper(): r for r in records}
    grouped: dict[str, float] = {}
    for p in storage_positions:
        t_up = (p.ticker or "").upper()
        rec = record_by_ticker.get(t_up)
        if rec is None:
            continue
        total_qty = float(qty_by_ticker.get(p.ticker, 0.0))
        if total_qty <= 0:
            continue
        share = float(p.amount) / total_qty
        sname = (p.storage_name or "").strip() or "—"
        bucket = grouped.setdefault(sname, 0.0)
        if rec.get("value") is not None:
            grouped[sname] = bucket + float(rec["value"]) * share

    rows = []
    for sname, value in sorted(grouped.items(), key=lambda x: x[1], reverse=True):
        rows.append(_value_only_row(display_ccy, value, **{"Место хранения": sname}))
    return rows


def _build_main_group_rows(
    records: list[dict],
    group_mode: str,
    display_ccy: str,
    storage_positions,
    qty_by_ticker: dict[str, float],
    *,
    column_mode: str = "targets",
) -> list[dict]:
    if group_mode == "Subclasses":
        return _rows_by_subclasses(records, display_ccy, column_mode=column_mode)
    if group_mode == "Classes":
        return _rows_by_classes(records, display_ccy, column_mode=column_mode)
    if group_mode == "Currencies":
        return _rows_by_currencies(records, display_ccy)
    if group_mode == "Storage":
        return _rows_by_storage(records, storage_positions, qty_by_ticker, display_ccy)

    sorted_records = sorted(
        records,
        key=lambda rec: (
            int(rec["class_sort"]),
            int(rec["subclass_sort"]),
            0 if _is_us_exchange_ticker(str(rec["ticker"])) else 1,
            str(rec["ticker"]),
        ),
    )
    rows = []
    for rec in sorted_records:
        value = rec.get("value")
        target = rec.get("target")
        row = {
            "Класс": rec["class_name"],
            "Подкласс": rec["subclass_name"],
            "Тикер": rec["ticker"],
            "Цена": rec["price_cell"],
            "Количество": rec["qty_disp"],
            "Стоимость": format_money(value, display_ccy) if value is not None else "—",
            "_value_num": rec.get("value"),
            "_target_num": rec.get("target") if column_mode == "targets" else None,
        }
        if column_mode == "targets":
            row["Целевая стоимость"] = (
                format_money(target, display_ccy) if target is not None else "—"
            )
            row["_deviation_pct"] = rec.get("deviation_pct")
            row["_deviation_abs"] = rec.get("deviation_abs")
            row["_is_blocked"] = rec.get("is_blocked", False)
        rows.append(row)
    return rows


def _render_summary_bar_chart(
    rows: list[dict],
    index_col: str,
    *,
    has_targets: bool,
    percent_mode: bool,
    display_ccy: str,
) -> None:
    if not rows:
        st.info("Нет данных для графика.")
        return

    total = sum(float(r["_value_num"]) for r in rows if r.get("_value_num") is not None)
    if total <= 0:
        st.info("Нет оценённых позиций для графика.")
        return

    if has_targets:
        cur_label = "Текущая доля, %" if percent_mode else "Текущая"
        tgt_label = "Целевая доля, %" if percent_mode else "Целевая"
    elif percent_mode:
        cur_label = "Доля, %"
        tgt_label = None
    else:
        cur_label = "Стоимость"
        tgt_label = None

    chart_rows = []
    for r in rows:
        cur = r.get("_value_num")
        if cur is None:
            continue
        entry = {index_col: r.get(index_col, "—")}
        entry[cur_label] = float(cur) / total * 100.0 if percent_mode else float(cur)
        if has_targets and tgt_label:
            tgt = r.get("_target_num")
            if tgt is not None:
                entry[tgt_label] = (
                    float(tgt) / total * 100.0 if percent_mode else float(tgt)
                )
        chart_rows.append(entry)

    if not chart_rows:
        st.info("Нет данных для графика.")
        return

    df = pd.DataFrame(chart_rows)
    y_cols = [cur_label]
    if tgt_label and tgt_label in df.columns and df[tgt_label].notna().any():
        y_cols.append(tgt_label)

    chart_df = df[[index_col] + y_cols].melt(
        id_vars=[index_col],
        value_vars=y_cols,
        var_name="Метрика",
        value_name="Значение",
    )
    chart_df = chart_df.dropna(subset=["Значение"])

    y_title = "%" if percent_mode else display_ccy
    tooltip_title = "Значение, %" if percent_mode else f"Значение, {display_ccy}"
    tooltip_format = ".3f" if percent_mode else ",.0f"

    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{index_col}:N", title=index_col),
            y=alt.Y("Значение:Q", title=y_title),
            color=alt.Color("Метрика:N", title=""),
            xOffset=alt.XOffset("Метрика:N"),
            tooltip=[
                alt.Tooltip(f"{index_col}:N", title=index_col),
                alt.Tooltip("Метрика:N", title="Метрика"),
                alt.Tooltip("Значение:Q", title=tooltip_title, format=tooltip_format),
            ],
        )
    )
    st.altair_chart(chart, width="stretch")


def _style_main_portfolio_rows(
    df: pd.DataFrame,
    deviations: list[float | None],
    blocked_flags: list[bool],
) -> pd.io.formats.style.Styler:
    def _row_style(row: pd.Series) -> list[str]:
        idx = int(row.name)
        if blocked_flags[idx]:
            return [_BLOCKED_STYLE] * len(row)
        dev = deviations[idx]
        if dev is None:
            return [""] * len(row)
        style = _DEVIATION_OK_STYLE if abs(dev) <= 10.0 else _DEVIATION_BAD_STYLE
        return [style] * len(row)

    return df.style.apply(_row_style, axis=1)


def _style_return_rows(
    df: pd.DataFrame,
    returns: list[float | None],
) -> pd.io.formats.style.Styler:
    def _row_style(row: pd.Series) -> list[str]:
        idx = int(row.name)
        ret = returns[idx]
        if ret is None:
            return [""] * len(row)
        style = _RETURN_POS_STYLE if float(ret) >= 0 else _RETURN_NEG_STYLE
        return [style] * len(row)

    return df.style.apply(_row_style, axis=1)


def _enrich_group_rows_with_returns(
    group_rows: list[dict],
    group_mode: str,
    return_by_key: dict[str, float | None],
) -> list[float | None]:
    returns: list[float | None] = []
    for row in group_rows:
        key = _return_row_key(row, group_mode)
        ret = return_by_key.get(key)
        row["_return_num"] = ret
        returns.append(ret)
    return returns


def _render_portfolio_total_metric_body():
    """Top-level metric for total portfolio value (above main tabs)."""
    positions = list_positions_by_ticker()
    display_ccy = st.session_state.get("display_currency", "RUB")
    if not positions:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Стоимость портфеля", "—")
        c2.metric(f"Основной", "—")
        c3.metric(f"Прочие", "—")
        c4.metric(f"Заблокировано", "—")
        return

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    tickers = list({p.ticker for p in positions})
    main_by_ticker = get_instrument_main_map(tickers)
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    quotes = get_app_quotes(tickers)

    portfolio_total, n_with_price, _ = compute_current_portfolio_market_value(
        display_ccy, rub, eur
    )
    main_total = 0.0
    other_total = 0.0
    blocked_total = 0.0
    blocked_tickers = {t.upper() for t in list_buy_blocked_tickers()}
    for p in positions:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is None:
            continue
        value_native = price * p.amount
        value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
        if (p.ticker or "").upper() in blocked_tickers:
            blocked_total += value_disp
        if bool(main_by_ticker.get((p.ticker or "").upper(), False)):
            main_total += value_disp
        else:
            other_total += value_disp

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        f"Стоимость портфеля",
        format_money(portfolio_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма стоимостей позиций по текущим котировкам в выбранной валюте.",
    )
    c2.metric(
        f"Основной портфель",
        format_money(main_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма по инструментам основного портфеля.",
    )
    c3.metric(
        f"Прочие активы",
        format_money(other_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма по инструментам прочих активов.",
    )
    c4.metric(
        f"Заблокировано",
        format_money(blocked_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма стоимостей заблокированных активов.",
    )


def _build_portfolio_positions_context() -> dict | None:
    positions = list_positions_by_ticker()
    if not positions:
        return None

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    display_ccy = st.session_state.get("display_currency", "RUB")

    sub_by_id = _subclass_by_id()
    class_by_id = _class_by_id()
    tickers = list({p.ticker for p in positions})
    main_by_ticker = get_instrument_main_map(tickers)
    quotes = get_app_quotes(tickers)
    meta = get_quotes_cache_meta()
    stale_tickers = set(meta.get("stale_tickers") or [])
    main_ticker_records: list[dict] = []
    other_ticker_records: list[dict] = []
    main_tpv_rows: list[TickerPositionValue] = []
    for p in positions:
        sub = sub_by_id.get(p.asset_subclass_id)
        ac = class_by_id.get(sub.asset_class_id) if sub else None
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = resolve_quote_currency(p.ticker, q.currency if q else None)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
        else:
            price_disp = None
            value_disp = None
        qty_disp = _format_qty_display(p.ticker, p.amount)
        price_cell = (
            (format_money(price_disp, display_ccy) + " *")
            if (price_disp is not None and p.ticker in stale_tickers)
            else (format_money(price_disp, display_ccy) if price_disp is not None else "—")
        )
        record = {
            "ticker": p.ticker,
            "class_name": ac.name if ac else "—",
            "subclass_name": sub.name if sub else "—",
            "class_id": ac.id if ac else None,
            "subclass_id": sub.id if sub else None,
            "class_sort": int(ac.sort_order) if ac else 10**9,
            "subclass_sort": int(sub.sort_order) if sub else 10**9,
            "value": float(value_disp) if value_disp is not None else None,
            "price_cell": price_cell,
            "qty_disp": qty_disp,
            "currency_bucket": bucket_diversification_currency(quote_ccy),
        }
        if bool(main_by_ticker.get((p.ticker or "").upper(), False)):
            main_ticker_records.append(record)
            main_tpv_rows.append(
                TickerPositionValue(
                    ticker=p.ticker,
                    asset_subclass_id=p.asset_subclass_id,
                    value_display=float(value_disp) if value_disp is not None else None,
                    price_display=float(price_disp) if price_disp is not None else None,
                )
            )
        else:
            other_ticker_records.append(record)

    blocked = {t.upper() for t in list_buy_blocked_tickers(main_only=True)}
    target_pct = {s.id: float(s.target_pct) for s in list_asset_subclasses()}
    _, target_by_ticker = compute_ticker_target_values(
        main_tpv_rows, target_pct, blocked_tickers=blocked
    )
    for rec in main_ticker_records:
        _enrich_main_ticker_record(rec, target_by_ticker, blocked)

    main_ticker_set = {(rec["ticker"] or "").upper() for rec in main_ticker_records}
    qty_by_ticker = {
        p.ticker: float(p.amount)
        for p in positions
        if (p.ticker or "").upper() in main_ticker_set
    }
    storage_positions = [
        p for p in list_positions() if (p.ticker or "").upper() in main_ticker_set
    ]

    other_ticker_set = {(rec["ticker"] or "").upper() for rec in other_ticker_records}
    other_qty_by_ticker = {
        p.ticker: float(p.amount)
        for p in positions
        if (p.ticker or "").upper() in other_ticker_set
    }
    other_storage_positions = [
        p for p in list_positions() if (p.ticker or "").upper() in other_ticker_set
    ]

    return {
        "display_ccy": display_ccy,
        "rub": rub,
        "eur": eur,
        "main_ticker_records": main_ticker_records,
        "other_ticker_records": other_ticker_records,
        "storage_positions": storage_positions,
        "qty_by_ticker": qty_by_ticker,
        "other_storage_positions": other_storage_positions,
        "other_qty_by_ticker": other_qty_by_ticker,
    }


def _render_summary_metric_controls(
    group_mode: str,
    *,
    key_prefix: str,
) -> tuple[str, str, str]:
    return_metric = "PNL"
    return_period = "ALL"
    pnl_display = "Percent"
    if group_mode in _GROUP_MODES_WITHOUT_TARGETS:
        return return_metric, return_period, pnl_display

    return_metric = st.segmented_control(
        "Метрика",
        options=list(_RETURN_METRIC_OPTIONS),
        format_func=lambda x: _RETURN_METRIC_LABELS[x],
        default="PNL",
        key=f"{key_prefix}_return_metric",
        width="content",
    )
    return_period = st.segmented_control(
        "Период",
        options=list(_RETURN_PERIOD_OPTIONS),
        format_func=lambda x: _RETURN_PERIOD_LABELS[x],
        default="ALL",
        key=f"{key_prefix}_return_period",
        width="content",
    )
    if return_metric == "PNL":
        pnl_display = st.segmented_control(
            "P&L",
            options=["Percent", "Absolute"],
            format_func=lambda x: "%" if x == "Percent" else "Абс.",
            default="Percent",
            key=f"{key_prefix}_pnl_display",
            width="content",
        )
    return return_metric, return_period, pnl_display


def _render_group_portfolio_table(
    ctx: dict,
    ticker_records: list[dict],
    *,
    storage_positions,
    qty_by_ticker: dict[str, float],
    column_mode: str,
    group_mode: str,
    key_prefix: str,
    return_metric: str = "PNL",
    return_period: str = "ALL",
    pnl_display: str = "Percent",
    deviation_mode: str = "Percent",
) -> None:
    display_ccy = ctx["display_ccy"]
    has_targets = column_mode == "targets" and group_mode not in _GROUP_MODES_WITHOUT_TARGETS
    group_rows = _build_main_group_rows(
        ticker_records,
        group_mode,
        display_ccy,
        storage_positions,
        qty_by_ticker,
        column_mode=column_mode,
    )

    if column_mode == "targets" and has_targets:
        percent_mode = deviation_mode == "Percent"
        main_deviations, main_deviations_abs, main_blocked = _extract_row_metrics(group_rows)
        display_rows = _build_main_display_rows(
            group_rows,
            main_deviations,
            main_deviations_abs,
            percent_mode,
            display_ccy,
        )
        main_df = pd.DataFrame(display_rows)
        styled_main = _style_main_portfolio_rows(main_df, main_deviations, main_blocked)
        st.dataframe(
            styled_main,
            width="stretch",
            height=1000,
            hide_index=True,
            key=f"{key_prefix}_main_df_{group_mode}_{deviation_mode}",
        )
        return

    if column_mode == "returns" and group_mode not in _GROUP_MODES_WITHOUT_TARGETS:
        db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
        db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0
        perf_result = _get_portfolio_performance(
            display_currency=display_ccy,
            rub_per_usd=ctx["rub"],
            eur_per_usd=ctx["eur"],
            mwr_curve_frequency="monthly",
            db_mtime=db_mtime,
        )
        pnl_mode = "absolute" if pnl_display == "Absolute" else "percent"
        return_by_key = compute_main_group_returns(
            perf_result,
            group_mode=group_mode,
            metric=return_metric,
            period=return_period,
            main_ticker_records=ticker_records,
            display_currency=display_ccy,
            rub_per_usd=ctx["rub"],
            eur_per_usd=ctx["eur"],
            pnl_display=pnl_mode,
        )
        return_column = _return_column_label(
            return_metric,
            return_period,
            pnl_display=pnl_display if return_metric == "PNL" else None,
        )
        returns = _enrich_group_rows_with_returns(group_rows, group_mode, return_by_key)
        display_rows = _build_main_returns_display_rows(
            group_rows,
            return_column,
            return_metric=return_metric,
            pnl_display=pnl_display if return_metric == "PNL" else None,
            display_ccy=display_ccy,
        )
        main_df = pd.DataFrame(display_rows)
        styled_main = _style_return_rows(main_df, returns)
        st.dataframe(
            styled_main,
            width="stretch",
            height=1000,
            hide_index=True,
            key=f"{key_prefix}_main_df_{group_mode}_{return_metric}_{return_period}_{pnl_display}",
        )
        return

    table_rows = [
        {k: v for k, v in r.items() if not str(k).startswith("_")} for r in group_rows
    ]
    st.dataframe(
        table_rows,
        width="stretch",
        height=1000,
        hide_index=True,
        key=f"{key_prefix}_main_df_{group_mode}_value",
    )


def _render_main_portfolio_content(
    ctx: dict,
    *,
    column_mode: str,
    key_prefix: str,
    show_charts: bool = True,
) -> None:
    display_ccy = ctx["display_ccy"]
    main_ticker_records = ctx["main_ticker_records"]
    if not main_ticker_records:
        st.info("Нет инструментов с `main = 1`.")
        return

    group_mode = "Tickers"
    display_mode = "Table"
    deviation_mode = "Percent"
    chart_scale_mode = "Percent"
    return_metric = "PNL"
    return_period = "ALL"
    pnl_display = "Percent"

    with st.container(horizontal=True, gap="small"):
        group_mode = st.segmented_control(
            "Группировка",
            options=["Tickers", "Subclasses", "Classes", "Currencies", "Storage"],
            format_func=lambda x: {
                "Tickers": "Тикеры",
                "Subclasses": "Подклассы",
                "Classes": "Классы",
                "Currencies": "Валюты",
                "Storage": "Места хранения",
            }[x],
            default="Tickers",
            key=f"{key_prefix}_group_mode",
            width="content",
        )
        if show_charts:
            display_mode = st.segmented_control(
                "Вид",
                options=["Table", "Chart"],
                format_func=lambda x: "Таблица" if x == "Table" else "График",
                default="Table",
                key=f"{key_prefix}_display_mode",
                width="content",
            )
        if column_mode == "targets":
            if display_mode == "Table" and group_mode not in _GROUP_MODES_WITHOUT_TARGETS:
                deviation_mode = st.segmented_control(
                    "Отклонение",
                    options=["Percent", "Absolute"],
                    format_func=lambda x: "%" if x == "Percent" else "Абс.",
                    default="Percent",
                    key=f"{key_prefix}_deviation_mode",
                    width="content",
                )
            elif show_charts and display_mode == "Chart":
                chart_scale_mode = st.segmented_control(
                    "Шкала",
                    options=["Percent", "Absolute"],
                    format_func=lambda x: "%" if x == "Percent" else "Абс.",
                    default="Percent",
                    key=f"{key_prefix}_chart_scale",
                    width="content",
                )

    has_targets = column_mode == "targets" and group_mode not in _GROUP_MODES_WITHOUT_TARGETS
    if show_charts and display_mode == "Chart":
        group_rows = _build_main_group_rows(
            main_ticker_records,
            group_mode,
            display_ccy,
            ctx["storage_positions"],
            ctx["qty_by_ticker"],
            column_mode=column_mode,
        )
        _render_summary_bar_chart(
            group_rows,
            _MAIN_GROUP_INDEX_COL[group_mode],
            has_targets=has_targets,
            percent_mode=chart_scale_mode == "Percent",
            display_ccy=display_ccy,
        )
        return

    _render_group_portfolio_table(
        ctx,
        main_ticker_records,
        storage_positions=ctx["storage_positions"],
        qty_by_ticker=ctx["qty_by_ticker"],
        column_mode=column_mode,
        group_mode=group_mode,
        key_prefix=key_prefix,
        return_metric=return_metric,
        return_period=return_period,
        pnl_display=pnl_display,
        deviation_mode=deviation_mode,
    )


def _render_portfolio_table_body():
    ctx = _build_portfolio_positions_context()
    if ctx is None:
        st.info("Нет позиций. Добавьте позиции в боковой панели.")
        return

    with st.container(horizontal=True, gap="small"):
        portfolio_view = st.segmented_control(
            "Портфель",
            options=["Main", "Other"],
            format_func=lambda x: "Основной портфель" if x == "Main" else "Прочие активы",
            default="Main",
            key="portfolio_summary_view",
            width="content",
        )
        group_key = (
            "portfolio_summary_group_mode"
            if portfolio_view == "Main"
            else "portfolio_summary_other_group_mode"
        )
        group_mode = st.segmented_control(
            "Группировка",
            options=["Tickers", "Subclasses", "Classes", "Currencies", "Storage"],
            format_func=lambda x: {
                "Tickers": "Тикеры",
                "Subclasses": "Подклассы",
                "Classes": "Классы",
                "Currencies": "Валюты",
                "Storage": "Места хранения",
            }[x],
            default="Tickers",
            key=group_key,
            width="content",
        )

    is_main = portfolio_view == "Main"
    ticker_records = ctx["main_ticker_records"] if is_main else ctx["other_ticker_records"]
    key_prefix = "portfolio_summary" if is_main else "portfolio_summary_other"
    empty_msg = (
        "Нет инструментов с `main = 1`."
        if is_main
        else "Нет инструментов с `main = 0`."
    )

    if not ticker_records:
        st.info(empty_msg)
        return

    with st.container(horizontal=True, gap="small"):
        return_metric, return_period, pnl_display = _render_summary_metric_controls(
            group_mode,
            key_prefix=key_prefix,
        )

    _render_group_portfolio_table(
        ctx,
        ticker_records,
        storage_positions=ctx["storage_positions"] if is_main else ctx["other_storage_positions"],
        qty_by_ticker=ctx["qty_by_ticker"] if is_main else ctx["other_qty_by_ticker"],
        column_mode="returns",
        group_mode=group_mode,
        key_prefix=key_prefix,
        return_metric=return_metric,
        return_period=return_period,
        pnl_display=pnl_display,
    )
def render_portfolio_total_metric():
    run_every = live_quotes_run_every()

    @st.fragment(run_every=run_every)
    def _fragment():
        _render_portfolio_total_metric_body()

    _fragment()


def render_portfolio_table():
    """Сводка с автообновлением цен (fragment)."""
    run_every = live_quotes_run_every()

    @st.fragment(run_every=run_every)
    def _fragment():
        _render_portfolio_table_body()

    _fragment()


def render_main_portfolio_diversification():
    """Диверсификация: основной портфель с целевыми долями и отклонениями."""
    run_every = live_quotes_run_every()

    @st.fragment(run_every=run_every)
    def _fragment():
        ctx = _build_portfolio_positions_context()
        if ctx is None:
            st.info("Нет позиций. Добавьте позиции в боковой панели.")
            return
        _render_main_portfolio_content(
            ctx,
            column_mode="targets",
            key_prefix="div_main",
        )

    _fragment()
