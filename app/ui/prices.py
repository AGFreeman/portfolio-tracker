"""Ticker historical price chart tab."""
from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from app.db import (
    get_instrument_main_map,
    get_instrument_subclass_id_map,
    list_asset_classes,
    list_asset_subclasses,
    list_distinct_tickers,
    list_transactions,
)
from app.services.fx import convert_amount
from app.services.historical_quotes import sync_portfolio_historical_quotes
from app.services.performance import (
    _build_active_intervals_by_ticker,
    _build_as_of_price_index,
    _iter_dates,
    _load_daily_transactions,
    _load_fx_exact_from_db,
    _load_price_series_from_cache,
    _normalize_price_series,
    _parse_date_prefix,
    _quote_as_of,
)
from app.services.portfolio_order import portfolio_ticker_sort_key
from app.services.price_currency import resolve_quote_currency
from app.services.prices import PriceQuote
from app.ui.performance import (
    _add_fx_to_rub_secondary_axis,
    _build_fx_to_rub_df,
    _filter_chart_df_by_frequency,
    _fx_secondary_axis_range,
)

_PRICE_LINE_COLOR = "#636EFA"
_BUY_MARKER_COLOR = "#2E7D32"
_SELL_MARKER_COLOR = "#C62828"


@dataclass(frozen=True)
class TradeMarker:
    date: str
    price: float
    side: str
    qty: float


@dataclass(frozen=True)
class TickerScopeMeta:
    ticker: str
    is_main: bool
    subclass_id: int
    class_id: int



def _class_id_by_name(class_name: str, class_by_id: Mapping[int, object]) -> Optional[int]:
    for cid, ac in class_by_id.items():
        if str(getattr(ac, "name", "")) == class_name:
            return int(cid)
    return None


def _subclass_id_by_name(subclass_name: str, subclass_by_id: Mapping[int, object]) -> Optional[int]:
    for sid, sub in subclass_by_id.items():
        if str(getattr(sub, "name", "")) == subclass_name:
            return int(sid)
    return None


def order_ticker_options(
    tickers: Sequence[str],
    *,
    subclass_id_map: Mapping[str, int],
    subclass_by_id: Mapping[int, object],
    class_sort_by_id: Mapping[int, int],
) -> List[str]:
    """Sort tickers like the portfolio summary table."""
    return sorted(
        tickers,
        key=lambda t: portfolio_ticker_sort_key(
            t,
            asset_subclass_id=subclass_id_map.get(str(t).upper()),
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        ),
    )


def class_options_for_portfolio(
    meta: Sequence[TickerScopeMeta],
    *,
    portfolio_main: bool,
    class_by_id: Mapping[int, object],
) -> List[str]:
    class_ids = {
        item.class_id
        for item in meta
        if item.is_main == portfolio_main and item.class_id in class_by_id
    }
    ordered_ids = sorted(
        class_ids,
        key=lambda cid: (
            int(getattr(class_by_id[cid], "sort_order", 10**9)),
            str(getattr(class_by_id[cid], "name", "")),
        ),
    )
    return [str(class_by_id[cid].name) for cid in ordered_ids]


def subclass_options_for_scope(
    meta: Sequence[TickerScopeMeta],
    *,
    portfolio_main: bool,
    class_name: str,
    subclass_by_id: Mapping[int, object],
    class_by_id: Mapping[int, object],
) -> List[str]:
    class_id = _class_id_by_name(class_name, class_by_id)
    if class_id is None:
        return []
    subclass_ids: set[int] = set()
    for item in meta:
        if item.is_main != portfolio_main:
            continue
        if item.class_id != class_id:
            continue
        if item.subclass_id in subclass_by_id:
            subclass_ids.add(item.subclass_id)
    ordered_ids = sorted(
        subclass_ids,
        key=lambda sid: (
            int(getattr(subclass_by_id[sid], "sort_order", 10**9)),
            str(getattr(subclass_by_id[sid], "name", "")),
        ),
    )
    return [str(subclass_by_id[sid].name) for sid in ordered_ids]


def filter_tickers_for_scope(
    ordered_tickers: Sequence[str],
    meta_by_ticker: Mapping[str, TickerScopeMeta],
    *,
    portfolio: str,
    class_name: str,
    subclass_name: str,
    class_by_id: Mapping[int, object],
    subclass_by_id: Mapping[int, object],
) -> List[str]:
    portfolio_main = str(portfolio or "Main") == "Main"
    class_id = _class_id_by_name(class_name, class_by_id)
    subclass_id = _subclass_id_by_name(subclass_name, subclass_by_id)
    if class_id is None or subclass_id is None:
        return []
    out: List[str] = []
    for ticker in ordered_tickers:
        item = meta_by_ticker.get(str(ticker).upper())
        if item is None:
            continue
        if item.is_main != portfolio_main:
            continue
        if item.class_id != class_id:
            continue
        if item.subclass_id != subclass_id:
            continue
        out.append(str(ticker))
    return out


def _resolve_scope_default(current: str | None, options: Sequence[str]) -> str:
    if current in options:
        return str(current)
    return str(options[0]) if options else ""


def ticker_chart_date_range(
    ticker: str,
    tx_by_day: Mapping[str, Sequence[tuple]],
    days: Sequence[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Date range for chart: union of holding intervals, else None."""
    ticker_up = str(ticker or "").upper().strip()
    if not ticker_up or not days:
        return None, None
    intervals = _build_active_intervals_by_ticker(dict(tx_by_day), list(days)).get(ticker_up, [])
    if intervals:
        return min(start for start, _end in intervals), max(end for _start, end in intervals)
    tx_days = sorted(
        day
        for day, rows in tx_by_day.items()
        if any(str(t).upper() == ticker_up for t, _amount, _tx_type in rows)
    )
    if tx_days:
        return tx_days[0], tx_days[-1]
    return None, None


def _fx_pair_as_of(
    day: str,
    fx_exact: Mapping[str, Tuple[float, float]],
    default: Tuple[float, float],
) -> Tuple[float, float]:
    if not fx_exact:
        return default
    sorted_days = sorted(fx_exact.keys())
    idx = bisect_right(sorted_days, day) - 1
    if idx < 0:
        return default
    return fx_exact[sorted_days[idx]]


def convert_price_value(
    price: float,
    quote_ccy: str,
    day: str,
    target_ccy: str,
    *,
    fx_exact: Mapping[str, Tuple[float, float]],
    spot_rub_per_usd: float,
    spot_eur_per_usd: float,
) -> float:
    """Convert a quote price to target currency using historical FX when available."""
    if str(target_ccy).upper() == str(quote_ccy).upper():
        return float(price)
    rub, eur = _fx_pair_as_of(
        day,
        fx_exact,
        (float(spot_rub_per_usd), float(spot_eur_per_usd)),
    )
    return float(
        convert_amount(
            amount=float(price),
            from_ccy=str(quote_ccy).upper(),
            to_ccy=str(target_ccy).upper(),
            rub_per_usd=rub,
            eur_per_usd=eur,
        )
    )


def build_trade_markers(
    ticker: str,
    transactions: Iterable[object],
    *,
    dates: Sequence[str],
    quotes: Sequence[PriceQuote],
    currency_mode: str,
    display_ccy: str,
    quote_ccy: str,
    fx_exact: Mapping[str, Tuple[float, float]],
    spot_rub_per_usd: float,
    spot_eur_per_usd: float,
) -> List[TradeMarker]:
    """Build buy/sell markers for one ticker (trade txs only)."""
    ticker_up = str(ticker or "").upper().strip()
    if not ticker_up:
        return []
    use_display = str(currency_mode or "quote").lower() == "display"
    target_ccy = str(display_ccy).upper() if use_display else str(quote_ccy).upper()
    markers: List[TradeMarker] = []
    for tx in transactions:
        if str(getattr(tx, "ticker", "") or "").upper() != ticker_up:
            continue
        if str(getattr(tx, "transaction_type", "") or "").strip().lower() != "trade":
            continue
        day = _parse_date_prefix(getattr(tx, "created_at", None))
        if day is None:
            continue
        amount = float(getattr(tx, "amount", 0.0) or 0.0)
        if amount == 0.0:
            continue
        q = _quote_as_of(list(dates), list(quotes), day)
        if q is None or q.price is None:
            continue
        native_ccy = str(q.currency or quote_ccy).upper()
        native_price = float(q.price)
        if use_display:
            marker_price = convert_price_value(
                native_price,
                native_ccy,
                day,
                target_ccy,
                fx_exact=fx_exact,
                spot_rub_per_usd=spot_rub_per_usd,
                spot_eur_per_usd=spot_eur_per_usd,
            )
        else:
            marker_price = native_price
        markers.append(
            TradeMarker(
                date=day,
                price=float(marker_price),
                side="buy" if amount > 0 else "sell",
                qty=abs(amount),
            )
        )
    return markers


def _build_ticker_price_df(
    ticker: str,
    date_from: str,
    date_to: str,
    *,
    currency_mode: str,
    display_ccy: str,
    quote_ccy: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> pd.DataFrame:
    series = _normalize_price_series(
        ticker,
        _load_price_series_from_cache(ticker, date_from, date_to),
    )
    if not series:
        return pd.DataFrame(columns=["date", "price"])
    use_display = str(currency_mode or "quote").lower() == "display"
    target_ccy = str(display_ccy).upper() if use_display else str(quote_ccy).upper()
    fx_exact = _load_fx_exact_from_db(date_from, date_to) if use_display else {}
    rows: list[dict[str, object]] = []
    for day in sorted(series.keys()):
        q = series[day]
        if q.price is None:
            continue
        native_ccy = str(q.currency or quote_ccy).upper()
        native_price = float(q.price)
        if use_display:
            price = convert_price_value(
                native_price,
                native_ccy,
                day,
                target_ccy,
                fx_exact=fx_exact,
                spot_rub_per_usd=rub_per_usd,
                spot_eur_per_usd=eur_per_usd,
            )
        else:
            price = native_price
        rows.append({"date": pd.Timestamp(day), "price": float(price)})
    if not rows:
        return pd.DataFrame(columns=["date", "price"])
    return pd.DataFrame(rows).sort_values("date")


@st.cache_data(show_spinner=False)
def _ticker_scope_cached(db_mtime: float) -> tuple[tuple[TickerScopeMeta, ...], tuple[str, ...]]:
    _ = db_mtime
    subclass_by_id = {s.id: s for s in list_asset_subclasses()}
    class_by_id = {c.id: c for c in list_asset_classes()}
    class_sort_by_id = {c.id: c.sort_order for c in list_asset_classes()}
    tickers = list_distinct_tickers()
    subclass_map = get_instrument_subclass_id_map(tickers)
    main_map = get_instrument_main_map(tickers)
    meta: list[TickerScopeMeta] = []
    for ticker in tickers:
        up = str(ticker).upper()
        sid = int(subclass_map.get(up) or 0)
        sub = subclass_by_id.get(sid)
        cid = int(getattr(sub, "asset_class_id", 0) or 0) if sub is not None else 0
        meta.append(
            TickerScopeMeta(
                ticker=up,
                is_main=bool(main_map.get(up, False)),
                subclass_id=sid,
                class_id=cid,
            )
        )
    ordered = order_ticker_options(
        tickers,
        subclass_id_map=subclass_map,
        subclass_by_id=subclass_by_id,
        class_sort_by_id=class_sort_by_id,
    )
    return tuple(meta), tuple(ordered)


@st.cache_data(show_spinner=False)
def _load_ticker_chart_data_cached(
    ticker: str,
    currency_mode: str,
    display_ccy: str,
    rub_per_usd: float,
    eur_per_usd: float,
    db_mtime: float,
) -> tuple[pd.DataFrame, tuple[str, ...], tuple[PriceQuote, ...], str, Optional[str], Optional[str]]:
    _ = db_mtime
    ticker_up = str(ticker or "").upper().strip()
    quote_ccy = resolve_quote_currency(ticker_up)
    tx_by_day, first_tx_date, last_tx_date = _load_daily_transactions()
    days = _iter_dates(first_tx_date, last_tx_date) if first_tx_date and last_tx_date else []
    date_from, date_to = ticker_chart_date_range(ticker_up, tx_by_day, days)
    if date_from is None or date_to is None:
        return pd.DataFrame(columns=["date", "price"]), (), (), quote_ccy, None, None
    end = max(date_to, date.today().isoformat())
    df = _build_ticker_price_df(
        ticker_up,
        date_from,
        end,
        currency_mode=currency_mode,
        display_ccy=display_ccy,
        quote_ccy=quote_ccy,
        rub_per_usd=rub_per_usd,
        eur_per_usd=eur_per_usd,
    )
    series = _normalize_price_series(
        ticker_up,
        _load_price_series_from_cache(ticker_up, date_from, end),
    )
    dates, quotes = _build_as_of_price_index(series)
    return df, tuple(dates), tuple(quotes), quote_ccy, date_from, end


def _y_axis_prefix(ccy: str) -> str:
    up = str(ccy or "").upper()
    if up == "RUB":
        return "₽ "
    if up == "USD":
        return "$ "
    if up == "EUR":
        return "€ "
    return f"{up} "


def _render_ticker_price_chart(
    df: pd.DataFrame,
    *,
    ticker: str,
    price_ccy: str,
    markers: Sequence[TradeMarker],
    show_trades: bool,
    fx_currencies: set[str],
    rub_per_usd: float,
    eur_per_usd: float,
) -> None:
    if df.empty or df["price"].notna().sum() == 0:
        st.info("Недостаточно данных для графика.")
        return

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["price"].astype(float),
            mode="lines",
            name=ticker,
            line={"width": 2.5, "color": _PRICE_LINE_COLOR},
            hovertemplate=(
                "Дата: %{x|%d.%m.%Y}<br>"
                f"Цена: {_y_axis_prefix(price_ccy)}%{{y:,.2f}}<extra></extra>"
            ),
        )
    )

    if show_trades and markers:
        buys = [m for m in markers if m.side == "buy"]
        sells = [m for m in markers if m.side == "sell"]
        if buys:
            fig.add_trace(
                go.Scatter(
                    x=[pd.Timestamp(m.date) for m in buys],
                    y=[m.price for m in buys],
                    mode="markers",
                    name="Покупки",
                    marker={
                        "symbol": "triangle-up",
                        "size": 11,
                        "color": _BUY_MARKER_COLOR,
                        "line": {"width": 1, "color": "white"},
                    },
                    customdata=[(m.qty, m.qty * m.price) for m in buys],
                    hovertemplate=(
                        "Покупка<br>"
                        "Дата: %{x|%d.%m.%Y}<br>"
                        f"Цена: {_y_axis_prefix(price_ccy)}%{{y:,.2f}}<br>"
                        "Кол-во: %{customdata[0]:,.4g}<br>"
                        f"Сумма: {_y_axis_prefix(price_ccy)}%{{customdata[1]:,.2f}}<extra></extra>"
                    ),
                )
            )
        if sells:
            fig.add_trace(
                go.Scatter(
                    x=[pd.Timestamp(m.date) for m in sells],
                    y=[m.price for m in sells],
                    mode="markers",
                    name="Продажи",
                    marker={
                        "symbol": "triangle-down",
                        "size": 11,
                        "color": _SELL_MARKER_COLOR,
                        "line": {"width": 1, "color": "white"},
                    },
                    customdata=[(m.qty, m.qty * m.price) for m in sells],
                    hovertemplate=(
                        "Продажа<br>"
                        "Дата: %{x|%d.%m.%Y}<br>"
                        f"Цена: {_y_axis_prefix(price_ccy)}%{{y:,.2f}}<br>"
                        "Кол-во: %{customdata[0]:,.4g}<br>"
                        f"Сумма: {_y_axis_prefix(price_ccy)}%{{customdata[1]:,.2f}}<extra></extra>"
                    ),
                )
            )

    fx_selection = {str(c).upper() for c in fx_currencies}
    has_fx_secondary = False
    fx_df = pd.DataFrame()
    if fx_selection:
        fx_df = _build_fx_to_rub_df(
            df["date"],
            rub_per_usd_spot=rub_per_usd,
            eur_per_usd_spot=eur_per_usd,
        )
        has_fx_secondary = _add_fx_to_rub_secondary_axis(
            fig,
            fx_df,
            currencies=fx_selection,
        )

    fig.update_layout(
        yaxis={
            "title": None,
            "tickprefix": _y_axis_prefix(price_ccy),
            "tickformat": ",.0f",
            "fixedrange": True,
        },
        xaxis={
            "title": None,
            "tickformat": "%m-%Y",
            "hoverformat": "%d.%m.%Y",
            "rangeslider": {"visible": False},
            "fixedrange": False,
        },
    )
    if has_fx_secondary:
        yaxis2: dict[str, object] = {
            "title": None,
            "overlaying": "y",
            "side": "right",
            "tickformat": ",.1f",
            "ticksuffix": " ₽",
            "showgrid": False,
            "fixedrange": True,
            "automargin": True,
            "ticklabelposition": "outside",
        }
        fx_range = _fx_secondary_axis_range(fx_df, currencies=fx_selection)
        if fx_range is not None:
            yaxis2["range"] = fx_range
        fig.update_layout(yaxis2=yaxis2)

    legend_right_margin = 180 if has_fx_secondary else 120
    legend_x = 1.14 if has_fx_secondary else 1.02
    fig.update_layout(
        margin={"l": 8, "r": legend_right_margin, "t": 24, "b": 24},
        height=720,
        hovermode="x unified",
        dragmode="zoom",
        legend={
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": legend_x,
            "tracegroupgap": 8,
            "itemsizing": "constant",
        },
    )
    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displaylogo": False,
            "scrollZoom": True,
            "modeBarButtonsToRemove": [
                "pan2d",
                "select2d",
                "lasso2d",
                "toImage",
            ],
        },
    )


def render_prices() -> None:
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
    db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0
    scope_meta, all_ordered_tickers = _ticker_scope_cached(db_mtime)
    if not all_ordered_tickers:
        st.info("Нет тикеров: добавьте хотя бы одну сделку.")
        return

    meta_by_ticker = {item.ticker: item for item in scope_meta}
    subclass_by_id = {s.id: s for s in list_asset_subclasses()}
    class_by_id = {c.id: c for c in list_asset_classes()}

    with st.container(horizontal=True, gap="small"):
        portfolio_view = st.segmented_control(
            "Портфель",
            options=["Main", "Other"],
            format_func=lambda x: "Основной портфель" if x == "Main" else "Прочие активы",
            default="Main",
            key="prices_portfolio",
            width="content",
        )
        portfolio_main = portfolio_view == "Main"
        class_options = class_options_for_portfolio(
            scope_meta,
            portfolio_main=portfolio_main,
            class_by_id=class_by_id,
        )
        if not class_options:
            st.info("Нет классов для выбранного портфеля.")
            return
        class_default = _resolve_scope_default(st.session_state.get("prices_class"), class_options)
        class_name = st.segmented_control(
            "Класс",
            options=class_options,
            default=class_default,
            key="prices_class",
            width="content",
        )
        subclass_options = subclass_options_for_scope(
            scope_meta,
            portfolio_main=portfolio_main,
            class_name=class_name,
            subclass_by_id=subclass_by_id,
            class_by_id=class_by_id,
        )
        if not subclass_options:
            st.info("Нет подклассов для выбранного класса.")
            return
        subclass_default = _resolve_scope_default(
            st.session_state.get("prices_subclass"),
            subclass_options,
        )
        subclass_name = st.segmented_control(
            "Подкласс",
            options=subclass_options,
            default=subclass_default,
            key="prices_subclass",
            width="content",
        )

    ticker_options = filter_tickers_for_scope(
        all_ordered_tickers,
        meta_by_ticker,
        portfolio=portfolio_view,
        class_name=class_name,
        subclass_name=subclass_name,
        class_by_id=class_by_id,
        subclass_by_id=subclass_by_id,
    )
    if not ticker_options:
        st.info("Нет тикеров для выбранного портфеля, класса и подкласса.")
        return

    default_ticker = _resolve_scope_default(st.session_state.get("prices_ticker"), ticker_options)

    with st.container(horizontal=True, gap="small"):
        ticker = st.segmented_control(
            "Тикер",
            options=ticker_options,
            default=default_ticker,
            key="prices_ticker",
            width="stretch",
        )

    with st.container(horizontal=True, gap="small"):
        freq_label = st.segmented_control(
            "Частота",
            options=["Months", "Weeks", "Days"],
            format_func=lambda x: (
                "Месяцы" if x == "Months" else "Недели" if x == "Weeks" else "Дни"
            ),
            default="Months",
            key="prices_chart_frequency",
            width="content",
        )
        trades_label = st.segmented_control(
            "Сделки",
            options=["Off", "On"],
            format_func=lambda x: "Выкл." if x == "Off" else "Вкл.",
            default="On",
            key="prices_trade_markers",
            width="content",
        )
        currency_mode_label = st.segmented_control(
            "Валюта цены",
            options=["Quote", "Display"],
            format_func=lambda x: "Котировка" if x == "Quote" else "Отображение",
            default="Quote",
            key="prices_currency_mode",
            width="content",
        )
        fx_label = st.segmented_control(
            "Курс к ₽",
            options=["USD", "EUR"],
            default="USD",
            key="prices_fx_currency",
            width="content",
        )
        fx_currencies = {str(fx_label).upper()}

    chart_frequency = (
        "monthly"
        if freq_label == "Months"
        else ("weekly" if freq_label == "Weeks" else "daily")
    )
    currency_mode = "quote" if currency_mode_label == "Quote" else "display"
    show_trades = trades_label == "On"
    ticker_up = str(ticker or ticker_options[0]).upper()

    today = date.today().isoformat()
    portfolio_sync_key = f"_portfolio_hist_quotes_synced_{today}"
    if portfolio_sync_key not in st.session_state:
        with st.spinner("Обновление исторических котировок портфеля…"):
            sync_portfolio_historical_quotes(date_to=today)
        st.session_state[portfolio_sync_key] = True
        db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0

    with st.spinner("Загрузка котировок…"):
        raw_df, dates, quotes, quote_ccy, date_from, date_to = _load_ticker_chart_data_cached(
            ticker_up,
            currency_mode,
            display_ccy,
            rub,
            eur,
            db_mtime,
        )

    if date_from is None or raw_df.empty:
        st.info(
            f"Нет исторических котировок для `{ticker_up}`. "
            f"Запустите: `python scripts/backfill_historical_quotes.py --ticker {ticker_up}`"
        )
        return

    chart_df = _filter_chart_df_by_frequency(raw_df, chart_frequency)
    price_ccy = str(display_ccy).upper() if currency_mode == "display" else str(quote_ccy).upper()
    markers: List[TradeMarker] = []
    if show_trades:
        fx_exact = (
            _load_fx_exact_from_db(date_from, date_to or date.today().isoformat())
            if currency_mode == "display"
            else {}
        )
        markers = build_trade_markers(
            ticker_up,
            list_transactions(),
            dates=dates,
            quotes=list(quotes),
            currency_mode=currency_mode,
            display_ccy=display_ccy,
            quote_ccy=quote_ccy,
            fx_exact=fx_exact,
            spot_rub_per_usd=rub,
            spot_eur_per_usd=eur,
        )
        if date_from and date_to:
            markers = [m for m in markers if date_from <= m.date <= date_to]

    _render_ticker_price_chart(
        chart_df,
        ticker=ticker_up,
        price_ccy=price_ccy,
        markers=markers,
        show_trades=show_trades,
        fx_currencies=fx_currencies,
        rub_per_usd=rub,
        eur_per_usd=eur,
    )
