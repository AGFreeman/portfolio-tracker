"""Portfolio performance UI (TWR + historical backfill)."""
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
from pathlib import Path

from app.db import list_positions_by_ticker
from app.services.fx import format_money
from app.services.prices import get_app_quotes, normalize_quote_price_for_valuation
from app.services.performance import (
    compute_benchmark_period_returns,
    compute_period_returns,
    compute_portfolio_performance,
)


def _fmt_pct(x: float) -> str:
    return f"{x * 100.0:+.2f}%"


def _render_plotly_line_chart(
    df: pd.DataFrame,
    y_col: str,
    title: str,
    is_percent: bool,
    hover_label: str,
    y_tick_prefix: str = "",
    benchmark_y_col: str = "",
    benchmark_label: str = "Benchmark",
) -> None:
    plot_df = df[df[y_col].notna()]
    if plot_df.empty:
        st.caption("Недостаточно данных для графика.")
        return

    y_values = plot_df[y_col].astype(float)
    custom_vals = y_values * 100.0 if is_percent else y_values
    hover_value_suffix = "%" if is_percent else ""
    hover_template = (
        "Date: %{x|%m-%Y}<br>"
        f"{hover_label}: "
        + f"{y_tick_prefix}%{{customdata:.2f}}{hover_value_suffix}"
        + "<extra></extra>"
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=plot_df["date"],
            y=y_values,
            mode="lines",
            line={"width": 2},
            customdata=custom_vals,
            hovertemplate=hover_template,
            name=title,
            showlegend=False,
        )
    )
    show_benchmark = bool(benchmark_y_col and benchmark_y_col in df.columns)
    if show_benchmark:
        benchmark_df = df[df[benchmark_y_col].notna()]
        if not benchmark_df.empty:
            benchmark_vals = benchmark_df[benchmark_y_col].astype(float)
            benchmark_custom_vals = (
                benchmark_vals * 100.0 if is_percent else benchmark_vals
            )
            fig.add_trace(
                go.Scatter(
                    x=benchmark_df["date"],
                    y=benchmark_vals,
                    mode="lines",
                    line={"width": 2, "dash": "dot"},
                    customdata=benchmark_custom_vals,
                    hovertemplate=(
                        "Date: %{x|%m-%Y}<br>"
                        f"{benchmark_label}: "
                        + f"{y_tick_prefix}%{{customdata:.2f}}{hover_value_suffix}"
                        + "<extra></extra>"
                    ),
                    name=benchmark_label,
                    showlegend=True,
                )
            )
    fig.update_layout(
        margin={"l": 8, "r": 8, "t": 32, "b": 8},
        height=280,
        title={"text": title, "x": 0.02, "xanchor": "left"},
        hovermode="x unified",
        dragmode="zoom",
        legend=(
            {"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0.0}
            if show_benchmark
            else None
        ),
    )
    fig.update_xaxes(
        title=None,
        tickformat="%m-%Y",
        hoverformat="%m-%Y",
        rangeslider={"visible": False},
        fixedrange=False,
    )
    if is_percent:
        fig.update_yaxes(title=None, tickformat=".1%", fixedrange=True)
    else:
        fig.update_yaxes(
            title=None, tickprefix=y_tick_prefix, tickformat=",.0f", fixedrange=True
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


def _filter_chart_df_by_frequency(
    df: pd.DataFrame, chart_frequency: str
) -> pd.DataFrame:
    freq = str(chart_frequency or "daily").strip().lower()
    if df.empty:
        return df
    if freq == "monthly":
        month_end_mask = df["date"].dt.to_period("M") != df["date"].shift(
            -1
        ).dt.to_period("M")
        return df[month_end_mask]
    if freq == "weekly":
        curr_week = (
            df["date"].dt.isocalendar().year.astype(str)
            + "-"
            + df["date"].dt.isocalendar().week.astype(str)
        )
        next_week = curr_week.shift(-1)
        week_end_mask = curr_week != next_week
        return df[week_end_mask]
    return df


@st.cache_data(show_spinner=False)
def _compute_portfolio_performance_cached(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    allow_fetch_missing_prices: bool,
    mwr_curve_frequency: str,
    db_mtime: float,
):
    _ = db_mtime  # cache key invalidation on local DB updates
    return compute_portfolio_performance(
        display_currency=display_currency,
        rub_per_usd=rub_per_usd,
        eur_per_usd=eur_per_usd,
        allow_fetch_missing_prices=allow_fetch_missing_prices,
        mwr_curve_frequency=mwr_curve_frequency,
    )


def render_performance_top_metrics() -> None:
    """Render key performance metrics above main tabs."""
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
    db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0
    live_updates_enabled = bool(
        st.session_state.get("live_price_updates_enabled", False)
    )
    result = _compute_portfolio_performance_cached(
        display_currency=display_ccy,
        rub_per_usd=rub,
        eur_per_usd=eur,
        allow_fetch_missing_prices=live_updates_enabled,
        mwr_curve_frequency="monthly",
        db_mtime=db_mtime,
    )
    if not result.points:
        return

    period = compute_period_returns(result.points)
    m1, m2, m3 = st.columns(3)
    m1.metric(
        "MWR (все время)",
        (
            _fmt_pct(result.points[-1].mwr_cum_return)
            if (result.points and result.points[-1].mwr_cum_return is not None)
            else "—"
        ),
        help="Money-weighted доходность за весь период наблюдений (не годовая).",
    )
    m2.metric("Return (все время)", _fmt_pct(period["ALL"]))
    m3.metric(f"P&L ({display_ccy})", format_money(result.total_pnl, display_ccy))


def render_performance() -> None:
    st.header(
        "Доходность",
        help=(
            "Главная метрика доходности — MWR (XIRR) по датам и объёму денежных потоков. "
            "Учитываются ручные вводы/выводы из раздела Cash Flows. "
            "Simple Return показывается как дополнительная справочная метрика. "
            "Стоимость портфеля считается только по инструментам (основной + прочие)."
        ),
    )
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    # Keep behavior consistent with top summary metrics: if no current quotes are
    # available for any active instrument, do not show historical performance charts.
    current_positions = list_positions_by_ticker()
    active_tickers = sorted(
        {
            str(p.ticker or "").upper().strip()
            for p in current_positions
            if str(p.ticker or "").strip() and float(p.amount or 0) > 0
        }
    )
    if active_tickers:
        live_quotes = get_app_quotes(active_tickers)
        live_priced_count = 0
        for ticker in active_tickers:
            q = live_quotes.get(ticker)
            raw_price = q.price if q is not None else None
            quote_ccy = q.currency if q is not None else None
            norm_price = normalize_quote_price_for_valuation(
                ticker=ticker,
                price=raw_price,
                currency=quote_ccy,
            )
            if norm_price is not None:
                live_priced_count += 1
        if live_priced_count == 0:
            st.warning(
                "Нет актуальных котировок по текущим позициям. "
                "Доходность скрыта, пока не появится хотя бы одна текущая цена."
            )
            return

    freq_label = st.segmented_control(
        "Частота графиков",
        options=["Months", "Weeks", "Days"],
        default="Months",
        help="Months быстрее, Weeks сбалансировано, Days детальнее. Применяется ко всем графикам.",
    )
    chart_frequency = (
        "monthly"
        if freq_label == "Months"
        else ("weekly" if freq_label == "Weeks" else "daily")
    )
    db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
    db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0

    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    result = _compute_portfolio_performance_cached(
        display_currency=display_ccy,
        rub_per_usd=rub,
        eur_per_usd=eur,
        allow_fetch_missing_prices=live_updates_enabled,
        mwr_curve_frequency=chart_frequency,
        db_mtime=db_mtime,
    )
    if not result.points:
        st.info("Недостаточно данных: добавьте хотя бы одну сделку.")
        return

    # Row 1: P&L + benchmark comparison
    benchmark_pnl = (
        float(result.benchmark_current_value) - float(result.net_invested)
        if result.benchmark_current_value is not None
        else None
    )
    r1c1, r1c2, r1c3 = st.columns(3)
    r1c1.metric(f"P&L ({display_ccy})", format_money(result.total_pnl, display_ccy))
    r1c2.metric(
        f"P&L Benchmark ({display_ccy})",
        format_money(benchmark_pnl, display_ccy) if benchmark_pnl is not None else "—",
    )
    r1c3.metric(
        "Дельта vs бенчмарк",
        (
            format_money(result.benchmark_delta_value, display_ccy)
            if result.benchmark_delta_value is not None
            else "—"
        ),
        help="Разница текущей стоимости портфеля и фонда-бенчмарка денежного рынка в валюте отображения.",
    )

    # Row 2: MWR (XIRR + all-time cumulative MWR)
    benchmark_all_time_mwr = next(
        (
            p.benchmark_mwr_cum_return
            for p in reversed(result.points)
            if p.benchmark_mwr_cum_return is not None
        ),
        None,
    )
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    r2c1.metric(
        "MWR (XIRR)",
        (
            _fmt_pct(result.mwr_xirr_annualized)
            if result.mwr_xirr_annualized is not None
            else "—"
        ),
        help="Money-weighted return: учитывает даты и объём всех денежных потоков.",
    )
    r2c2.metric(
        "MWR бенчмарка (XIRR)",
        (
            _fmt_pct(result.benchmark_mwr_xirr_annualized)
            if result.benchmark_mwr_xirr_annualized is not None
            else "—"
        ),
        help=(
            f"Money-weighted доходность бенчмарка в годовых (XIRR) для ({result.benchmark_ticker})."
            if result.benchmark_ticker
            else "Money-weighted доходность бенчмарка в годовых (XIRR)."
        ),
    )
    r2c3.metric(
        "MWR (все время)",
        (
            _fmt_pct(result.points[-1].mwr_cum_return)
            if result.points[-1].mwr_cum_return is not None
            else "—"
        ),
        help="Money-weighted доходность за весь период наблюдений (не годовая).",
    )
    r2c4.metric(
        "MWR бенчмарка (все время)",
        _fmt_pct(benchmark_all_time_mwr) if benchmark_all_time_mwr is not None else "—",
        help=(
            f"Money-weighted доходность бенчмарка за весь период ({result.benchmark_ticker})."
            if result.benchmark_ticker
            else "Money-weighted доходность бенчмарка за весь период."
        ),
    )

    # Row 3: Portfolio simple return by period
    period = compute_period_returns(result.points)
    r3c1, r3c2, r3c3, r3c4, r3c5, r3c6 = st.columns(6)
    r3c1.metric("Return - 1M", _fmt_pct(period["1M"]))
    r3c2.metric("Return - 3M", _fmt_pct(period["3M"]))
    r3c3.metric("Return - 6M", _fmt_pct(period["6M"]))
    r3c4.metric("Return - 1Y", _fmt_pct(period["1Y"]))
    r3c5.metric("Return - YTD", _fmt_pct(period["YTD"]))
    r3c6.metric("Return - ALL", _fmt_pct(period["ALL"]))

    # Row 4: Benchmark simple return by period
    benchmark_period = compute_benchmark_period_returns(result.points)
    r4c1, r4c2, r4c3, r4c4, r4c5, r4c6 = st.columns(6)
    r4c1.metric("Return Bench - 1M", _fmt_pct(benchmark_period["1M"]))
    r4c2.metric("Return Bench - 3M", _fmt_pct(benchmark_period["3M"]))
    r4c3.metric("Return Bench - 6M", _fmt_pct(benchmark_period["6M"]))
    r4c4.metric("Return Bench - 1Y", _fmt_pct(benchmark_period["1Y"]))
    r4c5.metric("Return Bench - YTD", _fmt_pct(benchmark_period["YTD"]))
    r4c6.metric("Return Bench - ALL", _fmt_pct(benchmark_period["ALL"]))

    df = pd.DataFrame(
        {
            "date": [p.date for p in result.points],
            "portfolio_value": [p.portfolio_value for p in result.points],
            "twr_cum_return": [p.twr_cum_return for p in result.points],
            "mwr_cum_return": [p.mwr_cum_return for p in result.points],
            "priced_ratio": [p.priced_ratio for p in result.points],
            "benchmark_value": [p.benchmark_value for p in result.points],
            "benchmark_cum_return": [p.benchmark_cum_return for p in result.points],
            "benchmark_mwr_cum_return": [
                p.benchmark_mwr_cum_return for p in result.points
            ],
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    chart_df = _filter_chart_df_by_frequency(df, chart_frequency)
    c1, c2, c3 = st.columns(3)
    with c1:
        _render_plotly_line_chart(
            chart_df,
            y_col="portfolio_value",
            title="Кривая стоимости",
            is_percent=False,
            hover_label=f"Value ({display_ccy})",
            y_tick_prefix=f"{display_ccy} ",
            benchmark_y_col="benchmark_value",
            benchmark_label=f"Benchmark ({result.benchmark_ticker})",
        )
    with c2:
        _render_plotly_line_chart(
            chart_df,
            y_col="twr_cum_return",
            title="Кумулятивная доходность",
            is_percent=True,
            hover_label="Return",
            benchmark_y_col="benchmark_cum_return",
            benchmark_label=f"Benchmark ({result.benchmark_ticker})",
        )
    with c3:
        _render_plotly_line_chart(
            chart_df,
            y_col="mwr_cum_return",
            title="Кумулятивная MWR",
            is_percent=True,
            hover_label="MWR",
            benchmark_y_col="benchmark_mwr_cum_return",
            benchmark_label=f"Benchmark ({result.benchmark_ticker})",
        )
    if (
        str(result.benchmark_ticker or "").upper() == "LQDT"
        and str(display_ccy or "").upper() == "RUB"
    ):
        st.caption(
            "Для benchmark `LQDT` на период до `2022-07-22` используется синтетическая "
            "оценка по ключевой ставке ЦБ РФ."
        )

    low_coverage_days = int((df["priced_ratio"] < 1.0).sum())
    if result.missing_price_tickers or low_coverage_days > 0:
        warn = []
        if result.missing_price_tickers:
            warn.append(
                "Нет исторических котировок для: "
                + ", ".join(sorted(result.missing_price_tickers))
            )
        if low_coverage_days > 0:
            warn.append(f"Дней с неполным покрытием цен: {low_coverage_days}")
        st.warning(" | ".join(warn))
