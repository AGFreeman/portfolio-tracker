"""Portfolio performance UI (TWR + historical backfill)."""
import pandas as pd
import streamlit as st

from app.services.fx import format_money
from app.services.performance import compute_period_returns, compute_portfolio_performance


def _fmt_pct(x: float) -> str:
    return f"{x * 100.0:+.2f}%"


def render_performance() -> None:
    st.caption(
        "Главная метрика доходности — **MWR (XIRR, годовая)** по датам и объёму денежных потоков. "
        "**TWR** показывается как дополнительная справочная метрика. "
        "История строится по сделкам и историческим котировкам."
    )
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    result = compute_portfolio_performance(
        display_currency=display_ccy,
        rub_per_usd=rub,
        eur_per_usd=eur,
        allow_fetch_missing_prices=live_updates_enabled,
    )
    if not result.points:
        st.info("Недостаточно данных: добавьте хотя бы одну сделку.")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric(
        "MWR (XIRR, годовая) — главная",
        (_fmt_pct(result.mwr_xirr_annualized) if result.mwr_xirr_annualized is not None else "—"),
        help="Money-weighted return: учитывает даты и объём всех денежных потоков.",
    )
    m2.metric(f"P&L ({display_ccy})", format_money(result.total_pnl, display_ccy))
    m3.metric(f"Текущая стоимость ({display_ccy})", format_money(result.current_value, display_ccy))

    s1, s2 = st.columns(2)
    s1.metric(f"Чистый денежный поток ({display_ccy})", format_money(result.net_invested, display_ccy))
    s2.metric("TWR (весь период, доп.)", _fmt_pct(result.total_twr))

    period = compute_period_returns(result.points)
    p1, p2, p3, p4, p5, p6 = st.columns(6)
    p1.metric("1M", _fmt_pct(period["1M"]))
    p2.metric("3M", _fmt_pct(period["3M"]))
    p3.metric("6M", _fmt_pct(period["6M"]))
    p4.metric("1Y", _fmt_pct(period["1Y"]))
    p5.metric("YTD", _fmt_pct(period["YTD"]))
    p6.metric("ALL", _fmt_pct(period["ALL"]))

    df = pd.DataFrame(
        {
            "date": [p.date for p in result.points],
            "portfolio_value": [p.portfolio_value for p in result.points],
            "twr_cum_return": [p.twr_cum_return for p in result.points],
            "priced_ratio": [p.priced_ratio for p in result.points],
        }
    )
    st.subheader("Кривая стоимости")
    st.line_chart(df.set_index("date")["portfolio_value"], width="stretch")
    st.subheader("Кумулятивная TWR доходность")
    st.line_chart(df.set_index("date")["twr_cum_return"], width="stretch")

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
