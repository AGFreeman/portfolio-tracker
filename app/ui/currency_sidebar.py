"""Верх сайдбара: курсы (автообновление) и валюта отображения портфеля."""
import time

import streamlit as st

from app.services.fx import fetch_usd_cross_rates
from app.services.prices import request_quotes_refresh
from app.services.performance import refresh_today_historical_quotes
from app.ui.cash_flows import render_cash_flow_sidebar

@st.fragment()
def render_fx_live_block():
    """Периодически тянет курсы и пишет в session_state для таблицы портфеля."""
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    cache = st.session_state.get("fx_cache") or {}
    first_bootstrap = not bool(cache)

    if live_updates_enabled or first_bootstrap:
        rub, eur, source, err = fetch_usd_cross_rates()
        st.session_state["fx_cache"] = {
            "ts": time.time(),
            "rub": rub,
            "eur": eur,
            "source": source,
            "err": err,
        }
    else:
        rub = float(cache.get("rub") or 95.0)
        eur = float(cache.get("eur") or 0.92)
        source = str(cache.get("source") or "cached")
        err = cache.get("err")
        if "fx_cache" not in st.session_state:
            st.session_state["fx_cache"] = {
                "ts": time.time(),
                "rub": rub,
                "eur": eur,
                "source": source,
                "err": err,
            }

    eur_rub = rub / eur if eur > 0 else 0.0

    st.subheader("Валюта и курсы")
    if err:
        st.caption(
            f"Курсы: ограниченный режим ({err}) · "
            f"{'автообновление включено' if live_updates_enabled else 'автообновление отключено'}"
        )
    else:
        st.caption(
            "Курсы к USD · "
            f"{'автообновление включено' if live_updates_enabled else 'автообновление отключено'}"
        )
    fx_cache = st.session_state.get("fx_cache") or {"ts": time.time()}
    last_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(fx_cache.get("ts") or time.time())))
    st.caption(f"Источник FX: `{source}` · последнее обновление: `{last_update}`")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("USD / RUB", f"{rub:,.2f}")
    with c2:
        st.metric("EUR / RUB", f"{eur_rub:,.2f}")


def render_currency_sidebar():
    st.session_state.setdefault("display_currency", "RUB")
    st.session_state.setdefault("live_price_updates_enabled", False)

    st.toggle(
        "Live price updates",
        key="live_price_updates_enabled",
        help="По умолчанию выключено. Когда включено, цены обновляются примерно раз в 60 секунд.",
    )
    prev_live = bool(st.session_state.get("_prev_live_price_updates_enabled", st.session_state["live_price_updates_enabled"]))
    curr_live = bool(st.session_state.get("live_price_updates_enabled", False))
    if curr_live != prev_live:
        request_quotes_refresh()
        refresh_today_historical_quotes()
        st.session_state["_prev_live_price_updates_enabled"] = curr_live
    if st.button("Force price update", key="force_price_update_now"):
        rub, eur, source, err = fetch_usd_cross_rates()
        st.session_state["fx_cache"] = {
            "ts": time.time(),
            "rub": rub,
            "eur": eur,
            "source": source,
            "err": err,
        }
        request_quotes_refresh()
        refresh_today_historical_quotes()
        st.rerun()

    render_fx_live_block()

    st.selectbox(
        "Портфель в валюте",
        options=["RUB", "USD", "EUR"],
        key="display_currency",
    )

    render_cash_flow_sidebar()

