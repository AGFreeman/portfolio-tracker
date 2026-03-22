"""Верх сайдбара: курсы (автообновление) и валюта отображения портфеля."""
import time
from datetime import timedelta

import streamlit as st

from app.services.fx import fetch_usd_cross_rates, format_money

# Интервал фонового обновления курсов (сек)
FX_REFRESH_SEC = 60


@st.fragment(run_every=timedelta(seconds=FX_REFRESH_SEC))
def render_fx_live_block():
    """Периодически тянет курсы и пишет в session_state для таблицы портфеля."""
    rub, eur, err = fetch_usd_cross_rates()
    st.session_state["fx_cache"] = {
        "ts": time.time(),
        "rub": rub,
        "eur": eur,
        "err": err,
    }

    eur_rub = rub / eur if eur > 0 else 0.0

    st.subheader("Валюта и курсы")
    if err:
        st.caption(f"Курсы: запасной режим ({err}) · автообновление ~{FX_REFRESH_SEC} с")
    else:
        st.caption(f"Курсы к USD · автообновление ~{FX_REFRESH_SEC} с")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("USD / RUB", f"{rub:,.2f}")
    with c2:
        st.metric("EUR / RUB", f"{eur_rub:,.2f}")


def render_currency_sidebar():
    st.session_state.setdefault("display_currency", "RUB")

    render_fx_live_block()

    st.selectbox(
        "Портфель в валюте",
        options=["RUB", "USD", "EUR"],
        key="display_currency",
    )

    pt = st.session_state.get("portfolio_total")
    if pt and pt.get("priced", 0) > 0:
        st.metric(
            "Портфель (оценка)",
            format_money(float(pt["total"]), str(pt["currency"])),
            help="Обновляется на вкладке «Сводка портфеля» вместе с ценами.",
        )
        if pt.get("priced", 0) < pt.get("total_tickers", 0):
            st.caption(
                f"Частично: {pt['priced']}/{pt['total_tickers']} тикеров с котировкой."
            )
