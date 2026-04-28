"""Журнал ввода/вывода денег: таблица на главной вкладке, ввод — в сайдбаре."""
from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from app.db import add_cash_flow, delete_cash_flow, list_cash_flows
from app.services.fx import convert_amount, format_money


def direction_label(value: float | str) -> str:
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"in", "input", "deposit"}:
            return "Ввод"
        if v in {"out", "output", "withdraw"}:
            return "Вывод"
    return "Ввод" if float(value) > 0 else "Вывод"


def _cash_flow_totals_display(flows, display_ccy: str, rub: float, eur: float) -> tuple[float, float, float]:
    total_in = 0.0
    total_out = 0.0
    for f in flows:
        v = convert_amount(float(f.amount), f.currency, display_ccy, rub, eur)
        if float(f.amount) > 0:
            total_in += v
        else:
            total_out += abs(v)
    return total_in, total_out, total_in - total_out


def render_cash_flows() -> None:
    """Только таблица вводов/выводов (без форм)."""
    st.caption(
        "Пополнения и снятия **в рублях**. Добавить или удалить запись — в боковой панели, блок **«Ввод и вывод»**. "
        "Итоги ниже пересчитаны в валюту отображения портфеля по курсам в сайдбаре."
    )
    flows = list_cash_flows()
    if not flows:
        st.info("Пока нет записей о вводе или выводе денег.")
        return

    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    t_in, t_out, t_net = _cash_flow_totals_display(flows, display_ccy, rub, eur)
    m1, m2, m3 = st.columns(3)
    m1.metric(f"Всего вводов ({display_ccy})", format_money(t_in, display_ccy))
    m2.metric(f"Всего выводов ({display_ccy})", format_money(t_out, display_ccy))
    m3.metric(f"Чистый ввод ({display_ccy})", format_money(t_net, display_ccy))

    df = pd.DataFrame(
        [
            {
                "Дата": f.flow_date,
                "Сумма": float(f.amount),
                "Направление": direction_label(float(f.amount)),
            }
            for f in flows
        ]
    )
    amounts = [float(f.amount) for f in flows]

    # Только цвет текста (без заливки ячеек): в тёмной теме светлый фон + inherit цвет текста дают нулевой контраст.
    _IN_STYLE = "color: #81c784; font-weight: 500"
    _OUT_STYLE = "color: #ef5350; font-weight: 500"

    def _row_text_color(_row: pd.Series) -> list[str]:
        amt = amounts[int(_row.name)]
        style = _IN_STYLE if amt > 0 else _OUT_STYLE
        return [style] * len(_row)

    styled = df.style.apply(_row_text_color, axis=1).format({"Сумма": "{:,.2f}"})
    st.dataframe(styled, width="stretch", hide_index=True)


def render_cash_flow_sidebar() -> None:
    """Форма добавления и удаление записей — только сайдбар."""
    st.divider()
    st.subheader("Ввод и вывод")
    st.caption("Суммы только в **₽** (отдельно от сделок по тикерам).")

    with st.form("sidebar_cash_flow_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            amt = st.number_input(
                "Сумма, ₽",
                min_value=0.0,
                format="%.2f",
                step=0.01,
                key="sidebar_cf_amt",
            )
        with c2:
            direction = st.selectbox(
                "Направление",
                options=["in", "out"],
                format_func=direction_label,
                index=0,
                key="sidebar_cf_dir",
            )
        flow_d = st.date_input("Дата", value=date.today(), key="sidebar_cf_date")
        submitted = st.form_submit_button("Записать")
        if submitted:
            if amt <= 0:
                st.error("Укажите сумму больше нуля.")
            else:
                try:
                    add_cash_flow(
                        amount=float(amt),
                        direction=str(direction),
                        currency="RUB",
                        flow_date=flow_d.isoformat(),
                    )
                    st.success("Запись добавлена.")
                except ValueError as e:
                    st.error(str(e))

    flows = list_cash_flows()
    if flows:
        labels = {
            f"{cf.flow_date} · {direction_label(float(cf.amount))} · {cf.amount:,.2f} {cf.currency}": cf.id
            for cf in flows
        }
        pick = st.selectbox("Удалить запись", options=list(labels.keys()), key="sidebar_cf_delete_pick")
        if st.button("Удалить выбранную", key="sidebar_cf_delete_btn"):
            delete_cash_flow(labels[pick])
            st.rerun()
