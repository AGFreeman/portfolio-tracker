"""Вкладка: подкласс актива для каждого тикера (один раз настроил — дальше не спрашиваем при сделках)."""
import streamlit as st

from app.db import (
    list_asset_subclasses,
    list_asset_classes,
    list_distinct_tickers,
    resolve_asset_subclass_id,
    set_instrument_asset_subclass,
)
def render_ticker_subclasses():
    st.caption(
        "Необязательно: переопределить, к какому **подклассу** относится тикер. "
        "Если не сохранять здесь, подкласс подставляется автоматически при сделке."
    )

    subclasses = list_asset_subclasses()
    classes = list_asset_classes()
    class_by_id = {c.id: c for c in classes}
    sub_options = []
    for s in subclasses:
        c = class_by_id.get(s.asset_class_id)
        label = f"{c.name} → {s.name}" if c else s.name
        sub_options.append((s.id, label))
    sub_ids = [o[0] for o in sub_options]
    sub_labels = {o[0]: o[1] for o in sub_options}

    tickers = list_distinct_tickers()
    if tickers:
        st.subheader("Тикеры в портфеле и справочнике")
        for t in tickers:
            # Тот же подкласс (и класс в подписи), что в сводке портфеля: настройка → сделки → авто → дефолт
            effective_sub_id = resolve_asset_subclass_id(t)
            idx = sub_ids.index(effective_sub_id) if effective_sub_id in sub_ids else 0
            col1, col2, col3 = st.columns([1, 3, 1])
            with col1:
                st.text(t)
            with col2:
                new_sub = st.selectbox(
                    "Подкласс",
                    options=sub_ids,
                    index=idx,
                    format_func=lambda i: sub_labels.get(i, str(i)),
                    key=f"sub_pick_{t}",
                    label_visibility="collapsed",
                )
            with col3:
                if st.button("Сохранить", key=f"save_sub_{t}"):
                    set_instrument_asset_subclass(t, new_sub)
                    st.success(f"{t}: подкласс сохранён.")
                    st.rerun()

    st.subheader("Добавить тикер заранее")
    with st.form("add_ticker_mapping"):
        nt = st.text_input("Тикер", placeholder="VOO, TMOS, BTC…").strip().upper()
        ns = st.selectbox(
            "Подкласс",
            options=sub_ids,
            format_func=lambda i: sub_labels.get(i, str(i)),
        )
        add_submitted = st.form_submit_button("Сохранить тикер")
        if add_submitted and nt:
            set_instrument_asset_subclass(nt, ns)
            st.success(f"Тикер {nt} добавлен с выбранным подклассом.")
            st.rerun()
        elif add_submitted and not nt:
            st.error("Введите тикер.")
