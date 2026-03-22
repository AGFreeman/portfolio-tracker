"""Журнал транзакций (покупки / продажи)."""
import streamlit as st

from app.db import list_transactions, list_asset_subclasses, list_asset_classes
from app.services.prices import is_crypto_ticker


def _subclass_by_id():
    return {s.id: s for s in list_asset_subclasses()}


def _class_by_id():
    return {c.id: c for c in list_asset_classes()}


def render_transactions_table():
    txs = list_transactions()
    if not txs:
        st.info("Пока нет транзакций.")
        return

    sub_by_id = _subclass_by_id()
    class_by_id = _class_by_id()

    rows = []
    for tx in txs:
        sub = sub_by_id.get(tx.asset_subclass_id)
        ac = class_by_id.get(sub.asset_class_id) if sub else None
        kind = "Покупка" if tx.amount > 0 else "Продажа"
        amt = abs(tx.amount)
        qty_disp = amt if is_crypto_ticker(tx.ticker) else int(round(amt))
        place = tx.storage_name or "—"
        rows.append({
            "Дата": tx.created_at or "—",
            "Тикер": tx.ticker,
            "Место хранения": place,
            "Тип": kind,
            "Количество": -qty_disp if tx.amount < 0 else qty_disp,
            "Класс": ac.name if ac else "—",
            "Подкласс": sub.name if sub else "—",
        })

    st.dataframe(rows, use_container_width=True, hide_index=True)
