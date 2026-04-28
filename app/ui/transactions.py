"""Журнал транзакций (покупки / продажи)."""
from collections import defaultdict

import streamlit as st

from app.db import list_transactions, list_asset_subclasses, list_asset_classes
from app.services.prices import is_crypto_ticker


def _subclass_by_id():
    return {s.id: s for s in list_asset_subclasses()}


def _class_by_id():
    return {c.id: c for c in list_asset_classes()}


def _group_transfer_pairs(txs):
    """
    Схлопывает парные transfer-записи (− из источника, + в назначение) в одну строку.
    Возвращает список записей для рендера:
    - {"kind": "raw", "tx": Transaction}
    - {"kind": "transfer_pair", "out_tx": Transaction, "in_tx": Transaction}
    """
    pending = defaultdict(list)
    grouped = []

    for tx in txs:
        if tx.transaction_type != "transfer":
            grouped.append({"kind": "raw", "tx": tx})
            continue

        key = (
            tx.ticker,
            int(tx.asset_subclass_id),
            tx.created_at or "",
        )
        opposite_sign = -1 if tx.amount > 0 else 1
        opposite = pending[(key, opposite_sign)]
        if opposite:
            # Комиссия может делать вход/выход неравными по модулю.
            # Поэтому берём ближайшую по объёму противоположную запись.
            target_abs = abs(float(tx.amount))
            best_i = min(
                range(len(opposite)),
                key=lambda i: abs(abs(float(opposite[i][1].amount)) - target_abs),
            )
            idx, other = opposite.pop(best_i)
            out_tx = tx if tx.amount < 0 else other
            in_tx = tx if tx.amount > 0 else other
            grouped[idx] = {"kind": "transfer_pair", "out_tx": out_tx, "in_tx": in_tx}
        else:
            sign = 1 if tx.amount > 0 else -1
            idx = len(grouped)
            grouped.append({"kind": "pending_transfer", "tx": tx})
            pending[(key, sign)].append((idx, tx))

    # Непарные transfer-записи показываем как есть (редкий edge-case).
    out = []
    for item in grouped:
        if item["kind"] == "pending_transfer":
            out.append({"kind": "raw", "tx": item["tx"]})
        else:
            out.append(item)
    return out


def render_transactions_table():
    txs = list_transactions()
    if not txs:
        st.info("Пока нет транзакций.")
        return

    sub_by_id = _subclass_by_id()
    class_by_id = _class_by_id()

    rows = []
    grouped = _group_transfer_pairs(txs)
    for item in grouped:
        if item["kind"] == "transfer_pair":
            out_tx = item["out_tx"]
            in_tx = item["in_tx"]
            sub = sub_by_id.get(out_tx.asset_subclass_id)
            ac = class_by_id.get(sub.asset_class_id) if sub else None
            amt = abs(float(in_tx.amount))
            qty_disp = amt if is_crypto_ticker(out_tx.ticker) else int(round(amt))
            from_place = out_tx.storage_name or "—"
            to_place = in_tx.storage_name or "—"
            rows.append({
                "Дата": out_tx.created_at or in_tx.created_at or "—",
                "Тикер": out_tx.ticker,
                "Место хранения": f"{from_place} → {to_place}",
                "Тип": "Перевод",
                "Количество": qty_disp,
                "Класс": ac.name if ac else "—",
                "Подкласс": sub.name if sub else "—",
            })
            continue

        tx = item["tx"]
        sub = sub_by_id.get(tx.asset_subclass_id)
        ac = class_by_id.get(sub.asset_class_id) if sub else None
        if tx.transaction_type == "transfer":
            kind = "Перевод (непарный)"
        elif tx.transaction_type == "split":
            kind = "Сплит"
        elif tx.transaction_type == "bond_redemption":
            kind = "Погашение облигации"
        elif tx.transaction_type == "conversion_blocked":
            kind = "Выделение заблокированных активов"
        elif tx.transaction_type == "conversion":
            kind = "Конвертация"
        elif tx.transaction_type == "merger":
            kind = "Слияние"
        else:
            kind = "Покупка" if tx.amount > 0 else "Продажа"
        amt = abs(float(tx.amount))
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

    st.dataframe(rows, width="stretch", hide_index=True)
