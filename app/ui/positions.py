"""Add / remove positions (transactions: buy/sell)."""
from typing import Optional

import streamlit as st

from app.db import (
    list_positions,
    list_storages,
    add_transaction,
    add_storage,
    get_asset_subclass_for_ticker,
    get_default_storage_id,
    set_instrument_provider,
)
from app.services.prices import _detect_provider, is_crypto_ticker, normalize_quantity


def _storage_names_for_ui() -> list[str]:
    """Имена мест в порядке `sort_order` из БД (как в DEFAULT_STORAGE_NAMES_ORDERED), без алфавитной пересортировки."""
    get_default_storage_id()
    out: list[str] = []
    seen_cf: set[str] = set()
    for s in list_storages():
        n = (s.name or "").strip()
        if not n:
            continue
        k = n.casefold()
        if k in seen_cf:
            continue
        seen_cf.add(k)
        out.append(n)
    return out


def render_add_position():
    positions = list_positions()
    existing_tickers = sorted({p.ticker for p in positions})
    NEW_SENTINEL = "__NEW__"

    choice: Optional[str] = None
    if existing_tickers:
        choice = st.selectbox(
            "Тикер",
            options=[NEW_SENTINEL] + existing_tickers,
            format_func=lambda x: "Новый тикер…" if x == NEW_SENTINEL else x,
            key="add_ticker_choice",
        )

    raw_ticker: Optional[str] = None
    if not existing_tickers:
        raw_ticker = st.text_input(
            "Тикер",
            placeholder="VOO, BTC, TMOS…",
            key="add_ticker_only",
        )
    elif choice == NEW_SENTINEL:
        raw_ticker = st.text_input(
            "Новый тикер",
            placeholder="VOO, BTC, TMOS…",
            key="add_ticker_new",
        )

    if existing_tickers and choice is not None and choice != NEW_SENTINEL:
        t_up = str(choice).strip().upper()
    else:
        t_up = (raw_ticker or "").strip().upper()

    crypto = is_crypto_ticker(t_up) if t_up else False

    # Место хранения: selectbox — места из БД + «Новое место…»
    STORAGE_NEW = "__NEW_STORAGE__"
    storage_names = _storage_names_for_ui()
    st.caption("Места хранения — из локальной базы; новое имя — пункт «Новое место…».")

    storage_choice = st.selectbox(
        "Место хранения",
        options=[STORAGE_NEW] + storage_names,
        format_func=lambda x: "Новое место…" if x == STORAGE_NEW else str(x),
        key="add_storage_choice",
        help="Пустые имена в базе не показываются. Стартовый список мест задаётся в коде (см. DEFAULT_STORAGE_NAMES_ORDERED).",
    )
    raw_storage: Optional[str] = None
    if storage_choice == STORAGE_NEW:
        raw_storage = st.text_input(
            "Новое место",
            placeholder="Тинькофф, Ledger, Metamask…",
            key="add_storage_new_name",
        )

    amount = st.number_input(
        "Количество",
        min_value=0.0,
        value=0.0,
        step=0.00000001 if crypto else 1.0,
        format="%.8f" if crypto else "%.0f",
        key=f"add_qty_{t_up or 'none'}_{int(crypto)}",
        help="Криптовалюта: дробное число. Акции/ETF: целое (округляется при сохранении).",
    )
    if st.button("Добавить", key="add_buy_btn"):
        if not t_up:
            st.error("Выберите тикер из списка или введите новый.")
        else:
            ticker_upper = t_up
            qty = normalize_quantity(ticker_upper, float(amount))
            if qty <= 0:
                st.error("Укажите количество больше нуля.")
            else:
                if storage_choice == STORAGE_NEW:
                    storage_name_final = (raw_storage or "").strip()
                else:
                    storage_name_final = (storage_choice or "").strip()

                if not storage_name_final:
                    st.error("Выберите место из списка или введите новое название.")
                else:
                    try:
                        sid = add_storage(storage_name_final)
                    except ValueError as e:
                        st.error(str(e))
                    else:
                        subclass_id = get_asset_subclass_for_ticker(ticker_upper)
                        provider, provider_symbol = _detect_provider(ticker_upper)
                        set_instrument_provider(ticker_upper, provider, provider_symbol or None)
                        add_transaction(ticker_upper, qty, subclass_id, storage_id=sid)
                        disp = qty if crypto else int(qty)
                        st.success(
                            f"Покупка {ticker_upper}: +{disp} ({storage_name_final})."
                        )
                        st.rerun()


def render_remove_position():
    """Продажа: списание с выбранного места хранения (тикер + брокер/кошелёк)."""
    positions = list_positions()
    if not positions:
        st.info("Нет позиций для продажи.")
        return

    def _pos_label(i: int) -> str:
        p = positions[i]
        crypto_p = is_crypto_ticker(p.ticker)
        qty_disp = p.amount if crypto_p else int(round(p.amount))
        return f"{p.ticker} — {p.storage_name} ({qty_disp})"

    idx = st.selectbox(
        "Позиция (тикер и место хранения)",
        options=list(range(len(positions))),
        format_func=_pos_label,
        key="sell_position_idx",
    )
    chosen = positions[idx]
    chosen_ticker = chosen.ticker
    max_amount = float(chosen.amount)
    crypto_sell = is_crypto_ticker(chosen_ticker)

    sell_amount = st.number_input(
        "Количество",
        min_value=0.0,
        max_value=max_amount,
        value=min(0.0001 if crypto_sell else 1.0, max_amount) if max_amount > 0 else 0.0,
        step=0.00000001 if crypto_sell else 1.0,
        format="%.8f" if crypto_sell else "%.0f",
        key=f"sell_qty_{chosen_ticker}_{chosen.storage_id}_{int(crypto_sell)}",
    )
    if st.button("Продать", key="sell_btn"):
        if sell_amount <= 0:
            st.error("Укажите количество больше нуля.")
        else:
            subclass_id = get_asset_subclass_for_ticker(chosen_ticker)
            qty = normalize_quantity(chosen_ticker, float(sell_amount))
            add_transaction(
                chosen_ticker,
                -qty,
                subclass_id,
                storage_id=chosen.storage_id,
            )
            disp = qty if crypto_sell else int(qty)
            st.success(
                f"Продажа {chosen_ticker} ({chosen.storage_name}): −{disp} записана."
            )
            st.rerun()
