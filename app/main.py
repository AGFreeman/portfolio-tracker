"""Streamlit entry: portfolio table, asset classes, add/remove positions."""
import sys
from pathlib import Path

# Ensure project root is on path when running: streamlit run app/main.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from app.db import (
    init_db,
    seed_asset_classes_if_empty,
    apply_allocation_user_sheet_migration,
    apply_tovary_broker_subclass_names_migration,
    apply_zoloto_broker_parens_subclass_migration,
    apply_default_target_percentages_if_unset,
    reconcile_asset_class_targets,
)
from app.ui.table import render_portfolio_table
from app.ui.storage_allocations import render_storage_allocations
from app.ui.transactions import render_transactions_table
from app.ui.asset_classes import render_asset_classes
from app.ui.ticker_subclasses import render_ticker_subclasses
from app.ui.positions import render_add_position, render_remove_position
from app.ui.currency_sidebar import render_currency_sidebar

st.set_page_config(page_title="Портфель", layout="wide")

# Миграции БД при каждом прогоне (иначе после обновления кода без новой сессии схема остаётся старой)
init_db()
seed_asset_classes_if_empty()
apply_allocation_user_sheet_migration()
apply_tovary_broker_subclass_names_migration()
apply_zoloto_broker_parens_subclass_migration()
apply_default_target_percentages_if_unset()
reconcile_asset_class_targets()

st.title("Портфель")

# Sidebar: FX + display currency, then actions
with st.sidebar:
    render_currency_sidebar()
    st.divider()
    st.header("Действия")
    st.caption(
        "В **Покупке** место выбирается как тикер (список из базы + «Новое место…»). "
        "Разбивка по счетам — вкладка **«По местам хранения»**; в «Транзакциях» — колонка **Место хранения**."
    )
    add_tab, remove_tab, ticker_tab, classes_tab = st.tabs(
        ["Покупка", "Продажа", "Тикеры и классы", "Классы активов"]
    )
    with add_tab:
        render_add_position()
    with remove_tab:
        render_remove_position()
    with ticker_tab:
        render_ticker_subclasses()
    with classes_tab:
        render_asset_classes()

# Main: portfolio summary, storage breakdown, transaction log
tab_summary, tab_storage, tab_tx = st.tabs(
    ["Сводка портфеля", "По местам хранения", "Транзакции"]
)
with tab_summary:
    render_portfolio_table()
with tab_storage:
    render_storage_allocations()
with tab_tx:
    st.caption("Все покупки и продажи по дате (новые сверху).")
    render_transactions_table()
