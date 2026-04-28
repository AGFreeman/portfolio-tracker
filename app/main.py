"""Streamlit entry: portfolio table, asset classes, add/remove positions."""
import sys
from pathlib import Path

# Ensure project root is on path when running: streamlit run app/main.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from app.db import (
    init_db,
    seed_asset_classes_if_empty,
    apply_default_target_percentages_if_unset,
    reconcile_asset_class_targets,
)
from app.ui.table import render_portfolio_table, render_portfolio_total_metric
from app.ui.storage_allocations import render_storage_allocations
from app.ui.transactions import render_transactions_table
from app.ui.asset_classes import render_asset_classes
from app.ui.ticker_subclasses import render_ticker_subclasses
from app.ui.positions import render_add_position, render_remove_position, render_transfer_position
from app.ui.currency_sidebar import render_currency_sidebar
from app.ui.rebalancing import render_rebalancing
from app.ui.performance import render_performance, render_performance_top_metrics
from app.ui.diversification import render_diversification
from app.ui.cash_flows import render_cash_flows
from app.services.performance import refresh_today_historical_quotes

st.set_page_config(page_title="Портфель", layout="wide")

# Базовая инициализация БД без автоприменения миграций:
# на этапе активной разработки структура и данные поддерживаются вручную.
init_db()
seed_asset_classes_if_empty()
apply_default_target_percentages_if_unset()
reconcile_asset_class_targets()

if not bool(st.session_state.get("historical_quotes_today_refreshed_once", False)):
    refresh_today_historical_quotes()
    st.session_state["historical_quotes_today_refreshed_once"] = True

# Sidebar: FX + display currency, then actions
with st.sidebar:
    render_currency_sidebar()
    st.divider()
    st.header("Действия")
    st.caption(
        "В **Покупке** место выбирается как тикер (список из базы + «Новое место…»). "
        "Разбивка по счетам — вкладка **«По местам хранения»**; в «Транзакциях» — колонка **Место хранения**."
    )
    add_tab, remove_tab, transfer_tab, ticker_tab, classes_tab = st.tabs(
        ["Покупка", "Продажа", "Перевод", "Тикеры и классы", "Классы активов"]
    )
    with add_tab:
        render_add_position()
    with remove_tab:
        render_remove_position()
    with transfer_tab:
        render_transfer_position()
    with ticker_tab:
        render_ticker_subclasses()
    with classes_tab:
        render_asset_classes()

st.title("Портфель")
render_portfolio_total_metric()
render_performance_top_metrics()
# Main: portfolio summary, storage breakdown, transactions, rebalancing and performance
tab_summary, tab_diversification, tab_storage, tab_tx, tab_cash, tab_rebalance, tab_performance = st.tabs(
    [
        "Сводка портфеля",
        "Диверсификация",
        "По местам хранения",
        "Транзакции",
        "Деньги",
        "Ребалансировка",
        "Доходность",
    ]
)
with tab_summary:
    render_portfolio_table()
with tab_diversification:
    render_diversification()
with tab_storage:
    render_storage_allocations()
with tab_tx:
    st.caption(
        "Все операции по дате: покупки, продажи, погашения облигаций, переводы и сплиты (новые сверху)."
    )
    render_transactions_table()
with tab_cash:
    render_cash_flows()
with tab_rebalance:
    render_rebalancing()
with tab_performance:
    render_performance()
