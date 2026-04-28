"""Одна строка на тикер: колонка на каждое место хранения + всего + цена."""
from collections import defaultdict
from datetime import timedelta

import streamlit as st

from app.db import list_positions, list_storages
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import get_app_quotes, is_crypto_ticker, normalize_quote_price_for_valuation


def _fmt_qty(ticker: str, amount: float):
    if is_crypto_ticker(ticker):
        return float(amount)
    return int(round(amount))


def _storage_column_names() -> list[str]:
    """Имена мест в порядке sort_order; только непустые."""
    return [s.name for s in list_storages() if (s.name or "").strip()]


@st.fragment()
def render_storage_allocations_fragment():
    positions = list_positions()
    if not positions:
        st.info("Нет позиций. Добавьте позиции в боковой панели.")
        return

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    display_ccy = st.session_state.get("display_currency", "RUB")

    # Колонки: все места из справочника + неизвестные имена из сделок
    base_places = _storage_column_names()
    seen_places = set(base_places)
    extra_places: list[str] = []
    for p in positions:
        n = (p.storage_name or "").strip() or "—"
        if n not in seen_places:
            seen_places.add(n)
            extra_places.append(n)
    place_columns = base_places + extra_places

    # тикер -> место -> количество
    qty_by_ticker_place: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for p in positions:
        place = (p.storage_name or "").strip() or "—"
        qty_by_ticker_place[p.ticker][place] += float(p.amount)

    tickers_sorted = sorted(qty_by_ticker_place.keys())
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    quotes = get_app_quotes(tickers_sorted)

    rows: list[dict] = []
    for ticker in tickers_sorted:
        row: dict = {"Тикер": ticker}
        total = 0.0
        for place in place_columns:
            amt = qty_by_ticker_place[ticker].get(place, 0.0)
            total += amt
            # Все ячейки — строки: иначе Arrow ругается на object-колонку с int и «—».
            if amt == 0:
                row[place] = "—"
            else:
                row[place] = str(_fmt_qty(ticker, amt))
        row["Всего"] = str(_fmt_qty(ticker, total))

        q = quotes.get(ticker)
        raw_price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(ticker)
        price = normalize_quote_price_for_valuation(ticker, raw_price, quote_ccy)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            row["Цена"] = format_money(price_disp, display_ccy)
        else:
            row["Цена"] = "—"

        rows.append(row)

    st.caption(
        f"По каждому тикеру — **одна строка**; столбцы — остатки по местам из справочника, затем **Всего** и **Цена** "
        f"({display_ccy} за единицу). "
        f"{'Автообновление котировок включено.' if live_updates_enabled else 'Автообновление котировок отключено.'}"
    )
    st.dataframe(
        rows,
        width="stretch",
        hide_index=True,
        key="storage_allocations_df",
    )


def render_storage_allocations():
    render_storage_allocations_fragment()
