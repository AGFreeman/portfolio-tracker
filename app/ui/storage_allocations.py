"""Одна строка на тикер: колонка на каждое место хранения + всего + цена."""
from collections import defaultdict
from datetime import timedelta

import streamlit as st

from app.db import list_positions, list_storages, get_instrument_provider
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import get_quotes_cached, is_crypto_ticker

PRICES_REFRESH_SEC = 60


def _fmt_qty(ticker: str, amount: float):
    if is_crypto_ticker(ticker):
        return float(amount)
    return int(round(amount))


def _storage_column_names() -> list[str]:
    """Имена мест в порядке sort_order; только непустые."""
    return [s.name for s in list_storages() if (s.name or "").strip()]


@st.fragment(run_every=timedelta(seconds=PRICES_REFRESH_SEC))
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
    provider_overrides = {}
    for t in tickers_sorted:
        prov = get_instrument_provider(t)
        if prov:
            provider_overrides[t] = prov
    quotes = get_quotes_cached(
        tickers_sorted,
        cache_ttl_sec=0,
        provider_overrides=provider_overrides,
    )

    rows: list[dict] = []
    for ticker in tickers_sorted:
        row: dict = {"Тикер": ticker}
        total = 0.0
        for place in place_columns:
            amt = qty_by_ticker_place[ticker].get(place, 0.0)
            total += amt
            if amt == 0:
                row[place] = "—"
            else:
                row[place] = _fmt_qty(ticker, amt)
        row["Всего"] = _fmt_qty(ticker, total)

        q = quotes.get(ticker)
        price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(ticker)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            row["Цена"] = format_money(price_disp, display_ccy)
        else:
            row["Цена"] = "—"

        rows.append(row)

    st.caption(
        f"По каждому тикеру — **одна строка**; столбцы — остатки по местам из справочника, затем **Всего** и **Цена** "
        f"({display_ccy} за единицу). Обновление котировок ~каждые **{PRICES_REFRESH_SEC} с**."
    )
    st.dataframe(
        rows,
        use_container_width=True,
        hide_index=True,
        key="storage_allocations_df",
    )


def render_storage_allocations():
    render_storage_allocations_fragment()
