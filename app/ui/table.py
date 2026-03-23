"""Read-only portfolio summary: Класс, Подкласс, Тикер, Цена, Количество, Стоимость (по тикеру, без разбивки по местам)."""
from datetime import timedelta

import streamlit as st

from app.db import list_positions_by_ticker, list_asset_classes, list_asset_subclasses, get_instrument_provider
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import get_quotes_cached, is_crypto_ticker

_SUBCLASS_BY_ID = None

# Интервал фонового обновления цен в сводке (сек)
PRICES_REFRESH_SEC = 60


def _subclass_by_id():
    global _SUBCLASS_BY_ID
    if _SUBCLASS_BY_ID is None:
        subclasses = list_asset_subclasses()
        _SUBCLASS_BY_ID = {s.id: s for s in subclasses}
    return _SUBCLASS_BY_ID


def _class_by_id():
    return {c.id: c for c in list_asset_classes()}


def render_portfolio_total_metric():
    """Top-level metric for total portfolio value (above main tabs)."""
    positions = list_positions_by_ticker()
    display_ccy = st.session_state.get("display_currency", "RUB")
    if not positions:
        st.session_state["portfolio_total"] = {
            "currency": display_ccy,
            "total": 0.0,
            "priced": 0,
            "total_tickers": 0,
        }
        st.metric(f"Стоимость портфеля ({display_ccy})", "—")
        return

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    tickers = list({p.ticker for p in positions})
    provider_overrides = {}
    for t in tickers:
        prov = get_instrument_provider(t)
        if prov:
            provider_overrides[t] = prov
    quotes = get_quotes_cached(
        tickers,
        cache_ttl_sec=PRICES_REFRESH_SEC,
        provider_overrides=provider_overrides,
    )

    portfolio_total = 0.0
    n_with_price = 0
    for p in positions:
        q = quotes.get(p.ticker)
        price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        if price is None:
            continue
        value_native = price * p.amount
        value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
        portfolio_total += value_disp
        n_with_price += 1

    st.session_state["portfolio_total"] = {
        "currency": display_ccy,
        "total": portfolio_total,
        "priced": n_with_price,
        "total_tickers": len(positions),
    }
    st.metric(
        f"Стоимость портфеля ({display_ccy})",
        format_money(portfolio_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма стоимостей позиций по текущим котировкам в выбранной валюте.",
    )


@st.fragment(run_every=timedelta(seconds=PRICES_REFRESH_SEC))
def render_portfolio_table_fragment():
    positions = list_positions_by_ticker()
    if not positions:
        st.session_state["portfolio_total"] = {
            "currency": st.session_state.get("display_currency", "RUB"),
            "total": 0.0,
            "priced": 0,
            "total_tickers": 0,
        }
        st.info("Нет позиций. Добавьте позиции в боковой панели.")
        return

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    display_ccy = st.session_state.get("display_currency", "RUB")

    sub_by_id = _subclass_by_id()
    class_by_id = _class_by_id()
    tickers = list({p.ticker for p in positions})
    provider_overrides = {}
    for t in tickers:
        prov = get_instrument_provider(t)
        if prov:
            provider_overrides[t] = prov
    # Короткий TTL убирает повторные медленные запросы внутри одного окна обновления fragment.
    quotes = get_quotes_cached(
        tickers,
        cache_ttl_sec=PRICES_REFRESH_SEC,
        provider_overrides=provider_overrides,
    )

    rows = []
    portfolio_total = 0.0
    n_with_price = 0
    for p in positions:
        sub = sub_by_id.get(p.asset_subclass_id)
        ac = class_by_id.get(sub.asset_class_id) if sub else None
        q = quotes.get(p.ticker)
        price = q.price if q else None
        # Валюта из ответа провайдера; если котировки нет — эвристика по провайдеру
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
            portfolio_total += value_disp
            n_with_price += 1
        else:
            price_disp = None
            value_disp = None
        qty_disp = (
            p.amount
            if is_crypto_ticker(p.ticker)
            else int(round(p.amount))
        )
        rows.append({
            "Класс": ac.name if ac else "—",
            "Подкласс": sub.name if sub else "—",
            "Тикер": p.ticker,
            "Цена": format_money(price_disp, display_ccy) if price_disp is not None else "—",
            "Количество": qty_disp,
            "Стоимость": format_money(value_disp, display_ccy) if value_disp is not None else "—",
        })

    st.session_state["portfolio_total"] = {
        "currency": display_ccy,
        "total": portfolio_total,
        "priced": n_with_price,
        "total_tickers": len(positions),
    }

    st.caption(
        f"Цены и стоимость в **{display_ccy}**; котировки обновляются ~каждые **{PRICES_REFRESH_SEC} с** "
        f"(валюта котировки — из API провайдера, затем конвертация по курсу в боковой панели). "
        f"Количество и стоимость **по тикеру** (все места хранения суммированы). Разбивка по счетам — вкладка **«По местам хранения»**."
    )
    st.dataframe(
        rows,
        use_container_width=True,
        hide_index=True,
        key="portfolio_summary_df",
    )
    if st.button("Обновить цены сейчас", key="refresh_prices_now"):
        st.session_state.pop("price_cache", None)
        st.rerun()


def render_portfolio_table():
    """Сводка с автообновлением цен (fragment)."""
    render_portfolio_table_fragment()
