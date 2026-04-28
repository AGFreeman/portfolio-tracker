"""Read-only portfolio summary: Класс, Подкласс, Тикер, Цена, Количество, Стоимость (по тикеру, без разбивки по местам)."""
from datetime import timedelta
import time

import streamlit as st

from app.db import (
    list_positions_by_ticker,
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    get_instrument_provider,
    get_instrument_main_map,
)
from app.services.fx import convert_amount, format_money
from app.services.price_currency import infer_quote_currency
from app.services.prices import (
    get_app_quotes,
    get_quotes_cache_meta,
    is_crypto_ticker,
    normalize_quote_price_for_valuation,
)

_SUBCLASS_BY_ID = None

_NON_US_YF_SUFFIXES = {
    ".AS", ".AT", ".AX", ".BE", ".BK", ".BR", ".CO", ".DE", ".DU", ".F", ".HE",
    ".HK", ".IR", ".JK", ".JO", ".KQ", ".KS", ".L", ".LS", ".MC", ".ME", ".MI",
    ".MX", ".NS", ".NZ", ".OL", ".PA", ".PR", ".SA", ".SG", ".SI", ".SN", ".SR",
    ".SS", ".ST", ".SW", ".SZ", ".T", ".TA", ".TLV", ".TO", ".TSX", ".TW", ".VI",
    ".WA",
}


def _is_us_exchange_ticker(ticker: str) -> bool:
    """Heuristic: US-related = Yahoo ticker without explicit non-US suffix."""
    up = (ticker or "").upper().strip()
    if not up:
        return False
    row = get_instrument_provider(up)
    provider = (row[0] if row else None) or ""
    if provider in ("moex_iss", "tbank", "coingecko"):
        return False
    if up.endswith("-EUR") or up.endswith("-RUB"):
        return False
    return not any(up.endswith(sfx) for sfx in _NON_US_YF_SUFFIXES)


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
            "main_total": 0.0,
            "other_total": 0.0,
            "blocked_total": 0.0,
            "priced": 0,
            "total_tickers": 0,
        }
        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Стоимость портфеля ({display_ccy})", "—")
        c2.metric(f"Основной ({display_ccy})", "—")
        c3.metric(f"Прочие ({display_ccy})", "—")
        c4.metric(f"Заблокировано ({display_ccy})", "—")
        return

    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)

    tickers = list({p.ticker for p in positions})
    main_by_ticker = get_instrument_main_map(tickers)
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    quotes = get_app_quotes(tickers)

    portfolio_total = 0.0
    main_total = 0.0
    other_total = 0.0
    blocked_total = 0.0
    n_with_price = 0
    blocked_tickers = {t.upper() for t in list_buy_blocked_tickers()}
    for p in positions:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is None:
            continue
        value_native = price * p.amount
        value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
        portfolio_total += value_disp
        if (p.ticker or "").upper() in blocked_tickers:
            blocked_total += value_disp
        if bool(main_by_ticker.get((p.ticker or "").upper(), False)):
            main_total += value_disp
        else:
            other_total += value_disp
        n_with_price += 1

    st.session_state["portfolio_total"] = {
        "currency": display_ccy,
        "total": portfolio_total,
        "main_total": main_total,
        "other_total": other_total,
        "blocked_total": blocked_total,
        "priced": n_with_price,
        "total_tickers": len(positions),
    }
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        f"Стоимость портфеля ({display_ccy})",
        format_money(portfolio_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма стоимостей позиций по текущим котировкам в выбранной валюте.",
    )
    c2.metric(
        f"Основной ({display_ccy})",
        format_money(main_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма по инструментам с флагом main = 1.",
    )
    c3.metric(
        f"Прочие ({display_ccy})",
        format_money(other_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма по инструментам с флагом main = 0.",
    )
    c4.metric(
        f"Заблокировано ({display_ccy})",
        format_money(blocked_total, display_ccy) if n_with_price > 0 else "—",
        help="Сумма стоимостей инструментов, которые заблокированы для покупок в ребалансировке.",
    )


@st.fragment()
def render_portfolio_table_fragment():
    positions = list_positions_by_ticker()
    if not positions:
        st.session_state["portfolio_total"] = {
            "currency": st.session_state.get("display_currency", "RUB"),
            "total": 0.0,
            "main_total": 0.0,
            "other_total": 0.0,
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
    main_by_ticker = get_instrument_main_map(tickers)
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    quotes = get_app_quotes(tickers)
    meta = get_quotes_cache_meta()
    stale_tickers = set(meta.get("stale_tickers") or [])

    main_rows = []
    other_rows = []
    portfolio_total = 0.0
    main_total = 0.0
    other_total = 0.0
    n_with_price = 0
    for p in positions:
        sub = sub_by_id.get(p.asset_subclass_id)
        ac = class_by_id.get(sub.asset_class_id) if sub else None
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        # Валюта из ответа провайдера; если котировки нет — эвристика по провайдеру
        quote_ccy = q.currency if q else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
            portfolio_total += value_disp
            if bool(main_by_ticker.get((p.ticker or "").upper(), False)):
                main_total += value_disp
            else:
                other_total += value_disp
            n_with_price += 1
        else:
            price_disp = None
            value_disp = None
        qty_disp = (
            p.amount
            if is_crypto_ticker(p.ticker)
            else int(round(p.amount))
        )
        price_cell = (
            (format_money(price_disp, display_ccy) + " *")
            if (price_disp is not None and p.ticker in stale_tickers)
            else (format_money(price_disp, display_ccy) if price_disp is not None else "—")
        )
        main_row = {
            "_class_sort": int(ac.sort_order) if ac else 10**9,
            "_subclass_sort": int(sub.sort_order) if sub else 10**9,
            "Класс": ac.name if ac else "—",
            "Подкласс": sub.name if sub else "—",
            "Тикер": p.ticker,
            "Цена": price_cell,
            "Количество": qty_disp,
            "Стоимость": format_money(value_disp, display_ccy) if value_disp is not None else "—",
        }
        other_row = {
            "_class_sort": int(ac.sort_order) if ac else 10**9,
            "_subclass_sort": int(sub.sort_order) if sub else 10**9,
            "Класс": ac.name if ac else "—",
            "Подкласс": sub.name if sub else "—",
            "Тикер": p.ticker,
            "Цена": price_cell,
            "Количество": qty_disp,
            "Стоимость": format_money(value_disp, display_ccy) if value_disp is not None else "—",
        }
        # Разделяем по флагу portfolio.main.
        if bool(main_by_ticker.get((p.ticker or "").upper(), False)):
            main_rows.append(main_row)
        else:
            other_rows.append(other_row)

    main_rows.sort(
        key=lambda r: (
            int(r["_class_sort"]),
            int(r["_subclass_sort"]),
            0 if _is_us_exchange_ticker(str(r["Тикер"])) else 1,
            str(r["Тикер"]),
        )
    )
    for r in main_rows:
        r.pop("_class_sort", None)
        r.pop("_subclass_sort", None)

    other_rows.sort(
        key=lambda r: (
            int(r["_class_sort"]),
            int(r["_subclass_sort"]),
            0 if _is_us_exchange_ticker(str(r["Тикер"])) else 1,
            str(r["Тикер"]),
        )
    )
    for r in other_rows:
        r.pop("_class_sort", None)
        r.pop("_subclass_sort", None)

    st.session_state["portfolio_total"] = {
        "currency": display_ccy,
        "total": portfolio_total,
        "main_total": main_total,
        "other_total": other_total,
        "priced": n_with_price,
        "total_tickers": len(positions),
    }

    table_help = (
        f"Цены и стоимость в {display_ccy}; "
        f"{'автообновление котировок включено' if live_updates_enabled else 'автообновление котировок отключено'}. "
        "Валюта котировки берется из API провайдера, затем применяется конвертация по курсу из боковой панели. "
        "Количество и стоимость показываются по тикеру (все места хранения суммированы). "
        "Разбивка по счетам доступна во вкладке «По местам хранения»."
    )
    meta_ts = meta.get("ts")
    meta_providers = meta.get("providers") or []
    if meta_ts:
        last_update = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(meta_ts)))
        providers_text = ", ".join(meta_providers) if meta_providers else "unknown"
        table_help += f" Источник цен: {providers_text}. Последнее обновление: {last_update}."
    if stale_tickers:
        table_help += " Звездочка * у цены означает последнюю доступную котировку при временной недоступности провайдера."

    tab_main, tab_other = st.tabs(["Основной портфель", "Прочие активы"])
    with tab_main:
        st.subheader("Основной портфель", help=table_help)
        if main_rows:
            st.dataframe(
                main_rows,
                width="stretch",
                height=1000,
                hide_index=True,
                key="portfolio_summary_main_df",
            )
        else:
            st.info("Нет инструментов с `main = 1`.")

    with tab_other:
        st.subheader("Прочие активы", help=table_help)
        if other_rows:
            st.dataframe(
                other_rows,
                width="stretch",
                height=1000,
                hide_index=True,
                key="portfolio_summary_other_df",
            )
        else:
            st.info("Нет инструментов с `main = 0`.")
def render_portfolio_table():
    """Сводка с автообновлением цен (fragment)."""
    render_portfolio_table_fragment()
