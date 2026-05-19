"""Одна строка на тикер: колонка на каждое место хранения + всего."""
from collections import defaultdict

import pandas as pd
import streamlit as st

from app.db import list_positions, list_storages

# Остатки в этих местах — дробные; во всех остальных колонках мест — целые штуки.
_CRYPTO_ONLY_STORAGE_KEYS = frozenset({"binance", "bybit", "metamask", "trustwallet"})


def _storage_name_key(name: str) -> str:
    return (name or "").strip().casefold().replace(" ", "").replace("-", "")


def _is_crypto_only_storage(place_name: str) -> bool:
    return _storage_name_key(place_name) in _CRYPTO_ONLY_STORAGE_KEYS


def _fmt_qty_for_storage(place_name: str, amount: float) -> float:
    if not amount:
        return 0.0
    if _is_crypto_only_storage(place_name):
        return float(amount)
    return float(int(round(amount)))


def _row_total_needs_decimal_display(row: pd.Series, place_columns: list[str]) -> bool:
    if any(
        _is_crypto_only_storage(c) and float(row[c]) != 0.0
        for c in place_columns
    ):
        return True
    total = float(row["Всего"])
    return abs(total - round(total)) > 1e-9


def _storage_column_names() -> list[str]:
    """Имена мест в порядке sort_order; только непустые."""
    return [s.name for s in list_storages() if (s.name or "").strip()]


def _storage_qty_display_styler(df: pd.DataFrame, place_columns: list[str]) -> pd.io.formats.style.Styler:
    """По имени места: Binance/Bybit/Metamask/TrustWallet — десятичные, остальные места — целые; «Всего» — см. строку."""
    sty = df.style
    for ridx in range(len(df)):
        for col in place_columns:
            fmt = "{:.10g}" if _is_crypto_only_storage(col) else "{:.0f}"
            sty = sty.format(formatter=fmt, subset=pd.IndexSlice[ridx, [col]])
    for ridx in range(len(df)):
        row = df.iloc[ridx]
        fmt = "{:.10g}" if _row_total_needs_decimal_display(row, place_columns) else "{:.0f}"
        sty = sty.format(formatter=fmt, subset=pd.IndexSlice[ridx, ["Всего"]])
    return sty


@st.fragment()
def render_storage_allocations_fragment():
    positions = list_positions()
    if not positions:
        st.info("Нет позиций. Добавьте позиции в боковой панели.")
        return

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

    rows: list[dict] = []
    for ticker in tickers_sorted:
        row: dict = {"Тикер": ticker}
        total = 0.0
        for place in place_columns:
            amt = qty_by_ticker_place[ticker].get(place, 0.0)
            total += amt
            # Числа (0 вместо пустой ячейки); дробность задаётся местом хранения, не тикером.
            row[place] = _fmt_qty_for_storage(place, amt)
        row["Всего"] = float(total)

        rows.append(row)

    st.caption(
        "По каждому тикеру — **одна строка**; остатки по местам (**Binance**, **Bybit**, **Metamask**, **TrustWallet** — "
        "дробные, остальные места — **целые штуки**), затем **Всего**."
    )
    df = pd.DataFrame(rows, columns=["Тикер", *place_columns, "Всего"])
    styled = _storage_qty_display_styler(df, place_columns)
    st.dataframe(
        styled,
        width="stretch",
        hide_index=True,
        key="storage_allocations_df",
        column_config={
            "Тикер": st.column_config.TextColumn("Тикер"),
        },
    )


def render_storage_allocations():
    render_storage_allocations_fragment()
