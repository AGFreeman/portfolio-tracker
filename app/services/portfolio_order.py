"""Shared portfolio ticker ordering (summary table, rebalancing, charts)."""
from __future__ import annotations

from typing import Mapping, Sequence

from app.db import get_instrument_provider

_NON_US_YF_SUFFIXES = {
    ".AS", ".AT", ".AX", ".BE", ".BK", ".BR", ".CO", ".DE", ".DU", ".F", ".HE",
    ".HK", ".IR", ".JK", ".JO", ".KQ", ".KS", ".L", ".LS", ".MC", ".ME", ".MI",
    ".MX", ".NS", ".NZ", ".OL", ".PA", ".PR", ".SA", ".SG", ".SI", ".SN", ".SR",
    ".SS", ".ST", ".SW", ".SZ", ".T", ".TA", ".TLV", ".TO", ".TSX", ".TW", ".VI",
    ".WA",
}


def is_us_exchange_ticker(ticker: str) -> bool:
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


def portfolio_ticker_sort_key(
    ticker: str,
    *,
    asset_subclass_id: int | None,
    subclass_by_id: Mapping[int, object],
    class_sort_by_id: Mapping[int, int],
) -> tuple:
    sub = subclass_by_id.get(asset_subclass_id) if asset_subclass_id is not None else None
    class_id = getattr(sub, "asset_class_id", None)
    return (
        class_sort_by_id.get(class_id, 10**9) if class_id is not None else 10**9,
        int(getattr(sub, "sort_order", 10**9)) if sub is not None else 10**9,
        0 if is_us_exchange_ticker(ticker) else 1,
        str(ticker).upper(),
    )


def portfolio_record_sort_key(
    record: Mapping[str, object],
    *,
    subclass_by_id: Mapping[int, object],
    class_sort_by_id: Mapping[int, int],
) -> tuple:
    subclass_id = record.get("subclass_id")
    if subclass_id is None:
        subclass_id = record.get("asset_subclass_id")
    return portfolio_ticker_sort_key(
        str(record.get("ticker") or ""),
        asset_subclass_id=int(subclass_id) if subclass_id is not None else None,
        subclass_by_id=subclass_by_id,
        class_sort_by_id=class_sort_by_id,
    )


def build_portfolio_ticker_order(
    tickers: Sequence[tuple[str, int]],
    *,
    subclass_by_id: Mapping[int, object],
    class_sort_by_id: Mapping[int, int],
) -> dict[str, int]:
    """Map UPPER(ticker) -> row index in the summary Tickers table."""
    ordered = sorted(
        tickers,
        key=lambda item: portfolio_ticker_sort_key(
            item[0],
            asset_subclass_id=int(item[1]),
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        ),
    )
    return {str(ticker).upper(): idx for idx, (ticker, _sid) in enumerate(ordered)}
