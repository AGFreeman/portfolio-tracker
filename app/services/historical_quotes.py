"""Incremental sync of historical_quotes cache from price providers."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Optional

from app.db import get_max_historical_quote_date, upsert_historical_quotes_bulk
from app.services.prices import (
    build_provider_overrides,
    fetch_historical_quotes,
    fetch_price_quote,
    is_excluded_from_coverage_metric,
)

_FETCH_CHUNK_DAYS = 120


def _provider_for_ticker(ticker_up: str, provider_overrides: dict) -> tuple[str, str]:
    if ticker_up in provider_overrides:
        return provider_overrides[ticker_up]
    from app.services.prices import _detect_provider

    return _detect_provider(ticker_up)


def _upsert_live_quote_if_stale(ticker_up: str, as_of: str, provider: str, symbol: str) -> int:
    """
    When providers have no daily history through as_of (delisted MOEX / no candles),
    store the current live quote on as_of so charts and performance reach today.
    """
    last = get_max_historical_quote_date(ticker_up)
    if last is not None and last >= as_of:
        return 0
    quote = fetch_price_quote(
        ticker_up,
        provider_override=provider,
        provider_symbol_override=symbol,
    )
    if quote.price is None or float(quote.price) <= 0:
        return 0
    upsert_historical_quotes_bulk(
        [(ticker_up, as_of, float(quote.price), str(quote.currency).upper())]
    )
    return 1


def sync_historical_quotes(
    ticker: str,
    date_from: str,
    date_to: Optional[str] = None,
) -> int:
    """
    Fetch missing daily quotes for ticker and upsert into historical_quotes.
    Returns number of rows written.
    """
    ticker_up = str(ticker or "").upper().strip()
    if not ticker_up or is_excluded_from_coverage_metric(ticker_up):
        return 0

    start = str(date_from)
    end = str(date_to or date.today().isoformat())
    if start > end:
        return 0

    last = get_max_historical_quote_date(ticker_up)
    if last is None:
        fetch_from = start
    elif last >= end:
        return 0
    else:
        fetch_from = (datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()

    provider_overrides = build_provider_overrides([ticker_up])
    provider, symbol = _provider_for_ticker(ticker_up, provider_overrides)

    rows: list[tuple[str, str, float, str]] = []
    if fetch_from <= end:
        cur = datetime.strptime(fetch_from, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end, "%Y-%m-%d").date()
        while cur <= end_dt:
            chunk_end = min(cur + timedelta(days=_FETCH_CHUNK_DAYS - 1), end_dt)
            fetched = fetch_historical_quotes(
                ticker=ticker_up,
                date_from=cur.isoformat(),
                date_to=chunk_end.isoformat(),
                provider_override=provider,
                provider_symbol_override=symbol,
            )
            for day, quote in fetched.items():
                if quote.price is not None:
                    rows.append((ticker_up, day, float(quote.price), str(quote.currency).upper()))
            cur = chunk_end + timedelta(days=1)

    if rows:
        upsert_historical_quotes_bulk(rows)

    written = len(rows)
    written += _upsert_live_quote_if_stale(ticker_up, end, provider, symbol)
    return written


def sync_portfolio_historical_quotes(date_to: Optional[str] = None) -> Dict[str, int]:
    """
    Sync historical quotes for all active portfolio holdings (and MM benchmarks).
    Returns {ticker: rows_written}.
    """
    from app.db import list_positions_by_ticker
    from app.services.performance import (
        _DEFAULT_MONEY_MARKET_BENCHMARKS,
        _build_active_intervals_by_ticker,
        _iter_dates,
        _load_daily_transactions,
    )

    end = str(date_to or date.today().isoformat())
    positions = list_positions_by_ticker()
    tickers = {
        str(p.ticker).upper().strip()
        for p in positions
        if float(p.amount or 0) > 0 and str(p.ticker or "").strip()
    }
    for bench in _DEFAULT_MONEY_MARKET_BENCHMARKS.values():
        b = str(bench or "").upper().strip()
        if b:
            tickers.add(b)

    tx_by_day, first_tx_date, _last_tx = _load_daily_transactions()
    days = _iter_dates(first_tx_date, end) if first_tx_date else []
    intervals = _build_active_intervals_by_ticker(tx_by_day, days) if days else {}

    results: Dict[str, int] = {}
    for ticker_up in sorted(tickers):
        ivals = intervals.get(ticker_up, [])
        if ivals:
            date_from = min(start for start, _end in ivals)
        elif first_tx_date:
            date_from = first_tx_date
        else:
            date_from = end
        results[ticker_up] = sync_historical_quotes(ticker_up, date_from, end)

    from app.services.fx import sync_historical_fx

    fx_from = first_tx_date or end
    results["__FX__"] = sync_historical_fx(date_from=fx_from, date_to=end)
    return results
