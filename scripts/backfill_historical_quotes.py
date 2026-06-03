#!/usr/bin/env python3
"""
One-off (or manual) rebuild of historical_quotes for all portfolio holding intervals.

Deletes existing rows per (ticker, interval) then fetches from providers and inserts.
Run outside Streamlit — the app only reads historical_quotes at runtime.

Usage:
  python scripts/backfill_historical_quotes.py
  python scripts/backfill_historical_quotes.py --ticker GMKN
  python scripts/backfill_historical_quotes.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import json  # noqa: E402

from app.db import (  # noqa: E402
    delete_historical_quotes_in_range,
    init_db,
    set_app_setting,
    upsert_historical_quotes_bulk,
)
from app.services.fx import get_historical_usd_cross_rates_exact  # noqa: E402
from app.services.performance import (  # noqa: E402
    _DEFAULT_MONEY_MARKET_BENCHMARKS,
    _build_active_intervals_by_ticker,
    _iter_dates,
    _load_daily_transactions,
)
from app.services.prices import (  # noqa: E402
    build_provider_overrides,
    fetch_historical_quotes,
)

_FETCH_CHUNK_DAYS = 120


def _fetch_interval(
    ticker: str,
    date_from: str,
    date_to: str,
    provider: str,
    provider_symbol: str,
) -> list:
    rows: list = []
    cur = datetime.strptime(date_from, "%Y-%m-%d").date()
    end_dt = datetime.strptime(date_to, "%Y-%m-%d").date()
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=_FETCH_CHUNK_DAYS - 1), end_dt)
        fetched = fetch_historical_quotes(
            ticker=ticker,
            date_from=cur.isoformat(),
            date_to=chunk_end.isoformat(),
            provider_override=provider,
            provider_symbol_override=provider_symbol,
        )
        for d, q in fetched.items():
            if q.price is not None:
                rows.append((ticker, d, q.price, q.currency))
        cur = chunk_end + timedelta(days=1)
    return rows


def _rebuild_ticker_intervals(
    ticker: str,
    intervals: list,
    provider_overrides: dict,
    *,
    dry_run: bool,
) -> tuple[int, int]:
    from app.services.prices import _detect_provider

    if ticker in provider_overrides:
        prov, sym = provider_overrides[ticker]
    else:
        prov, sym = _detect_provider(ticker)

    deleted = 0
    inserted = 0
    for i_start, i_end in intervals:
        if dry_run:
            print(f"  [dry-run] {ticker} {i_start}..{i_end} delete+fetch")
            continue
        n_del = delete_historical_quotes_in_range(ticker, i_start, i_end)
        deleted += n_del
        rows = _fetch_interval(ticker, i_start, i_end, prov, sym)
        if rows:
            upsert_historical_quotes_bulk(rows)
        inserted += len(rows)
        print(f"  {ticker} {i_start}..{i_end}: deleted={n_del} inserted={len(rows)}")
    return deleted, inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild historical_quotes from providers")
    parser.add_argument("--ticker", help="Only this ticker (uppercase)")
    parser.add_argument("--dry-run", action="store_true", help="Print work only, no DB/network writes")
    parser.add_argument("--include-benchmarks", action="store_true", default=True)
    args = parser.parse_args()

    init_db()
    tx_by_day, first_tx_date, _ = _load_daily_transactions()
    if not first_tx_date:
        print("No transactions — nothing to backfill.")
        return 0

    end = date.today().isoformat()
    days = _iter_dates(first_tx_date, end)
    active = _build_active_intervals_by_ticker(tx_by_day, days) if days else {}
    if args.ticker:
        key = str(args.ticker).strip().upper()
        active = {key: active.get(key, [])}

    extra: dict = {}
    if args.include_benchmarks:
        for _ccy, bench in _DEFAULT_MONEY_MARKET_BENCHMARKS.items():
            extra.setdefault(bench, []).append((first_tx_date, end))

    work: dict = {t: list(ivals) for t, ivals in active.items()}
    for ticker, ivals in extra.items():
        work.setdefault(ticker, []).extend(ivals)

    if not work:
        print("No tickers to process.")
        return 0

    tickers = sorted(work.keys())
    provider_overrides = build_provider_overrides(tickers)
    print(f"Backfill {len(tickers)} tickers, dry_run={args.dry_run}")

    total_deleted = 0
    total_inserted = 0
    workers = max(1, min(8, len(tickers)))

    def _job(ticker: str) -> tuple[int, int]:
        print(f"Start {ticker} ({len(work[ticker])} intervals)")
        return _rebuild_ticker_intervals(
            ticker,
            work[ticker],
            provider_overrides,
            dry_run=args.dry_run,
        )

    if args.dry_run:
        for t in tickers:
            _job(t)
    else:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(_job, t): t for t in tickers}
            for fut in as_completed(futures):
                t = futures[fut]
                try:
                    d, ins = fut.result()
                    total_deleted += d
                    total_inserted += ins
                except Exception as exc:
                    print(f"FAIL {t}: {exc}")

    if not args.dry_run:
        fx = get_historical_usd_cross_rates_exact(first_tx_date, end)
        set_app_setting(
            "historical_fx_v1",
            json.dumps({d: [rub, eur] for d, (rub, eur) in fx.items()}),
        )
        print(f"FX cache: {len(fx)} days stored in app_settings")

    print(f"Done. deleted≈{total_deleted} inserted≈{total_inserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
