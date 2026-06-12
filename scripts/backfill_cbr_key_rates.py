#!/usr/bin/env python3
"""
Fetch CBR key rate history and store in app_settings for LQDT benchmark accrual.

Usage:
  python scripts/backfill_cbr_key_rates.py
  python scripts/backfill_cbr_key_rates.py --from 2021-01-01 --to 2026-06-05
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.db import init_db  # noqa: E402
from app.services.cbr_key_rate import (  # noqa: E402
    default_backfill_end,
    default_backfill_start,
    fetch_cbr_key_rates_from_api,
    store_cbr_key_rate_series,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill CBR key rate into app_settings")
    parser.add_argument("--from", dest="date_from", default=default_backfill_start())
    parser.add_argument("--to", dest="date_to", default=default_backfill_end())
    args = parser.parse_args()

    init_db()
    series = fetch_cbr_key_rates_from_api(args.date_from, args.date_to)
    if not series:
        print("No key rate data fetched from CBR")
        return 1
    store_cbr_key_rate_series(series)
    print(f"Stored {len(series)} daily key rates ({args.date_from} .. {args.date_to})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
