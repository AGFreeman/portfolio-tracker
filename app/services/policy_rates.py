"""Policy rates for synthetic LQDT benchmark accrual (RUB only, before first quote)."""
from __future__ import annotations

from typing import Dict, Optional

from app.services.cbr_key_rate import (
    daily_compound_factor as cbr_daily_compound_factor,
    key_rate_as_of as cbr_key_rate_as_of,
    load_cbr_key_rate_series,
)


def uses_synthetic_policy_benchmark(display_currency: str) -> bool:
    """Synthetic accrual applies only to RUB/LQDT before the first fund quote."""
    return str(display_currency or "").upper().strip() == "RUB"


def policy_rate_as_of(
    display_currency: str,
    day: str,
    daily_series: Optional[Dict[str, float]] = None,
) -> float:
    ccy = str(display_currency or "").upper().strip()
    if ccy != "RUB":
        return 0.0
    return cbr_key_rate_as_of(day, daily_series)


def daily_policy_compound_factor(
    display_currency: str,
    day: str,
    daily_series: Optional[Dict[str, float]] = None,
) -> float:
    ccy = str(display_currency or "").upper().strip()
    if ccy != "RUB":
        return 1.0
    return cbr_daily_compound_factor(day, daily_series)


def load_policy_rate_series(date_from: str, date_to: str, display_currency: str) -> Dict[str, float]:
    ccy = str(display_currency or "").upper().strip()
    if ccy != "RUB":
        return {}
    return load_cbr_key_rate_series(date_from, date_to)


def synthetic_policy_label(display_currency: str) -> str:
    return "ключевой ставке ЦБ РФ"
