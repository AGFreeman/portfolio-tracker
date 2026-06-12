"""CBR key rate series for synthetic LQDT benchmark before first fund quote."""
from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from bisect import bisect_right
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen

from app.db import get_app_setting, set_app_setting

CBR_KEY_RATE_SETTING_KEY = "cbr_key_rate_v1"

# Effective dates of CBR key rate changes (annual rate as decimal). Fallback when DB cache is empty.
_CBR_KEY_RATE_STEPS: List[Tuple[str, float]] = [
    ("2013-09-17", 0.055),
    ("2014-03-03", 0.07),
    ("2014-04-28", 0.075),
    ("2014-07-28", 0.08),
    ("2014-11-05", 0.095),
    ("2014-12-12", 0.105),
    ("2014-12-16", 0.17),
    ("2015-02-02", 0.15),
    ("2015-03-16", 0.14),
    ("2015-05-05", 0.125),
    ("2015-06-16", 0.115),
    ("2015-08-03", 0.11),
    ("2016-06-14", 0.105),
    ("2016-09-19", 0.1),
    ("2017-03-27", 0.0975),
    ("2017-05-02", 0.0925),
    ("2017-06-19", 0.09),
    ("2017-09-18", 0.085),
    ("2017-10-30", 0.0825),
    ("2017-12-18", 0.0775),
    ("2018-02-12", 0.075),
    ("2018-03-26", 0.0725),
    ("2018-09-17", 0.075),
    ("2018-12-17", 0.0775),
    ("2019-06-17", 0.075),
    ("2019-07-29", 0.0725),
    ("2019-09-09", 0.07),
    ("2019-10-28", 0.065),
    ("2019-12-16", 0.0625),
    ("2020-02-10", 0.06),
    ("2020-04-27", 0.055),
    ("2020-06-22", 0.045),
    ("2020-07-27", 0.0425),
    ("2021-03-22", 0.045),
    ("2021-04-26", 0.05),
    ("2021-06-15", 0.055),
    ("2021-07-26", 0.065),
    ("2021-09-13", 0.0675),
    ("2021-10-25", 0.075),
    ("2021-12-20", 0.085),
    ("2022-02-14", 0.095),
    ("2022-02-28", 0.2),
    ("2022-04-11", 0.17),
    ("2022-05-04", 0.14),
    ("2022-05-27", 0.11),
    ("2022-06-14", 0.095),
    ("2022-07-25", 0.08),
    ("2022-09-19", 0.075),
    ("2023-07-24", 0.085),
    ("2023-08-15", 0.12),
    ("2023-09-18", 0.13),
    ("2023-10-30", 0.15),
    ("2023-12-18", 0.16),
    ("2024-07-29", 0.18),
    ("2024-09-16", 0.19),
    ("2024-10-28", 0.21),
    ("2025-06-09", 0.2),
    ("2025-07-28", 0.18),
    ("2025-09-15", 0.17),
    ("2025-10-27", 0.165),
    ("2025-12-22", 0.16),
    ("2026-02-16", 0.155),
    ("2026-03-23", 0.15),
    ("2026-04-27", 0.145),
]

_CBR_SOAP_URL = "http://www.cbr.ru/DailyInfoWebServ/DailyInfo.asmx"
_CBR_SOAP_ACTION = '"http://web.cbr.ru/KeyRateXML"'


def _iter_dates(date_from: str, date_to: str) -> List[str]:
    d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
    d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    if d1 < d0:
        return []
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _step_dates_and_rates() -> Tuple[List[str], List[float]]:
    dates = [d for d, _ in _CBR_KEY_RATE_STEPS]
    rates = [r for _, r in _CBR_KEY_RATE_STEPS]
    return dates, rates


def key_rate_as_of(day: str, daily_series: Optional[Dict[str, float]] = None) -> float:
    """Return annual CBR key rate (decimal) effective on `day` (YYYY-MM-DD)."""
    if daily_series:
        if day in daily_series:
            return float(daily_series[day])
        known_days = sorted(daily_series.keys())
        idx = bisect_right(known_days, day) - 1
        if idx >= 0:
            return float(daily_series[known_days[idx]])

    step_dates, step_rates = _step_dates_and_rates()
    idx = bisect_right(step_dates, day) - 1
    if idx < 0:
        return float(step_rates[0])
    return float(step_rates[idx])


def daily_compound_factor(day: str, daily_series: Optional[Dict[str, float]] = None) -> float:
    """One-day growth factor at CBR key rate (ACT/365 compound)."""
    annual_rate = key_rate_as_of(day, daily_series)
    return (1.0 + annual_rate) ** (1.0 / 365.0)


def load_cbr_key_rate_series(date_from: str, date_to: str) -> Dict[str, float]:
    """Load daily key rates from app_settings; missing days filled via step fallback."""
    raw = get_app_setting(CBR_KEY_RATE_SETTING_KEY)
    cached: Dict[str, float] = {}
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                for day, rate in data.items():
                    if date_from <= str(day) <= date_to:
                        cached[str(day)] = float(rate)
        except (json.JSONDecodeError, TypeError, ValueError):
            cached = {}

    out: Dict[str, float] = {}
    for d in _iter_dates(date_from, date_to):
        out[d] = key_rate_as_of(d, cached if cached else None)
    return out


def fetch_cbr_key_rates_from_api(date_from: str, date_to: str) -> Dict[str, float]:
    """Fetch daily CBR key rates via SOAP KeyRateXML."""
    from_dt = f"{date_from}T00:00:00"
    to_dt = f"{date_to}T00:00:00"
    body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <KeyRateXML xmlns="http://web.cbr.ru/">
      <fromDate>{from_dt}</fromDate>
      <ToDate>{to_dt}</ToDate>
    </KeyRateXML>
  </soap:Body>
</soap:Envelope>"""
    req = Request(
        _CBR_SOAP_URL,
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": _CBR_SOAP_ACTION,
        },
        method="POST",
    )
    with urlopen(req, timeout=120) as resp:
        text = resp.read().decode("utf-8")

    root = ET.fromstring(text)
    out: Dict[str, float] = {}
    for kr in root.findall(".//{*}KR"):
        dt_el = kr.find("{*}DT")
        rate_el = kr.find("{*}Rate")
        if dt_el is None or rate_el is None or not dt_el.text or not rate_el.text:
            continue
        day = dt_el.text[:10]
        out[day] = float(rate_el.text) / 100.0
    return out


def store_cbr_key_rate_series(series: Dict[str, float]) -> None:
    """Merge and persist daily key rates in app_settings."""
    existing: Dict[str, float] = {}
    raw = get_app_setting(CBR_KEY_RATE_SETTING_KEY)
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                existing = {str(k): float(v) for k, v in data.items()}
        except (json.JSONDecodeError, TypeError, ValueError):
            existing = {}
    existing.update(series)
    set_app_setting(CBR_KEY_RATE_SETTING_KEY, json.dumps(existing, sort_keys=True))


def default_backfill_start() -> str:
    return _CBR_KEY_RATE_STEPS[0][0]


def default_backfill_end() -> str:
    return date.today().isoformat()
