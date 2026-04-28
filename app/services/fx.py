"""Курсы валют (к USD) и конвертация для отображения портфеля."""
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

# Запасные курсы, если API недоступен (примерные)
_FALLBACK_RUB_PER_USD = 95.0
_FALLBACK_EUR_PER_USD = 0.92
_YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _iter_dates(date_from: str, date_to: str) -> List[str]:
    d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
    d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    if d1 < d0:
        return []
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _carry_forward_series(raw: Dict[str, float], days: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    last: Optional[float] = None
    for d in days:
        val = raw.get(d)
        if val is not None and val > 0:
            last = float(val)
            out[d] = float(val)
            continue
        if last is not None:
            out[d] = float(last)
    return out


def get_historical_usd_cross_rates(
    date_from: str,
    date_to: str,
    fallback_rub_per_usd: float,
    fallback_eur_per_usd: float,
) -> Dict[str, Tuple[float, float]]:
    """
    Return per-day USD cross rates for requested interval:
      - rub_per_usd: RUB for 1 USD
      - eur_per_usd: EUR for 1 USD
    Missing market days are filled by carry-forward.
    """
    days = _iter_dates(date_from, date_to)
    if not days:
        return {}

    # Lazy import to avoid heavy dependency loading unless historical FX is requested.
    from app.services.prices import fetch_historical_prices_yfinance

    usdrub_raw = fetch_historical_prices_yfinance("USDRUB=X", date_from, date_to)
    eurusd_raw = fetch_historical_prices_yfinance("EURUSD=X", date_from, date_to)
    rub_map = _carry_forward_series(
        {d: float(q.price) for d, q in usdrub_raw.items() if q and q.price is not None},
        days,
    )
    # Yahoo EURUSD is USD per 1 EUR, while app expects EUR per 1 USD.
    eur_per_usd_raw: Dict[str, float] = {}
    for d, q in eurusd_raw.items():
        if q is None or q.price is None:
            continue
        px = float(q.price)
        if px > 0:
            eur_per_usd_raw[d] = 1.0 / px
    eur_map = _carry_forward_series(eur_per_usd_raw, days)

    out: Dict[str, Tuple[float, float]] = {}
    for d in days:
        rub = float(rub_map.get(d, fallback_rub_per_usd))
        eur = float(eur_map.get(d, fallback_eur_per_usd))
        if rub <= 0:
            rub = float(fallback_rub_per_usd)
        if eur <= 0:
            eur = float(fallback_eur_per_usd)
        out[d] = (rub, eur)
    return out


def _fetch_yahoo_pair_rate(symbol: str) -> Optional[float]:
    """Возвращает последний close для Yahoo пары, например USDRUB=X."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"interval": "1m", "range": "1d"}
    r = requests.get(url, params=params, headers=_YAHOO_HEADERS, timeout=12)
    r.raise_for_status()
    data = r.json()

    result = ((data or {}).get("chart") or {}).get("result") or []
    if not result:
        return None

    quote = (((result[0] or {}).get("indicators") or {}).get("quote") or [])
    if not quote:
        return None

    closes = (quote[0] or {}).get("close") or []
    for value in reversed(closes):
        if value is None:
            continue
        rate = float(value)
        if rate > 0:
            return rate
    return None


def fetch_usd_cross_rates() -> Tuple[float, float, str, Optional[str]]:
    """
    Возвращает (rub_per_usd, eur_per_usd, source, error_or_none).
    rub_per_usd — сколько ₽ за 1 USD; eur_per_usd — сколько € за 1 USD.
    """
    # 1) Предпочитаем Yahoo (обычно более оперативные котировки, чем daily FX API).
    try:
        rub = _fetch_yahoo_pair_rate("USDRUB=X")
        eur_rub = _fetch_yahoo_pair_rate("EURRUB=X")
        if rub is not None and eur_rub is not None and eur_rub > 0:
            eur = rub / eur_rub
            if rub > 0 and eur > 0:
                return rub, eur, "Yahoo Finance", None
    except Exception:
        # Переходим к fallback-провайдеру ниже.
        pass

    # 2) Fallback: open.er-api (часто daily snapshot).
    url = "https://open.er-api.com/v6/latest/USD"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
        if data.get("result") != "success":
            return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, "Fallback (fixed)", "API: не success"
        rates = data.get("rates") or {}
        rub = float(rates.get("RUB") or _FALLBACK_RUB_PER_USD)
        eur = float(rates.get("EUR") or _FALLBACK_EUR_PER_USD)
        if rub <= 0 or eur <= 0:
            return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, "Fallback (fixed)", "некорректные курсы"
        return rub, eur, "open.er-api", "используется open.er-api (daily snapshot)"
    except Exception as e:
        return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, "Fallback (fixed)", str(e)


def to_usd(amount: float, from_ccy: str, rub_per_usd: float, eur_per_usd: float) -> float:
    """Сумма в from_ccy → USD."""
    c = from_ccy.upper()
    if c == "USD":
        return amount
    if c == "RUB":
        return amount / rub_per_usd
    if c == "EUR":
        return amount / eur_per_usd
    return amount


def from_usd(amount_usd: float, to_ccy: str, rub_per_usd: float, eur_per_usd: float) -> float:
    """USD → to_ccy."""
    c = to_ccy.upper()
    if c == "USD":
        return amount_usd
    if c == "RUB":
        return amount_usd * rub_per_usd
    if c == "EUR":
        return amount_usd * eur_per_usd
    return amount_usd


def convert_amount(
    amount: float,
    from_ccy: str,
    to_ccy: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> float:
    if from_ccy.upper() == to_ccy.upper():
        return amount
    usd = to_usd(amount, from_ccy, rub_per_usd, eur_per_usd)
    return from_usd(usd, to_ccy, rub_per_usd, eur_per_usd)


def format_money(value: float, currency: str) -> str:
    sym = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(currency.upper(), currency)
    return f"{value:,.2f} {sym}"
