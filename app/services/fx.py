"""Курсы валют (к USD) и конвертация для отображения портфеля."""
from typing import Optional, Tuple

import requests

# Запасные курсы, если API недоступен (примерные)
_FALLBACK_RUB_PER_USD = 95.0
_FALLBACK_EUR_PER_USD = 0.92
_YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0"}


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
