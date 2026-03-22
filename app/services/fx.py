"""Курсы валют (к USD) и конвертация для отображения портфеля."""
from typing import Optional, Tuple

import requests

# Запасные курсы, если API недоступен (примерные)
_FALLBACK_RUB_PER_USD = 95.0
_FALLBACK_EUR_PER_USD = 0.92


def fetch_usd_cross_rates() -> Tuple[float, float, Optional[str]]:
    """
    Возвращает (rub_per_usd, eur_per_usd, error_or_none).
    rub_per_usd — сколько ₽ за 1 USD; eur_per_usd — сколько € за 1 USD.
    """
    url = "https://open.er-api.com/v6/latest/USD"
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        data = r.json()
        if data.get("result") != "success":
            return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, "API: не success"
        rates = data.get("rates") or {}
        rub = float(rates.get("RUB") or _FALLBACK_RUB_PER_USD)
        eur = float(rates.get("EUR") or _FALLBACK_EUR_PER_USD)
        if rub <= 0 or eur <= 0:
            return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, "некорректные курсы"
        return rub, eur, None
    except Exception as e:
        return _FALLBACK_RUB_PER_USD, _FALLBACK_EUR_PER_USD, str(e)


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
