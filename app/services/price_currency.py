"""В какой валюте торгуется тикер / приходит котировка."""
from typing import Optional

from app.db import get_instrument_provider
from app.services.prices import _detect_provider, _normalize_currency_code

_DIVERSIFICATION_BUCKETS = frozenset({"RUB", "USD", "EUR"})


def bucket_diversification_currency(currency: str) -> str:
    """Свести ISO-валюту к корзине RUB / USD / EUR для вкладки диверсификации."""
    c = (currency or "").upper().strip()
    if c in _DIVERSIFICATION_BUCKETS:
        return c
    return "USD"


def resolve_quote_currency(ticker: str, live_quote_currency: Optional[str] = None) -> str:
    """
    Валюта котировки для оценки и диверсификации: live с провайдера,
    иначе эвристика по провайдеру/тикеру.
    """
    norm = _normalize_currency_code(live_quote_currency)
    if norm:
        return norm
    return infer_quote_currency(ticker)


def infer_quote_currency(ticker: str) -> str:
    """
    Валюта цены без live-котировки: MOEX/T-Bank → RUB; CoinGecko → USD; иначе USD.
    """
    prov: Optional[str] = None
    row = get_instrument_provider(ticker)
    if row:
        prov = row[0]
    else:
        prov, _ = _detect_provider(ticker)
    if prov in ("moex_iss", "tbank"):
        return "RUB"
    if prov == "coingecko":
        return "USD"
    return "USD"


def infer_trading_currency(ticker: str) -> str:
    """
    Торговая валюта тикера в ограниченном наборе RUB/USD/EUR.
    Нужна для агрегаций диверсификации по валюте активов.
    """
    up = (ticker or "").upper().strip()
    if not up:
        return "USD"

    row = get_instrument_provider(up)
    prov: Optional[str] = row[0] if row else None
    if prov is None:
        prov, _ = _detect_provider(up)

    if prov in ("moex_iss", "tbank"):
        return "RUB"
    if prov == "coingecko":
        return "USD"

    # Yahoo тикеры: суффикс биржи обычно отражает валюту торгов.
    eur_suffixes = {
        ".AS", ".AT", ".BE", ".BR", ".DE", ".DU", ".F", ".HE",
        ".IR", ".LS", ".MC", ".MI", ".PA", ".ST", ".VI",
    }
    if any(up.endswith(sfx) for sfx in eur_suffixes):
        return "EUR"
    if up.endswith("-EUR"):
        return "EUR"
    if up.endswith("-RUB"):
        return "RUB"
    return "USD"
