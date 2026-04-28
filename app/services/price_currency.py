"""В какой валюте торгуется тикер / приходит котировка."""
from typing import Optional, Tuple

from app.db import get_instrument_provider
from app.services.prices import _detect_provider


def infer_quote_currency(ticker: str) -> str:
    """
    Валюта цены: MOEX → RUB; Yahoo/CoinGecko → USD.
    """
    prov: Optional[str] = None
    row = get_instrument_provider(ticker)
    if row:
        prov = row[0]
    else:
        prov, _ = _detect_provider(ticker)
    if prov == "moex_iss":
        return "RUB"
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

    if prov == "moex_iss":
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
