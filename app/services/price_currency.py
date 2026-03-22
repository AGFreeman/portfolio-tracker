"""В какой валюте котировка по тикеру (для конвертации в валюту отображения)."""
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
