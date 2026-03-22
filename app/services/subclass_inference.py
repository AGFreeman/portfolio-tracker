"""
Автоопределение подкласса (и косвенно класса) по тикеру.
Если правило не сработало — используется дефолт из БД (см. DEFAULT_SUBCLASS_NAME).
"""
import os
from typing import Optional

from app.services.prices import MOEX_TICKERS, is_crypto_ticker

# Подкласс по умолчанию, если не удалось угадать (имя как в seed БД)
DEFAULT_SUBCLASS_NAME = os.environ.get("PORTFOLIO_DEFAULT_SUBCLASS", "Акции США")

# Явные соответствия тикер → имя подкласса (как в asset_subclasses.name)
TICKER_TO_SUBCLASS_NAME: dict[str, str] = {
    # США / мир ETF
    "VOO": "Акции США",
    "VUG": "Акции США",
    "QQQ": "Акции США",
    "TSPX": "Акции США",
    "VXUS": "Акции развитых стран кроме США",
    "TEUS": "Акции развитых стран кроме США",
    "VNQ": "Недвижимость США",
    "XLRE": "Недвижимость США",
    "VNQI": "Весь мир кроме США",
    "SCHP": "Гособлигации США",
    "LQD": "Корпоративные облигации США",
    "BNDX": "Облигации всего мира кроме США",
    "IAU": "Золото (Иностранный брокер)",
    # РФ / MOEX
    "TMOS": "Акции РФ",
    "SBGB": "Гособлигации РФ",
    "SBRB": "Корпоративные облигации РФ",
    "TGLD": "Золото (Российский брокер)",
    # Китай / EM
    "GXC": "Акции Китая",
    "MCHI": "Акции Китая",
    "CQQQ": "Акции Китая",
    "EMXC": "Акции развивающихся стран кроме Китая",
    "TEMS": "Акции развивающихся стран кроме Китая",
    # дубли / опечатки в списке MOEX из prices — при необходимости уточнить
    "EFG": "Акции развитых стран кроме США",
}


def infer_subclass_name(ticker: str) -> Optional[str]:
    """
    Угадать имя подкласса по тикеру. None — нужен дефолт из БД.
    """
    up = ticker.upper().strip()
    if not up:
        return None

    if up in TICKER_TO_SUBCLASS_NAME:
        return TICKER_TO_SUBCLASS_NAME[up]

    if is_crypto_ticker(up):
        if up == "BTC":
            return "Bitcoin"
        if up == "ETH":
            return "Ethereum"
        if up == "SOL":
            return "Solana"
        if up == "AVAX":
            return "Avalanche"
        if up == "BNB":
            return "BNB"
        if up == "XPR":
            return "Proton"
        return "Прочая криптовалюта"

    if up in MOEX_TICKERS:
        return "Акции РФ"

    # Неизвестный тикер — пусть сработает дефолт
    return None
