"""Data models for portfolio (dataclasses / types)."""
from dataclasses import dataclass
from typing import Optional


@dataclass
class AssetClass:
    id: int
    name: str
    target_pct: float
    sort_order: int


@dataclass
class AssetSubclass:
    id: int
    asset_class_id: int
    name: str
    target_pct: float
    sort_order: int


@dataclass
class Storage:
    """Где лежит актив: брокер, кошелёк и т.п."""

    id: int
    name: str
    sort_order: int


@dataclass
class Position:
    id: int
    ticker: str
    amount: float
    asset_subclass_id: int
    currency: Optional[str] = None
    storage_id: int = 0
    storage_name: str = ""


@dataclass
class Transaction:
    id: int
    ticker: str
    amount: float  # positive = buy, negative = sell
    asset_subclass_id: int
    transaction_type: str = "trade"  # trade | transfer | split | bond_redemption | conversion_blocked
    created_at: Optional[str] = None
    storage_id: int = 0
    storage_name: Optional[str] = None


@dataclass
class CashFlow:
    """Ручной учёт ввода/вывода денег (не сделки по тикерам)."""

    id: int
    amount: float
    currency: str
    flow_date: str


@dataclass
class Instrument:
    """Ticker + which price provider to use."""
    ticker: str
    provider: str  # yfinance | moex_iss | coingecko
    provider_symbol: Optional[str] = None  # e.g. CoinGecko coin_id, MOEX security id
