"""FIFO tax estimation for rebalancing sells (informational, RUB)."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date
from typing import Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from app.db import list_transactions_chronological
from app.models import Transaction
from app.services.fx import convert_amount
from app.services.performance import (
    build_price_index_by_tickers,
    fx_rates_for_day,
    load_historical_fx,
    lookup_transaction_value,
)
from app.services.rebalancing import SuggestedSell


@dataclass
class TaxLot:
    acquired_date: str
    qty: float
    cost_rub: float


@dataclass
class TaxLotDisposition:
    ticker: str
    storage_id: int
    storage_name: str
    sell_qty: float
    acquired_date: str
    cost_rub: float
    proceeds_rub: float
    gain_rub: float


@dataclass
class ExemptSell:
    ticker: str
    storage_id: int
    storage_name: str
    units: float


@dataclass
class RebalanceTaxSummary:
    dispositions: List[TaxLotDisposition] = field(default_factory=list)
    exempt_sells: List[ExemptSell] = field(default_factory=list)
    total_gain_rub: float = 0.0
    total_loss_rub: float = 0.0
    net_taxable_base_rub: float = 0.0
    estimated_tax_rub: float = 0.0
    tax_rate: float = 0.0
    warnings: List[str] = field(default_factory=list)


LotKey = Tuple[str, int]
LotQueue = Deque[TaxLot]


def _parse_tx_day(tx: Transaction) -> Optional[str]:
    if not tx.created_at:
        return None
    s = str(tx.created_at).strip()
    return s[:10] if len(s) >= 10 else None


def _lot_key(ticker: str, storage_id: int) -> LotKey:
    return (str(ticker or "").upper().strip(), int(storage_id))


def _consume_fifo(
    queue: LotQueue,
    qty: float,
) -> Tuple[List[TaxLot], float]:
    """Return consumed lot fragments and unfilled qty."""
    need = float(qty)
    taken: List[TaxLot] = []
    while need > 1e-12 and queue:
        lot = queue[0]
        take = min(need, float(lot.qty))
        if lot.qty > 1e-12:
            cost_taken = float(lot.cost_rub) * (take / float(lot.qty))
        else:
            cost_taken = float(lot.cost_rub)
        taken.append(
            TaxLot(
                acquired_date=lot.acquired_date,
                qty=float(take),
                cost_rub=float(cost_taken),
            )
        )
        lot.qty -= take
        lot.cost_rub -= cost_taken
        need -= take
        if lot.qty <= 1e-12:
            queue.popleft()
    return taken, need


def _move_lots_to_queue(lots: List[TaxLot], dest: LotQueue) -> None:
    for lot in lots:
        if lot.qty > 1e-12:
            dest.append(lot)


class _FifoLedger:
    def __init__(self) -> None:
        self._queues: Dict[LotKey, LotQueue] = {}
        self._pending_transfers: Deque[List[TaxLot]] = deque()

    def _queue(self, key: LotKey) -> LotQueue:
        if key not in self._queues:
            self._queues[key] = deque()
        return self._queues[key]

    def add_lot(self, key: LotKey, lot: TaxLot) -> None:
        self._queue(key).append(lot)

    def transfer_out(self, key: LotKey, qty: float) -> float:
        taken, unfilled = _consume_fifo(self._queue(key), qty)
        if taken:
            self._pending_transfers.append(taken)
        return unfilled

    def transfer_in(self, key: LotKey) -> None:
        if not self._pending_transfers:
            return
        lots = self._pending_transfers.popleft()
        _move_lots_to_queue(lots, self._queue(key))

    def sell(self, key: LotKey, qty: float) -> Tuple[List[TaxLot], float]:
        return _consume_fifo(self._queue(key), qty)


def _build_ledger_from_transactions(
    transactions: Sequence[Transaction],
    *,
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], object]],
    fx_exact: Mapping[str, Tuple[float, float]],
    spot_rub_per_usd: float,
    spot_eur_per_usd: float,
    warnings: List[str],
) -> _FifoLedger:
    ledger = _FifoLedger()
    for tx in transactions:
        day = _parse_tx_day(tx)
        if day is None:
            continue
        ticker = str(tx.ticker or "").upper().strip()
        if not ticker:
            continue
        storage_id = int(tx.storage_id)
        key = _lot_key(ticker, storage_id)
        tx_type = str(tx.transaction_type or "trade").strip().lower()
        amount = float(tx.amount)
        rub, eur = fx_rates_for_day(day, fx_exact, spot_rub_per_usd, spot_eur_per_usd)

        if tx_type == "transfer":
            if amount < 0:
                unfilled = ledger.transfer_out(key, abs(amount))
                if unfilled > 1e-9:
                    warnings.append(
                        f"Перевод {ticker} из storage {storage_id}: "
                        f"не хватило лотов FIFO на {unfilled:.4f} шт."
                    )
            elif amount > 0:
                ledger.transfer_in(key)
            continue

        if tx_type != "trade":
            continue

        if amount > 0:
            cost_rub = lookup_transaction_value(
                ticker,
                amount,
                day,
                "RUB",
                rub,
                eur,
                as_of_index_by_ticker,
            )
            if cost_rub is None:
                warnings.append(
                    f"Покупка {ticker} ({day}): нет котировки для себестоимости в ₽."
                )
                continue
            ledger.add_lot(
                key,
                TaxLot(acquired_date=day, qty=float(amount), cost_rub=float(cost_rub)),
            )
        elif amount < 0:
            _taken, unfilled = ledger.sell(key, abs(amount))
            if unfilled > 1e-9:
                warnings.append(
                    f"Продажа {ticker} ({day}): не хватило лотов FIFO на {unfilled:.4f} шт."
                )
    return ledger


def _proceeds_rub_for_sell(
    sell: SuggestedSell,
    *,
    sale_date: str,
    display_currency: str,
    spot_rub_per_usd: float,
    spot_eur_per_usd: float,
    fx_exact: Mapping[str, Tuple[float, float]],
) -> float:
    rub, eur = fx_rates_for_day(sale_date, fx_exact, spot_rub_per_usd, spot_eur_per_usd)
    return convert_amount(
        float(sell.implied_proceeds),
        display_currency,
        "RUB",
        rub,
        eur,
    )


def _aggregate_tax(
    dispositions: Sequence[TaxLotDisposition],
    tax_rate: float,
) -> Tuple[float, float, float, float]:
    total_gain = sum(d.gain_rub for d in dispositions if d.gain_rub > 0)
    total_loss = sum(abs(d.gain_rub) for d in dispositions if d.gain_rub < 0)
    net_base = max(0.0, total_gain - total_loss)
    tax = net_base * float(tax_rate)
    return total_gain, total_loss, net_base, tax


def compute_rebalance_tax_summary(
    suggested_sells: Sequence[SuggestedSell],
    *,
    taxable_storage_ids: Iterable[int],
    tax_rate: float,
    rub_per_usd: float,
    eur_per_usd: float,
    display_currency: str = "RUB",
    sale_date: Optional[str] = None,
    transactions: Optional[Sequence[Transaction]] = None,
) -> RebalanceTaxSummary:
    """
    Estimate FIFO tax on planned rebalancing sells in RUB.

    Only sells from storages in `taxable_storage_ids` contribute to the tax base.
    """
    warnings: List[str] = []
    taxable_ids = {int(s) for s in taxable_storage_ids}
    sale_day = sale_date or date.today().isoformat()
    txs = list(transactions) if transactions is not None else list_transactions_chronological()

    tickers = {str(tx.ticker or "").upper().strip() for tx in txs if str(tx.ticker or "").strip()}
    for sell in suggested_sells:
        tickers.add(str(sell.ticker or "").upper().strip())

    first_day = sale_day
    for tx in txs:
        d = _parse_tx_day(tx)
        if d and d < first_day:
            first_day = d

    fx_exact = load_historical_fx(first_day, sale_day)
    as_of_index = build_price_index_by_tickers(tickers, first_day, sale_day)
    ledger = _build_ledger_from_transactions(
        txs,
        as_of_index_by_ticker=as_of_index,
        fx_exact=fx_exact,
        spot_rub_per_usd=rub_per_usd,
        spot_eur_per_usd=eur_per_usd,
        warnings=warnings,
    )

    dispositions: List[TaxLotDisposition] = []
    exempt_sells: List[ExemptSell] = []

    for sell in suggested_sells:
        storage_id = int(sell.storage_id)
        if storage_id not in taxable_ids:
            exempt_sells.append(
                ExemptSell(
                    ticker=str(sell.ticker).upper(),
                    storage_id=storage_id,
                    storage_name=str(sell.storage_name or ""),
                    units=float(sell.units),
                )
            )
            continue

        key = _lot_key(sell.ticker, storage_id)
        units = float(sell.units)
        if units <= 0:
            continue

        taken, unfilled = ledger.sell(key, units)
        if unfilled > 1e-9:
            warnings.append(
                f"Плановая продажа {sell.ticker} ({sell.storage_name}): "
                f"не хватило лотов FIFO на {unfilled:.4f} шт."
            )

        total_proceeds_rub = _proceeds_rub_for_sell(
            sell,
            sale_date=sale_day,
            display_currency=display_currency,
            spot_rub_per_usd=rub_per_usd,
            spot_eur_per_usd=eur_per_usd,
            fx_exact=fx_exact,
        )
        sold_qty = units - unfilled
        if sold_qty <= 1e-12:
            continue
        price_rub_per_unit = total_proceeds_rub * (sold_qty / units) / sold_qty

        for lot in taken:
            proceeds_portion = float(lot.qty) * price_rub_per_unit
            dispositions.append(
                TaxLotDisposition(
                    ticker=str(sell.ticker).upper(),
                    storage_id=storage_id,
                    storage_name=str(sell.storage_name or ""),
                    sell_qty=float(lot.qty),
                    acquired_date=lot.acquired_date,
                    cost_rub=float(lot.cost_rub),
                    proceeds_rub=float(proceeds_portion),
                    gain_rub=float(proceeds_portion - lot.cost_rub),
                )
            )

    total_gain, total_loss, net_base, tax = _aggregate_tax(dispositions, tax_rate)
    return RebalanceTaxSummary(
        dispositions=dispositions,
        exempt_sells=exempt_sells,
        total_gain_rub=total_gain,
        total_loss_rub=total_loss,
        net_taxable_base_rub=net_base,
        estimated_tax_rub=tax,
        tax_rate=float(tax_rate),
        warnings=warnings,
    )
