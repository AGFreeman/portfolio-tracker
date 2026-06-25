"""Unit tests for FIFO tax estimation."""
import unittest
from unittest.mock import patch

from app.models import Transaction
from app.services.rebalancing import SuggestedSell
from app.services.tax import compute_rebalance_tax_summary


def _tx(
    tx_id: int,
    ticker: str,
    amount: float,
    day: str,
    storage_id: int,
    tx_type: str = "trade",
) -> Transaction:
    return Transaction(
        id=tx_id,
        ticker=ticker,
        amount=amount,
        asset_subclass_id=1,
        transaction_type=tx_type,
        created_at=f"{day} 12:00:00",
        storage_id=storage_id,
        storage_name=f"storage-{storage_id}",
    )


def _sell(
    ticker: str,
    units: float,
    proceeds: float,
    storage_id: int,
    storage_name: str = "",
) -> SuggestedSell:
    return SuggestedSell(
        ticker=ticker,
        asset_subclass_id=1,
        subclass_name="Test",
        units=units,
        implied_proceeds=proceeds,
        price_display=proceeds / units if units else 0.0,
        storage_id=storage_id,
        storage_name=storage_name or f"storage-{storage_id}",
    )


class TestFifoTax(unittest.TestCase):
    def _run(
        self,
        transactions,
        suggested_sells,
        *,
        taxable_storage_ids=None,
        tax_rate=0.13,
        rub=100.0,
        eur=0.92,
        price_rub_per_unit=None,
        fx_by_day=None,
    ):
        taxable_storage_ids = {1} if taxable_storage_ids is None else taxable_storage_ids
        price_rub_per_unit = price_rub_per_unit or {}
        fx_by_day = fx_by_day or {}

        def lookup(ticker, amount, day, target, rub_p, eur_p, idx):
            up = str(ticker).upper()
            unit_price = price_rub_per_unit.get((up, day), 100.0)
            rub_rate, _eur = fx_by_day.get(day, (rub_p, eur_p))
            return abs(float(amount)) * unit_price * (rub_rate / 100.0)

        with patch("app.services.tax.load_historical_fx") as load_fx, patch(
            "app.services.tax.build_price_index_by_tickers"
        ) as build_idx, patch(
            "app.services.tax.lookup_transaction_value", side_effect=lookup
        ):
            load_fx.return_value = fx_by_day or {
                "2024-01-01": (100.0, 0.92),
                "2024-02-01": (100.0, 0.92),
                "2024-06-01": (80.0, 0.90),
            }
            build_idx.return_value = {"AAPL": ([], [])}
            return compute_rebalance_tax_summary(
                suggested_sells,
                taxable_storage_ids=taxable_storage_ids,
                tax_rate=tax_rate,
                rub_per_usd=rub,
                eur_per_usd=eur,
                display_currency="RUB",
                sale_date="2024-06-01",
                transactions=transactions,
            )

    def test_fifo_consumes_older_lots_first(self):
        txs = [
            _tx(1, "AAPL", 10, "2024-01-01", 1),
            _tx(2, "AAPL", 5, "2024-02-01", 1),
        ]
        sells = [_sell("AAPL", 8, 1200.0, 1)]
        summary = self._run(
            txs,
            sells,
            price_rub_per_unit={
                ("AAPL", "2024-01-01"): 100.0,
                ("AAPL", "2024-02-01"): 150.0,
            },
        )
        self.assertEqual(len(summary.dispositions), 1)
        self.assertAlmostEqual(summary.dispositions[0].sell_qty, 8.0)
        self.assertEqual(summary.dispositions[0].acquired_date, "2024-01-01")
        self.assertAlmostEqual(summary.dispositions[0].cost_rub, 800.0)
        self.assertAlmostEqual(summary.dispositions[0].proceeds_rub, 1200.0)
        self.assertAlmostEqual(summary.dispositions[0].gain_rub, 400.0)
        self.assertAlmostEqual(summary.net_taxable_base_rub, 400.0)
        self.assertAlmostEqual(summary.estimated_tax_rub, 52.0)

    def test_transfer_preserves_fifo_chain(self):
        txs = [
            _tx(1, "AAPL", 10, "2024-01-01", 1),
            _tx(2, "AAPL", -10, "2024-03-01", 1, tx_type="transfer"),
            _tx(3, "AAPL", 10, "2024-03-01", 2, tx_type="transfer"),
        ]
        sells = [_sell("AAPL", 10, 1500.0, 2, storage_name="storage-2")]
        summary = self._run(
            txs,
            sells,
            taxable_storage_ids={2},
            price_rub_per_unit={("AAPL", "2024-01-01"): 100.0},
        )
        self.assertEqual(len(summary.dispositions), 1)
        self.assertEqual(summary.dispositions[0].acquired_date, "2024-01-01")
        self.assertAlmostEqual(summary.dispositions[0].cost_rub, 1000.0)
        self.assertAlmostEqual(summary.dispositions[0].gain_rub, 500.0)

    def test_fx_revaluation_on_proceeds(self):
        txs = [_tx(1, "AAPL", 10, "2024-01-01", 1)]
        sells = [_sell("AAPL", 10, 1000.0, 1)]
        summary = self._run(
            txs,
            sells,
            price_rub_per_unit={("AAPL", "2024-01-01"): 100.0},
            fx_by_day={
                "2024-01-01": (100.0, 0.92),
                "2024-06-01": (80.0, 0.90),
            },
        )
        self.assertAlmostEqual(summary.dispositions[0].cost_rub, 1000.0)
        self.assertAlmostEqual(summary.dispositions[0].proceeds_rub, 1000.0)

    def test_non_taxable_storage_excluded(self):
        txs = [_tx(1, "AAPL", 10, "2024-01-01", 1)]
        sells = [_sell("AAPL", 5, 700.0, 1)]
        summary = self._run(
            txs,
            sells,
            taxable_storage_ids=set(),
            price_rub_per_unit={("AAPL", "2024-01-01"): 100.0},
        )
        self.assertEqual(summary.dispositions, [])
        self.assertEqual(len(summary.exempt_sells), 1)
        self.assertAlmostEqual(summary.estimated_tax_rub, 0.0)

    def test_net_tax_base_offsets_losses(self):
        txs = [
            _tx(1, "AAPL", 5, "2024-01-01", 1),
            _tx(2, "AAPL", 5, "2024-02-01", 1),
        ]
        sells = [_sell("AAPL", 10, 900.0, 1)]
        summary = self._run(
            txs,
            sells,
            price_rub_per_unit={
                ("AAPL", "2024-01-01"): 100.0,
                ("AAPL", "2024-02-01"): 100.0,
            },
        )
        self.assertEqual(len(summary.dispositions), 2)
        self.assertAlmostEqual(summary.total_gain_rub, 0.0)
        self.assertAlmostEqual(summary.total_loss_rub, 100.0)
        self.assertAlmostEqual(summary.net_taxable_base_rub, 0.0)
        self.assertAlmostEqual(summary.estimated_tax_rub, 0.0)


if __name__ == "__main__":
    unittest.main()
