import unittest
from datetime import date
from unittest.mock import patch

from app.services.historical_quotes import (
    sync_historical_quotes,
    sync_portfolio_historical_quotes,
)
from app.services.prices import PriceQuote


class TestSyncHistoricalQuotes(unittest.TestCase):
    @patch("app.services.historical_quotes._upsert_live_quote_if_stale", return_value=0)
    @patch("app.services.historical_quotes.upsert_historical_quotes_bulk")
    @patch("app.services.historical_quotes.fetch_historical_quotes")
    @patch("app.services.historical_quotes.get_max_historical_quote_date")
    @patch("app.services.historical_quotes.build_provider_overrides")
    def test_fetches_gap_after_last_cached_day(
        self,
        mock_overrides,
        mock_max_date,
        mock_fetch,
        mock_upsert,
        _mock_live,
    ):
        mock_overrides.return_value = {"GMKN": ("moex_iss", "GMKN")}
        mock_max_date.return_value = "2026-06-03"
        mock_fetch.return_value = {
            "2026-06-04": PriceQuote(price=100.0, currency="RUB"),
            "2026-06-05": PriceQuote(price=101.0, currency="RUB"),
        }

        inserted = sync_historical_quotes("GMKN", "2022-01-01", "2026-06-05")

        self.assertEqual(inserted, 2)
        mock_fetch.assert_called_once_with(
            ticker="GMKN",
            date_from="2026-06-04",
            date_to="2026-06-05",
            provider_override="moex_iss",
            provider_symbol_override="GMKN",
        )
        mock_upsert.assert_called_once_with(
            [
                ("GMKN", "2026-06-04", 100.0, "RUB"),
                ("GMKN", "2026-06-05", 101.0, "RUB"),
            ]
        )

    @patch("app.services.historical_quotes._upsert_live_quote_if_stale", return_value=0)
    @patch("app.services.historical_quotes.fetch_historical_quotes")
    @patch("app.services.historical_quotes.get_max_historical_quote_date")
    def test_skips_when_cache_is_current(self, mock_max_date, mock_fetch, _mock_live):
        mock_max_date.return_value = date.today().isoformat()

        inserted = sync_historical_quotes("GMKN", "2022-01-01")

        self.assertEqual(inserted, 0)
        mock_fetch.assert_not_called()

    @patch("app.services.historical_quotes.fetch_historical_quotes")
    @patch("app.services.historical_quotes.get_max_historical_quote_date")
    def test_skips_coverage_excluded_tickers(self, mock_max_date, mock_fetch):
        mock_max_date.return_value = None

        inserted = sync_historical_quotes("TECH2", "2022-01-01")

        self.assertEqual(inserted, 0)
        mock_fetch.assert_not_called()

    @patch("app.services.historical_quotes.upsert_historical_quotes_bulk")
    @patch("app.services.historical_quotes.fetch_price_quote")
    @patch("app.services.historical_quotes.fetch_historical_quotes")
    @patch("app.services.historical_quotes.get_max_historical_quote_date")
    @patch("app.services.historical_quotes.build_provider_overrides")
    def test_falls_back_to_live_quote_when_history_missing(
        self,
        mock_overrides,
        mock_max_date,
        mock_fetch,
        mock_live,
        mock_upsert,
    ):
        mock_overrides.return_value = {"TECH": ("tbank", "TECH")}
        mock_max_date.side_effect = ["2024-08-16", "2024-08-16"]
        mock_fetch.return_value = {}
        mock_live.return_value = PriceQuote(price=8.58, currency="RUB")

        inserted = sync_historical_quotes("TECH", "2022-01-01", "2026-07-21")

        self.assertEqual(inserted, 1)
        mock_upsert.assert_called_once_with(
            [("TECH", "2026-07-21", 8.58, "RUB")]
        )


class TestSyncPortfolioHistoricalQuotes(unittest.TestCase):
    @patch("app.services.fx.sync_historical_fx", return_value=5)
    @patch("app.services.historical_quotes.sync_historical_quotes", return_value=3)
    @patch("app.services.performance._build_active_intervals_by_ticker")
    @patch("app.services.performance._load_daily_transactions")
    @patch("app.db.list_positions_by_ticker")
    def test_syncs_all_active_holdings(
        self,
        mock_positions,
        mock_load_tx,
        mock_intervals,
        mock_sync,
        mock_fx,
    ):
        class _Pos:
            def __init__(self, ticker, amount):
                self.ticker = ticker
                self.amount = amount

        mock_positions.return_value = [
            _Pos("GMKN", 10),
            _Pos("VOO", 5),
            _Pos("TECH2", 1),
            _Pos("ZERO", 0),
        ]
        mock_load_tx.return_value = ({}, "2022-01-01", "2026-07-01")
        mock_intervals.return_value = {
            "GMKN": [("2022-01-01", "2026-07-21")],
            "VOO": [("2023-01-01", "2026-07-21")],
        }

        result = sync_portfolio_historical_quotes(date_to="2026-07-21")

        synced = {call.args[0] for call in mock_sync.call_args_list}
        self.assertIn("GMKN", synced)
        self.assertIn("VOO", synced)
        self.assertIn("TECH2", synced)
        self.assertNotIn("ZERO", synced)
        self.assertEqual(result["GMKN"], 3)
        self.assertEqual(result["__FX__"], 5)
        mock_sync.assert_any_call("GMKN", "2022-01-01", "2026-07-21")
        mock_sync.assert_any_call("VOO", "2023-01-01", "2026-07-21")
        mock_fx.assert_called_once_with(date_from="2022-01-01", date_to="2026-07-21")


if __name__ == "__main__":
    unittest.main()
