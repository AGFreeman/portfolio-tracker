"""Unit tests for portfolio performance helpers."""
import unittest

from types import SimpleNamespace

from app.services.performance import (
    PerformancePoint,
    compute_net_cash_flow_total_spot,
    compute_period_returns,
    compute_twr_from_daily_values,
    compute_xirr_annualized,
)
from app.services.prices import PriceQuote


class TestTwrMath(unittest.TestCase):
    def test_twr_with_cash_flow_adjustment(self):
        twr = compute_twr_from_daily_values(
            values=[100.0, 160.0],
            cash_flows=[0.0, 50.0],
            include_cash_in_value=True,
        )
        self.assertAlmostEqual(twr, 0.10, places=6)

    def test_twr_deposit_invested_same_day(self):
        twr = compute_twr_from_daily_values(
            values=[7830.0, 442635.0],
            cash_flows=[0.0, 444757.0],
        )
        expected = 442635.0 / (7830.0 + 444757.0) - 1.0
        self.assertAlmostEqual(twr, expected, places=6)

    def test_twr_without_flows(self):
        twr = compute_twr_from_daily_values(
            values=[100.0, 110.0, 121.0],
            cash_flows=[0.0, 0.0, 0.0],
        )
        self.assertAlmostEqual(twr, 0.21, places=6)


class TestCoverageExclusion(unittest.TestCase):
    def test_excluded_tickers_do_not_affect_ratio(self):
        from app.services.performance import _build_as_of_price_index, _holdings_value_as_of_day

        idx = {
            "SBER": _build_as_of_price_index(
                {"2026-05-29": PriceQuote(price=100.0, currency="RUB")}
            ),
            "TSPX2": _build_as_of_price_index({}),
        }
        value, priced_pos, total_pos = _holdings_value_as_of_day(
            {"SBER": 10.0, "TSPX2": 1000.0},
            idx,
            "2026-05-31",
            "RUB",
            90.0,
            0.9,
        )
        self.assertAlmostEqual(value, 1000.0)
        self.assertEqual(priced_pos, 1)
        self.assertEqual(total_pos, 1)


class TestHoldingsValueAsOf(unittest.TestCase):
    def test_weekend_uses_friday_close(self):
        from app.services.performance import _build_as_of_price_index, _holdings_value_as_of_day

        series = {
            "2026-05-29": PriceQuote(price=100.0, currency="RUB"),
        }
        idx = {"SBER": _build_as_of_price_index(series)}
        value, priced_pos, total_pos = _holdings_value_as_of_day(
            {"SBER": 10.0},
            idx,
            "2026-05-31",
            "RUB",
            90.0,
            0.9,
        )
        self.assertAlmostEqual(value, 1000.0)
        self.assertEqual(priced_pos, 1)
        self.assertEqual(total_pos, 1)


class TestHoldingsValuesByTicker(unittest.TestCase):
    def test_per_ticker_values_sum_to_total(self):
        from app.services.performance import (
            _build_as_of_price_index,
            _holdings_values_by_ticker_as_of_day,
        )

        idx = {
            "SBER": _build_as_of_price_index(
                {"2026-05-29": PriceQuote(price=100.0, currency="RUB")}
            ),
            "GAZP": _build_as_of_price_index(
                {"2026-05-29": PriceQuote(price=200.0, currency="RUB")}
            ),
        }
        total, priced_pos, total_pos, ticker_values = _holdings_values_by_ticker_as_of_day(
            {"SBER": 10.0, "GAZP": 5.0},
            idx,
            "2026-05-31",
            "RUB",
            90.0,
            0.9,
        )
        self.assertAlmostEqual(total, 2000.0)
        self.assertAlmostEqual(sum(ticker_values.values()), total)
        self.assertAlmostEqual(ticker_values["SBER"], 1000.0)
        self.assertAlmostEqual(ticker_values["GAZP"], 1000.0)
        self.assertEqual(priced_pos, 2)
        self.assertEqual(total_pos, 2)


class TestSplitTickerValuesByMain(unittest.TestCase):
    def test_splits_main_and_other_assets(self):
        from app.services.performance import split_ticker_values_by_main

        ticker_values = {"SBER": 1000.0, "GAZP": 500.0, "BTC": 300.0}
        main_map = {"SBER": True, "GAZP": True, "BTC": False}
        main_vals, other_sum = split_ticker_values_by_main(ticker_values, main_map)
        self.assertEqual(main_vals, {"SBER": 1000.0, "GAZP": 500.0})
        self.assertAlmostEqual(other_sum, 300.0)


class TestPeriodReturns(unittest.TestCase):
    def test_period_helpers_non_empty(self):
        points = [
            PerformancePoint(
                "2026-01-01",
                100.0,
                0.0,
                0.0,
                None,
                1.0,
            ),
            PerformancePoint(
                "2026-01-15",
                105.0,
                0.0,
                0.05,
                None,
                1.0,
            ),
            PerformancePoint(
                "2026-02-01",
                110.0,
                0.0,
                0.10,
                None,
                1.0,
            ),
        ]
        ret = compute_period_returns(points)
        self.assertIn("1M", ret)
        self.assertIn("YTD", ret)
        self.assertIn("ALL", ret)
        self.assertGreaterEqual(ret["ALL"], 0.0)

    def test_all_period_matches_spot_net_invested(self):
        points = [
            PerformancePoint(
                "2026-01-01",
                100.0,
                80.0,
                0.0,
                None,
                1.0,
            ),
            PerformancePoint(
                "2026-02-01",
                120.0,
                0.0,
                0.0,
                None,
                1.0,
            ),
        ]
        spot_invested = 100.0
        ret = compute_period_returns(points, net_invested=spot_invested)
        expected_all = (120.0 / spot_invested) - 1.0
        self.assertAlmostEqual(ret["ALL"], expected_all, places=6)
        self.assertNotAlmostEqual(
            ret["ALL"],
            (120.0 / 80.0) - 1.0,
            places=6,
        )


class TestNetCashFlowSpot(unittest.TestCase):
    def test_converts_rub_flows_to_display_currency_with_spot_fx(self):
        flows = [
            SimpleNamespace(amount=100_000.0, currency="RUB"),
            SimpleNamespace(amount=-10_000.0, currency="RUB"),
        ]
        usd_total = compute_net_cash_flow_total_spot("USD", 100.0, 0.9, flows=flows)
        eur_total = compute_net_cash_flow_total_spot("EUR", 100.0, 0.9, flows=flows)
        self.assertAlmostEqual(usd_total, 900.0)
        self.assertAlmostEqual(eur_total, 810.0)


class TestXirr(unittest.TestCase):
    def test_xirr_known_case(self):
        xirr = compute_xirr_annualized(
            [
                ("2025-01-01", -1000.0),
                ("2026-01-01", 1100.0),
            ]
        )
        self.assertIsNotNone(xirr)
        self.assertAlmostEqual(float(xirr), 0.10, places=4)

    def test_xirr_invalid_without_sign_change(self):
        xirr = compute_xirr_annualized(
            [
                ("2025-01-01", -100.0),
                ("2025-06-01", -50.0),
            ]
        )
        self.assertIsNone(xirr)

    def test_xirr_non_convergence_guardrail(self):
        xirr = compute_xirr_annualized(
            [
                ("2025-01-01", -1000.0),
                ("2026-01-01", 1100.0),
            ],
            max_iter=0,
        )
        self.assertIsNone(xirr)


if __name__ == "__main__":
    unittest.main()
