"""Unit tests for portfolio performance helpers."""
import unittest

from app.services.performance import (
    PerformancePoint,
    _carry_forward_prices,
    compute_period_returns,
    compute_twr_from_daily_values,
    compute_xirr_annualized,
)
from app.services.prices import PriceQuote


class TestTwrMath(unittest.TestCase):
    def test_twr_with_cash_flow_adjustment(self):
        # Day0: 100, Day1: 160 with +50 flow => net market move +10%
        twr = compute_twr_from_daily_values(
            values=[100.0, 160.0],
            cash_flows=[0.0, 50.0],
        )
        self.assertAlmostEqual(twr, 0.10, places=6)

    def test_twr_without_flows(self):
        twr = compute_twr_from_daily_values(
            values=[100.0, 110.0, 121.0],
            cash_flows=[0.0, 0.0, 0.0],
        )
        self.assertAlmostEqual(twr, 0.21, places=6)


class TestCarryForward(unittest.TestCase):
    def test_carry_forward_last_known_price(self):
        series = {
            "2026-01-01": PriceQuote(price=100.0, currency="USD"),
            "2026-01-03": PriceQuote(price=103.0, currency="USD"),
        }
        days = ["2026-01-01", "2026-01-02", "2026-01-03"]
        out = _carry_forward_prices(series, days)
        self.assertIn("2026-01-02", out)
        self.assertAlmostEqual(float(out["2026-01-02"].price or 0.0), 100.0)
        self.assertAlmostEqual(float(out["2026-01-03"].price or 0.0), 103.0)


class TestPeriodReturns(unittest.TestCase):
    def test_period_helpers_non_empty(self):
        points = [
            PerformancePoint("2026-01-01", 100.0, 0.0, 0.0, 1.0),
            PerformancePoint("2026-01-15", 105.0, 0.0, 0.05, 1.0),
            PerformancePoint("2026-02-01", 110.0, 0.0, 0.10, 1.0),
        ]
        ret = compute_period_returns(points)
        self.assertIn("1M", ret)
        self.assertIn("YTD", ret)
        self.assertIn("ALL", ret)
        self.assertGreaterEqual(ret["ALL"], 0.0)


class TestXirr(unittest.TestCase):
    def test_xirr_known_case(self):
        # -1000 today, +1100 in 1 year -> 10% annualized.
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
