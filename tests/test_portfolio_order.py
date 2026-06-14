"""Unit tests for shared portfolio ticker ordering."""
import unittest
from types import SimpleNamespace

from app.services.portfolio_order import (
    build_portfolio_ticker_order,
    portfolio_ticker_sort_key,
)


class TestPortfolioOrder(unittest.TestCase):
    def test_sort_by_class_subclass_then_ticker(self):
        subclass_by_id = {
            1: SimpleNamespace(asset_class_id=10, sort_order=1),
            2: SimpleNamespace(asset_class_id=10, sort_order=2),
            3: SimpleNamespace(asset_class_id=20, sort_order=1),
        }
        class_sort_by_id = {10: 1, 20: 2}

        ordered = build_portfolio_ticker_order(
            [("ZZZ", 2), ("AAA", 1), ("BBB", 3)],
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        )
        self.assertEqual(list(ordered.keys()), ["AAA", "ZZZ", "BBB"])

    def test_us_tickers_before_non_us_within_subclass(self):
        subclass_by_id = {1: SimpleNamespace(asset_class_id=10, sort_order=1)}
        class_sort_by_id = {10: 1}

        key_us = portfolio_ticker_sort_key(
            "VOO",
            asset_subclass_id=1,
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        )
        key_moex = portfolio_ticker_sort_key(
            "TSPX",
            asset_subclass_id=1,
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        )
        self.assertLess(key_us, key_moex)


if __name__ == "__main__":
    unittest.main()
