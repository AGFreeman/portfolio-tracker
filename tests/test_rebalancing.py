"""Unit tests for buy-only rebalancing math."""
import unittest

from app.services.rebalancing import (
    TickerPositionValue,
    aggregate_values_by_subclass,
    allocate_cash_to_subclasses,
    compute_rebalance_plan,
    normalize_subclass_weights,
    split_subclass_budget_to_tickers,
    units_and_implied_spend,
)


class TestNormalizeWeights(unittest.TestCase):
    def test_sum_to_one(self):
        w, raw, norm = normalize_subclass_weights({1: 50.0, 2: 50.0})
        self.assertAlmostEqual(sum(w.values()), 1.0)
        self.assertAlmostEqual(raw, 100.0)
        self.assertFalse(norm)

    def test_renormalize_when_not_100(self):
        w, raw, norm = normalize_subclass_weights({1: 40.0, 2: 40.0})
        self.assertAlmostEqual(sum(w.values()), 1.0)
        self.assertAlmostEqual(raw, 80.0)
        self.assertTrue(norm)
        self.assertAlmostEqual(w[1], 0.5)


class TestAllocateCash(unittest.TestCase):
    def test_equal_weights_half_half(self):
        # S=200, V=100, T=300, w=0.5/0.5 -> ideal 150 each
        # v1=150 v2=50 -> gap1=0 gap2=100 -> all V to subclass 2
        v = {1: 150.0, 2: 50.0}
        w = {1: 0.5, 2: 0.5}
        bud, S, T, g = allocate_cash_to_subclasses(v, w, 100.0)
        self.assertAlmostEqual(S, 200.0)
        self.assertAlmostEqual(T, 300.0)
        self.assertAlmostEqual(bud.get(2, 0), 100.0)
        self.assertAlmostEqual(bud.get(1, 0), 0.0)

    def test_proportional_gaps(self):
        # T=300, w 50/50 -> ideal 150/150. v 100/100 -> gaps 50/50 -> split V=100 -> 50/50
        v = {1: 100.0, 2: 100.0}
        w = {1: 0.5, 2: 0.5}
        bud, _, _, _ = allocate_cash_to_subclasses(v, w, 100.0)
        self.assertAlmostEqual(bud[1], 50.0)
        self.assertAlmostEqual(bud[2], 50.0)


class TestSplitEqual(unittest.TestCase):
    def test_split(self):
        d = split_subclass_budget_to_tickers(100.0, [("A", 60.0), ("B", 40.0)])
        self.assertAlmostEqual(d["A"], 50.0)
        self.assertAlmostEqual(d["B"], 50.0)


class TestAggregate(unittest.TestCase):
    def test_by_sub(self):
        rows = [
            TickerPositionValue("A", 1, 30.0, 1.0),
            TickerPositionValue("B", 1, 70.0, 1.0),
            TickerPositionValue("C", 2, None, None),
        ]
        self.assertEqual(aggregate_values_by_subclass(rows), {1: 100.0})


class TestUnitsRounding(unittest.TestCase):
    def test_stock_floor(self):
        u, spend = units_and_implied_spend("VOO", 99.0, 50.0)
        self.assertEqual(u, 1.0)
        self.assertAlmostEqual(spend, 50.0)

    def test_crypto_fractional(self):
        u, spend = units_and_implied_spend("BTC", 100.0, 50000.0)
        self.assertGreater(u, 0)
        self.assertAlmostEqual(spend, u * 50000.0)


class TestComputePlan(unittest.TestCase):
    def test_end_to_end_two_tickers_one_sub(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        targets = {1: 100.0}
        names = {1: "Sub1"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0)
        self.assertAlmostEqual(plan.S, 100.0)
        self.assertAlmostEqual(plan.T, 200.0)
        self.assertEqual(len(plan.suggested_buys), 2)
        by_t = {b.ticker: b for b in plan.suggested_buys}
        self.assertAlmostEqual(by_t["AAA"].spend_allocated, 50.0, places=5)
        self.assertAlmostEqual(by_t["BBB"].spend_allocated, 50.0, places=5)

    def test_blocked_ticker_is_excluded(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        targets = {1: 100.0}
        names = {1: "Sub1"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0, blocked_tickers={"AAA"})
        self.assertEqual(len(plan.suggested_buys), 1)
        self.assertEqual(plan.suggested_buys[0].ticker, "BBB")

    def test_unallocated_empty_subclass(self):
        rows = [
            TickerPositionValue("X", 1, 100.0, 1.0),
        ]
        targets = {1: 50.0, 2: 50.0}
        names = {1: "A", 2: "B"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0)
        # subclass 2 has budget but no priced holdings
        self.assertTrue(any(u.subclass_id == 2 for u in plan.unallocated))


if __name__ == "__main__":
    unittest.main()
