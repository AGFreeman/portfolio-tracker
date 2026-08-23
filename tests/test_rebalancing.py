"""Unit tests for buy-only rebalancing math."""
import sqlite3
import unittest
from collections import defaultdict
from pathlib import Path

from app.services.rebalancing import (
    StoragePositionValue,
    TickerPositionValue,
    RebalanceConstraints,
    aggregate_values_by_subclass,
    allocate_cash_to_subclasses,
    build_rebalance_diagnostics,
    compute_constrained_rebalance_plan,
    compute_deviation_l1,
    compute_ideal_rebalance_plan,
    compute_ideal_portfolio_plan,
    compute_ideal_ticker_sells,
    compute_rebalance_plan,
    compute_suggested_sells,
    compute_ticker_target_values,
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


class TestTickerTargetValues(unittest.TestCase):
    def test_equal_split_within_subclass(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        s, targets = compute_ticker_target_values(rows, {1: 100.0})
        self.assertAlmostEqual(s, 100.0)
        self.assertAlmostEqual(targets["AAA"], 50.0)
        self.assertAlmostEqual(targets["BBB"], 50.0)

    def test_blocked_ticker_target_is_current_value(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        _, targets = compute_ticker_target_values(
            rows, {1: 100.0}, blocked_tickers={"AAA"}
        )
        self.assertAlmostEqual(targets["AAA"], 60.0)
        self.assertAlmostEqual(targets["BBB"], 40.0)

    def test_blocked_reserve_then_equal_split(self):
        rows = [
            TickerPositionValue("AAA", 1, 30.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
            TickerPositionValue("CCC", 1, 50.0, 20.0),
        ]
        _, targets = compute_ticker_target_values(
            rows, {1: 100.0}, blocked_tickers={"AAA"}
        )
        self.assertAlmostEqual(targets["AAA"], 30.0)
        self.assertAlmostEqual(targets["BBB"], 35.0)
        self.assertAlmostEqual(targets["CCC"], 35.0)

    def test_portfolio_total_override_scales_targets(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        _, targets_at_s = compute_ticker_target_values(rows, {1: 100.0})
        _, targets_at_t = compute_ticker_target_values(
            rows, {1: 100.0}, portfolio_total=200.0
        )
        self.assertAlmostEqual(targets_at_s["AAA"], 50.0)
        self.assertAlmostEqual(targets_at_s["BBB"], 50.0)
        self.assertAlmostEqual(targets_at_t["AAA"], 100.0)
        self.assertAlmostEqual(targets_at_t["BBB"], 100.0)


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
        # Цели по 100 каждый; пробелы 40 и 60 → бюджет 100 распределяется 40/60
        self.assertAlmostEqual(by_t["AAA"].spend_allocated, 40.0, places=5)
        self.assertAlmostEqual(by_t["BBB"].spend_allocated, 60.0, places=5)

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
        self.assertAlmostEqual(plan.suggested_buys[0].spend_allocated, 100.0, places=5)

    def test_blocked_reserve_splits_buy_budget(self):
        rows = [
            TickerPositionValue("AAA", 1, 30.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
            TickerPositionValue("CCC", 1, 50.0, 20.0),
        ]
        targets = {1: 100.0}
        names = {1: "Sub1"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0, blocked_tickers={"AAA"})
        by_t = {b.ticker: b for b in plan.suggested_buys}
        self.assertNotIn("AAA", by_t)
        self.assertAlmostEqual(by_t["BBB"].spend_allocated, 80.0, places=5)
        self.assertAlmostEqual(by_t["CCC"].spend_allocated, 20.0, places=5)

    def test_unallocated_empty_subclass(self):
        rows = [
            TickerPositionValue("X", 1, 100.0, 1.0),
        ]
        targets = {1: 50.0, 2: 50.0}
        names = {1: "A", 2: "B"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0)
        # subclass 2 has no holdings; cash is redeployed to subclass 1 but X is already at target
        self.assertFalse(any(u.subclass_id == 2 for u in plan.unallocated))
        self.assertEqual(plan.total_implied_spend, 0.0)

    def test_minimize_unsettled_cash_with_extra_lots(self):
        rows = [
            TickerPositionValue("AAA", 1, 50.0, 60.0),
            TickerPositionValue("BBB", 1, 50.0, 40.0),
        ]
        targets = {1: 100.0}
        names = {1: "Sub1"}
        plan = compute_rebalance_plan(rows, targets, names, 100.0)
        # Equal split gives 50/50 -> initial implied 40 and large residual.
        # Optimizer should use residual to buy extra cheapest lot(s), reducing unsettled cash.
        self.assertLessEqual(plan.residual_vs_V, 20.0)


class TestSellableTickers(unittest.TestCase):
    def test_non_sellable_overweight_no_sells(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            unrealized_pnl_pct_by_ticker={"AAA": 0.20},
        )
        self.assertEqual(plan.suggested_sells, [])

    def test_sellable_overweight_generates_sell(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15},
        )
        self.assertEqual(len(plan.suggested_sells), 1)
        sell = plan.suggested_sells[0]
        self.assertEqual(sell.ticker, "AAA")
        self.assertAlmostEqual(sell.implied_proceeds, 30.0, places=5)

    def test_sell_blocked_when_pnl_below_10pct(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.05},
        )
        self.assertEqual(plan.suggested_sells, [])
        self.assertIn("AAA", plan.skipped_sells_low_pnl)

    def test_sell_proceeds_deploy_across_underweight_tickers(self):
        rows = [
            TickerPositionValue("AAA", 1, 70.0, 10.0),
            TickerPositionValue("BBB", 1, 15.0, 15.0),
            TickerPositionValue("CCC", 1, 15.0, 15.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12, "CCC": 0.12},
        )
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertTrue({"BBB", "CCC"} & buy_tickers)
        sell_spend = sum(b.implied_spend for b in plan.suggested_buys)
        self.assertGreaterEqual(sell_spend, plan.total_sell_proceeds - 15.0)

    def test_sell_proceeds_fund_buys_with_zero_V(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        self.assertAlmostEqual(plan.total_sell_proceeds, 30.0, places=5)
        self.assertAlmostEqual(plan.V_effective, 0.0, places=5)
        self.assertLessEqual(plan.residual_sell_proceeds, 10.0)
        sell_spend = sum(b.implied_spend for b in plan.suggested_buys)
        self.assertGreaterEqual(sell_spend, 20.0)
        by_t = {b.ticker: b for b in plan.suggested_buys}
        self.assertIn("BBB", by_t)
        self.assertAlmostEqual(by_t["BBB"].spend_allocated, 20.0, places=5)

    def test_blocked_and_sellable_independent(self):
        """Sellable overweight AAA sells; sold ticker is not rebought (BBB receives proceeds)."""
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.20, "BBB": 0.15},
        )
        self.assertEqual(len(plan.suggested_sells), 1)
        self.assertEqual(plan.suggested_sells[0].ticker, "AAA")
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertNotIn("AAA", buy_tickers)
        self.assertIn("BBB", buy_tickers)

    def test_blocked_ticker_pinned_not_sold_even_if_sellable(self):
        """Blocked ticker target equals current value — no trim even when sellable."""
        rows = [
            TickerPositionValue("AAA", 1, 70.0, 10.0),
            TickerPositionValue("BBB", 1, 30.0, 10.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            blocked_tickers={"AAA"},
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.20, "BBB": 0.15},
        )
        self.assertEqual(plan.suggested_sells, [])
        self.assertNotIn("AAA", {b.ticker for b in plan.suggested_buys})

    def test_proceeds_never_rebuy_sold_ticker(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        self.assertTrue(plan.suggested_sells)
        sold = {str(s.ticker).upper() for s in plan.suggested_sells}
        bought = {str(b.ticker).upper() for b in plan.suggested_buys}
        self.assertFalse(sold & bought)


    def test_sell_proceeds_reinvested_at_same_storage(self):
        ticker_rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "Interactive Brokers", 1, 80.0, 10.0),
            StoragePositionValue("BBB", 1, "Interactive Brokers", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA", "BBB"}},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        self.assertEqual(len(plan.suggested_sells), 1)
        self.assertEqual(plan.suggested_sells[0].storage_id, 1)
        self.assertTrue(plan.suggested_buys)
        self.assertTrue(
            all(b.storage_id == 1 for b in plan.suggested_buys),
            "sell proceeds must fund buys at the same storage only",
        )

    def test_sell_proceeds_do_not_buy_tickers_only_at_other_storage(self):
        ticker_rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "Interactive Brokers", 1, 80.0, 10.0),
            StoragePositionValue("BBB", 2, "Т-Банк", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"BBB"}},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertNotIn("BBB", buy_tickers)

    def test_withdraw_enabled_routes_proceeds_to_deposit_storages(self):
        ticker_rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "Interactive Brokers", 1, 80.0, 10.0),
            StoragePositionValue("BBB", 2, "Т-Банк", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"BBB"}},
            deposit_storage_ids={2},
            withdraw_storage_ids={1},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertIn("BBB", buy_tickers)
        bbb_buys = [b for b in plan.suggested_buys if b.ticker == "BBB"]
        self.assertTrue(
            bbb_buys and all(b.storage_id == 2 for b in bbb_buys),
            "cross-storage proceeds should fund buys at deposit storage",
        )

    def test_external_V_only_at_deposit_storages(self):
        rows = [
            TickerPositionValue("AAA", 1, 50.0, 10.0),
            TickerPositionValue("BBB", 1, 50.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            100.0,
            storage_rows=[
                StoragePositionValue("AAA", 1, "Interactive Brokers", 1, 50.0, 10.0),
                StoragePositionValue("BBB", 2, "Т-Банк", 1, 50.0, 20.0),
            ],
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"BBB"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
        )
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertIn("BBB", buy_tickers)
        self.assertNotIn("AAA", buy_tickers)

    def test_sells_independent_of_external_V(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        kwargs = dict(
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        plan0 = compute_rebalance_plan(rows, {1: 100.0}, {1: "Sub1"}, 0.0, **kwargs)
        plan100 = compute_rebalance_plan(rows, {1: 100.0}, {1: "Sub1"}, 100.0, **kwargs)
        self.assertEqual(len(plan0.suggested_sells), 1)
        self.assertEqual(len(plan100.suggested_sells), 1)
        self.assertAlmostEqual(
            plan0.suggested_sells[0].implied_proceeds,
            plan100.suggested_sells[0].implied_proceeds,
            places=5,
        )

    def test_external_V_buys_after_sell_proceeds(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            100.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        self.assertTrue(plan.suggested_buys)
        self.assertAlmostEqual(plan.V_effective, 100.0, places=5)
        self.assertGreater(sum(b.implied_spend for b in plan.suggested_buys), 0.0)
        self.assertLess(plan.residual_vs_V, plan.V_effective)


class TestDeviationOptimizer(unittest.TestCase):
    def test_compute_deviation_l1(self):
        values = {"AAA": 80.0, "BBB": 30.0}
        targets = {"AAA": 50.0, "BBB": 50.0}
        self.assertAlmostEqual(compute_deviation_l1(values, targets), 50.0)

    def test_deviation_after_not_worse_than_before(self):
        rows = [
            TickerPositionValue("AAA", 1, 60.0, 10.0),
            TickerPositionValue("BBB", 1, 40.0, 20.0),
        ]
        plan = compute_rebalance_plan(rows, {1: 100.0}, {1: "Sub1"}, 100.0)
        self.assertLessEqual(plan.deviation_l1_after, plan.deviation_l1_before + 1e-6)
        self.assertGreaterEqual(plan.optimizer_iterations, 1)

    def test_multi_storage_deviation_uses_ticker_aggregate(self):
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 40.0, 10.0),
            StoragePositionValue("AAA", 2, "T-Bank", 1, 40.0, 10.0),
            StoragePositionValue("BBB", 1, "IB", 1, 20.0, 20.0),
        ]
        ticker_rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA", "BBB"}, 2: {"AAA"}},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        self.assertLessEqual(plan.deviation_l1_after, plan.deviation_l1_before + 1e-6)
        sold = {s.ticker for s in plan.suggested_sells}
        bought = {b.ticker for b in plan.suggested_buys}
        self.assertFalse(sold & bought)

    def test_sell_skipped_when_proceeds_cannot_deploy(self):
        """Overweight AAA with only blocked BBB in subclass — no sell when targets respect blocked."""
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            blocked_tickers={"BBB"},
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15},
        )
        self.assertFalse(plan.ideal_sells)
        self.assertEqual(plan.suggested_sells, [])
        self.assertEqual(plan.suggested_buys, [])

    def test_no_sell_without_buy_at_same_storage(self):
        """EMXC-like: overweight at IB, underweight elsewhere — phase 2 gap without withdraw."""
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 80.0, 50.0),
            TickerPositionValue("VOO", 1, 20.0, 400.0),
        ]
        storage_rows = [
            StoragePositionValue("EMXC", 1, "Interactive Brokers", 1, 80.0, 50.0),
            StoragePositionValue("VOO", 2, "Т-Банк", 1, 20.0, 400.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_positions={("EMXC", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"EMXC"}, 2: {"VOO"}},
            deposit_storage_ids={1, 2},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"EMXC": 0.15, "VOO": 0.12},
        )
        self.assertEqual(plan.suggested_sells, [])
        self.assertEqual(plan.suggested_buys, [])

    def test_sold_ticker_not_underweight_after_rebalance(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        if not plan.suggested_sells:
            return
        sells = {s.ticker.upper() for s in plan.suggested_sells}
        self.assertTrue(plan.suggested_buys)
        post_rows = list(rows)
        for s in plan.suggested_sells:
            t_up = s.ticker.upper()
            post_rows = [
                TickerPositionValue(
                    r.ticker,
                    r.asset_subclass_id,
                    max(0.0, float(r.value_display or 0) - s.implied_proceeds)
                    if r.ticker.upper() == t_up
                    else float(r.value_display or 0),
                    r.price_display,
                )
                for r in post_rows
            ]
        for b in plan.suggested_buys:
            t_up = b.ticker.upper()
            found = any(r.ticker.upper() == t_up for r in post_rows)
            if found:
                post_rows = [
                    TickerPositionValue(
                        r.ticker,
                        r.asset_subclass_id,
                        float(r.value_display or 0) + b.implied_spend
                        if r.ticker.upper() == t_up
                        else float(r.value_display or 0),
                        r.price_display,
                    )
                    for r in post_rows
                ]
            else:
                post_rows.append(
                    TickerPositionValue(
                        b.ticker,
                        b.asset_subclass_id,
                        b.implied_spend,
                        b.price_display,
                    )
                )
        s_post = sum(float(r.value_display or 0) for r in post_rows)
        _, targets = compute_ticker_target_values(
            post_rows, {1: 100.0}, portfolio_total=s_post
        )
        for t in sells:
            val = next(
                (float(r.value_display or 0) for r in post_rows if r.ticker.upper() == t),
                0.0,
            )
            self.assertGreaterEqual(val, float(targets.get(t, 0.0)) - 1e-6)


class TestRebalanceDiagnostics(unittest.TestCase):
    def test_lists_underweight_and_withdraw_hint_when_cross_broker(self):
        storage_rows = [
            StoragePositionValue("EMXC", 3, "Freedom Finance", 1, 3000.0, 50.0),
            StoragePositionValue("VOO", 1, "Interactive Brokers", 1, 500.0, 400.0),
        ]
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 3000.0, 50.0),
            TickerPositionValue("VOO", 1, 500.0, 400.0),
        ]
        constraints = RebalanceConstraints(
            unblocked_tickers_by_storage={3: {"EMXC"}, 1: {"VOO"}},
            deposit_storage_ids={1, 3},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"EMXC": 0.15, "VOO": 0.12},
        )
        notes = build_rebalance_diagnostics(
            storage_rows,
            ticker_rows,
            {1: 100.0},
            constraints,
            sellable_positions={("EMXC", 3)},
            blocked=set(),
            executed_sells=[],
        )
        joined = " ".join(notes)
        self.assertIn("Недовесные инструменты", joined)
        self.assertIn("VOO", joined)
        self.assertIn("Вывод денег", joined)

    def test_no_withdraw_hint_when_same_storage_deploy_works(self):
        storage_rows = [
            StoragePositionValue("EMXC", 3, "Freedom Finance", 1, 3_000_000.0, 5000.0),
            StoragePositionValue("VOO", 3, "Freedom Finance", 1, 500_000.0, 40000.0),
            StoragePositionValue("VOO", 1, "IB", 1, 500_000.0, 40000.0),
        ]
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 3_000_000.0, 5000.0),
            TickerPositionValue("VOO", 1, 1_000_000.0, 40000.0),
        ]
        constraints = RebalanceConstraints(
            unblocked_tickers_by_storage={3: {"EMXC", "VOO"}, 1: {"VOO"}},
            deposit_storage_ids={1, 3},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"EMXC": 0.5, "VOO": 0.2},
        )
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "S"},
            0.0,
            sellable_positions={("EMXC", 3)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage=constraints.unblocked_tickers_by_storage,
            deposit_storage_ids=constraints.deposit_storage_ids,
            unrealized_pnl_pct_by_ticker=constraints.unrealized_pnl_pct_by_ticker,
        )
        self.assertTrue(plan.suggested_sells)
        joined = " ".join(plan.rebalance_diagnostics)
        self.assertNotIn("Вывод денег", joined)

    def test_blocked_at_freedom_explains_voo(self):
        storage_rows = [
            StoragePositionValue("EMXC", 3, "Freedom Finance", 1, 3_000_000.0, 5000.0),
            StoragePositionValue("VOO", 3, "Freedom Finance", 1, 500_000.0, 40000.0),
            StoragePositionValue("VOO", 1, "IB", 1, 500_000.0, 40000.0),
        ]
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 3_000_000.0, 5000.0),
            TickerPositionValue("VOO", 1, 1_000_000.0, 40000.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "S"},
            0.0,
            sellable_positions={("EMXC", 3)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={3: {"EMXC"}, 1: {"VOO"}},
            deposit_storage_ids={1, 3},
            unrealized_pnl_pct_by_ticker={"EMXC": 0.5, "VOO": 0.2},
        )
        joined = " ".join(plan.rebalance_diagnostics)
        self.assertIn("заблокирован", joined.lower())
        self.assertIn("VOO", joined)

    def test_diagnose_prior_sell_blocks_rebuy_on_freedom(self):
        from app.services import rebalancing as rb

        storage_rows = [
            StoragePositionValue("EMXC", 3, "Freedom Finance", 1, 9_000_000.0, 5000.0),
            StoragePositionValue("VOO", 3, "Freedom Finance", 1, 6_000_000.0, 40000.0),
        ]
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 9_000_000.0, 5000.0),
            TickerPositionValue("VOO", 1, 6_000_000.0, 40000.0),
        ]
        w, _, _ = normalize_subclass_weights({1: 100.0})
        constraints = RebalanceConstraints(
            unblocked_tickers_by_storage={3: {"EMXC", "VOO"}},
            deposit_storage_ids={3},
        )
        sells, _ = compute_suggested_sells(
            ticker_rows,
            storage_rows,
            {1: 100.0},
            {1: "S"},
            0.0,
            sellable_positions={("EMXC", 3)},
            unrealized_pnl_pct_by_ticker={"EMXC": 0.5, "VOO": 0.2},
        )
        msg = rb._diagnose_undeployable_sell(
            storage_rows,
            sells[0],
            w,
            {1: "S"},
            set(),
            constraints,
            {1: 100.0},
            0.0,
            {"VOO"},
        )
        self.assertIn("продавались", msg)
        self.assertIn("VOO", msg)
        self.assertIn("Freedom Finance", msg)


class TestTwoPhaseRebalance(unittest.TestCase):
    def test_freedom_emxc_ideal_full_actual_gap(self):
        """EMXC sell at Freedom, VOO blocked there — ideal plan full, actual shows gap."""
        storage_rows = [
            StoragePositionValue("EMXC", 3, "Freedom Finance", 1, 3_000_000.0, 5000.0),
            StoragePositionValue("VOO", 3, "Freedom Finance", 1, 500_000.0, 40000.0),
            StoragePositionValue("VOO", 1, "IB", 1, 500_000.0, 40000.0),
        ]
        ticker_rows = [
            TickerPositionValue("EMXC", 1, 3_000_000.0, 5000.0),
            TickerPositionValue("VOO", 1, 1_000_000.0, 40000.0),
        ]
        plan = compute_rebalance_plan(
            ticker_rows,
            {1: 100.0},
            {1: "S"},
            0.0,
            sellable_positions={("EMXC", 3)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={3: {"EMXC"}, 1: {"VOO"}},
            deposit_storage_ids={1, 3},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"EMXC": 0.5, "VOO": 0.2},
        )
        self.assertTrue(plan.ideal_sells)
        self.assertTrue(plan.ideal_buys)
        ideal_sell = sum(s.amount for s in plan.ideal_sells)
        ideal_buy = sum(b.amount for b in plan.ideal_buys)
        self.assertGreater(ideal_sell, 100_000.0)
        self.assertGreater(ideal_buy, 100_000.0)
        joined = " ".join(plan.constraint_gaps + plan.rebalance_diagnostics).lower()
        self.assertTrue(
            plan.total_sell_proceeds < ideal_sell - 1000.0
            or plan.constraint_gaps
            or "неразмещённая выручка" in joined
        )
        self.assertIn("заблокирован", joined)


class TestIdealRebalancePlan(unittest.TestCase):
    def test_external_V_generates_buys_without_storage(self):
        rows = [
            TickerPositionValue("AAA", 1, 50.0, 10.0),
            TickerPositionValue("BBB", 1, 50.0, 20.0),
        ]
        plan = compute_ideal_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            100.0,
        )
        self.assertTrue(plan.suggested_buys)
        for buy in plan.suggested_buys:
            self.assertIsNone(buy.storage_id)
            self.assertIsNone(buy.storage_name)
        self.assertEqual(plan.constraint_gaps, [])
        self.assertEqual(plan.rebalance_diagnostics, [])

    def test_blocked_ticker_not_bought(self):
        rows = [
            TickerPositionValue("AAA", 1, 30.0, 10.0),
            TickerPositionValue("BBB", 1, 70.0, 10.0),
        ]
        plan = compute_ideal_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            100.0,
            blocked_tickers={"AAA"},
        )
        buy_tickers = {b.ticker for b in plan.suggested_buys}
        self.assertNotIn("AAA", buy_tickers)
        _, targets = compute_ticker_target_values(
            rows, {1: 100.0}, blocked_tickers={"AAA"}, portfolio_total=200.0
        )
        self.assertAlmostEqual(targets["AAA"], 30.0, places=5)

    def test_sellable_overweight_generates_ticker_level_sell(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        plan = compute_ideal_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15},
        )
        self.assertEqual(len(plan.suggested_sells), 1)
        sell = plan.suggested_sells[0]
        self.assertEqual(sell.ticker, "AAA")
        self.assertEqual(sell.storage_id, 0)
        self.assertAlmostEqual(sell.implied_proceeds, 30.0, places=5)

    def test_suggested_matches_ideal_and_no_constraint_gaps(self):
        rows = [
            TickerPositionValue("AAA", 1, 70.0, 10.0),
            TickerPositionValue("BBB", 1, 30.0, 10.0),
        ]
        plan = compute_ideal_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            50.0,
            sellable_tickers={"AAA"},
            unrealized_pnl_pct_by_ticker={"AAA": 0.20, "BBB": 0.15},
        )
        self.assertEqual(plan.constraint_gaps, [])
        self.assertEqual(
            {s.ticker for s in plan.suggested_sells},
            {s.ticker for s in plan.ideal_sells},
        )
        self.assertEqual(
            {b.ticker for b in plan.suggested_buys},
            {b.ticker for b in plan.ideal_buys},
        )
        self.assertAlmostEqual(
            sum(s.implied_proceeds for s in plan.suggested_sells),
            sum(s.amount for s in plan.ideal_sells),
            places=5,
        )
        self.assertAlmostEqual(
            sum(b.implied_spend for b in plan.suggested_buys),
            sum(b.amount for b in plan.ideal_buys),
            places=5,
        )

    def test_ideal_sells_respect_blocked_ticker_targets(self):
        """Blocked tickers shift targets; sells must use the same blocked set."""
        rows = [
            TickerPositionValue("OVER", 1, 400.0, 10.0),
            TickerPositionValue("BLOCK", 1, 100.0, 10.0),
            TickerPositionValue("UNDER", 1, 100.0, 10.0),
        ]
        blocked = {"BLOCK"}
        sells, _ = compute_ideal_ticker_sells(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            {"OVER"},
            {"OVER": 0.20},
            blocked,
            0.0,
        )
        self.assertEqual(len(sells), 1)
        self.assertEqual(sells[0].ticker, "OVER")
        w, _, _ = normalize_subclass_weights({1: 100.0})
        constraints = RebalanceConstraints(
            blocked_tickers=blocked,
            unrealized_pnl_pct_by_ticker={"OVER": 0.20},
        )
        ideal = compute_ideal_portfolio_plan(
            rows, w, {1: "Sub1"}, {1: 100.0}, 0.0, blocked, constraints,
            sellable_tickers={"OVER"},
        )
        self.assertTrue(ideal.sells)
        self.assertLess(ideal.deviation_l1_after, ideal.deviation_l1_before)


class TestConstrainedRebalancePlan(unittest.TestCase):
    def test_redeploy_when_target_blocked_everywhere(self):
        """Underweight ticker buyable nowhere -> cash redeploys to an allowed ticker."""
        rows = [
            TickerPositionValue("AAA", 1, 800.0, 10.0),
            TickerPositionValue("BBB", 1, 100.0, 10.0),
            TickerPositionValue("CCC", 1, 100.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "S1", 1, 800.0, 10.0),
            StoragePositionValue("BBB", 1, "S1", 1, 100.0, 10.0),
            StoragePositionValue("CCC", 1, "S1", 1, 100.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"CCC"}},
            deposit_storage_ids={1},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"AAA": 0.20},
        )
        buy_tickers = {b.ticker.upper() for b in plan.suggested_buys}
        self.assertEqual(buy_tickers, {"CCC"})
        self.assertNotIn("BBB", buy_tickers)
        sold = {s.ticker.upper() for s in plan.suggested_sells}
        self.assertFalse(sold & buy_tickers)
        self.assertLess(plan.deviation_l1_after, plan.deviation_l1_before)

    def test_withdraw_routes_proceeds_to_deposit_storage(self):
        rows = [
            TickerPositionValue("AAA", 1, 800.0, 10.0),
            TickerPositionValue("CCC", 1, 200.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 800.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 200.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids={1},
            unrealized_pnl_pct_by_ticker={"AAA": 0.20},
        )
        self.assertTrue(plan.suggested_buys)
        self.assertTrue(all(int(b.storage_id) == 2 for b in plan.suggested_buys))
        self.assertEqual({b.ticker.upper() for b in plan.suggested_buys}, {"CCC"})

    def test_no_withdraw_keeps_proceeds_local(self):
        rows = [
            TickerPositionValue("AAA", 1, 800.0, 10.0),
            TickerPositionValue("CCC", 1, 200.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 800.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 200.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"AAA": 0.20},
        )
        # AAA proceeds cannot be deployed; the sell is trimmed away rather than
        # leaving idle cash -> no buys, no idle cash, asset kept.
        self.assertEqual(plan.suggested_buys, [])
        idle = plan.total_sell_proceeds + 0.0 - plan.total_implied_spend
        self.assertLess(abs(idle), 1.0)
        self.assertTrue(plan.constraint_gaps)

    def test_external_V_fully_deployed_even_past_target(self):
        """External cash must be fully placed; overshoot the only reachable ticker."""
        rows = [
            TickerPositionValue("AAA", 1, 500.0, 10.0),
            TickerPositionValue("CCC", 1, 500.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 500.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 500.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            400.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
        )
        self.assertTrue(plan.suggested_buys)
        self.assertTrue(all(int(b.storage_id) == 2 for b in plan.suggested_buys))
        self.assertEqual({b.ticker.upper() for b in plan.suggested_buys}, {"CCC"})
        # All external cash deployed (no idle cash), overshooting CCC past target.
        self.assertAlmostEqual(plan.total_implied_spend, 400.0, places=6)
        self.assertLess(abs(plan.residual_vs_V), 1.0)
        ccc_after = 500.0 + sum(
            float(b.implied_spend) for b in plan.suggested_buys
        )
        _, targets = compute_ticker_target_values(
            rows, {1: 100.0}, set(), portfolio_total=1400.0
        )
        self.assertGreater(ccc_after, targets["CCC"])  # forced overshoot

    def test_external_V_only_at_deposit_storage(self):
        rows = [
            TickerPositionValue("AAA", 1, 500.0, 10.0),
            TickerPositionValue("CCC", 1, 500.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 500.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 500.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            200.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
        )
        self.assertTrue(plan.suggested_buys)
        self.assertTrue(all(int(b.storage_id) == 2 for b in plan.suggested_buys))
        self.assertEqual({b.ticker.upper() for b in plan.suggested_buys}, {"CCC"})

    def test_sold_ticker_not_rebought_and_lots_whole(self):
        rows = [
            TickerPositionValue("AAA", 1, 800.0, 10.0),
            TickerPositionValue("CCC", 1, 200.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "S1", 1, 800.0, 10.0),
            StoragePositionValue("CCC", 1, "S1", 1, 200.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            0.0,
            sellable_tickers={"AAA"},
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"CCC"}},
            deposit_storage_ids={1},
            withdraw_storage_ids=set(),
            unrealized_pnl_pct_by_ticker={"AAA": 0.20},
        )
        sold = {s.ticker.upper() for s in plan.suggested_sells}
        bought = {b.ticker.upper() for b in plan.suggested_buys}
        self.assertFalse(sold & bought)
        for b in plan.suggested_buys:
            self.assertAlmostEqual(b.units, round(b.units), places=6)
        self.assertLessEqual(plan.deviation_l1_after, plan.deviation_l1_before)

    def test_min_purchase_blocks_lots_below_threshold(self):
        rows = [
            TickerPositionValue("AAA", 1, 500.0, 10.0),
            TickerPositionValue("CCC", 1, 500.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 500.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 500.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            100.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
            min_purchase_amount=150.0,
        )
        self.assertEqual(plan.suggested_buys, [])
        self.assertLess(plan.total_implied_spend, 1.0)

    def test_min_deposit_blocks_small_external_inflow(self):
        rows = [
            TickerPositionValue("AAA", 1, 500.0, 10.0),
            TickerPositionValue("CCC", 1, 500.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 500.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 500.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            400.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
            min_deposit_amount=1000.0,
        )
        self.assertEqual(plan.suggested_buys, [])
        self.assertLess(plan.total_implied_spend, 1.0)

    def test_min_deposit_allows_large_external_inflow(self):
        rows = [
            TickerPositionValue("AAA", 1, 500.0, 10.0),
            TickerPositionValue("CCC", 1, 500.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "IB", 1, 500.0, 10.0),
            StoragePositionValue("CCC", 2, "TBank", 1, 500.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            2500.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"CCC"}},
            deposit_storage_ids={2},
            withdraw_storage_ids=set(),
            min_deposit_amount=1000.0,
        )
        self.assertTrue(plan.suggested_buys)
        self.assertTrue(all(int(b.storage_id) == 2 for b in plan.suggested_buys))
        self.assertGreaterEqual(plan.total_implied_spend, 2400.0)

    def test_max_gap_concentrates_on_largest_underweight(self):
        rows = [
            TickerPositionValue("BIG", 1, 100.0, 10.0),
            TickerPositionValue("MID", 1, 400.0, 10.0),
            TickerPositionValue("SMALL", 1, 400.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("BIG", 1, "S1", 1, 100.0, 10.0),
            StoragePositionValue("MID", 1, "S1", 1, 400.0, 10.0),
            StoragePositionValue("SMALL", 1, "S1", 1, 400.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            600.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"BIG", "MID", "SMALL"}},
            deposit_storage_ids={1},
            withdraw_storage_ids=set(),
            buy_allocation_mode="max_gap",
        )
        spend_by = defaultdict(float)
        for b in plan.suggested_buys:
            spend_by[str(b.ticker).upper()] += float(b.implied_spend)
        self.assertGreater(spend_by.get("BIG", 0.0), spend_by.get("MID", 0.0))
        self.assertGreater(spend_by.get("BIG", 0.0), spend_by.get("SMALL", 0.0))

    def test_proportional_spreads_across_underweights(self):
        rows = [
            TickerPositionValue("BIG", 1, 100.0, 10.0),
            TickerPositionValue("MID", 1, 100.0, 10.0),
            TickerPositionValue("SMALL", 1, 100.0, 10.0),
        ]
        storage_rows = [
            StoragePositionValue("BIG", 1, "S1", 1, 100.0, 10.0),
            StoragePositionValue("MID", 1, "S1", 1, 100.0, 10.0),
            StoragePositionValue("SMALL", 1, "S1", 1, 100.0, 10.0),
        ]
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            600.0,
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"BIG", "MID", "SMALL"}},
            deposit_storage_ids={1},
            withdraw_storage_ids=set(),
            buy_allocation_mode="proportional",
        )
        bought = {b.ticker.upper() for b in plan.suggested_buys}
        self.assertGreaterEqual(len(bought), 2)
        self.assertIn("BIG", bought)
        self.assertTrue(bought & {"MID", "SMALL"})

    def test_storage_cash_flows_track_v_and_cross_storage(self):
        rows = [
            TickerPositionValue("AAA", 1, 80.0, 10.0),
            TickerPositionValue("BBB", 1, 20.0, 20.0),
        ]
        storage_rows = [
            StoragePositionValue("AAA", 1, "Src", 1, 80.0, 10.0),
            StoragePositionValue("BBB", 2, "Dst", 1, 20.0, 20.0),
        ]
        v = 100.0
        plan = compute_constrained_rebalance_plan(
            rows,
            {1: 100.0},
            {1: "Sub1"},
            v,
            sellable_positions={("AAA", 1)},
            storage_rows=storage_rows,
            unblocked_tickers_by_storage={1: {"AAA"}, 2: {"BBB"}},
            deposit_storage_ids={2},
            withdraw_storage_ids={1},
            unrealized_pnl_pct_by_ticker={"AAA": 0.15, "BBB": 0.12},
        )
        for cf in plan.storage_cash_flows.values():
            if cf.purchases <= 1e-6 and cf.sell_proceeds <= 1e-6:
                continue
            self.assertAlmostEqual(
                cf.sell_proceeds - cf.transfer_out + cf.external_inflow + cf.transfer_in,
                cf.purchases,
                places=0,
            )
        self.assertGreater(
            sum(f.external_inflow for f in plan.storage_cash_flows.values()), 0.0
        )
        if plan.suggested_sells:
            self.assertGreater(
                sum(f.transfer_out for f in plan.storage_cash_flows.values()), 0.0
            )


def _load_portfolio_rows_from_db(db_path: Path):
    from app.db import (
        list_asset_subclasses,
        list_buy_blocked_tickers,
        list_portfolio_blocks,
        list_positions_by_ticker,
    )
    from app.services.fx import convert_amount
    from app.services.performance import compute_ticker_unrealized_pnl_pct
    from app.services.price_currency import resolve_quote_currency
    from app.services.prices import PriceQuote, normalize_quote_price_for_valuation

    subclasses = list_asset_subclasses()
    target_pct = {s.id: float(s.target_pct) for s in subclasses}
    sub_names = {s.id: s.name for s in subclasses}
    blocked = {t.upper() for t in list_buy_blocked_tickers(main_only=True)}
    storage_block_rows = list_portfolio_blocks(main_only=True)
    sellable_positions = {
        (str(r["ticker"]).upper(), int(r["storage_id"]))
        for r in storage_block_rows
        if bool(r["sellable"])
    }

    positions = list_positions_by_ticker(main_only=True)
    tickers = sorted({p.ticker.upper() for p in positions} | blocked)
    conn = sqlite3.connect(str(db_path))
    quotes = {}
    for t in tickers:
        row = conn.execute(
            "SELECT price, currency FROM historical_quotes WHERE ticker=? ORDER BY quote_date DESC LIMIT 1",
            (t,),
        ).fetchone()
        if row and row[0] is not None:
            quotes[t] = PriceQuote(price=float(row[0]), currency=row[1] or "USD")
    conn.close()

    display_ccy = "RUB"
    rub, eur = 95.0, 0.92
    rows = []
    for p in positions:
        q = quotes.get(p.ticker.upper())
        if q is None:
            continue
        quote_ccy = resolve_quote_currency(p.ticker, q.currency)
        price = normalize_quote_price_for_valuation(p.ticker, q.price, quote_ccy)
        if price is None:
            continue
        price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
        value_disp = convert_amount(price * p.amount, quote_ccy, display_ccy, rub, eur)
        rows.append(
            TickerPositionValue(
                p.ticker, p.asset_subclass_id, float(value_disp), float(price_disp)
            )
        )

    pnl = {}
    for r in rows:
        pct = compute_ticker_unrealized_pnl_pct(
            r.ticker, r.value_display, display_ccy, rub, eur
        )
        if pct is not None:
            pnl[str(r.ticker).upper()] = float(pct)

    return rows, target_pct, sub_names, blocked, sellable_positions, pnl


class TestIdealRebalancePortfolioData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = Path(__file__).resolve().parents[1] / "data" / "portfolio.db"
        if not cls.db_path.exists():
            cls.rows = None
            return
        (
            cls.rows,
            cls.target_pct,
            cls.sub_names,
            cls.blocked,
            cls.sellable_positions,
            cls.pnl,
        ) = _load_portfolio_rows_from_db(cls.db_path)

    def setUp(self):
        if self.rows is None:
            self.skipTest("portfolio.db not available")

    def test_emxc_overweight_triggers_sell_with_blocked_tickers(self):
        plan = compute_ideal_rebalance_plan(
            self.rows,
            self.target_pct,
            self.sub_names,
            0.0,
            blocked_tickers=self.blocked,
            sellable_positions=self.sellable_positions,
            unrealized_pnl_pct_by_ticker=self.pnl,
        )
        sell_tickers = {s.ticker.upper() for s in plan.suggested_sells}
        self.assertIn("EMXC", sell_tickers)
        self.assertGreater(plan.total_sell_proceeds, 1_000_000.0)

    def test_ideal_rebalance_reduces_l1_deviation(self):
        plan = compute_ideal_rebalance_plan(
            self.rows,
            self.target_pct,
            self.sub_names,
            0.0,
            blocked_tickers=self.blocked,
            sellable_positions=self.sellable_positions,
            unrealized_pnl_pct_by_ticker=self.pnl,
        )
        self.assertLess(plan.deviation_l1_after, plan.deviation_l1_before * 0.5)
        self.assertLess(plan.deviation_l1_after, 2_500_000.0)

    def test_post_trade_max_ticker_deviation_is_modest(self):
        plan = compute_ideal_rebalance_plan(
            self.rows,
            self.target_pct,
            self.sub_names,
            0.0,
            blocked_tickers=self.blocked,
            sellable_positions=self.sellable_positions,
            unrealized_pnl_pct_by_ticker=self.pnl,
        )
        value_by = {str(r.ticker).upper(): float(r.value_display) for r in self.rows}
        for s in plan.suggested_sells:
            value_by[str(s.ticker).upper()] -= float(s.implied_proceeds)
        for b in plan.suggested_buys:
            value_by[str(b.ticker).upper()] = value_by.get(str(b.ticker).upper(), 0) + float(
                b.implied_spend
            )
        s_total = sum(float(r.value_display) for r in self.rows)
        _, targets = compute_ticker_target_values(
            self.rows, self.target_pct, self.blocked, portfolio_total=s_total
        )
        max_dev_pct = max(
            abs(value_by[t] - targets[t]) / s_total * 100.0
            for t in value_by
            if t in targets and s_total > 0
        )
        self.assertLess(max_dev_pct, 2.5)


def _load_full_portfolio_context(db_path: Path):
    from app.db import (
        list_portfolio_blocks,
        list_positions,
        list_storages,
    )
    from app.services.fx import convert_amount
    from app.services.price_currency import resolve_quote_currency
    from app.services.prices import PriceQuote, normalize_quote_price_for_valuation

    rows, target_pct, sub_names, blocked, sellable_positions, pnl = (
        _load_portfolio_rows_from_db(db_path)
    )
    storage_block_rows = list_portfolio_blocks(main_only=True)
    main_block_keys = {
        (str(r["ticker"]).upper(), int(r["storage_id"])) for r in storage_block_rows
    }
    unblocked_by_storage: dict[int, set[str]] = {}
    for r in storage_block_rows:
        if not bool(r["blocked"]):
            unblocked_by_storage.setdefault(int(r["storage_id"]), set()).add(
                str(r["ticker"]).upper()
            )
    all_storages = list_storages()
    deposit_ids = {int(s.id) for s in all_storages if bool(s.rebalance_deposit)}
    withdraw_ids = {int(s.id) for s in all_storages if bool(s.rebalance_withdraw)}

    conn = sqlite3.connect(str(db_path))
    quotes = {}
    for t in sorted({k[0] for k in main_block_keys} | blocked):
        row = conn.execute(
            "SELECT price, currency FROM historical_quotes WHERE ticker=? ORDER BY quote_date DESC LIMIT 1",
            (t,),
        ).fetchone()
        if row and row[0] is not None:
            quotes[t] = PriceQuote(price=float(row[0]), currency=row[1] or "USD")
    conn.close()

    display_ccy = "RUB"
    rub, eur = 95.0, 0.92
    storage_rows = []
    for p in list_positions():
        key = (str(p.ticker).upper(), int(p.storage_id))
        if key not in main_block_keys:
            continue
        q = quotes.get(p.ticker.upper())
        if q is None:
            continue
        quote_ccy = resolve_quote_currency(p.ticker, q.currency)
        price = normalize_quote_price_for_valuation(p.ticker, q.price, quote_ccy)
        if price is None:
            continue
        price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
        value_disp = convert_amount(price * p.amount, quote_ccy, display_ccy, rub, eur)
        storage_rows.append(
            StoragePositionValue(
                p.ticker,
                int(p.storage_id),
                str(p.storage_name or "—"),
                int(p.asset_subclass_id),
                float(value_disp),
                float(price_disp),
            )
        )

    return {
        "rows": rows,
        "storage_rows": storage_rows,
        "target_pct": target_pct,
        "sub_names": sub_names,
        "blocked": blocked,
        "sellable_positions": sellable_positions,
        "unblocked_by_storage": unblocked_by_storage,
        "deposit_ids": deposit_ids,
        "withdraw_ids": withdraw_ids,
        "pnl": pnl,
    }


class TestConstrainedRebalancePortfolioData(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = Path(__file__).resolve().parents[1] / "data" / "portfolio.db"
        cls.ctx = (
            _load_full_portfolio_context(cls.db_path)
            if cls.db_path.exists()
            else None
        )

    def setUp(self):
        if self.ctx is None:
            self.skipTest("portfolio.db not available")

    def _plan(self, V: float):
        c = self.ctx
        return compute_constrained_rebalance_plan(
            c["rows"],
            c["target_pct"],
            c["sub_names"],
            V,
            blocked_tickers=c["blocked"],
            sellable_positions=c["sellable_positions"],
            storage_rows=c["storage_rows"],
            unblocked_tickers_by_storage=c["unblocked_by_storage"],
            deposit_storage_ids=c["deposit_ids"],
            withdraw_storage_ids=c["withdraw_ids"],
            unrealized_pnl_pct_by_ticker=c["pnl"],
        )

    def test_constrained_reduces_deviation_but_not_below_ideal(self):
        plan = self._plan(0.0)
        ideal = compute_ideal_rebalance_plan(
            self.ctx["rows"],
            self.ctx["target_pct"],
            self.ctx["sub_names"],
            0.0,
            blocked_tickers=self.ctx["blocked"],
            sellable_positions=self.ctx["sellable_positions"],
            unrealized_pnl_pct_by_ticker=self.ctx["pnl"],
        )
        self.assertLess(plan.deviation_l1_after, plan.deviation_l1_before * 0.9)
        # Constraints cannot beat the unconstrained ideal end-state.
        self.assertGreaterEqual(
            plan.deviation_l1_after, ideal.deviation_l1_after - 1.0
        )

    def test_constrained_does_not_rebuy_sold_and_respects_spend(self):
        plan = self._plan(500_000.0)
        sold = {s.ticker.upper() for s in plan.suggested_sells}
        bought = {b.ticker.upper() for b in plan.suggested_buys}
        self.assertFalse(sold & bought)
        # Cannot spend more than proceeds + external V (plus tiny rounding slack).
        self.assertLessEqual(
            plan.total_implied_spend,
            plan.total_sell_proceeds + 500_000.0 + 1.0,
        )

    def test_constrained_leaves_no_idle_cash_beyond_sublot(self):
        """Proceeds + V are fully deployed; only a sub-lot remainder may linger."""
        for V in (0.0, 500_000.0, 3_000_000.0):
            plan = self._plan(V)
            cash_in = plan.total_sell_proceeds + V
            idle = cash_in - plan.total_implied_spend
            # Idle cash must be tiny (below a single lot of the cheapest
            # reachable instrument), never negative (no overspend).
            self.assertGreaterEqual(idle, -1.0, f"overspend at V={V}")
            self.assertLess(idle, 50_000.0, f"too much idle cash at V={V}")


if __name__ == "__main__":
    unittest.main()
