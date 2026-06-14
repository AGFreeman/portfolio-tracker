import unittest
from types import SimpleNamespace

from app.services.portfolio_order import portfolio_ticker_sort_key
from app.services.prices import PriceQuote
from app.ui.prices import (
    TickerScopeMeta,
    build_trade_markers,
    class_options_for_portfolio,
    convert_price_value,
    filter_tickers_for_scope,
    order_ticker_options,
    subclass_options_for_scope,
    ticker_chart_date_range,
)


class _Sub:
    def __init__(self, asset_class_id: int, sort_order: int, name: str):
        self.asset_class_id = asset_class_id
        self.sort_order = sort_order
        self.name = name


class _Class:
    def __init__(self, sort_order: int, name: str):
        self.sort_order = sort_order
        self.name = name


class TestTickerScopeFilters(unittest.TestCase):
    def setUp(self):
        self.meta = (
            TickerScopeMeta("BTC", True, 1, 10),
            TickerScopeMeta("ETH", True, 1, 10),
            TickerScopeMeta("SBER", True, 2, 20),
            TickerScopeMeta("SOL", False, 3, 10),
        )
        self.meta_by_ticker = {m.ticker: m for m in self.meta}
        self.subclass_by_id = {
            1: _Sub(10, 1, "BTC+ETH"),
            2: _Sub(20, 1, "Акции РФ"),
            3: _Sub(10, 2, "Прочая крипта"),
        }
        self.class_by_id = {
            10: _Class(1, "Криптовалюта"),
            20: _Class(2, "Акции"),
        }

    def test_class_options_for_main_portfolio(self):
        options = class_options_for_portfolio(
            self.meta,
            portfolio_main=True,
            class_by_id=self.class_by_id,
        )
        self.assertEqual(options, ["Криптовалюта", "Акции"])

    def test_subclass_options_respect_selected_class(self):
        options = subclass_options_for_scope(
            self.meta,
            portfolio_main=True,
            class_name="Криптовалюта",
            subclass_by_id=self.subclass_by_id,
            class_by_id=self.class_by_id,
        )
        self.assertEqual(options, ["BTC+ETH"])

    def test_filter_tickers_for_scope(self):
        filtered = filter_tickers_for_scope(
            ["BTC", "ETH", "SBER", "SOL"],
            self.meta_by_ticker,
            portfolio="Main",
            class_name="Криптовалюта",
            subclass_name="BTC+ETH",
            class_by_id=self.class_by_id,
            subclass_by_id=self.subclass_by_id,
        )
        self.assertEqual(filtered, ["BTC", "ETH"])

        filtered_other = filter_tickers_for_scope(
            ["BTC", "ETH", "SBER", "SOL"],
            self.meta_by_ticker,
            portfolio="Other",
            class_name="Криптовалюта",
            subclass_name="Прочая крипта",
            class_by_id=self.class_by_id,
            subclass_by_id=self.subclass_by_id,
        )
        self.assertEqual(filtered_other, ["SOL"])


class TestOrderTickerOptions(unittest.TestCase):
    def test_sorts_by_portfolio_key(self):
        subclass_by_id = {1: _Sub(10, 1, "A"), 2: _Sub(20, 1, "B")}
        class_sort_by_id = {10: 1, 20: 2}
        subclass_map = {"BTC": 1, "SBER": 2}
        ordered = order_ticker_options(
            ["SBER", "BTC"],
            subclass_id_map=subclass_map,
            subclass_by_id=subclass_by_id,
            class_sort_by_id=class_sort_by_id,
        )
        self.assertEqual(ordered, ["BTC", "SBER"])
        self.assertLess(
            portfolio_ticker_sort_key(
                "BTC",
                asset_subclass_id=1,
                subclass_by_id=subclass_by_id,
                class_sort_by_id=class_sort_by_id,
            ),
            portfolio_ticker_sort_key(
                "SBER",
                asset_subclass_id=2,
                subclass_by_id=subclass_by_id,
                class_sort_by_id=class_sort_by_id,
            ),
        )


class TestTickerChartDateRange(unittest.TestCase):
    def test_uses_holding_intervals(self):
        tx_by_day = {
            "2024-01-01": [("BTC", 1.0, "trade")],
            "2024-06-01": [("BTC", -1.0, "trade")],
        }
        days = [
            "2024-01-01",
            "2024-02-01",
            "2024-03-01",
            "2024-04-01",
            "2024-05-01",
            "2024-06-01",
        ]
        start, end = ticker_chart_date_range("BTC", tx_by_day, days)
        self.assertEqual(start, "2024-01-01")
        self.assertEqual(end, "2024-05-01")


class TestConvertPriceValue(unittest.TestCase):
    def test_display_mode_converts_with_historical_fx(self):
        converted = convert_price_value(
            100.0,
            "USD",
            "2024-01-15",
            "RUB",
            fx_exact={"2024-01-15": (90.0, 0.9)},
            spot_rub_per_usd=95.0,
            spot_eur_per_usd=0.92,
        )
        self.assertAlmostEqual(converted, 9000.0)

    def test_same_currency_is_identity(self):
        converted = convert_price_value(
            123.45,
            "USD",
            "2024-01-15",
            "USD",
            fx_exact={},
            spot_rub_per_usd=95.0,
            spot_eur_per_usd=0.92,
        )
        self.assertAlmostEqual(converted, 123.45)


class TestBuildTradeMarkers(unittest.TestCase):
    def _tx(self, ticker, amount, tx_type, created_at):
        return SimpleNamespace(
            ticker=ticker,
            amount=amount,
            transaction_type=tx_type,
            created_at=created_at,
        )

    def test_trade_markers_buy_and_sell_only(self):
        dates = ["2024-01-01", "2024-02-01"]
        quotes = [
            PriceQuote(price=100.0, currency="USD"),
            PriceQuote(price=110.0, currency="USD"),
        ]
        txs = [
            self._tx("BTC", 0.1, "trade", "2024-01-01 10:00:00"),
            self._tx("BTC", -0.05, "trade", "2024-02-01 10:00:00"),
            self._tx("BTC", 0.2, "transfer", "2024-02-02 10:00:00"),
            self._tx("ETH", 1.0, "trade", "2024-01-01 10:00:00"),
        ]
        markers = build_trade_markers(
            "BTC",
            txs,
            dates=dates,
            quotes=quotes,
            currency_mode="quote",
            display_ccy="RUB",
            quote_ccy="USD",
            fx_exact={},
            spot_rub_per_usd=95.0,
            spot_eur_per_usd=0.92,
        )
        self.assertEqual(len(markers), 2)
        self.assertEqual(markers[0].side, "buy")
        self.assertAlmostEqual(markers[0].price, 100.0)
        self.assertAlmostEqual(markers[0].qty, 0.1)
        self.assertEqual(markers[1].side, "sell")
        self.assertAlmostEqual(markers[1].price, 110.0)

    def test_display_mode_converts_marker_price(self):
        dates = ["2024-01-01"]
        quotes = [PriceQuote(price=100.0, currency="USD")]
        txs = [self._tx("BTC", 0.1, "trade", "2024-01-01 10:00:00")]
        markers = build_trade_markers(
            "BTC",
            txs,
            dates=dates,
            quotes=quotes,
            currency_mode="display",
            display_ccy="RUB",
            quote_ccy="USD",
            fx_exact={"2024-01-01": (90.0, 0.9)},
            spot_rub_per_usd=95.0,
            spot_eur_per_usd=0.92,
        )
        self.assertEqual(len(markers), 1)
        self.assertAlmostEqual(markers[0].price, 9000.0)


if __name__ == "__main__":
    unittest.main()
