import json
import unittest
from unittest.mock import patch

from app.services.fx import sync_historical_fx


class TestSyncHistoricalFx(unittest.TestCase):
    @patch("app.services.fx.fetch_usd_cross_rates")
    @patch("app.services.fx.get_historical_usd_cross_rates_exact")
    @patch("app.db.set_app_setting")
    @patch("app.db.get_app_setting")
    def test_fetches_gap_after_last_cached_day(
        self,
        mock_get,
        mock_set,
        mock_fetch,
        mock_live,
    ):
        mock_get.return_value = json.dumps(
            {
                "2026-06-24": [74.0, 0.87],
                "2026-06-25": [75.0, 0.88],
            }
        )
        mock_fetch.return_value = {
            "2026-06-26": (76.0, 0.89),
            "2026-07-20": (77.0, 0.90),
        }
        mock_live.return_value = (78.0, 0.91, "Yahoo Finance", None)

        written = sync_historical_fx(date_from="2021-01-01", date_to="2026-07-21")

        self.assertEqual(written, 3)
        mock_fetch.assert_called_once_with("2026-06-26", "2026-07-21")
        saved = json.loads(mock_set.call_args.args[1])
        self.assertEqual(saved["2026-06-25"], [75.0, 0.88])
        self.assertEqual(saved["2026-06-26"], [76.0, 0.89])
        self.assertEqual(saved["2026-07-20"], [77.0, 0.90])
        self.assertEqual(saved["2026-07-21"], [78.0, 0.91])

    @patch("app.services.fx.get_historical_usd_cross_rates_exact")
    @patch("app.db.get_app_setting")
    def test_skips_when_cache_is_current(self, mock_get, mock_fetch):
        today = "2026-07-21"
        mock_get.return_value = json.dumps({today: [78.0, 0.91]})

        written = sync_historical_fx(date_to=today)

        self.assertEqual(written, 0)
        mock_fetch.assert_not_called()

    @patch("app.services.fx.fetch_usd_cross_rates")
    @patch("app.services.fx.get_historical_usd_cross_rates_exact")
    @patch("app.db.set_app_setting")
    @patch("app.db.get_app_setting")
    def test_empty_cache_requires_date_from(
        self,
        mock_get,
        mock_set,
        mock_fetch,
        mock_live,
    ):
        mock_get.return_value = None

        written = sync_historical_fx(date_to="2026-07-21")

        self.assertEqual(written, 0)
        mock_fetch.assert_not_called()
        mock_set.assert_not_called()


if __name__ == "__main__":
    unittest.main()
