"""Unit tests for CBR key rate helpers."""
import unittest

from app.services.cbr_key_rate import (
    daily_compound_factor,
    key_rate_as_of,
)


class TestCbrKeyRate(unittest.TestCase):
    def test_key_rate_as_of_uses_step_schedule(self):
        self.assertAlmostEqual(key_rate_as_of("2021-03-01", None), 0.0425, places=6)
        self.assertAlmostEqual(key_rate_as_of("2021-04-26", None), 0.05, places=6)
        self.assertAlmostEqual(key_rate_as_of("2022-02-28", None), 0.2, places=6)
        self.assertAlmostEqual(key_rate_as_of("2022-07-22", None), 0.095, places=6)

    def test_key_rate_as_of_daily_series(self):
        series = {"2022-01-01": 0.08, "2022-07-01": 0.09}
        self.assertAlmostEqual(key_rate_as_of("2022-03-15", series), 0.08, places=6)
        self.assertAlmostEqual(key_rate_as_of("2022-07-15", series), 0.09, places=6)

    def test_daily_compound_factor(self):
        annual = 0.365
        factor = daily_compound_factor("2022-01-01", {"2022-01-01": annual})
        expected = (1.0 + annual) ** (1.0 / 365.0)
        self.assertAlmostEqual(factor, expected, places=9)


if __name__ == "__main__":
    unittest.main()
