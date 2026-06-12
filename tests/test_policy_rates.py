"""Unit tests for policy rate helpers."""
import unittest

from app.services.policy_rates import (
    daily_policy_compound_factor,
    policy_rate_as_of,
    uses_synthetic_policy_benchmark,
)


class TestPolicyRates(unittest.TestCase):
    def test_synthetic_only_for_rub(self):
        self.assertTrue(uses_synthetic_policy_benchmark("RUB"))
        self.assertFalse(uses_synthetic_policy_benchmark("USD"))
        self.assertFalse(uses_synthetic_policy_benchmark("EUR"))

    def test_rub_rate_steps(self):
        self.assertAlmostEqual(policy_rate_as_of("RUB", "2021-03-01"), 0.0425, places=6)
        self.assertAlmostEqual(policy_rate_as_of("RUB", "2022-02-28"), 0.2, places=6)

    def test_non_rub_rates_are_zero(self):
        self.assertEqual(policy_rate_as_of("USD", "2023-01-01"), 0.0)
        self.assertEqual(policy_rate_as_of("EUR", "2023-01-01"), 0.0)

    def test_daily_compound_factor(self):
        rub_factor = daily_policy_compound_factor("RUB", "2022-01-01")
        usd_factor = daily_policy_compound_factor("USD", "2022-01-01")
        self.assertGreater(rub_factor, 1.0)
        self.assertEqual(usd_factor, 1.0)


if __name__ == "__main__":
    unittest.main()
