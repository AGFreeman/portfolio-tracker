"""Tests for quote / diversification currency helpers."""
from app.services.price_currency import (
    bucket_diversification_currency,
    infer_quote_currency,
    infer_trading_currency,
    resolve_quote_currency,
)


def test_bucket_diversification_currency():
    assert bucket_diversification_currency("RUB") == "RUB"
    assert bucket_diversification_currency("EUR") == "EUR"
    assert bucket_diversification_currency("GBP") == "USD"
    assert bucket_diversification_currency("") == "USD"


def test_infer_quote_currency_tbank_moex():
    assert infer_quote_currency("TMOS") == "RUB"  # MOEX_TICKERS default moex
    # Provider from DB is authoritative when set — tested via resolve with live.


def test_resolve_quote_currency_prefers_live():
    assert resolve_quote_currency("FXES", "RUB") == "RUB"
    assert resolve_quote_currency("FXES", "RUR") == "RUB"
    assert resolve_quote_currency("AAPL", "USD") == "USD"


def test_infer_trading_currency_eur_suffix():
    assert infer_trading_currency("XEON.DE") == "EUR"


def test_infer_trading_currency_tbank_fallback(monkeypatch):
    monkeypatch.setattr(
        "app.services.price_currency.get_instrument_provider",
        lambda _t: ("tbank", "TMOS"),
    )
    assert infer_trading_currency("TMOS") == "RUB"
    assert infer_quote_currency("TMOS") == "RUB"
