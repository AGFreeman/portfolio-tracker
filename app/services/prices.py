"""Fetch live price by ticker. Multi-provider: yfinance, MOEX ISS, CoinGecko. Session cache."""
import re
import time
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests

# Crypto tickers -> CoinGecko id
COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "AVAX": "avalanche-2",
    "BNB": "binancecoin",
    "XRP": "ripple",
}

# Tickers we know are on MOEX (can extend via DB)
MOEX_TICKERS = {
    "TMOS", "SBGB", "SBRB", "TGLD", "TSPX", "TEUS", "TEMS", "GXC", "EFG",
}

_HTTP = requests.Session()


@dataclass
class PriceQuote:
    """Цена и валюта котировки согласно данным провайдера."""

    price: Optional[float]
    currency: str  # ISO 4217 (RUR нормализуем в RUB)


def is_crypto_ticker(ticker: str) -> bool:
    """True for tickers we treat as crypto (fractional quantity allowed)."""
    return ticker.upper().strip() in COINGECKO_IDS


def normalize_quantity(ticker: str, amount: float) -> float:
    """Shares/ETF: whole units. Crypto: fractional."""
    if is_crypto_ticker(ticker):
        return float(amount)
    return float(int(round(amount)))


def _normalize_currency_code(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    c = str(raw).strip().upper()
    if c in ("RUR", "RUB"):
        return "RUB"
    if len(c) == 3 and c.isalpha():
        return c
    return None


def _detect_provider(ticker: str) -> tuple:
    """Return (provider, provider_symbol). provider: yfinance | moex_iss | coingecko."""
    up = ticker.upper().strip()
    if up in COINGECKO_IDS:
        return ("coingecko", COINGECKO_IDS[up])
    if up in MOEX_TICKERS:
        return ("moex_iss", up)
    if re.match(r"^[A-Z0-9\-\.]{2,10}$", up):
        return ("yfinance", up)
    return ("yfinance", up)


def _fallback_currency(provider: str) -> str:
    if provider == "moex_iss":
        return "RUB"
    return "USD"


def _price_yfinance(symbol: str) -> PriceQuote:
    """Валюта: fast_info / info у Yahoo."""
    fallback = _fallback_currency("yfinance")
    try:
        import yfinance as yf

        t = yf.Ticker(symbol)
        ccy = fallback
        try:
            fi = t.fast_info
            raw = None
            if hasattr(fi, "currency"):
                raw = fi.currency
            elif isinstance(fi, dict):
                raw = fi.get("currency")
            else:
                try:
                    raw = fi["currency"]  # type: ignore[index]
                except (TypeError, KeyError, AttributeError):
                    pass
            norm = _normalize_currency_code(raw)
            if norm:
                ccy = norm
        except Exception:
            pass

        if ccy == fallback:
            try:
                info = t.info
                if isinstance(info, dict):
                    norm = _normalize_currency_code(info.get("currency"))
                    if norm:
                        ccy = norm
            except Exception:
                pass

        info = t.fast_info
        price = None
        if hasattr(info, "last_price") and info.last_price is not None:
            price = float(info.last_price)
        else:
            try:
                lp = info["last_price"] if isinstance(info, dict) else None
                if lp is not None:
                    price = float(lp)
            except (TypeError, KeyError, AttributeError):
                pass
        if price is None:
            hist = t.history(period="1d")
            if hist is not None and not hist.empty:
                price = float(hist["Close"].iloc[-1])
        return PriceQuote(price=price, currency=ccy)
    except Exception:
        return PriceQuote(price=None, currency=fallback)


def _price_moex_board(security: str, board: str) -> PriceQuote:
    """MOEX ISS: цена + CURRENCYID из таблицы securities."""
    url = (
        f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}/securities.json"
        "?iss.only=marketdata,securities&iss.meta=off"
    )
    secid_upper = security.upper()
    ccy_default = "RUB"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        md = data.get("marketdata", {})
        sec = data.get("securities", {})
        cols_md = md.get("columns", [])
        cols_sec = sec.get("columns", [])
        idx_sec = cols_sec.index("SECID") if "SECID" in cols_sec else -1
        idx_cur = cols_sec.index("CURRENCYID") if "CURRENCYID" in cols_sec else -1
        if idx_sec < 0:
            return PriceQuote(price=None, currency=ccy_default)

        for i, row in enumerate(sec.get("data", [])):
            if row[idx_sec] != secid_upper:
                continue
            ccy = ccy_default
            if idx_cur >= 0 and idx_cur < len(row) and row[idx_cur]:
                norm = _normalize_currency_code(str(row[idx_cur]))
                if norm:
                    ccy = norm
            row_md = md.get("data", [])[i] if i < len(md.get("data", [])) else []
            if not row_md:
                break
            col_map = {c: j for j, c in enumerate(cols_md)}
            for key in ("LAST", "PREVPRICE", "OPEN"):
                if key in col_map and col_map[key] < len(row_md) and row_md[col_map[key]] is not None:
                    try:
                        return PriceQuote(price=float(row_md[col_map[key]]), currency=ccy)
                    except (TypeError, ValueError):
                        pass
            return PriceQuote(price=None, currency=ccy)

        url2 = (
            f"https://iss.moex.com/iss/engines/stock/markets/shares/boards/{board}/securities/{security}.json"
            "?iss.only=marketdata,securities&iss.meta=off"
        )
        r2 = requests.get(url2, timeout=10)
        if r2.ok:
            d = r2.json()
            sec2 = d.get("securities", {})
            cols2 = sec2.get("columns", [])
            rows2 = sec2.get("data", [])
            ccy = ccy_default
            if "CURRENCYID" in cols2 and rows2:
                ic = cols2.index("CURRENCYID")
                if ic < len(rows2[0]) and rows2[0][ic]:
                    norm = _normalize_currency_code(str(rows2[0][ic]))
                    if norm:
                        ccy = norm
            md2 = d.get("marketdata", {})
            cols = md2.get("columns", [])
            rows = md2.get("data", [])
            if rows:
                row = rows[0]
                for key in ("LAST", "PREVPRICE", "OPEN"):
                    if key in cols:
                        idx = cols.index(key)
                        if idx < len(row) and row[idx] is not None:
                            try:
                                return PriceQuote(price=float(row[idx]), currency=ccy)
                            except (TypeError, ValueError):
                                pass
    except Exception:
        pass
    return PriceQuote(price=None, currency=ccy_default)


def _price_moex_iss(security: str) -> PriceQuote:
    for board in ("TQBR", "TQTF"):
        q = _price_moex_board(security, board)
        if q.price is not None:
            return q
    return PriceQuote(price=None, currency="RUB")


def _price_coingecko(coin_id: str) -> PriceQuote:
    """Запрос vs_currencies=usd — валюта котировки USD."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
    try:
        r = _HTTP.get(url, timeout=6)
        r.raise_for_status()
        data = r.json()
        if coin_id in data and "usd" in data[coin_id]:
            return PriceQuote(price=float(data[coin_id]["usd"]), currency="USD")
    except Exception:
        pass
    return PriceQuote(price=None, currency="USD")


def _price_coingecko_many(coin_ids: List[str]) -> Dict[str, PriceQuote]:
    """Batch CoinGecko request to reduce latency for multiple crypto tickers."""
    ids = [c for c in dict.fromkeys(coin_ids) if c]
    if not ids:
        return {}
    url = "https://api.coingecko.com/api/v3/simple/price"
    out: Dict[str, PriceQuote] = {c: PriceQuote(price=None, currency="USD") for c in ids}
    try:
        r = _HTTP.get(
            url,
            params={"ids": ",".join(ids), "vs_currencies": "usd"},
            timeout=6,
        )
        r.raise_for_status()
        data = r.json() or {}
        for coin_id in ids:
            usd = (data.get(coin_id) or {}).get("usd")
            if usd is None:
                continue
            out[coin_id] = PriceQuote(price=float(usd), currency="USD")
    except Exception:
        pass
    return out


def _resolve_provider_symbol(
    ticker: str,
    provider_overrides: Optional[dict] = None,
) -> Tuple[str, str]:
    overrides = provider_overrides or {}
    ov = overrides.get(ticker)
    if ov:
        provider, symbol = ov[0], (ov[1] or "").strip()
        return provider, symbol or ticker.upper().strip()
    return _detect_provider(ticker)


def fetch_price_quote(
    ticker: str,
    provider_override: Optional[str] = None,
    provider_symbol_override: Optional[str] = None,
) -> PriceQuote:
    """Цена и валюта согласно провайдеру (API)."""
    if provider_override:
        symbol = (provider_symbol_override or "").strip() or ticker.upper().strip()
        provider = provider_override
    else:
        provider, symbol = _detect_provider(ticker)

    if provider == "yfinance":
        if symbol in COINGECKO_IDS and "-" not in symbol:
            symbol = f"{symbol}-USD"
        return _price_yfinance(symbol)
    if provider == "moex_iss":
        return _price_moex_iss(symbol)
    if provider == "coingecko":
        return _price_coingecko(symbol)
    return _price_yfinance(symbol)


def get_price(
    ticker: str,
    provider_override: Optional[str] = None,
    provider_symbol_override: Optional[str] = None,
) -> Optional[float]:
    return fetch_price_quote(ticker, provider_override, provider_symbol_override).price


def _daterange_iso(date_from: str, date_to: str) -> List[str]:
    d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
    d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    if d1 < d0:
        return []
    days = (d1 - d0).days
    return [(d0 + timedelta(days=i)).isoformat() for i in range(days + 1)]


def fetch_historical_prices_yfinance(symbol: str, date_from: str, date_to: str) -> Dict[str, PriceQuote]:
    out: Dict[str, PriceQuote] = {}
    try:
        import yfinance as yf

        # yfinance end is exclusive; shift by +1 day
        d_end = (datetime.strptime(date_to, "%Y-%m-%d").date() + timedelta(days=1)).isoformat()
        hist = yf.Ticker(symbol).history(start=date_from, end=d_end, interval="1d", auto_adjust=False)
        ccy = "USD"
        try:
            info = yf.Ticker(symbol).fast_info
            raw = getattr(info, "currency", None) if not isinstance(info, dict) else info.get("currency")
            norm = _normalize_currency_code(raw)
            if norm:
                ccy = norm
        except Exception:
            pass
        if hist is None or hist.empty:
            return out
        for idx, row in hist.iterrows():
            if "Close" not in row or row["Close"] is None:
                continue
            day = idx.date().isoformat()
            try:
                out[day] = PriceQuote(price=float(row["Close"]), currency=ccy)
            except (TypeError, ValueError):
                continue
    except Exception:
        return {}
    return out


def fetch_historical_prices_moex(symbol: str, date_from: str, date_to: str) -> Dict[str, PriceQuote]:
    out: Dict[str, PriceQuote] = {}
    sec = symbol.upper().strip()
    for board in ("TQBR", "TQTF"):
        url = (
            f"https://iss.moex.com/iss/history/engines/stock/markets/shares/boards/{board}/securities/{sec}.json"
            f"?from={date_from}&till={date_to}&iss.meta=off"
        )
        try:
            r = _HTTP.get(url, timeout=12)
            if not r.ok:
                continue
            data = r.json() or {}
            hist = data.get("history", {})
            cols = hist.get("columns", [])
            rows = hist.get("data", [])
            if not cols or not rows:
                continue
            idx_trade_date = cols.index("TRADEDATE") if "TRADEDATE" in cols else -1
            idx_close = cols.index("CLOSE") if "CLOSE" in cols else -1
            idx_legal_close = cols.index("LEGALCLOSEPRICE") if "LEGALCLOSEPRICE" in cols else -1
            idx_waprice = cols.index("WAPRICE") if "WAPRICE" in cols else -1
            if idx_trade_date < 0:
                continue
            for row in rows:
                day = row[idx_trade_date]
                if not day:
                    continue
                px = None
                for idx_px in (idx_close, idx_legal_close, idx_waprice):
                    if idx_px >= 0 and idx_px < len(row) and row[idx_px] is not None:
                        px = row[idx_px]
                        break
                try:
                    if px is not None:
                        out[str(day)] = PriceQuote(price=float(px), currency="RUB")
                except (TypeError, ValueError):
                    continue
            if out:
                break
        except Exception:
            continue
    return out


def fetch_historical_prices_coingecko(coin_id: str, date_from: str, date_to: str) -> Dict[str, PriceQuote]:
    out: Dict[str, PriceQuote] = {}
    try:
        d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
        d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    except ValueError:
        return out
    if d1 < d0:
        return out
    days = max(1, (d1 - d0).days + 1)
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    try:
        r = _HTTP.get(url, params={"vs_currency": "usd", "days": days, "interval": "daily"}, timeout=12)
        r.raise_for_status()
        data = r.json() or {}
        for item in data.get("prices", []):
            if not isinstance(item, list) or len(item) < 2:
                continue
            ts_ms, px = item[0], item[1]
            try:
                day = datetime.utcfromtimestamp(float(ts_ms) / 1000.0).date()
                if d0 <= day <= d1:
                    out[day.isoformat()] = PriceQuote(price=float(px), currency="USD")
            except (TypeError, ValueError, OSError):
                continue
    except Exception:
        return out
    return out


def fetch_historical_quotes(
    ticker: str,
    date_from: str,
    date_to: str,
    provider_override: Optional[str] = None,
    provider_symbol_override: Optional[str] = None,
) -> Dict[str, PriceQuote]:
    """Fetch historical quotes by provider for an inclusive date range."""
    if provider_override:
        provider = provider_override
        symbol = (provider_symbol_override or "").strip() or ticker.upper().strip()
    else:
        provider, symbol = _detect_provider(ticker)

    if provider == "moex_iss":
        return fetch_historical_prices_moex(symbol, date_from, date_to)
    if provider == "coingecko":
        return fetch_historical_prices_coingecko(symbol, date_from, date_to)
    if provider == "yfinance":
        yf_symbol = f"{symbol}-USD" if symbol in COINGECKO_IDS and "-" not in symbol else symbol
        return fetch_historical_prices_yfinance(yf_symbol, date_from, date_to)
    return fetch_historical_prices_yfinance(symbol, date_from, date_to)


def _unpack_cached_quote(ticker: str, cached) -> PriceQuote:
    """Обратная совместимость: в кэше мог быть только float."""
    if isinstance(cached, PriceQuote):
        return cached
    if isinstance(cached, tuple) and len(cached) == 2:
        p, c = cached
        return PriceQuote(
            price=p if isinstance(p, (int, float)) else None,
            currency=_normalize_currency_code(str(c)) or _fallback_currency(_detect_provider(ticker)[0]),
        )
    if isinstance(cached, (int, float)) or cached is None:
        prov = _detect_provider(ticker)[0]
        return PriceQuote(price=float(cached) if cached is not None else None, currency=_fallback_currency(prov))
    return PriceQuote(price=None, currency=_fallback_currency(_detect_provider(ticker)[0]))


def get_quotes_cached(
    tickers: List[str],
    cache_ttl_sec: float = 120,
    session_state_key: str = "price_cache",
    provider_overrides: Optional[dict] = None,
) -> Dict[str, PriceQuote]:
    """{ticker: PriceQuote} с учётом session_state кэша."""
    import streamlit as st

    now = time.time()
    if session_state_key not in st.session_state:
        st.session_state[session_state_key] = {"ts": 0, "data": {}}
    cache = st.session_state[session_state_key]
    overrides = provider_overrides or {}
    result: Dict[str, PriceQuote] = {}

    if cache_ttl_sec <= 0:
        missing = list(dict.fromkeys(tickers))
        plan = {t: _resolve_provider_symbol(t, provider_overrides=overrides) for t in missing}
        cg_targets = {t: sym for t, (prov, sym) in plan.items() if prov == "coingecko"}
        if cg_targets:
            cg_quotes = _price_coingecko_many(list(cg_targets.values()))
            for t, coin_id in cg_targets.items():
                result[t] = cg_quotes.get(coin_id, PriceQuote(price=None, currency="USD"))
        for t in missing:
            if t in result:
                continue
            prov, sym = plan[t]
            result[t] = fetch_price_quote(
                t,
                provider_override=prov,
                provider_symbol_override=sym,
            )
        return result

    if cache["ts"] + cache_ttl_sec < now or not cache["data"]:
        cache["ts"] = now
        cache["data"] = {}

    missing: List[str] = []
    for t in tickers:
        if t in cache["data"]:
            result[t] = _unpack_cached_quote(t, cache["data"][t])
            continue
        missing.append(t)

    if missing:
        plan = {t: _resolve_provider_symbol(t, provider_overrides=overrides) for t in missing}
        cg_targets = {t: sym for t, (prov, sym) in plan.items() if prov == "coingecko"}
        if cg_targets:
            cg_quotes = _price_coingecko_many(list(cg_targets.values()))
            for t, coin_id in cg_targets.items():
                q = cg_quotes.get(coin_id, PriceQuote(price=None, currency="USD"))
                cache["data"][t] = q
                result[t] = q
        for t in missing:
            if t in result:
                continue
            prov, sym = plan[t]
            q = fetch_price_quote(t, provider_override=prov, provider_symbol_override=sym)
            cache["data"][t] = q
            result[t] = q
    return result


def get_prices_cached(
    tickers: list,
    cache_ttl_sec: float = 120,
    session_state_key: str = "price_cache",
    provider_overrides: Optional[dict] = None,
) -> dict:
    """Только цены (совместимость)."""
    q = get_quotes_cached(tickers, cache_ttl_sec, session_state_key, provider_overrides)
    return {k: v.price for k, v in q.items()}
