"""Fetch live price by ticker. Multi-provider: yfinance, MOEX ISS, CoinGecko, T-Bank. Session cache."""
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import urllib3

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
_TBANK_FIGI_BY_SYMBOL: Dict[str, tuple] = {}  # SYMBOL -> (figi, currency)
LIVE_QUOTES_REFRESH_SEC = 60
DISABLED_QUOTES_TTL_SEC = 24 * 60 * 60
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
_PROVIDER_SYMBOL_BY_TICKER: Dict[str, Tuple[str, str]] = {}
_INSTRUMENT_KIND_BY_PROVIDER_SYMBOL: Dict[Tuple[str, str], Optional[str]] = {}


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
    # MOEX can return legacy/alt ruble codes (RUR/SUR).
    if c in ("RUR", "RUB", "SUR"):
        return "RUB"
    if len(c) == 3 and c.isalpha():
        return c
    return None


def _get_provider_symbol_cached(ticker: str) -> Tuple[str, str]:
    up = (ticker or "").upper().strip()
    if not up:
        return "", ""
    cached = _PROVIDER_SYMBOL_BY_TICKER.get(up)
    if cached is not None:
        return cached
    provider = ""
    symbol = up
    try:
        from app.db import get_instrument_provider

        row = get_instrument_provider(up)
        if row:
            provider = str(row[0] or "").strip().lower()
            symbol = str(row[1] or "").strip() or up
    except Exception:
        provider = ""
        symbol = up
    val = (provider, symbol)
    _PROVIDER_SYMBOL_BY_TICKER[up] = val
    return val


def _detect_moex_instrument_kind(symbol: str) -> Optional[str]:
    sec = (symbol or "").strip().upper()
    if not sec:
        return None
    # Fast path: if security appears in bond market board, treat as bond.
    bond_url = (
        f"https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities/{sec}.json"
        "?iss.only=securities&iss.meta=off"
    )
    try:
        r = _HTTP.get(bond_url, timeout=6)
        if r.ok:
            data = r.json() or {}
            rows = ((data.get("securities") or {}).get("data") or [])
            if rows:
                return "bond"
    except Exception:
        pass
    # Fallback: generic card and inspect textual TYPE/GROUP fields.
    desc_url = f"https://iss.moex.com/iss/securities/{sec}.json?iss.only=description&iss.meta=off"
    try:
        r = _HTTP.get(desc_url, timeout=6)
        if not r.ok:
            return None
        data = r.json() or {}
        desc = data.get("description") or {}
        cols = desc.get("columns") or []
        rows = desc.get("data") or []
        if not cols or not rows:
            return None
        name_idx = cols.index("name") if "name" in cols else -1
        value_idx = cols.index("value") if "value" in cols else -1
        if name_idx < 0 or value_idx < 0:
            return None
        probe = {}
        for row in rows:
            if name_idx >= len(row) or value_idx >= len(row):
                continue
            probe[str(row[name_idx] or "").upper()] = str(row[value_idx] or "").lower()
        blob = " ".join(
            filter(
                None,
                [
                    probe.get("TYPE", ""),
                    probe.get("GROUP", ""),
                    probe.get("SECURITYTYPE", ""),
                ],
            )
        )
        if "bond" in blob or "облигац" in blob:
            return "bond"
    except Exception:
        return None
    return None


def _detect_tbank_instrument_kind(symbol: str) -> Optional[str]:
    up = (symbol or "").strip().upper()
    if not up:
        return None
    token = _get_tbank_token()
    if not token:
        return None
    url = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"query": up, "instrumentKind": "INSTRUMENT_TYPE_UNSPECIFIED"}
    try:
        r = _tbank_post(url, headers=headers, payload=payload, timeout=8)
        if not r.ok:
            return None
        data = r.json() or {}
        instruments = data.get("instruments", [])
        if not instruments:
            return None
        exact = [i for i in instruments if str(i.get("ticker") or "").upper() == up]
        picked = exact[0] if exact else instruments[0]
        kind = str(picked.get("instrumentType") or "").strip().lower()
        return kind or None
    except Exception:
        return None


def _detect_instrument_kind(
    ticker: str,
    provider: Optional[str] = None,
    provider_symbol: Optional[str] = None,
) -> Optional[str]:
    prov = (provider or "").strip().lower()
    sym = (provider_symbol or "").strip() or (ticker or "").upper().strip()
    if not prov:
        prov, sym_cached = _get_provider_symbol_cached(ticker)
        sym = sym or sym_cached
    if not prov or not sym:
        return None
    key = (prov, sym.upper())
    if key in _INSTRUMENT_KIND_BY_PROVIDER_SYMBOL:
        return _INSTRUMENT_KIND_BY_PROVIDER_SYMBOL[key]
    kind: Optional[str] = None
    if prov == "tbank":
        kind = _detect_tbank_instrument_kind(sym)
        if not kind:
            kind = _detect_moex_instrument_kind(sym)
    elif prov == "moex_iss":
        kind = _detect_moex_instrument_kind(sym)
    if not kind and _ISIN_RE.match((ticker or "").upper().strip()):
        # Fallback for ISIN-like symbols when provider metadata is unavailable.
        kind = "bond"
    _INSTRUMENT_KIND_BY_PROVIDER_SYMBOL[key] = kind
    return kind


def normalize_quote_price_for_valuation(
    ticker: str,
    price: Optional[float],
    currency: Optional[str],
    provider: Optional[str] = None,
    provider_symbol: Optional[str] = None,
) -> Optional[float]:
    """
    Convert raw quote to per-unit valuation price.
    For many RU bonds, quote is in % of nominal (100 = 100% of 1000 RUB).
    """
    if price is None:
        return None
    p = float(price)
    ccy = _normalize_currency_code(currency) or ""
    prov = (provider or "").strip().lower()
    sym = (provider_symbol or "").strip()
    if not prov:
        prov, sym_cached = _get_provider_symbol_cached(ticker)
        if not sym:
            sym = sym_cached
    kind = _detect_instrument_kind(ticker, provider=prov, provider_symbol=sym)
    if kind != "bond":
        return p
    # RU bonds from MOEX/T-Bank are typically quoted as % of 1000 RUB nominal.
    if ccy == "RUB" and prov in ("moex_iss", "tbank") and 0 < p <= 300:
        return p * 10.0
    return p


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
    if provider in ("moex_iss", "tbank"):
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
            for key in (
                "LAST",
                "MARKETPRICE2",
                "MARKETPRICE",
                "LCURRENTPRICE",
                "LASTBID",
                "LASTOFFER",
                "WAPRICE",
                "PREVPRICE",
                "OPEN",
            ):
                if key in col_map and col_map[key] < len(row_md) and row_md[col_map[key]] is not None:
                    try:
                        return PriceQuote(price=float(row_md[col_map[key]]), currency=ccy)
                    except (TypeError, ValueError):
                        pass
            # Some boards expose only reference price in securities table.
            sec_map = {c: j for j, c in enumerate(cols_sec)}
            for key in ("PREVPRICE", "PREVWAPRICE", "PREVLEGALCLOSEPRICE"):
                idx = sec_map.get(key, -1)
                if idx >= 0 and idx < len(row) and row[idx] is not None:
                    try:
                        return PriceQuote(price=float(row[idx]), currency=ccy)
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
                for key in (
                    "LAST",
                    "MARKETPRICE2",
                    "MARKETPRICE",
                    "LCURRENTPRICE",
                    "LASTBID",
                    "LASTOFFER",
                    "WAPRICE",
                    "PREVPRICE",
                    "OPEN",
                ):
                    if key in cols:
                        idx = cols.index(key)
                        if idx < len(row) and row[idx] is not None:
                            try:
                                return PriceQuote(price=float(row[idx]), currency=ccy)
                            except (TypeError, ValueError):
                                pass
            if rows2:
                sec_row = rows2[0]
                for key in ("PREVPRICE", "PREVWAPRICE", "PREVLEGALCLOSEPRICE"):
                    if key in cols2:
                        idx = cols2.index(key)
                        if idx < len(sec_row) and sec_row[idx] is not None:
                            try:
                                return PriceQuote(price=float(sec_row[idx]), currency=ccy)
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


def _parse_tbank_money(raw: object) -> Tuple[Optional[float], Optional[str]]:
    """
    Parse T-Bank MoneyValue payload.
    Returns (amount, currency_code_or_none).
    """
    if not isinstance(raw, dict):
        return None, None
    try:
        units = float(raw.get("units", 0) or 0)
        nano = float(raw.get("nano", 0) or 0)
        amount = units + nano / 1_000_000_000.0
    except Exception:
        return None, None
    ccy = _normalize_currency_code(raw.get("currency"))
    return amount, ccy


def _get_tbank_token() -> Optional[str]:
    keys = ("T_INVEST_TOKEN", "TINKOFF_INVEST_TOKEN", "INVEST_TOKEN", "TBANK_INVEST_TOKEN")
    for key in keys:
        tok = (os.environ.get(key) or "").strip()
        if tok:
            return tok
    # Fallback to local .env in project root (keeps secrets out of git via .gitignore).
    env_path = Path(__file__).resolve().parents[2] / ".env"
    try:
        if env_path.exists():
            for raw in env_path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                if k not in keys:
                    continue
                token = v.strip().strip("'\"")
                if token:
                    return token
    except Exception:
        pass
    return None


def _tbank_post(url: str, headers: dict, payload: dict, timeout: int = 8):
    """
    T-Bank POST with secure-first strategy.
    If local cert trust is broken, retry once without verify to keep functionality.
    """
    try:
        return _HTTP.post(url, headers=headers, json=payload, timeout=timeout)
    except requests.exceptions.SSLError:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
            return _HTTP.post(url, headers=headers, json=payload, timeout=timeout, verify=False)


def _tbank_find_instrument(symbol: str) -> Optional[tuple]:
    """
    Resolve symbol to (figi, currency) via T-Bank API.
    Caches results in-process for faster repeated calls.
    """
    up = symbol.strip().upper()
    if not up:
        return None
    cached = _TBANK_FIGI_BY_SYMBOL.get(up)
    if cached:
        return cached
    # Allow passing FIGI directly in provider_symbol to avoid ambiguous ticker lookup.
    if up.startswith("BBG") or up.startswith("TCS") or up.startswith("FUT"):
        val = (up, _fallback_currency("tbank"))
        _TBANK_FIGI_BY_SYMBOL[up] = val
        return val

    token = _get_tbank_token()
    if not token:
        return None
    url = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"query": up, "instrumentKind": "INSTRUMENT_TYPE_UNSPECIFIED"}
    try:
        r = _tbank_post(url, headers=headers, payload=payload, timeout=8)
        if not r.ok:
            return None
        data = r.json() or {}
        instruments = data.get("instruments", [])
        if not instruments:
            return None
        exact = [i for i in instruments if str(i.get("ticker") or "").upper() == up]
        candidates = exact if exact else instruments

        def _rank_tbank_candidate(item: dict) -> tuple:
            # Lower rank is better.
            class_code = str(item.get("classCode") or "").upper()
            kind = str(item.get("instrumentType") or "").strip().lower()
            ticker_exact = 0 if str(item.get("ticker") or "").upper() == up else 1
            # Prefer liquid MOEX boards and ETF board for fund tickers like TEUS.
            board_rank = 5
            if class_code == "TQTF":
                board_rank = 0
            elif class_code in ("TQBR", "TQCB", "TQIF", "TQTD", "TQIR"):
                board_rank = 1
            elif class_code.startswith("TQ"):
                board_rank = 2
            elif class_code.startswith("SPB"):
                board_rank = 3
            kind_rank = 0 if kind in ("etf", "share", "bond", "currency") else 1
            # Keep deterministic fallback.
            figi = str(item.get("figi") or "")
            return (ticker_exact, board_rank, kind_rank, figi)

        picked = sorted(candidates, key=_rank_tbank_candidate)[0]
        figi = (picked.get("figi") or "").strip()
        if not figi:
            return None
        ccy = _normalize_currency_code(picked.get("currency")) or _fallback_currency("tbank")
        val = (figi, ccy)
        _TBANK_FIGI_BY_SYMBOL[up] = val
        return val
    except Exception:
        return None


def _price_tbank(symbol: str) -> PriceQuote:
    """
    Quote from T-Bank:
    - resolve symbol -> FIGI
    - fetch last price by FIGI
    """
    fallback = _fallback_currency("tbank")
    token = _get_tbank_token()
    if not token:
        # If T-Bank token is unavailable, try MOEX quote by symbol.
        moex_q = _price_moex_iss(symbol)
        if moex_q.price is not None:
            return moex_q
        return PriceQuote(price=None, currency=fallback)
    resolved = _tbank_find_instrument(symbol)
    if not resolved:
        moex_q = _price_moex_iss(symbol)
        if moex_q.price is not None:
            return moex_q
        return PriceQuote(price=None, currency=fallback)
    figi, ccy = resolved
    url = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.MarketDataService/GetLastPrices"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = _tbank_post(url, headers=headers, payload={"instrumentId": [figi]}, timeout=8)
        if not r.ok:
            moex_q = _price_moex_iss(symbol)
            if moex_q.price is not None:
                return moex_q
            return PriceQuote(price=None, currency=ccy or fallback)
        data = r.json() or {}
        rows = data.get("lastPrices", [])
        if not rows:
            moex_q = _price_moex_iss(symbol)
            if moex_q.price is not None:
                return moex_q
            return PriceQuote(price=None, currency=ccy or fallback)
        px, px_ccy = _parse_tbank_money((rows[0] or {}).get("price"))
        if px is None:
            moex_q = _price_moex_iss(symbol)
            if moex_q.price is not None:
                return moex_q
        return PriceQuote(price=px, currency=px_ccy or ccy or fallback)
    except Exception:
        moex_q = _price_moex_iss(symbol)
        if moex_q.price is not None:
            return moex_q
        return PriceQuote(price=None, currency=ccy or fallback)


def _resolve_provider_symbol(
    ticker: str,
    provider_overrides: Optional[dict] = None,
) -> Tuple[str, str]:
    overrides = provider_overrides or {}
    t_norm = (ticker or "").upper().strip()
    ov = overrides.get(ticker)
    if ov is None:
        ov = overrides.get(t_norm)
    if ov:
        provider = (ov[0] or "").strip().lower() if isinstance(ov[0], str) else ""
        symbol = (ov[1] or "").strip() if len(ov) > 1 else ""
        if provider:
            return provider, symbol or t_norm
    return _detect_provider(t_norm or ticker)


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
    if provider == "tbank":
        return _price_tbank(symbol)
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
    if provider == "tbank":
        # Do not fallback to Yahoo for explicitly configured T-Bank instruments.
        # Most T-Bank exchange symbols are MOEX-traded; try MOEX history first.
        moex_hist = fetch_historical_prices_moex(symbol, date_from, date_to)
        if moex_hist:
            return moex_hist
        return {}
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
    force_fetch: bool = False,
) -> Dict[str, PriceQuote]:
    """{ticker: PriceQuote} с учётом session_state кэша."""
    import streamlit as st

    now = time.time()
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    meta_key = f"{session_state_key}_meta"
    if session_state_key not in st.session_state:
        st.session_state[session_state_key] = {"ts": 0, "data": {}}
    cache = st.session_state[session_state_key]
    overrides = provider_overrides or {}
    result: Dict[str, PriceQuote] = {}

    # Fast path for disabled live updates: reuse cache only when it already contains
    # all requested tickers. If some are missing, fall through and fetch missing ones.
    if (
        not live_updates_enabled
        and not force_fetch
        and cache["data"]
        and all(t in cache["data"] for t in tickers)
    ):
        for t in tickers:
            if t in cache["data"]:
                result[t] = _unpack_cached_quote(t, cache["data"][t])
            else:
                prov, _sym = _resolve_provider_symbol(t, provider_overrides=overrides)
                result[t] = PriceQuote(price=None, currency=_fallback_currency(prov))
        providers_in_use = sorted({_resolve_provider_symbol(t, provider_overrides=overrides)[0] for t in tickers})
        st.session_state[meta_key] = {
            "ts": float(cache.get("ts") or now),
            "providers": providers_in_use,
            "tickers_count": len(set(tickers)),
            "stale_tickers": [],
            "live_updates_enabled": False,
        }
        return result

    def _fetch_parallel(plan_items: List[Tuple[str, Tuple[str, str]]]) -> Dict[str, PriceQuote]:
        if not plan_items:
            return {}
        out: Dict[str, PriceQuote] = {}
        workers = max(1, min(16, len(plan_items)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_to_ticker = {
                ex.submit(
                    fetch_price_quote,
                    t,
                    provider_override=prov,
                    provider_symbol_override=sym,
                ): t
                for t, (prov, sym) in plan_items
            }
            for fut in as_completed(fut_to_ticker):
                t = fut_to_ticker[fut]
                try:
                    out[t] = fut.result()
                except Exception:
                    prov, _sym = dict(plan_items).get(t, ("yfinance", t))
                    out[t] = PriceQuote(price=None, currency=_fallback_currency(prov))
        return out

    if cache_ttl_sec <= 0:
        missing = list(dict.fromkeys(tickers))
        plan = {t: _resolve_provider_symbol(t, provider_overrides=overrides) for t in missing}
        cg_targets = {t: sym for t, (prov, sym) in plan.items() if prov == "coingecko"}
        if cg_targets:
            cg_quotes = _price_coingecko_many(list(cg_targets.values()))
            for t, coin_id in cg_targets.items():
                result[t] = cg_quotes.get(coin_id, PriceQuote(price=None, currency="USD"))
        non_batch = [(t, plan[t]) for t in missing if t not in result]
        result.update(_fetch_parallel(non_batch))
        st.session_state[meta_key] = {
            "ts": now,
            "providers": sorted({prov for prov, _ in plan.values()}),
            "tickers_count": len(missing),
            "stale_tickers": [],
        }
        return result

    refresh_expired = cache["ts"] + cache_ttl_sec < now
    if refresh_expired or not cache["data"]:
        # Keep previous quotes as fallback if provider temporarily fails.
        cache["ts"] = now

    missing: List[str] = []
    for t in tickers:
        if not refresh_expired and t in cache["data"]:
            result[t] = _unpack_cached_quote(t, cache["data"][t])
            continue
        missing.append(t)

    stale_tickers = set()
    if missing:
        plan = {t: _resolve_provider_symbol(t, provider_overrides=overrides) for t in missing}
        cg_targets = {t: sym for t, (prov, sym) in plan.items() if prov == "coingecko"}
        if cg_targets:
            cg_quotes = _price_coingecko_many(list(cg_targets.values()))
            for t, coin_id in cg_targets.items():
                q = cg_quotes.get(coin_id, PriceQuote(price=None, currency="USD"))
                prev = cache["data"].get(t)
                prev_q = _unpack_cached_quote(t, prev) if prev is not None else None
                if q.price is None and prev_q and prev_q.price is not None:
                    prov, _sym = plan.get(t, ("coingecko", coin_id))
                    expected_ccy = _fallback_currency(prov)
                    if _normalize_currency_code(prev_q.currency) == expected_ccy:
                        q = prev_q
                        stale_tickers.add(t)
                cache["data"][t] = q
                result[t] = q
        non_batch = [(t, plan[t]) for t in missing if t not in result]
        fetched_parallel = _fetch_parallel(non_batch)
        for t, q in fetched_parallel.items():
            prev = cache["data"].get(t)
            prev_q = _unpack_cached_quote(t, prev) if prev is not None else None
            if q.price is None and prev_q and prev_q.price is not None:
                prov, _sym = plan.get(t, ("yfinance", t))
                expected_ccy = _fallback_currency(prov)
                if _normalize_currency_code(prev_q.currency) == expected_ccy:
                    q = prev_q
                    stale_tickers.add(t)
            cache["data"][t] = q
            result[t] = q
    providers_in_use = set()
    for t in tickers:
        prov, _ = _resolve_provider_symbol(t, provider_overrides=overrides)
        providers_in_use.add(prov)
    st.session_state[meta_key] = {
        "ts": float(cache.get("ts") or now),
        "providers": sorted(providers_in_use),
        "tickers_count": len(set(tickers)),
        "stale_tickers": sorted(stale_tickers),
        "live_updates_enabled": live_updates_enabled,
    }
    return result


def get_quotes_cache_meta(session_state_key: str = "price_cache") -> dict:
    """Метаданные кэша котировок: ts, providers, tickers_count."""
    import streamlit as st

    return dict(st.session_state.get(f"{session_state_key}_meta") or {})


def get_prices_cached(
    tickers: list,
    cache_ttl_sec: float = 120,
    session_state_key: str = "price_cache",
    provider_overrides: Optional[dict] = None,
) -> dict:
    """Только цены (совместимость)."""
    q = get_quotes_cached(tickers, cache_ttl_sec, session_state_key, provider_overrides)
    return {k: v.price for k, v in q.items()}


def build_provider_overrides(tickers: List[str]) -> Dict[str, Tuple[str, str]]:
    """
    Build provider map from instruments table for a ticker set.
    Output format is compatible with get_quotes_cached provider_overrides.
    """
    from app.db import get_instrument_provider

    out: Dict[str, Tuple[str, str]] = {}
    for raw in tickers:
        t = (raw or "").upper().strip()
        if not t:
            continue
        row = get_instrument_provider(t)
        if not row:
            continue
        provider = str(row[0] or "").strip().lower()
        if not provider:
            continue
        symbol = str(row[1] or "").strip() or t
        out[t] = (provider, symbol)
    return out


def request_quotes_refresh(session_state_key: str = "price_cache") -> None:
    """Force quotes refresh on next read by dropping quote cache."""
    import streamlit as st

    st.session_state.pop(session_state_key, None)
    st.session_state.pop(f"{session_state_key}_meta", None)
    _TBANK_FIGI_BY_SYMBOL.clear()
    _PROVIDER_SYMBOL_BY_TICKER.clear()
    _INSTRUMENT_KIND_BY_PROVIDER_SYMBOL.clear()
    st.session_state["force_price_refresh_once"] = True


def get_app_quotes(tickers: List[str], session_state_key: str = "price_cache") -> Dict[str, PriceQuote]:
    """
    Single app-wide entrypoint for quotes.
    - Source of truth for provider selection: instruments table.
    - Live mode (session toggle) refreshes every 60 seconds.
    - Disabled mode uses long-lived session cache.
    """
    import streamlit as st

    normalized = [str(t or "").upper().strip() for t in tickers if str(t or "").strip()]
    normalized = list(dict.fromkeys(normalized))
    if not normalized:
        return {}
    # Guardrail: never fetch quotes for tickers that are not in active portfolio positions.
    from app.db import list_positions_by_ticker

    portfolio_tickers = {
        str(p.ticker or "").upper().strip()
        for p in list_positions_by_ticker()
        if float(p.amount or 0) > 0
    }
    live_updates_enabled = bool(st.session_state.get("live_price_updates_enabled", False))
    force_refresh_once = bool(st.session_state.get("force_price_refresh_once", False))
    cache = st.session_state.get(session_state_key) or {}
    cache_has_data = bool((cache.get("data") if isinstance(cache, dict) else {}) or {})
    # Keep showing cached values on ordinary reruns.
    # Restrict to active portfolio tickers only when we are about to fetch from providers.
    if (not live_updates_enabled) and (not force_refresh_once) and cache_has_data:
        effective_tickers = normalized
    else:
        effective_tickers = [t for t in normalized if t in portfolio_tickers]
    if not effective_tickers:
        return {}
    ttl = LIVE_QUOTES_REFRESH_SEC if live_updates_enabled else DISABLED_QUOTES_TTL_SEC
    provider_overrides = build_provider_overrides(effective_tickers)
    quotes = get_quotes_cached(
        effective_tickers,
        cache_ttl_sec=ttl,
        session_state_key=session_state_key,
        provider_overrides=provider_overrides,
        force_fetch=force_refresh_once,
    )
    if force_refresh_once:
        st.session_state["force_price_refresh_once"] = False
    return quotes
