"""Portfolio performance engine: daily valuation + TWR with historical backfill."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Mapping, Optional, Tuple

from app.db import (
    list_transactions,
    list_cached_historical_quotes,
    upsert_historical_quotes_bulk,
)
from app.services.fx import convert_amount
from app.services.prices import (
    PriceQuote,
    fetch_historical_quotes,
    build_provider_overrides,
    normalize_quote_price_for_valuation,
)


@dataclass
class PerformancePoint:
    date: str
    portfolio_value: float
    net_cash_flow: float
    twr_cum_return: float
    priced_ratio: float


@dataclass
class PerformanceResult:
    points: List[PerformancePoint]
    missing_price_tickers: List[str]
    net_invested: float
    current_value: float
    total_pnl: float
    total_twr: float
    mwr_xirr_annualized: Optional[float]


def _parse_date_prefix(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    s = str(ts).strip()
    if len(s) >= 10:
        return s[:10]
    return None


def _iter_dates(date_from: str, date_to: str) -> List[str]:
    d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
    d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    if d1 < d0:
        return []
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _load_daily_transactions() -> Tuple[Dict[str, List[tuple]], Optional[str], Optional[str]]:
    tx_by_day: Dict[str, List[tuple]] = defaultdict(list)
    dates: List[str] = []
    for tx in list_transactions():
        d = _parse_date_prefix(tx.created_at)
        if d is None:
            continue
        tx_by_day[d].append((tx.ticker.upper(), float(tx.amount)))
        dates.append(d)
    if not dates:
        return tx_by_day, None, None
    return tx_by_day, min(dates), max(dates)


def _load_price_series_with_cache(
    ticker: str,
    date_from: str,
    date_to: str,
    provider: str,
    provider_symbol: str,
    allow_fetch_missing: bool = True,
) -> Dict[str, PriceQuote]:
    cached_rows = list_cached_historical_quotes(ticker, date_from, date_to)
    requested_provider = (provider or "").strip().lower()
    requested_symbol = (provider_symbol or "").strip().upper()
    series: Dict[str, PriceQuote] = {}
    mismatched_days = set()
    for d, p, ccy, row_provider, row_symbol in cached_rows:
        row_provider_norm = str(row_provider or "").strip().lower()
        row_symbol_norm = str(row_symbol or "").strip().upper()
        provider_matches = row_provider_norm == requested_provider
        symbol_matches = (not requested_symbol) or (row_symbol_norm == requested_symbol)
        if provider_matches and symbol_matches:
            series[d] = PriceQuote(price=p, currency=ccy)
        else:
            mismatched_days.add(d)
    requested_days = set(_iter_dates(date_from, date_to))
    missing_days = sorted(d for d in requested_days if d not in series)

    if (missing_days or mismatched_days) and allow_fetch_missing:
        fetched = fetch_historical_quotes(
            ticker=ticker,
            date_from=date_from,
            date_to=date_to,
            provider_override=provider,
            provider_symbol_override=provider_symbol,
        )
        if fetched:
            to_upsert = []
            for d, q in fetched.items():
                to_upsert.append((ticker, d, provider, provider_symbol, q.price, q.currency))
                series[d] = q
            upsert_historical_quotes_bulk(to_upsert)
    return series


def _carry_forward_prices(series: Dict[str, PriceQuote], dates: List[str]) -> Dict[str, PriceQuote]:
    out: Dict[str, PriceQuote] = {}
    last: Optional[PriceQuote] = None
    for d in dates:
        q = series.get(d)
        if q and q.price is not None:
            last = q
            out[d] = q
            continue
        if last is not None:
            out[d] = PriceQuote(price=last.price, currency=last.currency)
    return out


def compute_portfolio_performance(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    allow_fetch_missing_prices: bool = True,
) -> PerformanceResult:
    tx_by_day, first_tx_date, last_tx_date = _load_daily_transactions()
    if not first_tx_date:
        return PerformanceResult(
            points=[],
            missing_price_tickers=[],
            net_invested=0.0,
            current_value=0.0,
            total_pnl=0.0,
            total_twr=0.0,
            mwr_xirr_annualized=None,
        )

    start = first_tx_date
    end = date.today().isoformat()
    days = _iter_dates(start, end)
    all_tickers = sorted({t for day_rows in tx_by_day.values() for t, _ in day_rows})
    provider_overrides: Dict[str, Tuple[str, str]] = build_provider_overrides(all_tickers)

    prices_by_ticker: Dict[str, Dict[str, PriceQuote]] = {}
    missing_price_tickers: List[str] = []
    for t in all_tickers:
        if t in provider_overrides:
            prov, sym = provider_overrides[t]
        else:
            from app.services.prices import _detect_provider

            prov, sym = _detect_provider(t)
        raw = _load_price_series_with_cache(
            t,
            start,
            end,
            prov,
            sym,
            allow_fetch_missing=allow_fetch_missing_prices,
        )
        carried = _carry_forward_prices(raw, days)
        prices_by_ticker[t] = carried
        if not carried:
            missing_price_tickers.append(t)

    holdings: Dict[str, float] = defaultdict(float)
    net_invested = 0.0
    points: List[PerformancePoint] = []
    xirr_cashflows_by_day: Dict[str, float] = defaultdict(float)
    prev_value: Optional[float] = None
    twr_factor = 1.0

    for d in days:
        day_tx = tx_by_day.get(d, [])
        day_cash_flow = 0.0
        for ticker, amount in day_tx:
            holdings[ticker] += amount
            q = prices_by_ticker.get(ticker, {}).get(d)
            if q is None or q.price is None:
                continue
            q_price = normalize_quote_price_for_valuation(ticker, q.price, q.currency)
            if q_price is None:
                continue
            trade_value = convert_amount(
                amount=float(amount) * float(q_price),
                from_ccy=q.currency,
                to_ccy=display_currency,
                rub_per_usd=rub_per_usd,
                eur_per_usd=eur_per_usd,
            )
            day_cash_flow += trade_value
            # XIRR convention: buy = outflow (negative), sell = inflow (positive).
            xirr_cashflows_by_day[d] += -trade_value
        net_invested += day_cash_flow

        total_value = 0.0
        total_pos = 0
        priced_pos = 0
        for ticker, amount in holdings.items():
            if amount <= 0:
                continue
            total_pos += 1
            q = prices_by_ticker.get(ticker, {}).get(d)
            if q is None or q.price is None:
                continue
            q_price = normalize_quote_price_for_valuation(ticker, q.price, q.currency)
            if q_price is None:
                continue
            total_value += convert_amount(
                amount=float(amount) * float(q_price),
                from_ccy=q.currency,
                to_ccy=display_currency,
                rub_per_usd=rub_per_usd,
                eur_per_usd=eur_per_usd,
            )
            priced_pos += 1

        if prev_value is not None and prev_value > 0:
            gross = (total_value - day_cash_flow) / prev_value
            if gross > 0:
                twr_factor *= gross
        prev_value = total_value
        points.append(
            PerformancePoint(
                date=d,
                portfolio_value=float(total_value),
                net_cash_flow=float(day_cash_flow),
                twr_cum_return=float(twr_factor - 1.0),
                priced_ratio=(float(priced_pos) / float(total_pos)) if total_pos > 0 else 1.0,
            )
        )

    current_value = points[-1].portfolio_value if points else 0.0
    total_pnl = current_value - net_invested
    total_twr = points[-1].twr_cum_return if points else 0.0
    xirr_cashflows_by_day[end] += current_value
    xirr_flows = sorted(xirr_cashflows_by_day.items(), key=lambda x: x[0])
    mwr_xirr = compute_xirr_annualized(xirr_flows)
    return PerformanceResult(
        points=points,
        missing_price_tickers=sorted(set(missing_price_tickers)),
        net_invested=float(net_invested),
        current_value=float(current_value),
        total_pnl=float(total_pnl),
        total_twr=float(total_twr),
        mwr_xirr_annualized=mwr_xirr,
    )


def compute_period_returns(points: List[PerformancePoint]) -> Dict[str, float]:
    """Return simple period returns from value curve for dashboard chips."""
    if not points:
        return {"1M": 0.0, "3M": 0.0, "6M": 0.0, "1Y": 0.0, "YTD": 0.0, "ALL": 0.0}
    by_day = {p.date: p for p in points}
    last_day = datetime.strptime(points[-1].date, "%Y-%m-%d").date()
    last_val = points[-1].portfolio_value

    def _ret_from_days(days_back: int) -> float:
        start = (last_day - timedelta(days=days_back)).isoformat()
        cand = [d for d in by_day if d >= start]
        if not cand:
            return 0.0
        first = by_day[min(cand)].portfolio_value
        if first <= 0:
            return 0.0
        return (last_val / first) - 1.0

    ytd_start = date(last_day.year, 1, 1).isoformat()
    ytd_cand = [d for d in by_day if d >= ytd_start]
    ytd_first = by_day[min(ytd_cand)].portfolio_value if ytd_cand else 0.0
    all_first = points[0].portfolio_value
    return {
        "1M": _ret_from_days(30),
        "3M": _ret_from_days(90),
        "6M": _ret_from_days(180),
        "1Y": _ret_from_days(365),
        "YTD": ((last_val / ytd_first) - 1.0) if ytd_first > 0 else 0.0,
        "ALL": ((last_val / all_first) - 1.0) if all_first > 0 else 0.0,
    }


def compute_twr_from_daily_values(values: List[float], cash_flows: List[float]) -> float:
    """
    Compute cumulative TWR from aligned daily series:
    factor_t = (V_t - CF_t) / V_{t-1}.
    """
    if not values or len(values) != len(cash_flows):
        return 0.0
    factor = 1.0
    prev = None
    for v, cf in zip(values, cash_flows):
        if prev is not None and prev > 0:
            gross = (float(v) - float(cf)) / float(prev)
            if gross > 0:
                factor *= gross
        prev = float(v)
    return factor - 1.0


def compute_xirr_annualized(
    dated_cashflows: List[Tuple[str, float]],
    tol: float = 1e-8,
    max_iter: int = 200,
) -> Optional[float]:
    """
    Annualized XIRR for irregular dated cash flows.
    `dated_cashflows` format: [("YYYY-MM-DD", amount), ...]
    """
    if len(dated_cashflows) < 2:
        return None
    amounts = [float(a) for _d, a in dated_cashflows]
    if not (any(a > 0 for a in amounts) and any(a < 0 for a in amounts)):
        return None

    parsed = []
    for d, a in dated_cashflows:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
        except ValueError:
            return None
        parsed.append((dt, float(a)))
    parsed.sort(key=lambda x: x[0])
    t0 = parsed[0][0]
    years = [((dt - t0).days / 365.0) for dt, _a in parsed]
    amts = [a for _dt, a in parsed]

    def _xnpv(rate: float) -> float:
        if rate <= -0.999999999:
            return float("inf")
        s = 0.0
        for cf, y in zip(amts, years):
            s += cf / ((1.0 + rate) ** y)
        return s

    lo = -0.9999
    hi = 1.0
    f_lo = _xnpv(lo)
    f_hi = _xnpv(hi)
    expand = 0
    while f_lo * f_hi > 0 and expand < 50:
        hi *= 2.0
        f_hi = _xnpv(hi)
        expand += 1
        if hi > 1e6:
            break
    if f_lo * f_hi > 0:
        return None

    if max_iter <= 0:
        return None
    for _ in range(max_iter):
        mid = (lo + hi) / 2.0
        f_mid = _xnpv(mid)
        if abs(f_mid) < tol:
            return mid
        if f_lo * f_mid <= 0:
            hi = mid
            f_hi = f_mid
        else:
            lo = mid
            f_lo = f_mid
    return None
