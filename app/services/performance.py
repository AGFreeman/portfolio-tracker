"""Portfolio performance engine: daily valuation + TWR with historical backfill."""
from __future__ import annotations

import json
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Mapping, Optional, Tuple

from app.db import (
    get_app_setting,
    list_cash_flows,
    list_transactions,
    list_cached_historical_quotes,
)
from app.services.fx import convert_amount
from app.services.prices import (
    PriceQuote,
    build_provider_overrides,
    is_excluded_from_coverage_metric,
    normalize_quote_price_for_valuation,
)


@dataclass
class PerformancePoint:
    date: str
    portfolio_value: float
    net_cash_flow: float
    twr_cum_return: float
    mwr_cum_return: Optional[float]
    priced_ratio: float
    benchmark_value: Optional[float] = None
    benchmark_cum_return: Optional[float] = None
    benchmark_mwr_cum_return: Optional[float] = None


@dataclass
class PerformanceResult:
    points: List[PerformancePoint]
    missing_price_tickers: List[str]
    net_invested: float
    current_value: float
    total_pnl: float
    total_twr: float
    mwr_xirr_annualized: Optional[float]
    benchmark_mwr_xirr_annualized: Optional[float] = None
    benchmark_ticker: Optional[str] = None
    benchmark_current_value: Optional[float] = None
    benchmark_total_return: Optional[float] = None
    benchmark_delta_value: Optional[float] = None


_DEFAULT_MONEY_MARKET_BENCHMARKS: Dict[str, str] = {
    # currency -> benchmark ticker
    "RUB": "LQDT",
    "USD": "IB01",
    "EUR": "XEON.DE",
}


def _get_money_market_benchmark_for_currency(display_currency: str) -> Optional[str]:
    ccy = str(display_currency or "").upper().strip()
    if not ccy:
        return None
    return _DEFAULT_MONEY_MARKET_BENCHMARKS.get(ccy)


def _append_dated_cashflow(
    rows: List[Tuple[str, float]], day: str, amount: float
) -> None:
    """Append or merge cashflow for `day` (days are processed in chronological order)."""
    if abs(float(amount)) <= 1e-12:
        return
    if rows and rows[-1][0] == day:
        rows[-1] = (day, float(rows[-1][1]) + float(amount))
    else:
        rows.append((day, float(amount)))


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
    """
    Load transactions grouped by day.
    Row shape: (ticker, amount, transaction_type).
    """
    tx_by_day: Dict[str, List[tuple]] = defaultdict(list)
    dates: List[str] = []
    for tx in list_transactions():
        d = _parse_date_prefix(tx.created_at)
        if d is None:
            continue
        tx_by_day[d].append(
            (
                tx.ticker.upper(),
                float(tx.amount),
                str(tx.transaction_type or "trade").strip().lower(),
            )
        )
        dates.append(d)
    if not dates:
        return tx_by_day, None, None
    return tx_by_day, min(dates), max(dates)


def _load_daily_manual_cash_flows(
    ) -> Tuple[Dict[str, List[Tuple[float, str]]], Optional[str], Optional[str]]:
    """Load manual portfolio cash flows by day in source currency."""
    flows_by_day: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
    dates: List[str] = []
    for flow in list_cash_flows():
        d = _parse_date_prefix(flow.flow_date)
        if d is None:
            continue
        flows_by_day[d].append((float(flow.amount), str(flow.currency or "RUB").upper()))
        dates.append(d)
    if not dates:
        return flows_by_day, None, None
    return flows_by_day, min(dates), max(dates)


_HISTORICAL_FX_SETTING_KEY = "historical_fx_v1"


def _load_fx_exact_from_db(date_from: str, date_to: str) -> Dict[str, Tuple[float, float]]:
    """Load pre-built FX series from app_settings (filled by backfill script)."""
    raw = get_app_setting(_HISTORICAL_FX_SETTING_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    out: Dict[str, Tuple[float, float]] = {}
    for day, pair in data.items():
        if not (date_from <= str(day) <= date_to):
            continue
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        rub, eur = float(pair[0]), float(pair[1])
        if rub > 0 and eur > 0:
            out[str(day)] = (rub, eur)
    return out


def _load_price_series_from_cache(
    ticker: str,
    date_from: str,
    date_to: str,
) -> Dict[str, PriceQuote]:
    """Read historical quotes from DB only (no network)."""
    series: Dict[str, PriceQuote] = {}
    for d, p, ccy in list_cached_historical_quotes(ticker, date_from, date_to):
        series[d] = PriceQuote(price=p, currency=ccy)
    return series


def _build_as_of_price_index(
    series: Mapping[str, PriceQuote],
) -> Tuple[List[str], List[PriceQuote]]:
    """Sorted trading-day quotes for bisect lookup (last quote on or before day D)."""
    dates = sorted(series.keys())
    return dates, [series[d] for d in dates]


def _quote_as_of(
    dates: List[str],
    quotes: List[PriceQuote],
    day: str,
) -> Optional[PriceQuote]:
    if not dates:
        return None
    idx = bisect_right(dates, day) - 1
    if idx < 0:
        return None
    q = quotes[idx]
    if q.price is None:
        return None
    return q


def _holdings_value_as_of_day(
    holdings: Mapping[str, float],
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], List[PriceQuote]]],
    day: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[float, int, int]:
    """Sum holdings using the latest available quote on or before `day` per ticker."""
    total_value = 0.0
    total_pos = 0
    priced_pos = 0
    for ticker, amount in holdings.items():
        if float(amount) <= 0:
            continue
        dates, quotes = as_of_index_by_ticker.get(ticker, ([], []))
        q = _quote_as_of(dates, quotes, day)
        if is_excluded_from_coverage_metric(ticker):
            if q is not None:
                total_value += convert_amount(
                    amount=float(amount) * float(q.price),
                    from_ccy=q.currency,
                    to_ccy=display_currency,
                    rub_per_usd=rub_per_usd,
                    eur_per_usd=eur_per_usd,
                )
            continue
        total_pos += 1
        if q is None:
            continue
        total_value += convert_amount(
            amount=float(amount) * float(q.price),
            from_ccy=q.currency,
            to_ccy=display_currency,
            rub_per_usd=rub_per_usd,
            eur_per_usd=eur_per_usd,
        )
        priced_pos += 1
    return float(total_value), priced_pos, total_pos


def _build_active_intervals_by_ticker(
    tx_by_day: Dict[str, List[tuple]],
    days: List[str],
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Build contiguous date intervals where ticker position is strictly > 0.
    """
    deltas_by_ticker: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for d, rows in tx_by_day.items():
        for ticker, amount, _tx_type in rows:
            deltas_by_ticker[ticker][d] += float(amount)

    intervals: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for ticker, day_deltas in deltas_by_ticker.items():
        qty = 0.0
        active_start: Optional[str] = None
        prev_day: Optional[str] = None
        for d in days:
            qty += float(day_deltas.get(d, 0.0))
            if qty > 0 and active_start is None:
                active_start = d
            if qty <= 0 and active_start is not None:
                end_day = prev_day if prev_day is not None else d
                intervals[ticker].append((active_start, end_day))
                active_start = None
            prev_day = d
        if active_start is not None:
            intervals[ticker].append((active_start, days[-1]))
    return intervals


def _sum_holdings_market_value(
    holdings: Mapping[str, float],
    quotes: Mapping[str, PriceQuote],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[float, int, int]:
    """Sum position values using live quotes (same rules as portfolio summary header)."""
    total_value = 0.0
    total_pos = 0
    priced_pos = 0
    for ticker, amount in holdings.items():
        if float(amount) <= 0:
            continue
        q = quotes.get(ticker)
        q_price = None
        if q is not None and q.price is not None:
            q_price = normalize_quote_price_for_valuation(ticker, q.price, q.currency)
        if is_excluded_from_coverage_metric(ticker):
            if q_price is not None:
                total_value += convert_amount(
                    amount=float(amount) * float(q_price),
                    from_ccy=q.currency,
                    to_ccy=display_currency,
                    rub_per_usd=rub_per_usd,
                    eur_per_usd=eur_per_usd,
                )
            continue
        total_pos += 1
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
    return float(total_value), priced_pos, total_pos


def _normalize_price_series(
    ticker: str,
    series: Dict[str, PriceQuote],
) -> Dict[str, PriceQuote]:
    """Normalize bond % quotes once per ticker (not per day in the main loop)."""
    if not series:
        return series
    out: Dict[str, PriceQuote] = {}
    for d, q in series.items():
        if q.price is None:
            out[d] = q
            continue
        norm_px = normalize_quote_price_for_valuation(ticker, q.price, q.currency)
        out[d] = PriceQuote(price=norm_px, currency=q.currency)
    return out


def compute_portfolio_performance(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    mwr_curve_frequency: str = "daily",
) -> PerformanceResult:
    tx_by_day, first_tx_date, _last_tx_date = _load_daily_transactions()
    manual_flows_by_day, first_manual_flow_date, _last_manual_flow_date = _load_daily_manual_cash_flows()
    if not first_tx_date and not first_manual_flow_date:
        return PerformanceResult(
            points=[],
            missing_price_tickers=[],
            net_invested=0.0,
            current_value=0.0,
            total_pnl=0.0,
            total_twr=0.0,
            mwr_xirr_annualized=None,
        )

    start_candidates = [d for d in (first_tx_date, first_manual_flow_date) if d]
    start = min(start_candidates) if start_candidates else date.today().isoformat()
    end = date.today().isoformat()
    days = _iter_dates(start, end)
    fx_exact = _load_fx_exact_from_db(start, end)
    all_tickers = sorted({t for day_rows in tx_by_day.values() for t, _amount, _tx_type in day_rows})
    active_intervals_by_ticker = _build_active_intervals_by_ticker(tx_by_day, days) if days else {}

    benchmark_cfg = _get_money_market_benchmark_for_currency(display_currency)
    benchmark_ticker: Optional[str] = None
    benchmark_prices: Dict[str, PriceQuote] = {}
    if benchmark_cfg is not None:
        benchmark_ticker = benchmark_cfg
        benchmark_raw = _load_price_series_from_cache(benchmark_ticker, start, end)
        benchmark_raw = {
            d: q
            for d, q in benchmark_raw.items()
            if str(q.currency or "").upper() == str(display_currency or "").upper()
        }
        benchmark_prices = _normalize_price_series(benchmark_ticker, benchmark_raw)

    prices_by_ticker: Dict[str, Dict[str, PriceQuote]] = {}
    as_of_index_by_ticker: Dict[str, Tuple[List[str], List[PriceQuote]]] = {}
    missing_price_tickers: List[str] = []
    for t in all_tickers:
        ticker_series: Dict[str, PriceQuote] = {}
        intervals = active_intervals_by_ticker.get(t, [])
        for i_start, i_end in intervals:
            ticker_series.update(_load_price_series_from_cache(t, i_start, i_end))
        prices_by_ticker[t] = _normalize_price_series(t, ticker_series)
        as_of_index_by_ticker[t] = _build_as_of_price_index(prices_by_ticker[t])
        if intervals and not ticker_series and not is_excluded_from_coverage_metric(t):
            missing_price_tickers.append(t)

    holdings: Dict[str, float] = defaultdict(float)
    net_invested = 0.0
    points: List[PerformancePoint] = []
    xirr_flow_rows: List[Tuple[str, float]] = []
    benchmark_xirr_flow_rows: List[Tuple[str, float]] = []
    prev_value: Optional[float] = None
    twr_factor = 1.0
    benchmark_units = 0.0
    benchmark_cash_balance = 0.0
    prev_benchmark_value: Optional[float] = None
    prev_benchmark_instrument_value = 0.0
    benchmark_twr_factor = 1.0
    first_cashflow_day: Optional[str] = None
    mwr_anchor_days = _build_mwr_anchor_days(days, mwr_curve_frequency)
    recent_fx: Tuple[float, float] = (float(rub_per_usd), float(eur_per_usd))
    last_full_benchmark_value: Optional[float] = None

    for d in days:
        day_tx = tx_by_day.get(d, [])
        if d in fx_exact:
            recent_fx = fx_exact[d]
        day_rub_per_usd, day_eur_per_usd = recent_fx

        day_external_cash_flow = 0.0
        for amount, from_ccy in manual_flows_by_day.get(d, []):
            day_external_cash_flow += convert_amount(
                amount=float(amount),
                from_ccy=from_ccy,
                to_ccy=display_currency,
                rub_per_usd=day_rub_per_usd,
                eur_per_usd=day_eur_per_usd,
            )
        net_invested += day_external_cash_flow
        benchmark_cash_balance += day_external_cash_flow
        # XIRR convention: portfolio deposit is investor outflow (negative).
        _append_dated_cashflow(xirr_flow_rows, d, -day_external_cash_flow)
        _append_dated_cashflow(benchmark_xirr_flow_rows, d, -day_external_cash_flow)
        if first_cashflow_day is None and xirr_flow_rows:
            first_cashflow_day = d

        for ticker, amount, _tx_type in day_tx:
            holdings[ticker] += amount

        total_pos = 0
        priced_pos = 0
        exact_value = 0.0

        if d == end:
            try:
                from app.services.prices import get_app_quotes

                active_live_tickers = sorted(
                    t for t, amount in holdings.items() if float(amount) > 0
                )
                live_quotes = get_app_quotes(active_live_tickers)
                exact_value, priced_pos, total_pos = _sum_holdings_market_value(
                    holdings,
                    live_quotes,
                    display_currency,
                    rub_per_usd,
                    eur_per_usd,
                )
                if priced_pos <= 0:
                    raise RuntimeError("live quotes unavailable")
            except Exception:
                exact_value, priced_pos, total_pos = _holdings_value_as_of_day(
                    holdings,
                    as_of_index_by_ticker,
                    d,
                    display_currency,
                    day_rub_per_usd,
                    day_eur_per_usd,
                )
        else:
            exact_value, priced_pos, total_pos = _holdings_value_as_of_day(
                holdings,
                as_of_index_by_ticker,
                d,
                display_currency,
                day_rub_per_usd,
                day_eur_per_usd,
            )

        total_value = float(exact_value)
        priced_ratio = (float(priced_pos) / float(total_pos)) if total_pos > 0 else 1.0

        benchmark_value: Optional[float] = None
        benchmark_cum_return: Optional[float] = None
        benchmark_mwr_cum_return: Optional[float] = None
        bq = benchmark_prices.get(d) if benchmark_ticker else None
        bench_fx = recent_fx
        if bq is not None and bq.price is not None:
            b_price = float(bq.price)
            if b_price > 0:
                if abs(float(benchmark_cash_balance)) > 1e-12:
                    benchmark_cash_in_quote = convert_amount(
                        amount=float(benchmark_cash_balance),
                        from_ccy=display_currency,
                        to_ccy=bq.currency,
                        rub_per_usd=bench_fx[0],
                        eur_per_usd=bench_fx[1],
                    )
                    benchmark_units += float(benchmark_cash_in_quote) / float(b_price)
                    benchmark_cash_balance = 0.0
                benchmark_value = convert_amount(
                    amount=float(benchmark_units) * float(b_price),
                    from_ccy=bq.currency,
                    to_ccy=display_currency,
                    rub_per_usd=bench_fx[0],
                    eur_per_usd=bench_fx[1],
                )
                prev_benchmark_instrument_value = float(benchmark_value)
                last_full_benchmark_value = float(benchmark_value)
        elif last_full_benchmark_value is not None:
            benchmark_value = float(last_full_benchmark_value) + float(benchmark_cash_balance)
            benchmark_cash_balance = 0.0
        else:
            benchmark_value = float(prev_benchmark_instrument_value) + float(benchmark_cash_balance)
        if prev_benchmark_value is not None and prev_benchmark_value > 0:
            benchmark_gross = (float(benchmark_value) - float(day_external_cash_flow)) / float(prev_benchmark_value)
            if benchmark_gross > 0:
                benchmark_twr_factor *= benchmark_gross
        prev_benchmark_value = float(benchmark_value)
        benchmark_cum_return = float(benchmark_twr_factor - 1.0)
        if first_cashflow_day is not None and d in mwr_anchor_days:
            benchmark_xirr_flows = [*benchmark_xirr_flow_rows, (d, float(benchmark_value))]
            benchmark_xirr_annualized = compute_xirr_annualized(benchmark_xirr_flows)
            if benchmark_xirr_annualized is not None:
                benchmark_years_elapsed = _years_between(first_cashflow_day, d)
                benchmark_mwr_cum_return = _annualized_to_period_return(
                    benchmark_xirr_annualized,
                    benchmark_years_elapsed,
                )
        if prev_value is not None and prev_value > 0:
            gross = (total_value - day_external_cash_flow) / prev_value
            if gross > 0:
                twr_factor *= gross
        day_mwr_cum_return: Optional[float] = None
        if first_cashflow_day is not None and d in mwr_anchor_days:
            day_xirr_flows = [*xirr_flow_rows, (d, float(total_value))]
            day_xirr_annualized = compute_xirr_annualized(day_xirr_flows)
            if day_xirr_annualized is not None:
                years_elapsed = _years_between(first_cashflow_day, d)
                day_mwr_cum_return = _annualized_to_period_return(day_xirr_annualized, years_elapsed)
        prev_value = total_value
        points.append(
            PerformancePoint(
                date=d,
                portfolio_value=float(total_value),
                net_cash_flow=float(day_external_cash_flow),
                twr_cum_return=float(twr_factor - 1.0),
                mwr_cum_return=(float(day_mwr_cum_return) if day_mwr_cum_return is not None else None),
                priced_ratio=float(priced_ratio),
                benchmark_value=float(benchmark_value) if benchmark_ticker else None,
                benchmark_cum_return=(
                    float(benchmark_cum_return) if benchmark_ticker else None
                ),
                benchmark_mwr_cum_return=(
                    float(benchmark_mwr_cum_return)
                    if (benchmark_ticker and benchmark_mwr_cum_return is not None)
                    else None
                ),
            )
        )

    # Align current portfolio value with the same live quote path used in top UI metric.
    if points:
        try:
            from app.services.prices import get_app_quotes

            active_live_tickers = sorted(
                t for t, amount in holdings.items() if float(amount) > 0
            )
            live_quotes = get_app_quotes(active_live_tickers)
            live_value, live_priced_pos, live_total_pos = _sum_holdings_market_value(
                holdings,
                live_quotes,
                display_currency,
                rub_per_usd,
                eur_per_usd,
            )

            if live_priced_pos > 0:
                points[-1].portfolio_value = float(live_value)
                points[-1].priced_ratio = float(live_priced_pos) / float(live_total_pos)
                values = [p.portfolio_value for p in points]
                cash_flows = [p.net_cash_flow for p in points]
                points[-1].twr_cum_return = float(
                    compute_twr_from_daily_values(values, cash_flows)
                )
        except Exception:
            # Keep historical-cache based last point if live quotes are unavailable in this context.
            pass

    current_value = points[-1].portfolio_value if points else 0.0
    total_pnl = current_value - net_invested
    total_twr = points[-1].twr_cum_return if points else 0.0
    benchmark_current_value = points[-1].benchmark_value if (points and benchmark_ticker) else None
    benchmark_total_return = points[-1].benchmark_cum_return if (points and benchmark_ticker) else None
    benchmark_delta_value = (
        float(current_value) - float(benchmark_current_value)
        if benchmark_current_value is not None
        else None
    )
    xirr_flows = [*xirr_flow_rows, (end, float(current_value))]
    mwr_xirr = compute_xirr_annualized(xirr_flows)
    benchmark_mwr_xirr: Optional[float] = None
    if benchmark_current_value is not None:
        benchmark_xirr_flows = [
            *benchmark_xirr_flow_rows,
            (end, float(benchmark_current_value)),
        ]
        benchmark_mwr_xirr = compute_xirr_annualized(benchmark_xirr_flows)
    if points and mwr_xirr is not None and first_cashflow_day is not None:
        years_elapsed = _years_between(first_cashflow_day, end)
        points[-1].mwr_cum_return = float(_annualized_to_period_return(mwr_xirr, years_elapsed))
    if points and benchmark_mwr_xirr is not None and first_cashflow_day is not None:
        benchmark_years_elapsed = _years_between(first_cashflow_day, end)
        points[-1].benchmark_mwr_cum_return = float(
            _annualized_to_period_return(benchmark_mwr_xirr, benchmark_years_elapsed)
        )
    result = PerformanceResult(
        points=points,
        missing_price_tickers=sorted(set(missing_price_tickers)),
        net_invested=float(net_invested),
        current_value=float(current_value),
        total_pnl=float(total_pnl),
        total_twr=float(total_twr),
        mwr_xirr_annualized=mwr_xirr,
        benchmark_mwr_xirr_annualized=benchmark_mwr_xirr,
        benchmark_ticker=benchmark_ticker,
        benchmark_current_value=(
            float(benchmark_current_value) if benchmark_current_value is not None else None
        ),
        benchmark_total_return=(
            float(benchmark_total_return) if benchmark_total_return is not None else None
        ),
        benchmark_delta_value=(
            float(benchmark_delta_value) if benchmark_delta_value is not None else None
        ),
    )
    return result


def _years_between(date_from: str, date_to: str) -> float:
    try:
        d0 = datetime.strptime(date_from, "%Y-%m-%d").date()
        d1 = datetime.strptime(date_to, "%Y-%m-%d").date()
    except ValueError:
        return 0.0
    days = max(0, (d1 - d0).days)
    return float(days) / 365.0


def _annualized_to_period_return(annualized_rate: float, years: float) -> float:
    if years <= 0:
        return 0.0
    base = 1.0 + float(annualized_rate)
    if base <= 0:
        return 0.0
    return (base ** years) - 1.0


def _build_mwr_anchor_days(days: List[str], frequency: str) -> set[str]:
    if not days:
        return set()
    freq = str(frequency or "daily").strip().lower()
    if freq == "daily":
        return set(days)
    if freq == "monthly":
        anchors: set[str] = set()
        for i, d in enumerate(days):
            curr_month = d[:7]
            next_month = days[i + 1][:7] if i + 1 < len(days) else None
            if next_month != curr_month:
                anchors.add(d)
        return anchors
    if freq == "weekly":
        anchors = set()
        for i, d in enumerate(days):
            curr_week = datetime.strptime(d, "%Y-%m-%d").date().isocalendar()[:2]
            next_week = (
                datetime.strptime(days[i + 1], "%Y-%m-%d").date().isocalendar()[:2]
                if i + 1 < len(days)
                else None
            )
            if next_week != curr_week:
                anchors.add(d)
        return anchors
    return set(days)


def compute_period_returns(points: List[PerformancePoint]) -> Dict[str, float]:
    """
    Return simple period returns for dashboard chips.

    Period return is simple price return between start and end valuation points:
      period_return = V_end / V_start - 1
    ALL return is based on invested capital:
      all_return = V_end / NetInvested - 1
    """
    if not points:
        return {"1M": 0.0, "3M": 0.0, "6M": 0.0, "1Y": 0.0, "YTD": 0.0, "ALL": 0.0}
    by_day = {p.date: p for p in points}
    last_day = datetime.strptime(points[-1].date, "%Y-%m-%d").date()
    last_value = float(points[-1].portfolio_value)
    net_invested = float(sum(float(p.net_cash_flow) for p in points))

    def _period_return_from_start_date(start_date_iso: str) -> float:
        cand = [d for d in by_day if d >= start_date_iso]
        if not cand or not points:
            return 0.0
        start_value = float(by_day[min(cand)].portfolio_value)
        if start_value <= 0:
            return 0.0
        return (last_value / start_value) - 1.0

    def _ret_from_days(days_back: int) -> float:
        start = (last_day - timedelta(days=days_back)).isoformat()
        return _period_return_from_start_date(start)

    ytd_start = date(last_day.year, 1, 1).isoformat()
    return {
        "1M": _ret_from_days(30),
        "3M": _ret_from_days(90),
        "6M": _ret_from_days(180),
        "1Y": _ret_from_days(365),
        "YTD": _period_return_from_start_date(ytd_start),
        "ALL": ((last_value / net_invested) - 1.0) if net_invested > 0 else 0.0,
    }


def compute_benchmark_period_returns(points: List[PerformancePoint]) -> Dict[str, float]:
    """Return benchmark simple period returns for dashboard chips."""
    benchmark_points = [
        p for p in points if p.benchmark_value is not None and p.date is not None
    ]
    if not benchmark_points:
        return {"1M": 0.0, "3M": 0.0, "6M": 0.0, "1Y": 0.0, "YTD": 0.0, "ALL": 0.0}

    by_day = {p.date: p for p in benchmark_points}
    last_day = datetime.strptime(benchmark_points[-1].date, "%Y-%m-%d").date()
    last_value = float(benchmark_points[-1].benchmark_value or 0.0)
    net_invested = float(sum(float(p.net_cash_flow) for p in benchmark_points))

    def _period_return_from_start_date(start_date_iso: str) -> float:
        cand = [d for d in by_day if d >= start_date_iso]
        if not cand:
            return 0.0
        start_value = float(by_day[min(cand)].benchmark_value or 0.0)
        if start_value <= 0:
            return 0.0
        return (last_value / start_value) - 1.0

    def _ret_from_days(days_back: int) -> float:
        start = (last_day - timedelta(days=days_back)).isoformat()
        return _period_return_from_start_date(start)

    ytd_start = date(last_day.year, 1, 1).isoformat()
    return {
        "1M": _ret_from_days(30),
        "3M": _ret_from_days(90),
        "6M": _ret_from_days(180),
        "1Y": _ret_from_days(365),
        "YTD": _period_return_from_start_date(ytd_start),
        "ALL": ((last_value / net_invested) - 1.0) if net_invested > 0 else 0.0,
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
