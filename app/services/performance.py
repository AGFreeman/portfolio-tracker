"""Portfolio performance engine: daily valuation + TWR with historical backfill."""
from __future__ import annotations

import json
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from app.db import (
    get_app_setting,
    get_instrument_main_map,
    list_cash_flows,
    list_transactions,
    list_cached_historical_quotes,
)
from app.services.policy_rates import (
    daily_policy_compound_factor,
    load_policy_rate_series,
    uses_synthetic_policy_benchmark,
)
from app.services.fx import HISTORICAL_FX_SETTING_KEY, convert_amount
from app.services.price_currency import infer_quote_currency
from app.services.prices import (
    PriceQuote,
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
    main_ticker_values: Optional[Dict[str, float]] = None
    other_assets_value: Optional[float] = None
    ticker_values: Optional[Dict[str, float]] = None


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
    benchmark_first_quote_date: Optional[str] = None


_DEFAULT_MONEY_MARKET_BENCHMARKS: Dict[str, str] = {
    # currency -> benchmark ticker
    "RUB": "LQDT",
    "USD": "IB01",
    "EUR": "XEON.DE",
}

_BENCHMARK_HISTORY_START = "2013-01-01"


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


def compute_net_cash_flow_total_spot(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    flows: Optional[Iterable] = None,
) -> float:
    """Sum manual cash flows in display currency using current FX (matches Cash Flows UI)."""
    total = 0.0
    for flow in flows if flows is not None else list_cash_flows():
        total += convert_amount(
            float(flow.amount),
            str(flow.currency or "RUB").upper(),
            display_currency,
            rub_per_usd,
            eur_per_usd,
        )
    return float(total)


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


def _load_fx_exact_from_db(date_from: str, date_to: str) -> Dict[str, Tuple[float, float]]:
    """Load pre-built FX series from app_settings (filled by backfill / sync)."""
    raw = get_app_setting(HISTORICAL_FX_SETTING_KEY)
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


def _holdings_values_by_ticker_as_of_day(
    holdings: Mapping[str, float],
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], List[PriceQuote]]],
    day: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[float, int, int, Dict[str, float]]:
    """Value each holding using the latest available quote on or before `day`."""
    total_value = 0.0
    total_pos = 0
    priced_pos = 0
    ticker_values: Dict[str, float] = {}
    for ticker, amount in holdings.items():
        if float(amount) <= 0:
            continue
        dates, quotes = as_of_index_by_ticker.get(ticker, ([], []))
        q = _quote_as_of(dates, quotes, day)
        if is_excluded_from_coverage_metric(ticker):
            if q is not None:
                value = convert_amount(
                    amount=float(amount) * float(q.price),
                    from_ccy=q.currency,
                    to_ccy=display_currency,
                    rub_per_usd=rub_per_usd,
                    eur_per_usd=eur_per_usd,
                )
                total_value += value
                ticker_values[str(ticker).upper()] = float(value)
            continue
        total_pos += 1
        if q is None:
            continue
        value = convert_amount(
            amount=float(amount) * float(q.price),
            from_ccy=q.currency,
            to_ccy=display_currency,
            rub_per_usd=rub_per_usd,
            eur_per_usd=eur_per_usd,
        )
        total_value += value
        ticker_values[str(ticker).upper()] = float(value)
        priced_pos += 1
    return float(total_value), priced_pos, total_pos, ticker_values


def _holdings_value_as_of_day(
    holdings: Mapping[str, float],
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], List[PriceQuote]]],
    day: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[float, int, int]:
    """Sum holdings using the latest available quote on or before `day` per ticker."""
    total_value, priced_pos, total_pos, _ticker_values = _holdings_values_by_ticker_as_of_day(
        holdings,
        as_of_index_by_ticker,
        day,
        display_currency,
        rub_per_usd,
        eur_per_usd,
    )
    return float(total_value), priced_pos, total_pos


def split_ticker_values_by_main(
    ticker_values: Mapping[str, float],
    main_map: Mapping[str, bool],
) -> Tuple[Dict[str, float], float]:
    """Split per-ticker values into main-portfolio tickers and other assets sum."""
    main_ticker_values: Dict[str, float] = {}
    other_assets_value = 0.0
    for ticker, value in ticker_values.items():
        up = str(ticker or "").upper().strip()
        if not up:
            continue
        amount = float(value)
        if amount <= 0:
            continue
        if bool(main_map.get(up, False)):
            main_ticker_values[up] = amount
        else:
            other_assets_value += amount
    return main_ticker_values, float(other_assets_value)


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


def _current_positions_value_by_ticker(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[Dict[str, float], int, int]:
    """
    Current per-ticker values using the same rules as the portfolio summary header:
    session quote cache via get_app_quotes + display FX from the UI.
    """
    from app.db import list_positions_by_ticker
    from app.services.prices import get_app_quotes

    positions = list_positions_by_ticker()
    tickers = list({p.ticker for p in positions if float(p.amount or 0) > 0})
    if not tickers:
        return {}, 0, 0

    quotes = get_app_quotes(tickers)
    today = date.today().isoformat()
    hist_fallback: Dict[str, PriceQuote] = {}
    for t in tickers:
        up = str(t or "").upper().strip()
        if not up:
            continue
        q = quotes.get(t) or quotes.get(up)
        if q is not None and q.price is not None:
            continue
        start = (date.today() - timedelta(days=365)).isoformat()
        last_q: Optional[PriceQuote] = None
        last_day: Optional[str] = None
        for d, p, ccy in list_cached_historical_quotes(up, start, today):
            if last_day is None or str(d) > last_day:
                last_day = str(d)
                last_q = PriceQuote(price=float(p), currency=str(ccy))
        if last_q is not None and last_q.price is not None:
            hist_fallback[up] = last_q

    ticker_values: Dict[str, float] = {}
    total_pos = 0
    priced_pos = 0
    for p in positions:
        amount = float(p.amount or 0)
        if amount <= 0:
            continue
        ticker = str(p.ticker or "").upper().strip()
        q = quotes.get(p.ticker) or quotes.get(ticker)
        if q is None or q.price is None:
            q = hist_fallback.get(ticker) or q
        raw_price = q.price if q is not None else None
        quote_ccy = q.currency if q is not None else infer_quote_currency(p.ticker)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if is_excluded_from_coverage_metric(ticker):
            if price is not None:
                ticker_values[ticker] = convert_amount(
                    amount=amount * float(price),
                    from_ccy=quote_ccy,
                    to_ccy=display_currency,
                    rub_per_usd=rub_per_usd,
                    eur_per_usd=eur_per_usd,
                )
            continue
        total_pos += 1
        if price is None:
            continue
        ticker_values[ticker] = convert_amount(
            amount=amount * float(price),
            from_ccy=quote_ccy,
            to_ccy=display_currency,
            rub_per_usd=rub_per_usd,
            eur_per_usd=eur_per_usd,
        )
        priced_pos += 1
    return ticker_values, priced_pos, total_pos


def compute_current_portfolio_market_value(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Tuple[float, int, int]:
    ticker_values, priced_pos, total_pos = _current_positions_value_by_ticker(
        display_currency,
        rub_per_usd,
        eur_per_usd,
    )
    return float(sum(ticker_values.values())), priced_pos, total_pos


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
    main_map = get_instrument_main_map(all_tickers)
    active_intervals_by_ticker = _build_active_intervals_by_ticker(tx_by_day, days) if days else {}

    benchmark_cfg = _get_money_market_benchmark_for_currency(display_currency)
    benchmark_ticker: Optional[str] = None
    benchmark_prices: Dict[str, PriceQuote] = {}
    if benchmark_cfg is not None:
        benchmark_ticker = benchmark_cfg
        benchmark_raw = _load_price_series_from_cache(
            benchmark_ticker, _BENCHMARK_HISTORY_START, end
        )
        benchmark_raw = {
            d: q
            for d, q in benchmark_raw.items()
            if str(q.currency or "").upper() == str(display_currency or "").upper()
        }
        benchmark_prices = _normalize_price_series(benchmark_ticker, benchmark_raw)

    benchmark_first_quote_date: Optional[str] = (
        min(benchmark_prices.keys()) if benchmark_prices else None
    )
    benchmark_as_of_dates: List[str] = []
    benchmark_as_of_quotes: List[PriceQuote] = []
    if benchmark_prices:
        benchmark_as_of_dates, benchmark_as_of_quotes = _build_as_of_price_index(
            benchmark_prices
        )
    policy_rate_series: Dict[str, float] = {}
    if uses_synthetic_policy_benchmark(display_currency):
        policy_rate_series = load_policy_rate_series(start, end, display_currency)

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

        exact_value, priced_pos, total_pos, ticker_values = _holdings_values_by_ticker_as_of_day(
            holdings,
            as_of_index_by_ticker,
            d,
            display_currency,
            day_rub_per_usd,
            day_eur_per_usd,
        )
        main_ticker_values, other_assets_value = split_ticker_values_by_main(
            ticker_values, main_map
        )

        total_value = float(exact_value)
        priced_ratio = (float(priced_pos) / float(total_pos)) if total_pos > 0 else 1.0

        benchmark_value: Optional[float] = None
        benchmark_cum_return: Optional[float] = None
        benchmark_mwr_cum_return: Optional[float] = None
        has_fresh_benchmark_quote = False
        bq: Optional[PriceQuote] = None
        bench_fx = recent_fx
        if benchmark_ticker:
            fresh_q = benchmark_prices.get(d)
            has_fresh_benchmark_quote = (
                fresh_q is not None and fresh_q.price is not None and float(fresh_q.price) > 0
            )
            bq = _quote_as_of(benchmark_as_of_dates, benchmark_as_of_quotes, d)

        if has_fresh_benchmark_quote and bq is not None and bq.price is not None:
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
        elif (
            bq is not None
            and bq.price is not None
            and float(bq.price) > 0
            and abs(float(benchmark_cash_balance)) > 1e-12
        ):
            b_price = float(bq.price)
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
        elif (
            bq is not None
            and bq.price is not None
            and float(bq.price) > 0
            and benchmark_units > 1e-12
        ):
            b_price = float(bq.price)
            benchmark_value = convert_amount(
                amount=float(benchmark_units) * float(b_price),
                from_ccy=bq.currency,
                to_ccy=display_currency,
                rub_per_usd=bench_fx[0],
                eur_per_usd=bench_fx[1],
            )
            prev_benchmark_instrument_value = float(benchmark_value)
            last_full_benchmark_value = float(benchmark_value)
        elif uses_synthetic_policy_benchmark(display_currency) and (
            benchmark_first_quote_date is None or d < benchmark_first_quote_date
        ):
            base = float(prev_benchmark_value) if prev_benchmark_value is not None else 0.0
            if base > 0:
                base *= daily_policy_compound_factor(
                    display_currency, d, policy_rate_series
                )
            base += float(day_external_cash_flow)
            benchmark_value = base
            benchmark_cash_balance = base
            benchmark_units = 0.0
            prev_benchmark_instrument_value = 0.0
            last_full_benchmark_value = None
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
            gross = _daily_twr_gross_factor(
                float(prev_value),
                float(total_value),
                float(day_external_cash_flow),
                include_cash_in_value=False,
            )
            if gross is not None:
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
                main_ticker_values=main_ticker_values,
                other_assets_value=other_assets_value,
                ticker_values=dict(ticker_values),
            )
        )

    # Align the last point with the portfolio summary header (session quote cache + UI FX).
    if points:
        try:
            ticker_values, header_priced_pos, header_total_pos = _current_positions_value_by_ticker(
                display_currency,
                rub_per_usd,
                eur_per_usd,
            )
            if header_priced_pos <= 0:
                raise RuntimeError("header quotes unavailable")
            main_ticker_values, other_assets_value = split_ticker_values_by_main(
                ticker_values, main_map
            )
            header_value = float(sum(ticker_values.values()))
            points[-1].portfolio_value = header_value
            points[-1].main_ticker_values = main_ticker_values
            points[-1].other_assets_value = other_assets_value
            points[-1].ticker_values = dict(ticker_values)
            points[-1].priced_ratio = (
                float(header_priced_pos) / float(header_total_pos)
                if header_total_pos > 0
                else 1.0
            )
            _apply_portfolio_twr_cum_returns(points)
        except Exception:
            header_value, header_priced_pos, header_total_pos, ticker_values = (
                _holdings_values_by_ticker_as_of_day(
                    holdings,
                    as_of_index_by_ticker,
                    end,
                    display_currency,
                    rub_per_usd,
                    eur_per_usd,
                )
            )
            if header_priced_pos > 0:
                main_ticker_values, other_assets_value = split_ticker_values_by_main(
                    ticker_values, main_map
                )
                points[-1].portfolio_value = float(header_value)
                points[-1].main_ticker_values = main_ticker_values
                points[-1].other_assets_value = other_assets_value
                points[-1].ticker_values = dict(ticker_values)
                points[-1].priced_ratio = (
                    float(header_priced_pos) / float(header_total_pos)
                    if header_total_pos > 0
                    else 1.0
                )
                _apply_portfolio_twr_cum_returns(points)

    current_value = points[-1].portfolio_value if points else 0.0
    # P&L must use spot FX for net invested so it matches portfolio value and
    # Cash Flows "чистый ввод". Historical FX is kept in the daily loop for MWR/XIRR.
    net_invested_display = compute_net_cash_flow_total_spot(
        display_currency,
        rub_per_usd,
        eur_per_usd,
    )
    total_pnl = current_value - net_invested_display
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
        net_invested=float(net_invested_display),
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
        benchmark_first_quote_date=benchmark_first_quote_date,
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


def compute_period_returns(
    points: List[PerformancePoint],
    net_invested: Optional[float] = None,
) -> Dict[str, float]:
    """
    Return simple period returns for dashboard chips.

    Period return is simple price return between start and end valuation points:
      period_return = V_end / V_start - 1
    ALL return is based on invested capital:
      all_return = V_end / NetInvested - 1

    Pass `net_invested` from PerformanceResult (spot FX) so ALL matches top-level P&L.
    Daily point cash flows use historical FX and must not be summed for ALL.
    """
    if not points:
        return {"1M": 0.0, "3M": 0.0, "6M": 0.0, "1Y": 0.0, "YTD": 0.0, "ALL": 0.0}
    by_day = {p.date: p for p in points}
    last_day = datetime.strptime(points[-1].date, "%Y-%m-%d").date()
    last_value = float(points[-1].portfolio_value)
    if net_invested is not None:
        invested_capital = float(net_invested)
    else:
        invested_capital = float(sum(float(p.net_cash_flow) for p in points))

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
        "ALL": ((last_value / invested_capital) - 1.0) if invested_capital > 0 else 0.0,
    }


def compute_benchmark_period_returns(
    points: List[PerformancePoint],
    net_invested: Optional[float] = None,
) -> Dict[str, float]:
    """Return benchmark simple period returns for dashboard chips."""
    benchmark_points = [
        p for p in points if p.benchmark_value is not None and p.date is not None
    ]
    if not benchmark_points:
        return {"1M": 0.0, "3M": 0.0, "6M": 0.0, "1Y": 0.0, "YTD": 0.0, "ALL": 0.0}

    by_day = {p.date: p for p in benchmark_points}
    last_day = datetime.strptime(benchmark_points[-1].date, "%Y-%m-%d").date()
    last_value = float(benchmark_points[-1].benchmark_value or 0.0)
    if net_invested is not None:
        invested_capital = float(net_invested)
    else:
        invested_capital = float(sum(float(p.net_cash_flow) for p in benchmark_points))

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
        "ALL": ((last_value / invested_capital) - 1.0) if invested_capital > 0 else 0.0,
    }


def _daily_twr_gross_factor(
    prev_value: float,
    value: float,
    cash_flow: float,
    *,
    include_cash_in_value: bool,
) -> Optional[float]:
    """
    Daily TWR gross factor for chain-linking.

    Portfolio holdings are valued as securities only (no idle cash balance).
    Deposits are assumed to be invested during the day, so the return denominator
    is previous value plus same-day contributions. Withdrawals use end-of-day
    convention. Benchmark series include cash flows inside benchmark_value.
    """
    if float(prev_value) <= 0:
        return None
    cf = float(cash_flow)
    v = float(value)
    prev = float(prev_value)
    if include_cash_in_value:
        gross = (v - cf) / prev
    elif cf > 0:
        denom = prev + cf
        gross = v / denom if denom > 0 else None
    elif cf < 0:
        gross = (v - cf) / prev
    else:
        gross = v / prev
    if gross is None or gross <= 0:
        return None
    return float(gross)


def _apply_portfolio_twr_cum_returns(points: List[PerformancePoint]) -> None:
    """Recompute linked TWR cumulative returns from daily portfolio values."""
    factor = 1.0
    prev: Optional[float] = None
    for point in points:
        if prev is not None:
            gross = _daily_twr_gross_factor(
                prev,
                float(point.portfolio_value),
                float(point.net_cash_flow),
                include_cash_in_value=False,
            )
            if gross is not None:
                factor *= gross
        point.twr_cum_return = float(factor - 1.0)
        prev = float(point.portfolio_value)


def compute_twr_from_daily_values(
    values: List[float],
    cash_flows: List[float],
    *,
    include_cash_in_value: bool = False,
) -> float:
    """
    Compute cumulative TWR from aligned daily series.

    Default (`include_cash_in_value=False`) matches securities-only portfolio
    valuation where same-day deposits are invested during the day.
    """
    if not values or len(values) != len(cash_flows):
        return 0.0
    factor = 1.0
    prev: Optional[float] = None
    for v, cf in zip(values, cash_flows):
        if prev is not None:
            gross = _daily_twr_gross_factor(
                float(prev),
                float(v),
                float(cf),
                include_cash_in_value=include_cash_in_value,
            )
            if gross is not None:
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


RETURN_PERIOD_KEYS: Tuple[str, ...] = ("ALL", "YTD", "1Y", "6M", "3M", "1M")


def _period_start_iso(last_day: date, period: str) -> Optional[str]:
    if period == "ALL":
        return None
    if period == "YTD":
        return date(last_day.year, 1, 1).isoformat()
    days_back = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365}.get(period)
    if days_back is None:
        return None
    return (last_day - timedelta(days=days_back)).isoformat()


def _value_series_from_points(
    points: List[PerformancePoint],
    *,
    ticker_filter: Optional[Mapping[str, bool]] = None,
    scope_tickers: Optional[Iterable[str]] = None,
) -> List[PerformancePoint]:
    scope_up: Optional[set[str]] = None
    if scope_tickers is not None:
        scope_up = {str(t or "").upper().strip() for t in scope_tickers if str(t or "").strip()}
    out: List[PerformancePoint] = []
    for p in points:
        if scope_up is not None:
            vals = dict(p.ticker_values or {})
        else:
            vals = dict(p.main_ticker_values or {})
        total = 0.0
        for ticker, value in vals.items():
            up = str(ticker or "").upper().strip()
            if scope_up is not None and up not in scope_up:
                continue
            if ticker_filter is not None and not bool(ticker_filter.get(up, False)):
                continue
            total += float(value)
        out.append(
            PerformancePoint(
                date=p.date,
                portfolio_value=float(total),
                net_cash_flow=0.0,
                twr_cum_return=0.0,
                mwr_cum_return=None,
                priced_ratio=1.0,
            )
        )
    return out


def _lookup_tx_value(
    ticker: str,
    amount: float,
    day: str,
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], List[PriceQuote]]],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Optional[float]:
    idx = as_of_index_by_ticker.get(str(ticker).upper())
    if idx is None:
        return None
    dates, quotes = idx
    q = _quote_as_of(list(dates), list(quotes), day)
    if q is None or q.price is None:
        return None
    quote_ccy = q.currency or infer_quote_currency(ticker)
    price = normalize_quote_price_for_valuation(ticker, q.price, quote_ccy)
    if price is None:
        return None
    value_native = float(amount) * float(price)
    return convert_amount(
        value_native,
        quote_ccy,
        display_currency,
        rub_per_usd,
        eur_per_usd,
    )


def build_price_index_by_tickers(
    tickers: Iterable[str],
    date_from: str,
    date_to: str,
) -> Dict[str, Tuple[List[str], List[PriceQuote]]]:
    """Build as-of price indexes for tickers over [date_from, date_to]."""
    out: Dict[str, Tuple[List[str], List[PriceQuote]]] = {}
    for ticker in {str(t or "").upper().strip() for t in tickers if str(t or "").strip()}:
        series = _load_price_series_from_cache(ticker, date_from, date_to)
        series = _normalize_price_series(ticker, series)
        out[ticker] = _build_as_of_price_index(series)
    return out


def lookup_transaction_value(
    ticker: str,
    amount: float,
    day: str,
    target_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    as_of_index_by_ticker: Mapping[str, Tuple[List[str], List[PriceQuote]]],
) -> Optional[float]:
    """Transaction notional in target currency using quote on or before `day`."""
    return _lookup_tx_value(
        ticker,
        amount,
        day,
        as_of_index_by_ticker,
        target_currency,
        rub_per_usd,
        eur_per_usd,
    )


def load_historical_fx(date_from: str, date_to: str) -> Dict[str, Tuple[float, float]]:
    """Historical (rub_per_usd, eur_per_usd) by day from app_settings."""
    return _load_fx_exact_from_db(date_from, date_to)


def fx_rates_for_day(
    day: str,
    fx_exact: Mapping[str, Tuple[float, float]],
    spot_rub_per_usd: float,
    spot_eur_per_usd: float,
) -> Tuple[float, float]:
    """Last known FX on or before `day`, falling back to spot."""
    recent = (float(spot_rub_per_usd), float(spot_eur_per_usd))
    for d in sorted(fx_exact.keys()):
        if d > day:
            break
        recent = fx_exact[d]
    return recent


def _series_value_on_or_after(
    series: List[PerformancePoint],
    date_iso: str,
) -> Tuple[Optional[str], Optional[float]]:
    by_day = {p.date: p for p in series}
    candidates = [d for d in by_day if d >= date_iso]
    if not candidates:
        return None, None
    day = min(candidates)
    value = float(by_day[day].portfolio_value)
    return day, value


def _cost_basis_for_tickers_as_of(
    tickers: Iterable[str],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    as_of_date: str,
) -> Optional[float]:
    """
    Remaining cost basis for tickers using average-cost accounting, as of `as_of_date`.
    """
    wanted = {str(t or "").upper().strip() for t in tickers if str(t or "").strip()}
    if not wanted:
        return None
    tx_by_day, first_tx_date, _last_tx_date = _load_daily_transactions()
    if not first_tx_date:
        return None
    end = str(as_of_date)
    as_of_index_by_ticker: Dict[str, Tuple[List[str], List[PriceQuote]]] = {}
    for ticker in wanted:
        series = _load_price_series_from_cache(ticker, first_tx_date, end)
        series = _normalize_price_series(ticker, series)
        as_of_index_by_ticker[ticker] = _build_as_of_price_index(series)

    qty_by_ticker: Dict[str, float] = defaultdict(float)
    cost_by_ticker: Dict[str, float] = defaultdict(float)
    has_trade = False

    for day in sorted(tx_by_day.keys()):
        if day > end:
            break
        for ticker, amount, tx_type in tx_by_day[day]:
            up = str(ticker or "").upper().strip()
            if up not in wanted:
                continue
            tx_type_l = str(tx_type or "").strip().lower()
            delta = float(amount)

            if tx_type_l == "transfer":
                qty_by_ticker[up] += delta
                continue
            if tx_type_l != "trade":
                continue

            tx_value = _lookup_tx_value(
                up,
                delta,
                day,
                as_of_index_by_ticker,
                display_currency,
                rub_per_usd,
                eur_per_usd,
            )
            has_trade = True
            if delta > 0:
                qty_by_ticker[up] += delta
                if tx_value is not None:
                    cost_by_ticker[up] += float(tx_value)
                continue

            sell_qty = abs(delta)
            held = float(qty_by_ticker.get(up, 0.0))
            if held > 1e-12:
                fraction = min(sell_qty / held, 1.0)
                cost_by_ticker[up] = max(0.0, float(cost_by_ticker.get(up, 0.0)) * (1.0 - fraction))
            qty_by_ticker[up] = held - sell_qty

    if not has_trade:
        return None

    total_cost = sum(float(cost_by_ticker.get(t, 0.0)) for t in wanted)
    if total_cost <= 0:
        return 0.0
    return float(total_cost)


def _cost_basis_for_tickers(
    tickers: Iterable[str],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Optional[float]:
    """Remaining cost basis for tickers as of today."""
    return _cost_basis_for_tickers_as_of(
        tickers,
        display_currency,
        rub_per_usd,
        eur_per_usd,
        date.today().isoformat(),
    )


def compute_ticker_unrealized_pnl_pct(
    ticker: str,
    market_value_display: float,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Optional[float]:
    """
    Unrealized P&L % for a single ticker (period ALL): market_value / cost_basis − 1.
    Returns None when basis cannot be computed or market value is not positive.
    """
    if float(market_value_display) <= 0:
        return None
    up = str(ticker or "").upper().strip()
    if not up:
        return None
    basis = _cost_basis_for_tickers(
        [up], display_currency, rub_per_usd, eur_per_usd
    )
    if basis is None or float(basis) <= 0:
        return None
    return float(market_value_display) / float(basis) - 1.0


def _unrealized_pnl_at_date(
    series: List[PerformancePoint],
    tickers: Iterable[str],
    as_of_date: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Optional[float]:
    """Unrealized P&L = market value − cost basis on `as_of_date`."""
    _day, value = _series_value_on_or_after(series, as_of_date)
    if _day is None or value is None:
        return None
    basis = _cost_basis_for_tickers_as_of(
        tickers,
        display_currency,
        rub_per_usd,
        eur_per_usd,
        _day,
    )
    if basis is None:
        return None
    return float(value) - float(basis)


def _unrealized_pnl_period_return(
    series: List[PerformancePoint],
    tickers: Iterable[str],
    period: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    *,
    pnl_display: str,
    net_invested: Optional[float],
) -> Optional[float]:
    """
    Unrealized P&L for a period.

    ALL: unrealized on the current remainder (value − cost basis today).
    Other periods: change in unrealized P&L from period start to end:
      [V_end − basis_end] − [V_start − basis_start]
    Percent uses basis at period start as denominator.
    """
    _ = net_invested
    if not series:
        return None
    period_up = str(period or "ALL").upper()
    end_day = series[-1].date
    end_value = float(series[-1].portfolio_value)
    is_absolute = str(pnl_display or "percent").lower() == "absolute"

    if period_up == "ALL":
        end_basis = _cost_basis_for_tickers_as_of(
            tickers, display_currency, rub_per_usd, eur_per_usd, end_day
        )
        if end_basis is None:
            return None
        if is_absolute:
            return float(end_value) - float(end_basis)
        if float(end_basis) <= 0:
            return None
        return (float(end_value) / float(end_basis)) - 1.0

    last_day = datetime.strptime(end_day, "%Y-%m-%d").date()
    start_iso = _period_start_iso(last_day, period_up)
    if not start_iso:
        return None
    start_day, start_value = _series_value_on_or_after(series, start_iso)
    if start_day is None or start_value is None:
        return None

    start_basis = _cost_basis_for_tickers_as_of(
        tickers, display_currency, rub_per_usd, eur_per_usd, start_day
    )
    end_basis = _cost_basis_for_tickers_as_of(
        tickers, display_currency, rub_per_usd, eur_per_usd, end_day
    )
    if start_basis is None or end_basis is None:
        return None

    unrealized_start = float(start_value) - float(start_basis)
    unrealized_end = float(end_value) - float(end_basis)
    delta = unrealized_end - unrealized_start

    if is_absolute:
        return float(delta)

    if float(start_basis) > 0:
        return float(delta) / float(start_basis)
    if float(start_value) > 0:
        return float(delta) / float(start_value)
    return None


def _net_invested_for_tickers(
    tickers: Iterable[str],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
) -> Optional[float]:
    return _cost_basis_for_tickers(
        tickers, display_currency, rub_per_usd, eur_per_usd
    )


def _mwr_flows_for_tickers(
    tickers: Iterable[str],
    terminal_value: float,
    terminal_day: str,
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    *,
    period_start: Optional[str] = None,
    start_value: Optional[float] = None,
) -> List[Tuple[str, float]]:
    wanted = {str(t or "").upper().strip() for t in tickers if str(t or "").strip()}
    if not wanted:
        return []
    tx_by_day, first_tx_date, _last_tx_date = _load_daily_transactions()
    if not first_tx_date:
        return []
    end = terminal_day
    as_of_index_by_ticker: Dict[str, Tuple[List[str], List[PriceQuote]]] = {}
    for ticker in wanted:
        series = _load_price_series_from_cache(ticker, first_tx_date, end)
        series = _normalize_price_series(ticker, series)
        as_of_index_by_ticker[ticker] = _build_as_of_price_index(series)

    flows: List[Tuple[str, float]] = []
    if period_start and start_value is not None and float(start_value) > 0:
        flows.append((period_start, -float(start_value)))

    for day in sorted(tx_by_day.keys()):
        if period_start and day < period_start:
            continue
        if day > end:
            continue
        for ticker, amount, tx_type in tx_by_day.get(day, []):
            up = str(ticker or "").upper().strip()
            if up not in wanted:
                continue
            if str(tx_type or "").strip().lower() != "trade":
                continue
            tx_value = _lookup_tx_value(
                up,
                float(amount),
                day,
                as_of_index_by_ticker,
                display_currency,
                rub_per_usd,
                eur_per_usd,
            )
            if tx_value is None:
                continue
            flows.append((day, -float(tx_value)))

    if float(terminal_value) > 0:
        flows.append((terminal_day, float(terminal_value)))
    return flows


def _return_from_value_series(
    series: List[PerformancePoint],
    *,
    metric: str,
    period: str,
    net_invested: Optional[float],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    tickers: Iterable[str],
    pnl_display: str = "percent",
) -> Optional[float]:
    if not series:
        return None
    metric_up = str(metric or "PNL").upper()
    period_up = str(period or "ALL").upper()
    last_day = datetime.strptime(series[-1].date, "%Y-%m-%d").date()
    last_value = float(series[-1].portfolio_value)
    if last_value <= 0:
        return None

    if metric_up == "PNL":
        return _unrealized_pnl_period_return(
            series,
            tickers,
            period_up,
            display_currency,
            rub_per_usd,
            eur_per_usd,
            pnl_display=pnl_display,
            net_invested=net_invested,
        )

    start_iso = _period_start_iso(last_day, period_up)
    start_value: Optional[float] = None
    if start_iso:
        by_day = {p.date: p for p in series}
        candidates = [d for d in by_day if d >= start_iso]
        if candidates:
            start_value = float(by_day[min(candidates)].portfolio_value)
    elif series:
        for p in series:
            if float(p.portfolio_value) > 0:
                start_value = float(p.portfolio_value)
                start_iso = p.date
                break

    flows = _mwr_flows_for_tickers(
        tickers,
        last_value,
        series[-1].date,
        display_currency,
        rub_per_usd,
        eur_per_usd,
        period_start=start_iso,
        start_value=start_value if period_up != "ALL" else None,
    )
    xirr = compute_xirr_annualized(flows)
    if xirr is None:
        return None
    if metric_up == "MWR_XIRR":
        return float(xirr)
    first_flow_day = flows[0][0] if flows else series[0].date
    years = _years_between(first_flow_day, series[-1].date)
    return _annualized_to_period_return(float(xirr), years)


def compute_main_group_returns(
    result: PerformanceResult,
    *,
    group_mode: str,
    metric: str,
    period: str,
    main_ticker_records: List[Mapping[str, object]],
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    pnl_display: str = "percent",
) -> Dict[str, Optional[float]]:
    """
    Return metric values keyed by row label for a portfolio summary table.

    group_mode: Tickers | Subclasses | Classes | Currencies
    metric: PNL | MWR | MWR_XIRR
    period: ALL | YTD | 1Y | 6M | 3M | 1M
    main_ticker_records: ticker rows for the portfolio slice (main or other).
    """
    points = list(result.points or [])
    if not points or not main_ticker_records:
        return {}

    mode = str(group_mode or "Tickers")
    if mode == "Storage":
        return {}

    ticker_meta: Dict[str, dict] = {}
    for rec in main_ticker_records:
        up = str(rec.get("ticker") or "").upper().strip()
        if not up:
            continue
        ticker_meta[up] = dict(rec)

    scope_tickers = set(ticker_meta.keys())

    def _group_key(ticker_up: str) -> str:
        rec = ticker_meta.get(ticker_up, {})
        if mode == "Tickers":
            return str(rec.get("ticker") or ticker_up)
        if mode == "Subclasses":
            return str(rec.get("subclass_name") or "—")
        if mode == "Classes":
            return str(rec.get("class_name") or "—")
        if mode == "Currencies":
            return str(rec.get("currency_bucket") or "USD")
        return ticker_up

    groups: Dict[str, set[str]] = defaultdict(set)
    for up in ticker_meta:
        groups[_group_key(up)].add(up)

    out: Dict[str, Optional[float]] = {}
    for label, tickers in groups.items():
        ticker_filter = {t: True for t in tickers}
        series = _value_series_from_points(
            points,
            ticker_filter=ticker_filter,
            scope_tickers=scope_tickers,
        )
        net_invested = _net_invested_for_tickers(
            tickers, display_currency, rub_per_usd, eur_per_usd
        )
        out[label] = _return_from_value_series(
            series,
            metric=metric,
            period=period,
            net_invested=net_invested,
            display_currency=display_currency,
            rub_per_usd=rub_per_usd,
            eur_per_usd=eur_per_usd,
            tickers=tickers,
            pnl_display=pnl_display,
        )
    return out
