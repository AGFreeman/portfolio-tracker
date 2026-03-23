"""
Buy-only rebalancing: allocate new cash across underweight asset subclasses (target %),
then split each subclass budget across existing tickers pro-rata by current value.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from app.services.prices import is_crypto_ticker


@dataclass
class TickerPositionValue:
    """Per-ticker state in display currency (from quotes + FX)."""

    ticker: str
    asset_subclass_id: int
    value_display: Optional[float]  # None if no quote
    price_display: Optional[float]  # unit price in display ccy; None if no quote


@dataclass
class SuggestedBuy:
    ticker: str
    asset_subclass_id: int
    subclass_name: str
    spend_allocated: float  # pro-rata target before unit rounding
    units: float
    implied_spend: float  # units * price_display
    price_display: float


@dataclass
class SubclassBudgetUnallocated:
    subclass_id: int
    subclass_name: str
    budget: float
    reason: str


@dataclass
class RebalancePlan:
    suggested_buys: List[SuggestedBuy] = field(default_factory=list)
    unallocated: List[SubclassBudgetUnallocated] = field(default_factory=list)
    unpriced_tickers: List[str] = field(default_factory=list)
    weights_were_normalized: bool = False
    target_sum_pct: float = 0.0
    S: float = 0.0
    V: float = 0.0
    T: float = 0.0
    total_gap: float = 0.0
    total_implied_spend: float = 0.0
    residual_vs_V: float = 0.0  # V - total_implied_spend (rounding slack)


def normalize_subclass_weights(target_pct_by_sub: Mapping[int, float]) -> Tuple[Dict[int, float], float, bool]:
    """
    target_pct_by_sub: subclass_id -> percent of portfolio (expected sum 100).
    Returns (w summing to 1.0, raw sum of targets, normalized_flag).
    """
    raw_sum = sum(float(x) for x in target_pct_by_sub.values())
    if raw_sum <= 0:
        return {}, raw_sum, False
    normalized = abs(raw_sum - 100.0) > 0.05
    w = {sid: float(p) / raw_sum for sid, p in target_pct_by_sub.items()}
    return w, raw_sum, normalized


def aggregate_values_by_subclass(rows: Sequence[TickerPositionValue]) -> Dict[int, float]:
    out: Dict[int, float] = defaultdict(float)
    for r in rows:
        if r.value_display is not None:
            out[r.asset_subclass_id] += float(r.value_display)
    return dict(out)


def allocate_cash_to_subclasses(
    v_by_sub: Mapping[int, float],
    w_by_sub: Mapping[int, float],
    V: float,
) -> Tuple[Dict[int, float], float, float, float]:
    """
    Proportional to max(0, T*w_j - v_j). Returns (budget_by_sub, S, T, total_gap).
    """
    S = sum(float(x) for x in v_by_sub.values())
    T = S + float(V)
    gaps: Dict[int, float] = {}
    for sid, w in w_by_sub.items():
        v = float(v_by_sub.get(sid, 0.0))
        ideal = T * float(w)
        gaps[sid] = max(0.0, ideal - v)
    total_gap = sum(gaps.values())
    if V <= 0 or total_gap <= 0:
        return {}, S, T, total_gap
    budget = {sid: float(V) * g / total_gap for sid, g in gaps.items()}
    return budget, S, T, total_gap


def split_subclass_budget_to_tickers(
    budget: float,
    ticker_values: Sequence[Tuple[str, float]],
) -> Dict[str, float]:
    """Equal split across eligible tickers within subclass."""
    eligible = [(t, float(v)) for t, v in ticker_values if float(v) > 0]
    n = len(eligible)
    if budget <= 0 or n == 0:
        return {}
    each = budget / float(n)
    return {t: each for t, _ in eligible}


def units_and_implied_spend(ticker: str, spend: float, price_display: float) -> Tuple[float, float]:
    """Stock/ETF: whole units (floor). Crypto: fractional."""
    if price_display <= 0 or spend <= 0:
        return 0.0, 0.0
    raw = spend / price_display
    if is_crypto_ticker(ticker):
        u = round(raw, 8)
        return u, u * price_display
    u = float(math.floor(raw))
    return u, u * price_display


def compute_rebalance_plan(
    rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked_tickers: Optional[set[str]] = None,
) -> RebalancePlan:
    """
    rows: all held tickers with optional values/prices in display currency.
    target_pct_by_sub: subclass_id -> target % (sum ~100).
    subclass_names: subclass_id -> display name.
    V: new cash to invest (same currency as values/prices).
    """
    plan = RebalancePlan(V=float(V))
    blocked = {x.upper() for x in (blocked_tickers or set())}
    unpriced: List[str] = []
    for r in rows:
        if r.value_display is None or r.price_display is None:
            unpriced.append(r.ticker)
    plan.unpriced_tickers = sorted(set(unpriced))

    w, raw_sum, norm = normalize_subclass_weights(target_pct_by_sub)
    plan.weights_were_normalized = norm
    plan.target_sum_pct = raw_sum
    if not w:
        return plan

    v_by_sub = aggregate_values_by_subclass(rows)
    # include subclasses that only appear in targets (zero current value)
    for sid in w:
        v_by_sub.setdefault(sid, 0.0)

    budget_by_sub, S, T, total_gap = allocate_cash_to_subclasses(v_by_sub, w, V)
    plan.S = S
    plan.T = T
    plan.total_gap = total_gap

    if V <= 0:
        return plan

    if total_gap <= 0:
        return plan

    # ticker -> row lookup
    by_ticker = {r.ticker.upper(): r for r in rows}

    # group value by subclass (priced only)
    tickers_by_sub: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
    for r in rows:
        if (
            r.value_display is not None
            and float(r.value_display) > 0
            and r.ticker.upper() not in blocked
        ):
            tickers_by_sub[r.asset_subclass_id].append((r.ticker, float(r.value_display)))

    for sid, bud in budget_by_sub.items():
        if bud <= 1e-12:
            continue
        name = subclass_names.get(sid, str(sid))
        tlist = tickers_by_sub.get(sid, [])
        if not tlist:
            plan.unallocated.append(
                SubclassBudgetUnallocated(
                    subclass_id=sid,
                    subclass_name=name,
                    budget=bud,
                    reason="Нет доступных (не заблокированных) позиций с котировкой в этом подклассе",
                )
            )
            continue
        alloc = split_subclass_budget_to_tickers(bud, tlist)
        for tkr, spend in alloc.items():
            r = by_ticker.get(tkr.upper())
            if r is None or r.price_display is None:
                continue
            price = float(r.price_display)
            units, implied = units_and_implied_spend(tkr, spend, price)
            if units <= 0 and spend > 0:
                continue
            plan.suggested_buys.append(
                SuggestedBuy(
                    ticker=tkr,
                    asset_subclass_id=sid,
                    subclass_name=name,
                    spend_allocated=spend,
                    units=units,
                    implied_spend=implied,
                    price_display=price,
                )
            )

    plan.total_implied_spend = sum(x.implied_spend for x in plan.suggested_buys)
    plan.residual_vs_V = float(V) - plan.total_implied_spend
    return plan
