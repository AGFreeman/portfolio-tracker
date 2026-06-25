"""
Rebalancing: optional sells from sellable overweight tickers (P&L >= 10%),
then allocate cash (new money + sell proceeds) across underweight subclasses.
Within each subclass, split budget across unblocked tickers toward equal-share targets.
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from app.services.prices import is_crypto_ticker

MIN_SELL_UNREALIZED_PNL_PCT = 0.10


@dataclass
class TickerPositionValue:
    """Per-ticker state in display currency (from quotes + FX)."""

    ticker: str
    asset_subclass_id: int
    value_display: Optional[float]  # None if no quote
    price_display: Optional[float]  # unit price in display ccy; None if no quote


@dataclass
class StoragePositionValue:
    """Per (ticker, storage) state in display currency."""

    ticker: str
    storage_id: int
    storage_name: str
    asset_subclass_id: int
    value_display: Optional[float]
    price_display: Optional[float]


@dataclass
class SuggestedBuy:
    ticker: str
    asset_subclass_id: int
    subclass_name: str
    spend_allocated: float  # pro-rata target before unit rounding
    units: float
    implied_spend: float  # units * price_display
    price_display: float
    storage_id: Optional[int] = None
    storage_name: Optional[str] = None


@dataclass
class SuggestedSell:
    ticker: str
    asset_subclass_id: int
    subclass_name: str
    units: float
    implied_proceeds: float
    price_display: float
    storage_id: int = 0
    storage_name: str = ""


@dataclass
class SubclassBudgetUnallocated:
    subclass_id: int
    subclass_name: str
    budget: float
    reason: str


@dataclass
class IdealTrade:
    """Ticker-level trade from phase-1 ideal plan (no storage)."""

    ticker: str
    units: float
    amount: float
    price_display: float
    asset_subclass_id: int
    subclass_name: str


@dataclass
class IdealPortfolioPlan:
    sells: List[IdealTrade] = field(default_factory=list)
    buys: List[IdealTrade] = field(default_factory=list)
    deviation_l1_before: float = 0.0
    deviation_l1_after: float = 0.0
    iterations: int = 0


@dataclass
class RebalancePlan:
    suggested_buys: List[SuggestedBuy] = field(default_factory=list)
    suggested_sells: List[SuggestedSell] = field(default_factory=list)
    skipped_sells_low_pnl: List[str] = field(default_factory=list)
    unallocated: List[SubclassBudgetUnallocated] = field(default_factory=list)
    unpriced_tickers: List[str] = field(default_factory=list)
    weights_were_normalized: bool = False
    target_sum_pct: float = 0.0
    S: float = 0.0
    V: float = 0.0
    V_effective: float = 0.0
    total_sell_proceeds: float = 0.0
    T: float = 0.0
    total_gap: float = 0.0
    total_implied_spend: float = 0.0
    residual_vs_V: float = 0.0  # V - external buy implied spend (rounding slack)
    residual_sell_proceeds: float = 0.0  # sell proceeds not deployed (lot slack)
    deviation_l1_before: float = 0.0
    deviation_l1_after: float = 0.0
    deviation_l1_after_ideal: float = 0.0
    optimizer_iterations: int = 0
    ideal_sells: List[IdealTrade] = field(default_factory=list)
    ideal_buys: List[IdealTrade] = field(default_factory=list)
    constraint_gaps: List[str] = field(default_factory=list)
    skipped_sells_undeployable: List[str] = field(default_factory=list)
    rebalance_diagnostics: List[str] = field(default_factory=list)
    storage_cash_flows: Dict[int, "StorageCashFlow"] = field(default_factory=dict)


@dataclass
class StorageCashFlow:
    """Per-storage cash movements that must balance:

    ``sell_proceeds - transfer_out + external_inflow + transfer_in ≈ purchases``
    """

    sell_proceeds: float = 0.0
    external_inflow: float = 0.0
    transfer_in: float = 0.0
    transfer_out: float = 0.0
    purchases: float = 0.0


@dataclass
class RebalanceConstraints:
    blocked_tickers: set[str] = field(default_factory=set)
    sellable_positions: set[tuple[str, int]] = field(default_factory=set)
    unblocked_tickers_by_storage: Dict[int, set[str]] = field(default_factory=dict)
    deposit_storage_ids: Optional[set[int]] = None
    withdraw_storage_ids: set[int] = field(default_factory=set)
    unrealized_pnl_pct_by_ticker: Dict[str, float] = field(default_factory=dict)
    min_purchase_amount: float = 0.0
    min_deposit_amount: float = 0.0
    buy_allocation_mode: str = "max_gap"  # "max_gap" | "proportional"


MAX_OPTIMIZER_ITERATIONS = 5
DEVIATION_IMPROVEMENT_EPS = 0.01


def compute_deviation_l1(
    value_by_ticker: Mapping[str, float],
    target_by_ticker: Mapping[str, float],
) -> float:
    """Sum of |current − target| over tickers present in either map."""
    tickers = set(value_by_ticker.keys()) | {str(t).upper() for t in target_by_ticker}
    total = 0.0
    for t_up in tickers:
        cur = float(value_by_ticker.get(t_up, 0.0))
        tgt = float(target_by_ticker.get(t_up, 0.0))
        total += abs(cur - tgt)
    return total


def _aggregate_ticker_values(
    storage_rows: Sequence[StoragePositionValue],
) -> Dict[str, float]:
    out: Dict[str, float] = defaultdict(float)
    for sr in storage_rows:
        if sr.value_display is not None:
            out[str(sr.ticker).upper()] += float(sr.value_display)
    return dict(out)


def storage_to_ticker_rows(
    storage_rows: Sequence[StoragePositionValue],
) -> List[TickerPositionValue]:
    by_ticker: Dict[str, TickerPositionValue] = {}
    for sr in storage_rows:
        if sr.value_display is None:
            continue
        t_up = str(sr.ticker).upper()
        prev = by_ticker.get(t_up)
        if prev is None:
            by_ticker[t_up] = TickerPositionValue(
                ticker=str(sr.ticker),
                asset_subclass_id=int(sr.asset_subclass_id),
                value_display=float(sr.value_display),
                price_display=sr.price_display,
            )
        else:
            by_ticker[t_up] = TickerPositionValue(
                ticker=prev.ticker,
                asset_subclass_id=prev.asset_subclass_id,
                value_display=float(prev.value_display or 0) + float(sr.value_display),
                price_display=prev.price_display or sr.price_display,
            )
    return list(by_ticker.values())


def _copy_storage_rows(
    storage_rows: Sequence[StoragePositionValue],
) -> List[StoragePositionValue]:
    return [
        StoragePositionValue(
            ticker=str(sr.ticker),
            storage_id=int(sr.storage_id),
            storage_name=str(sr.storage_name),
            asset_subclass_id=int(sr.asset_subclass_id),
            value_display=sr.value_display,
            price_display=sr.price_display,
        )
        for sr in storage_rows
    ]


def _apply_sell_to_storage(
    storage_rows: List[StoragePositionValue],
    sell: SuggestedSell,
) -> None:
    t_up = str(sell.ticker).upper()
    sid = int(sell.storage_id)
    sold = float(sell.implied_proceeds)
    for i, sr in enumerate(storage_rows):
        if str(sr.ticker).upper() != t_up or int(sr.storage_id) != sid:
            continue
        if sr.value_display is None:
            return
        storage_rows[i] = StoragePositionValue(
            ticker=sr.ticker,
            storage_id=sr.storage_id,
            storage_name=sr.storage_name,
            asset_subclass_id=sr.asset_subclass_id,
            value_display=max(0.0, float(sr.value_display) - sold),
            price_display=sr.price_display,
        )
        return


def _apply_buy_to_storage(
    storage_rows: List[StoragePositionValue],
    buy: SuggestedBuy,
    constraints: RebalanceConstraints,
) -> None:
    t_up = str(buy.ticker).upper()
    spend = float(buy.implied_spend)
    if spend <= 0:
        return
    if buy.storage_id is not None:
        sid = int(buy.storage_id)
        sname = str(buy.storage_name or "")
        for i, sr in enumerate(storage_rows):
            if str(sr.ticker).upper() != t_up or int(sr.storage_id) != sid:
                continue
            if sr.value_display is None:
                return
            storage_rows[i] = StoragePositionValue(
                ticker=sr.ticker,
                storage_id=sr.storage_id,
                storage_name=sr.storage_name,
                asset_subclass_id=sr.asset_subclass_id,
                value_display=float(sr.value_display) + spend,
                price_display=sr.price_display,
            )
            return
        storage_rows.append(
            StoragePositionValue(
                ticker=str(buy.ticker),
                storage_id=sid,
                storage_name=sname,
                asset_subclass_id=int(buy.asset_subclass_id),
                value_display=spend,
                price_display=float(buy.price_display),
            )
        )
        return
    deposit_ids = constraints.deposit_storage_ids
    names = constraints.unblocked_tickers_by_storage
    targets: List[int] = []
    for stor_id, tickers in names.items():
        if t_up not in {str(x).upper() for x in tickers}:
            continue
        if deposit_ids is not None and int(stor_id) not in deposit_ids:
            continue
        targets.append(int(stor_id))
    if not targets:
        return
    per = spend / float(len(targets))
    for stor_id in targets:
        applied = False
        for i, sr in enumerate(storage_rows):
            if str(sr.ticker).upper() != t_up or int(sr.storage_id) != stor_id:
                continue
            if sr.value_display is None:
                continue
            storage_rows[i] = StoragePositionValue(
                ticker=sr.ticker,
                storage_id=sr.storage_id,
                storage_name=sr.storage_name,
                asset_subclass_id=sr.asset_subclass_id,
                value_display=float(sr.value_display) + per,
                price_display=sr.price_display,
            )
            applied = True
            break
        if not applied:
            sname = ""
            for sr in storage_rows:
                if int(sr.storage_id) == stor_id and str(sr.storage_name or "").strip():
                    sname = str(sr.storage_name)
                    break
            storage_rows.append(
                StoragePositionValue(
                    ticker=str(buy.ticker),
                    storage_id=int(stor_id),
                    storage_name=sname,
                    asset_subclass_id=int(buy.asset_subclass_id),
                    value_display=per,
                    price_display=float(buy.price_display),
                )
            )


def _portfolio_buyable_tickers(
    constraints: RebalanceConstraints,
) -> Optional[set[str]]:
    """Tickers that may receive buys at any deposit-enabled storage."""
    out: set[str] = set()
    deposit_ids = constraints.deposit_storage_ids
    for stor_id, tickers in constraints.unblocked_tickers_by_storage.items():
        if deposit_ids is not None and int(stor_id) not in deposit_ids:
            continue
        out |= {str(t).upper() for t in tickers}
    return out if out else None


def _buy_eligible_tickers_for_pool(
    source_storage_id: int,
    constraints: RebalanceConstraints,
) -> Optional[set[str]]:
    can_withdraw = int(source_storage_id) in constraints.withdraw_storage_ids
    unblocked = constraints.unblocked_tickers_by_storage
    if can_withdraw:
        return _portfolio_buyable_tickers(constraints)
    local = {str(t).upper() for t in unblocked.get(int(source_storage_id), set())}
    return local if local else None


MIN_PROCEED_DEPLOY_FRACTION = 0.01  # require meaningful deployment of proceeds


def _ticker_gap_candidates(
    rows: Sequence[TickerPositionValue],
    target_by_ticker: Mapping[str, float],
    blocked: set[str],
    *,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> List[Tuple[TickerPositionValue, float, float]]:
    """Eligible rows with (row, gap, price), sorted by price ascending."""
    out: List[Tuple[TickerPositionValue, float, float]] = []
    for r in rows:
        if r.value_display is None or r.price_display is None:
            continue
        t_up = str(r.ticker).upper()
        if t_up in blocked:
            continue
        if excluded_tickers is not None and t_up in excluded_tickers:
            continue
        if eligible_tickers is not None and t_up not in eligible_tickers:
            continue
        price = float(r.price_display)
        if price <= 0:
            continue
        gap = float(target_by_ticker.get(t_up, 0.0)) - float(r.value_display)
        if gap > 1e-9:
            out.append((r, gap, price))
    out.sort(key=lambda x: x[2])
    return out


def _try_affordable_lot_buy(
    rows: Sequence[TickerPositionValue],
    budget: float,
    target_by_ticker: Mapping[str, float],
    blocked: set[str],
    *,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> Optional[Tuple[TickerPositionValue, float, float]]:
    """Buy the cheapest eligible underweight ticker affordable at whole-lot granularity."""
    if budget <= 1e-12:
        return None
    for r, gap, price in _ticker_gap_candidates(
        rows,
        target_by_ticker,
        blocked,
        eligible_tickers=eligible_tickers,
        excluded_tickers=excluded_tickers,
    ):
        if price > float(budget) + 1e-9:
            break
        units, implied = units_and_implied_spend(
            r.ticker, min(float(budget), gap), price
        )
        if units > 0 and implied > 1e-9:
            return r, float(units), float(implied)
    return None


def _storage_id_to_name(
    storage_rows: Sequence[StoragePositionValue],
) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for sr in storage_rows:
        sid = int(sr.storage_id)
        out.setdefault(sid, str(sr.storage_name or sid))
    return out


def _buyable_storage_ids(
    constraints: RebalanceConstraints,
    ticker: str,
    storage_rows: Optional[Sequence[StoragePositionValue]] = None,
) -> set[int]:
    t_up = str(ticker).upper()
    deposit_ids = constraints.deposit_storage_ids
    out: set[int] = set()
    for sid, tickers in constraints.unblocked_tickers_by_storage.items():
        if deposit_ids is not None and int(sid) not in deposit_ids:
            continue
        if t_up in {str(t).upper() for t in tickers}:
            out.add(int(sid))
    if not out and storage_rows is not None:
        for sr in storage_rows:
            if str(sr.ticker).upper() != t_up:
                continue
            sid = int(sr.storage_id)
            if deposit_ids is not None and sid not in deposit_ids:
                continue
            out.add(sid)
    return out


def _fundable_storage_ids(
    constraints: RebalanceConstraints,
    sells: Sequence[SuggestedSell],
    V_external: float,
) -> set[int]:
    """Storages that can receive cash from sell proceeds and/or external V."""
    deposit_ids = constraints.deposit_storage_ids
    withdraw_ids = constraints.withdraw_storage_ids
    out: set[int] = set()
    sell_storages = {int(s.storage_id) for s in sells}
    out |= sell_storages

    if withdraw_ids:
        for src in sell_storages:
            if src in withdraw_ids:
                if deposit_ids is not None:
                    out |= {int(d) for d in deposit_ids}
                else:
                    out |= sell_storages

    if float(V_external) > 1e-6:
        if deposit_ids is not None:
            out |= {int(d) for d in deposit_ids}
        elif sell_storages:
            out |= sell_storages

    return out


def _underweight_needs_withdraw_hint(
    underweight: Sequence[Tuple[str, float]],
    constraints: RebalanceConstraints,
    sell_source_storage_ids: set[int],
) -> bool:
    """True when some underweight ticker cannot be bought at any sell-source storage."""
    if not underweight or not sell_source_storage_ids:
        return False
    for ticker, _ in underweight:
        buy_at = _buyable_storage_ids(constraints, ticker)
        if not buy_at:
            continue
        if not sell_source_storage_ids & buy_at:
            return True
    return False


def _format_underweight_diagnostics(
    underweight: Sequence[Tuple[str, float]],
    storage_rows: Sequence[StoragePositionValue],
    constraints: RebalanceConstraints,
) -> str:
    storage_names = _storage_id_to_name(storage_rows)
    lines: List[str] = []
    for ticker, gap in underweight:
        t_up = str(ticker).upper()
        holding_sids = {
            int(sr.storage_id)
            for sr in storage_rows
            if str(sr.ticker).upper() == t_up
            and sr.value_display is not None
            and float(sr.value_display) > 0
        }
        holdings = sorted(storage_names.get(sid, str(sid)) for sid in holding_sids)
        buy_sids = _buyable_storage_ids(constraints, ticker)
        buy_names = sorted(storage_names.get(sid, str(sid)) for sid in buy_sids)
        part = f"**{ticker}** (−{gap:,.0f})"
        if holdings:
            part += f" — счета: {', '.join(holdings)}"
        if buy_names:
            part += f"; купить можно на: {', '.join(buy_names)}"
        blocked_on = sorted(
            storage_names.get(sid, str(sid))
            for sid in holding_sids
            if sid not in buy_sids
        )
        if blocked_on:
            part += (
                f" (**заблокирован для покупки** на: {', '.join(blocked_on)})"
            )
        elif not buy_names:
            part += " (нет счетов с разрешённой покупкой)"
        lines.append(part)
    preview = "; ".join(lines[:6])
    if len(lines) > 6:
        preview += " …"
    return f"Недовесные инструменты: {preview}"


def _tickers_at_storage(
    storage_rows: Sequence[StoragePositionValue],
    storage_id: int,
) -> set[str]:
    sid = int(storage_id)
    return {
        str(sr.ticker).upper()
        for sr in storage_rows
        if int(sr.storage_id) == sid
        and sr.value_display is not None
        and float(sr.value_display) > 0
    }


def _explain_zero_deploy_on_storage(
    post_ticker_rows: Sequence[TickerPositionValue],
    post_storage_rows: Sequence[StoragePositionValue],
    targets: Mapping[str, float],
    amount: float,
    storage_id: int,
    storage_name: str,
    eligible: Optional[set[str]],
    excluded: set[str],
    blocked: set[str],
    *,
    sold_tickers: Optional[set[str]] = None,
    sell_ticker: Optional[str] = None,
) -> List[str]:
    """Why sell proceeds could not buy any whole lot at this storage."""
    sid = int(storage_id)
    lines: List[str] = []
    eligible_up = {str(t).upper() for t in (eligible or set())}
    held_here = _tickers_at_storage(post_storage_rows, sid)
    sold_up = {str(t).upper() for t in (sold_tickers or set())}
    sell_up = str(sell_ticker or "").upper()

    excluded_by_prior_sell: List[Tuple[str, float]] = []
    for r in post_ticker_rows:
        if r.value_display is None:
            continue
        t_up = str(r.ticker).upper()
        if t_up == sell_up or t_up in blocked:
            continue
        if t_up not in sold_up or t_up not in excluded:
            continue
        if eligible is not None and t_up not in eligible_up:
            continue
        gap = float(targets.get(t_up, 0.0)) - float(r.value_display)
        if gap > 1000.0:
            excluded_by_prior_sell.append((str(r.ticker), gap))

    candidates: List[Tuple[str, float, float, bool]] = []
    for r in post_ticker_rows:
        if r.value_display is None or r.price_display is None:
            continue
        t_up = str(r.ticker).upper()
        if t_up in blocked or t_up in excluded:
            continue
        if eligible is not None and t_up not in eligible_up:
            continue
        gap = float(targets.get(t_up, 0.0)) - float(r.value_display)
        price = float(r.price_display)
        candidates.append((str(r.ticker), gap, price, t_up in held_here))

    if excluded_by_prior_sell:
        preview = ", ".join(f"{t} (−{g:,.0f})" for t, g in excluded_by_prior_sell[:5])
        lines.append(
            f"недовес по портфелю у {preview}, но эти тикеры уже **продавались** "
            "в текущем плане — повторная покупка в этой сессии не предлагается"
        )
        if not candidates:
            return lines

    if not candidates:
        if held_here - excluded:
            blocked_here = sorted(
                t for t in held_here if t not in eligible_up and t not in excluded
            )
            if blocked_here:
                lines.append(
                    f"на **{storage_name}** есть {', '.join(blocked_here)}, "
                    "но они **заблокированы для покупки** на этом счёте"
                )
            else:
                lines.append(
                    f"на **{storage_name}** нет других инструментов, доступных для покупки"
                )
        else:
            lines.append(
                f"на **{storage_name}** нет инструментов с разрешённой покупкой"
            )
        return lines

    with_gap = [(t, g, p) for t, g, p, _ in candidates if g > 1000.0]
    at_target = [(t, g) for t, g, _, _ in candidates if g <= 1000.0]

    if with_gap:
        cheapest = min(p for _, _, p in with_gap)
        tickers = ", ".join(t for t, _, _ in with_gap[:6])
        if amount + 1e-6 < cheapest:
            lines.append(
                f"недовес по портфелю у {tickers}, но выручка {amount:,.0f} "
                f"меньше цены одного лота ({cheapest:,.0f})"
            )
        else:
            lines.append(
                f"недовес по портфелю у {tickers}, но бюджет не удалось разложить "
                "по лотам (доли подклассов или кратность)"
            )
        return lines

    if at_target:
        preview = ", ".join(f"{t} (gap {g:,.0f})" for t, g in at_target[:6])
        extra = " …" if len(at_target) > 6 else ""
        lines.append(
            f"после продажи на **{storage_name}** доступны {preview}{extra}, "
            "но у всех **нет положительного недовеса по портфелю** — "
            "целевые доли пересчитаются вниз вместе с продажей, "
            "а стоимость этих позиций не изменится"
        )
        return lines

    lines.append("нет инструментов с положительным gap для покупки")
    return lines


def _diagnose_undeployable_sell(
    storage_rows: Sequence[StoragePositionValue],
    sell: SuggestedSell,
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    blocked: set[str],
    constraints: RebalanceConstraints,
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    sold_tickers: set[str],
) -> str:
    """Concrete reason simulate_sell_and_deploy rejected this sell."""
    sid = int(sell.storage_id)
    sname = str(sell.storage_name or sid)
    t_up = str(sell.ticker).upper()
    proceeds = float(sell.implied_proceeds)

    trial = _copy_storage_rows(storage_rows)
    _apply_sell_to_storage(trial, sell)
    post_ticker_rows = storage_to_ticker_rows(trial)

    eligible = _buy_eligible_tickers_for_pool(sid, constraints)
    exclude = set(sold_tickers) | {t_up}

    pool_buys, _ = _deploy_cash_pool(
        post_ticker_rows,
        w,
        subclass_names,
        proceeds,
        blocked,
        constraints,
        source_storage_id=sid,
        source_storage_name=sname,
        eligible_tickers=eligible,
        excluded_tickers=exclude,
    )
    spent = _spent_from_buys(pool_buys)

    _, targets_post_sell = _post_sell_targets(
        post_ticker_rows, target_pct_by_sub, blocked, V_external
    )

    detail_parts: List[str] = []

    if spent < 1e-6:
        detail_parts.extend(
            _explain_zero_deploy_on_storage(
                post_ticker_rows,
                trial,
                targets_post_sell,
                proceeds,
                sid,
                sname,
                eligible,
                exclude,
                blocked,
                sold_tickers=sold_tickers,
                sell_ticker=t_up,
            )
        )
        can_withdraw = sid in constraints.withdraw_storage_ids
        if not can_withdraw:
            buy_elsewhere: List[str] = []
            for r in post_ticker_rows:
                rt = str(r.ticker).upper()
                if rt == t_up or rt in blocked or rt in exclude:
                    continue
                gap = float(targets_post_sell.get(rt, 0.0)) - float(
                    r.value_display or 0.0
                )
                if gap <= 1000.0:
                    continue
                other_sids = _buyable_storage_ids(constraints, rt) - {sid}
                if other_sids:
                    names = _storage_id_to_name(storage_rows)
                    buy_elsewhere.append(
                        f"{r.ticker} на {', '.join(names.get(s, str(s)) for s in sorted(other_sids)[:2])}"
                    )
            if buy_elsewhere:
                detail_parts.append(
                    "недовес остаётся на других брокерах "
                    f"({'; '.join(buy_elsewhere[:4])}) — включите **«Вывод денег»** на "
                    f"**{sname}**"
                )
    else:
        for b in pool_buys:
            _apply_buy_to_storage(trial, b, constraints)
        post_after_buys = storage_to_ticker_rows(trial)
        _, targets_final = _post_sell_targets(
            post_after_buys, target_pct_by_sub, blocked, V_external
        )
        val_final = float(_aggregate_ticker_values(trial).get(t_up, 0.0))
        tgt_final = float(targets_final.get(t_up, 0.0))
        if val_final < tgt_final - 1e-6:
            shortfall = tgt_final - val_final
            detail_parts.append(
                f"продажа на {proceeds:,.0f} опустит **{sell.ticker}** ниже целевой доли "
                f"портфеля на ~{shortfall:,.0f} (останется ~{val_final:,.0f} "
                f"при цели ~{tgt_final:,.0f}) — алгоритм не продаёт ниже цели"
            )
            other_locs = [
                str(sr.storage_name or sr.storage_id)
                for sr in storage_rows
                if str(sr.ticker).upper() == t_up
                and int(sr.storage_id) != sid
                and sr.value_display is not None
                and float(sr.value_display) > 0
            ]
            if other_locs:
                detail_parts.append(
                    f"локальный перевес только на **{sname}**; позиция **{sell.ticker}** "
                    f"также на: {', '.join(sorted(set(other_locs)))}"
                )
        else:
            detail_parts.append(
                "выручку не удалось полностью разместить (кратность лотов или gap)"
            )

    if not detail_parts:
        detail_parts.append(
            "выручку не удалось разместить в недовесные позиции"
        )

    return (
        f"**{sell.ticker}** ({sname}): перевес ~{proceeds:,.0f}, "
        + "; ".join(detail_parts)
        + "."
    )


def _explain_undeployable_sell(
    sell: SuggestedSell,
    constraints: RebalanceConstraints,
    *,
    storage_rows: Optional[Sequence[StoragePositionValue]] = None,
    ticker_rows: Optional[Sequence[TickerPositionValue]] = None,
    target_pct_by_sub: Optional[Mapping[int, float]] = None,
    blocked: Optional[set[str]] = None,
    w: Optional[Mapping[int, float]] = None,
    subclass_names: Optional[Mapping[int, str]] = None,
    sold_tickers: Optional[set[str]] = None,
    V_external: float = 0.0,
) -> str:
    sid = int(sell.storage_id)
    sname = str(sell.storage_name or sid)
    t_up = str(sell.ticker).upper()
    can_withdraw = sid in constraints.withdraw_storage_ids
    local = {str(t).upper() for t in constraints.unblocked_tickers_by_storage.get(sid, set())}
    portfolio_buyable = _portfolio_buyable_tickers(constraints) or set()
    others_local = local - {t_up}
    others_portfolio = portfolio_buyable - {t_up}

    underweight_elsewhere: List[str] = []
    if ticker_rows and target_pct_by_sub is not None and blocked is not None:
        _, targets = compute_ticker_target_values(
            ticker_rows, target_pct_by_sub, blocked_tickers=blocked
        )
        for r in ticker_rows:
            rt = str(r.ticker).upper()
            if rt == t_up or rt in blocked or r.value_display is None:
                continue
            if rt not in others_portfolio:
                continue
            gap = float(targets.get(rt, 0.0)) - float(r.value_display)
            if gap > 1000.0:
                underweight_elsewhere.append(r.ticker)
        underweight_elsewhere = sorted(set(underweight_elsewhere))[:5]

    blocked_local_holdings: List[str] = []
    if storage_rows:
        for sr in storage_rows:
            if int(sr.storage_id) != sid:
                continue
            rt = str(sr.ticker).upper()
            if rt == t_up:
                continue
            if sr.value_display is None or float(sr.value_display) <= 0:
                continue
            if rt not in local:
                blocked_local_holdings.append(str(sr.ticker))
        blocked_local_holdings = sorted(set(blocked_local_holdings))[:5]

    if not can_withdraw and not others_local:
        if blocked_local_holdings:
            storage_names = _storage_id_to_name(storage_rows or ())
            buy_elsewhere = sorted(
                {
                    storage_names.get(bsid, str(bsid))
                    for uw in underweight_elsewhere
                    for bsid in _buyable_storage_ids(constraints, uw)
                    if bsid != sid
                }
            )[:3]
            buy_hint = (
                f" Покупка возможна на: {', '.join(buy_elsewhere)}."
                if buy_elsewhere
                else ""
            )
            return (
                f"**{sell.ticker}** ({sname}): перевес ~{sell.implied_proceeds:,.0f} можно снять, "
                f"но **{', '.join(blocked_local_holdings)}** на этом счёте "
                "**заблокированы для покупки**. Разблокируйте тикер или включите "
                f"**«Вывод денег»** — тогда выручку можно направить на другой брокер.{buy_hint}"
            )
        hint = (
            f" Недовесные инструменты в портфеле: {', '.join(underweight_elsewhere)}."
            if underweight_elsewhere
            else ""
        )
        return (
            f"**{sell.ticker}** ({sname}): перевес ~{sell.implied_proceeds:,.0f} можно снять, "
            "но на этом месте хранения нет других доступных для покупки позиций. "
            "Включите **«Вывод денег»** для этого места хранения — "
            f"тогда выручку можно направить на покупки у других брокеров.{hint}"
        )
    if (
        storage_rows is not None
        and target_pct_by_sub is not None
        and blocked is not None
        and w is not None
        and subclass_names is not None
        and sold_tickers is not None
    ):
        return _diagnose_undeployable_sell(
            storage_rows,
            sell,
            w,
            subclass_names,
            blocked,
            constraints,
            target_pct_by_sub,
            V_external,
            sold_tickers,
        )
    return (
        f"**{sell.ticker}** ({sname}): перевес ~{sell.implied_proceeds:,.0f}, "
        "но выручку не удалось разместить в недовесные позиции "
        "(кратность лотов или нет доступных инструментов с положительным gap)."
    )


def _post_sell_targets(
    ticker_rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    blocked: set[str],
    V_external: float,
) -> Tuple[float, Dict[str, float]]:
    s_post = sum(float(r.value_display or 0.0) for r in ticker_rows)
    t_post = s_post + float(V_external)
    _, targets = compute_ticker_target_values(
        ticker_rows,
        target_pct_by_sub,
        blocked_tickers=blocked,
        portfolio_total=t_post,
    )
    return t_post, targets


def _sell_keeps_ticker_at_or_above_target(
    sell: SuggestedSell,
    storage_rows: Sequence[StoragePositionValue],
    target_pct_by_sub: Mapping[int, float],
    blocked: set[str],
    V_external: float,
) -> bool:
    """After sell, aggregated ticker value must not fall below its new target (tax-safe)."""
    trial = _copy_storage_rows(storage_rows)
    _apply_sell_to_storage(trial, sell)
    post_rows = storage_to_ticker_rows(trial)
    _, targets = _post_sell_targets(
        post_rows, target_pct_by_sub, blocked, V_external
    )
    t_up = str(sell.ticker).upper()
    val = float(_aggregate_ticker_values(trial).get(t_up, 0.0))
    tgt = float(targets.get(t_up, 0.0))
    return val >= tgt - 1e-6


def _simulate_sell_and_deploy(
    storage_rows: Sequence[StoragePositionValue],
    sell: SuggestedSell,
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    blocked: set[str],
    constraints: RebalanceConstraints,
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    sold_tickers: set[str],
) -> Optional[
    Tuple[
        List[StoragePositionValue],
        List[SuggestedBuy],
        List[SubclassBudgetUnallocated],
    ]
]:
    """
    Apply sell on a copy, deploy proceeds on post-sell state, verify ticker stays >= target.
    Returns None if proceeds cannot be deployed into at least one whole lot.
    """
    trial = _copy_storage_rows(storage_rows)
    _apply_sell_to_storage(trial, sell)
    post_rows = storage_to_ticker_rows(trial)

    t_up = str(sell.ticker).upper()
    sid = int(sell.storage_id)
    eligible = _buy_eligible_tickers_for_pool(sid, constraints)
    exclude = set(sold_tickers) | {t_up}

    pool_buys, pool_unalloc = _deploy_cash_pool(
        post_rows,
        w,
        subclass_names,
        float(sell.implied_proceeds),
        blocked,
        constraints,
        source_storage_id=sid,
        source_storage_name=str(sell.storage_name),
        eligible_tickers=eligible,
        excluded_tickers=exclude,
    )
    spent = _spent_from_buys(pool_buys)
    if spent < 1e-6:
        return None

    for b in pool_buys:
        _apply_buy_to_storage(trial, b, constraints)

    post_rows = storage_to_ticker_rows(trial)
    _, targets = _post_sell_targets(
        post_rows, target_pct_by_sub, blocked, V_external
    )
    val = float(_aggregate_ticker_values(trial).get(t_up, 0.0))
    if val < float(targets.get(t_up, 0.0)) - 1e-6:
        return None

    return trial, pool_buys, pool_unalloc


def _can_deploy_cash(
    ticker_rows: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    amount: float,
    blocked: set[str],
    *,
    buy_storage_id: Optional[int],
    buy_storage_name: Optional[str],
    eligible_tickers: Optional[set[str]],
    excluded_tickers: set[str],
) -> bool:
    if amount <= 1e-12:
        return False
    buys, _, _, _, _ = _allocate_buys_full_deploy(
        ticker_rows,
        w,
        subclass_names,
        float(amount),
        blocked,
        buy_storage_id=buy_storage_id,
        buy_storage_name=buy_storage_name,
        eligible_tickers=eligible_tickers,
        excluded_tickers=excluded_tickers or None,
    )
    return _spent_from_buys(buys) > 1e-6


def _deploy_cash_pool(
    ticker_rows: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    amount: float,
    blocked: set[str],
    constraints: RebalanceConstraints,
    *,
    source_storage_id: Optional[int] = None,
    source_storage_name: Optional[str] = None,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> Tuple[List[SuggestedBuy], List[SubclassBudgetUnallocated]]:
    buy_sid: Optional[int] = None
    buy_sname: Optional[str] = None
    if source_storage_id is not None:
        sid = int(source_storage_id)
        if sid not in constraints.withdraw_storage_ids:
            buy_sid = sid
            buy_sname = source_storage_name
    buys, unalloc, _, _, _ = _allocate_buys_full_deploy(
        ticker_rows,
        w,
        subclass_names,
        float(amount),
        blocked,
        buy_storage_id=buy_sid,
        buy_storage_name=buy_sname,
        eligible_tickers=eligible_tickers,
        excluded_tickers=excluded_tickers,
    )
    return buys, unalloc


def _copy_ticker_rows(
    rows: Sequence[TickerPositionValue],
) -> List[TickerPositionValue]:
    return [
        TickerPositionValue(
            ticker=str(r.ticker),
            asset_subclass_id=int(r.asset_subclass_id),
            value_display=r.value_display,
            price_display=r.price_display,
        )
        for r in rows
    ]


def _sellable_tickers_set(
    constraints: RebalanceConstraints,
    sellable_tickers: Optional[set[str]],
) -> set[str]:
    if sellable_tickers:
        return {str(t).upper() for t in sellable_tickers}
    return {str(t).upper() for t, _ in constraints.sellable_positions}


def _suggested_buy_to_ideal(buy: SuggestedBuy) -> IdealTrade:
    return IdealTrade(
        ticker=str(buy.ticker),
        units=float(buy.units),
        amount=float(buy.implied_spend),
        price_display=float(buy.price_display),
        asset_subclass_id=int(buy.asset_subclass_id),
        subclass_name=str(buy.subclass_name),
    )


def _ideal_to_suggested_sell(ideal: IdealTrade) -> SuggestedSell:
    return SuggestedSell(
        ticker=str(ideal.ticker),
        asset_subclass_id=int(ideal.asset_subclass_id),
        subclass_name=str(ideal.subclass_name),
        units=float(ideal.units),
        implied_proceeds=float(ideal.amount),
        price_display=float(ideal.price_display),
        storage_id=0,
        storage_name="",
    )


def _ideal_to_suggested_buy(ideal: IdealTrade) -> SuggestedBuy:
    return SuggestedBuy(
        ticker=str(ideal.ticker),
        asset_subclass_id=int(ideal.asset_subclass_id),
        subclass_name=str(ideal.subclass_name),
        spend_allocated=float(ideal.amount),
        units=float(ideal.units),
        implied_spend=float(ideal.amount),
        price_display=float(ideal.price_display),
    )


def _ticker_values_map(
    rows: Sequence[TickerPositionValue],
) -> Dict[str, float]:
    out: Dict[str, float] = defaultdict(float)
    for r in rows:
        if r.value_display is not None:
            out[str(r.ticker).upper()] += float(r.value_display)
    return dict(out)


def compute_ideal_ticker_sells(
    ticker_rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    sellable_tickers: set[str],
    unrealized_pnl_pct_by_ticker: Mapping[str, float],
    blocked: set[str],
    V: float = 0.0,
) -> Tuple[List[IdealTrade], List[str]]:
    """Portfolio-level overweight sells among sellable tickers (phase 1)."""
    if not sellable_tickers:
        return [], []

    w, _, _ = normalize_subclass_weights(target_pct_by_sub)
    if not w:
        return [], []

    S = sum(float(r.value_display or 0.0) for r in ticker_rows)
    T = S + float(V)
    if T <= 0:
        return [], []

    _, targets = compute_ticker_target_values(
        ticker_rows, target_pct_by_sub, blocked_tickers=blocked, portfolio_total=T
    )

    skipped: List[str] = []
    sells: List[IdealTrade] = []

    for r in ticker_rows:
        if r.value_display is None or r.price_display is None:
            continue
        t_up = str(r.ticker).upper()
        if t_up not in sellable_tickers:
            continue
        target = targets.get(t_up)
        if target is None:
            continue
        val = float(r.value_display)
        if float(target) >= val - 1e-12:
            continue
        pnl_pct = _ticker_pnl_pct(t_up, unrealized_pnl_pct_by_ticker)
        if pnl_pct is None or float(pnl_pct) < MIN_SELL_UNREALIZED_PNL_PCT:
            skipped.append(str(r.ticker))
            continue
        sell_gap = val - float(target)
        price = float(r.price_display)
        units, proceeds = units_and_implied_spend(r.ticker, sell_gap, price)
        if units <= 0 or proceeds <= 0:
            continue
        sid = int(r.asset_subclass_id)
        sells.append(
            IdealTrade(
                ticker=str(r.ticker),
                units=float(units),
                amount=float(proceeds),
                price_display=price,
                asset_subclass_id=sid,
                subclass_name=subclass_names.get(sid, str(sid)),
            )
        )

    return sells, sorted(set(skipped))


def _ideal_sell_keeps_ticker_at_target(
    rows: Sequence[TickerPositionValue],
    sell: IdealTrade,
    target_pct_by_sub: Mapping[int, float],
    blocked: set[str],
    V_external: float,
) -> bool:
    s_post = sum(float(r.value_display or 0.0) for r in rows)
    _, targets = compute_ticker_target_values(
        rows,
        target_pct_by_sub,
        blocked_tickers=blocked,
        portfolio_total=s_post + float(V_external),
    )
    t_up = str(sell.ticker).upper()
    val = float(_ticker_values_map(rows).get(t_up, 0.0))
    return val >= float(targets.get(t_up, 0.0)) - 1e-6


def _simulate_ideal_sell_and_deploy(
    rows: Sequence[TickerPositionValue],
    sell: IdealTrade,
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    blocked: set[str],
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    sold_tickers: set[str],
) -> Optional[
    Tuple[List[TickerPositionValue], List[IdealTrade], List[SubclassBudgetUnallocated]]
]:
    trial = _rows_after_sells(rows, [_ideal_to_suggested_sell(sell)])
    t_up = str(sell.ticker).upper()
    exclude = set(sold_tickers) | {t_up}

    pool_buys, pool_unalloc, _, _, _ = _allocate_buys_full_deploy(
        trial,
        w,
        subclass_names,
        float(sell.amount),
        blocked,
        excluded_tickers=exclude,
    )
    if _spent_from_buys(pool_buys) < 1e-6:
        return None

    trial = _rows_after_buys(trial, pool_buys)
    if not _ideal_sell_keeps_ticker_at_target(
        trial, sell, target_pct_by_sub, blocked, V_external
    ):
        return None

    ideal_buys = [_suggested_buy_to_ideal(b) for b in pool_buys]
    return trial, ideal_buys, pool_unalloc


def compute_ideal_portfolio_plan(
    ticker_rows_initial: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    blocked: set[str],
    constraints: RebalanceConstraints,
    *,
    sellable_tickers: Optional[set[str]] = None,
) -> IdealPortfolioPlan:
    """Phase 1: iterative ideal trades on aggregated portfolio (no storage constraints)."""
    sellable_up = _sellable_tickers_set(constraints, sellable_tickers)
    working = _copy_ticker_rows(ticker_rows_initial)
    all_sells: List[IdealTrade] = []
    all_buys: List[IdealTrade] = []
    sold_tickers: set[str] = set()

    s_initial = sum(float(r.value_display or 0.0) for r in ticker_rows_initial)
    t_final = s_initial + float(V_external)
    _, targets = compute_ticker_target_values(
        ticker_rows_initial,
        target_pct_by_sub,
        blocked_tickers=blocked,
        portfolio_total=t_final,
    )
    deviation_before = compute_deviation_l1(_ticker_values_map(working), targets)
    prev_deviation = deviation_before
    iterations_run = 0

    for iteration in range(MAX_OPTIMIZER_ITERATIONS):
        iterations_run = iteration + 1
        candidate_sells, _ = compute_ideal_ticker_sells(
            working,
            target_pct_by_sub,
            subclass_names,
            sellable_up,
            constraints.unrealized_pnl_pct_by_ticker,
            blocked,
            0.0,
        )

        iter_sells: List[IdealTrade] = []
        iter_buys: List[IdealTrade] = []
        for sell in candidate_sells:
            if str(sell.ticker).upper() in sold_tickers:
                continue
            result = _simulate_ideal_sell_and_deploy(
                working,
                sell,
                w,
                subclass_names,
                blocked,
                target_pct_by_sub,
                0.0,
                sold_tickers,
            )
            if result is None:
                trial = _rows_after_sells(working, [_ideal_to_suggested_sell(sell)])
                if not _ideal_sell_keeps_ticker_at_target(
                    trial, sell, target_pct_by_sub, blocked, 0.0
                ):
                    continue
                working = trial
                iter_sells.append(sell)
                sold_tickers.add(str(sell.ticker).upper())
                continue
            working, pool_buys, _ = result
            iter_sells.append(sell)
            sold_tickers.add(str(sell.ticker).upper())
            iter_buys.extend(pool_buys)

        all_sells.extend(iter_sells)
        all_buys.extend(iter_buys)

        if not iter_sells and iteration > 0:
            break

        cur_deviation = compute_deviation_l1(_ticker_values_map(working), targets)
        if not iter_sells:
            break
        if iteration > 0 and prev_deviation - cur_deviation < DEVIATION_IMPROVEMENT_EPS:
            break
        prev_deviation = cur_deviation

    if float(V_external) > 0:
        ext_buys, _, _, _, _ = _allocate_buys_full_deploy(
            working,
            w,
            subclass_names,
            float(V_external),
            blocked,
            excluded_tickers=sold_tickers if sold_tickers else None,
        )
        working = _rows_after_buys(working, ext_buys)
        all_buys.extend(_suggested_buy_to_ideal(b) for b in ext_buys)

    deviation_after = compute_deviation_l1(_ticker_values_map(working), targets)
    return IdealPortfolioPlan(
        sells=all_sells,
        buys=_merge_ideal_buys(all_buys),
        deviation_l1_before=deviation_before,
        deviation_l1_after=deviation_after,
        iterations=iterations_run,
    )


def _merge_ideal_buys(buys: Sequence[IdealTrade]) -> List[IdealTrade]:
    merged: Dict[str, IdealTrade] = {}
    for b in buys:
        t_up = str(b.ticker).upper()
        prev = merged.get(t_up)
        if prev is None:
            merged[t_up] = b
            continue
        merged[t_up] = IdealTrade(
            ticker=str(prev.ticker),
            units=float(prev.units) + float(b.units),
            amount=float(prev.amount) + float(b.amount),
            price_display=float(prev.price_display),
            asset_subclass_id=int(prev.asset_subclass_id),
            subclass_name=str(prev.subclass_name),
        )
    return list(merged.values())


def _storage_targets(
    storage_rows: Sequence[StoragePositionValue],
    ticker_rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    blocked: set[str],
    V: float,
) -> Tuple[Dict[str, float], Dict[str, float]]:
    S = sum(float(r.value_display or 0.0) for r in ticker_rows)
    T = S + float(V)
    _, targets = compute_ticker_target_values(
        ticker_rows, target_pct_by_sub, blocked_tickers=blocked, portfolio_total=T
    )
    value_by_ticker: Dict[str, float] = defaultdict(float)
    for sr in storage_rows:
        if sr.value_display is not None and float(sr.value_display) > 0:
            value_by_ticker[str(sr.ticker).upper()] += float(sr.value_display)
    storage_target: Dict[str, float] = {}
    for sr in storage_rows:
        if sr.value_display is None:
            continue
        t_up = str(sr.ticker).upper()
        ticker_total = float(value_by_ticker.get(t_up, 0.0))
        target = targets.get(t_up)
        if target is None or ticker_total <= 0:
            continue
        key = f"{t_up}:{int(sr.storage_id)}"
        storage_target[key] = float(target) * (float(sr.value_display) / ticker_total)
    return dict(targets), storage_target


def _split_ideal_sells_to_storages(
    ideal_sells: Sequence[IdealTrade],
    storage_rows: Sequence[StoragePositionValue],
    sellable_keys: set[tuple[str, int]],
    targets: Mapping[str, float],
    value_by_ticker: Mapping[str, float],
    subclass_names: Mapping[int, str],
) -> Tuple[List[SuggestedSell], List[str]]:
    sells: List[SuggestedSell] = []
    gaps: List[str] = []

    for ideal in ideal_sells:
        t_up = str(ideal.ticker).upper()
        target = float(targets.get(t_up, 0.0))
        ticker_total = float(value_by_ticker.get(t_up, 0.0))
        if ticker_total <= 0:
            gaps.append(
                f"**{ideal.ticker}**: идеальная продажа ~{ideal.amount:,.0f} — нет позиции в портфеле."
            )
            continue

        candidates: List[Tuple[StoragePositionValue, float]] = []
        for sr in storage_rows:
            if str(sr.ticker).upper() != t_up:
                continue
            if sr.value_display is None or sr.price_display is None:
                continue
            key = (t_up, int(sr.storage_id))
            if key not in sellable_keys:
                continue
            storage_val = float(sr.value_display)
            storage_target = target * (storage_val / ticker_total)
            overweight = storage_val - storage_target
            if overweight > 1e-6:
                candidates.append((sr, overweight))

        if not candidates:
            gaps.append(
                f"**{ideal.ticker}**: идеальная продажа ~{ideal.amount:,.0f} — "
                "нет продажных счетов с локальным перевесом."
            )
            continue

        candidates.sort(key=lambda x: -x[1])
        remaining_units = float(ideal.units)
        total_ow = sum(ow for _, ow in candidates)

        for sr, ow in candidates:
            if remaining_units <= 1e-9:
                break
            price = float(sr.price_display)
            share = remaining_units * (ow / total_ow) if total_ow > 0 else 0.0
            max_units, _ = units_and_implied_spend(sr.ticker, ow, price)
            take_units = min(share, float(max_units))
            if is_crypto_ticker(sr.ticker):
                take_units = round(take_units, 8)
            else:
                take_units = float(math.floor(take_units))
            if take_units <= 0:
                continue
            proceeds = take_units * price
            sid = int(sr.asset_subclass_id)
            sells.append(
                SuggestedSell(
                    ticker=str(sr.ticker),
                    asset_subclass_id=sid,
                    subclass_name=subclass_names.get(sid, str(sid)),
                    units=float(take_units),
                    implied_proceeds=float(proceeds),
                    price_display=price,
                    storage_id=int(sr.storage_id),
                    storage_name=str(sr.storage_name),
                )
            )
            remaining_units -= take_units

        if remaining_units > 1e-6:
            short_proceeds = remaining_units * float(ideal.price_display)
            gaps.append(
                f"**{ideal.ticker}**: из идеальной продажи ~{ideal.amount:,.0f} "
                f"на счетах размещено ~{ideal.amount - short_proceeds:,.0f}; "
                f"остаток ~{short_proceeds:,.0f} (лоты или локальный перевес)."
            )

    return sells, gaps


def _split_ideal_buys_to_storages(
    ideal_buys: Sequence[IdealTrade],
    storage_rows: Sequence[StoragePositionValue],
    constraints: RebalanceConstraints,
    targets: Mapping[str, float],
    value_by_ticker: Mapping[str, float],
    subclass_names: Mapping[int, str],
    sells: Sequence[SuggestedSell],
    V_external: float,
) -> Tuple[List[SuggestedBuy], List[str]]:
    draft_buys: List[SuggestedBuy] = []
    gaps: List[str] = []
    storage_names = _storage_id_to_name(storage_rows)

    for ideal in ideal_buys:
        t_up = str(ideal.ticker).upper()
        ticker_total = float(value_by_ticker.get(t_up, 0.0))
        target = float(targets.get(t_up, 0.0))
        portfolio_gap = max(0.0, target - ticker_total)

        buy_sids = _buyable_storage_ids(constraints, t_up, storage_rows)
        fundable = _fundable_storage_ids(constraints, sells, V_external)
        if fundable:
            buy_sids &= fundable
        if not buy_sids:
            blocked_names: List[str] = []
            storage_names = _storage_id_to_name(storage_rows)
            deposit_ids = constraints.deposit_storage_ids
            seen_blocked: set[int] = set()
            for sr in storage_rows:
                if str(sr.ticker).upper() != t_up:
                    continue
                sid = int(sr.storage_id)
                if sid in seen_blocked or sid in buy_sids:
                    continue
                if deposit_ids is not None and sid not in deposit_ids:
                    continue
                unblocked = constraints.unblocked_tickers_by_storage.get(sid, set())
                if t_up not in {str(x).upper() for x in unblocked}:
                    blocked_names.append(storage_names.get(sid, str(sid)))
                    seen_blocked.add(sid)
            if blocked_names:
                gaps.append(
                    f"**{ideal.ticker}**: идеальная покупка ~{ideal.amount:,.0f} — "
                    f"заблокирована на: {', '.join(blocked_names)}."
                )
            else:
                gaps.append(
                    f"**{ideal.ticker}**: идеальная покупка ~{ideal.amount:,.0f} — "
                    "нет счетов с разрешённой покупкой."
                )
            continue

        weights: Dict[int, float] = {}
        for sid in buy_sids:
            storage_val = sum(
                float(sr.value_display or 0.0)
                for sr in storage_rows
                if str(sr.ticker).upper() == t_up and int(sr.storage_id) == sid
            )
            if ticker_total > 0 and storage_val > 0:
                storage_target = target * (storage_val / ticker_total)
                weights[sid] = max(0.0, storage_target - storage_val)
            elif portfolio_gap > 0:
                weights[sid] = portfolio_gap / float(len(buy_sids))

        if not weights or sum(weights.values()) <= 1e-12:
            each = 1.0 / float(len(buy_sids))
            weights = {sid: each for sid in buy_sids}

        total_w = sum(weights.values())
        remaining_units = float(ideal.units)
        sorted_sids = sorted(weights.keys(), key=lambda s: -weights[s])

        for sid in sorted_sids:
            if remaining_units <= 1e-9:
                break
            w_share = weights[sid] / total_w
            alloc_units = remaining_units * w_share
            price = float(ideal.price_display)
            if is_crypto_ticker(ideal.ticker):
                take_units = round(alloc_units, 8)
            else:
                take_units = float(math.floor(alloc_units))
            if take_units <= 0:
                continue
            implied = take_units * price
            draft_buys.append(
                SuggestedBuy(
                    ticker=str(ideal.ticker),
                    asset_subclass_id=int(ideal.asset_subclass_id),
                    subclass_name=str(ideal.subclass_name),
                    spend_allocated=float(implied),
                    units=float(take_units),
                    implied_spend=float(implied),
                    price_display=price,
                    storage_id=int(sid),
                    storage_name=storage_names.get(int(sid), str(sid)),
                )
            )
            remaining_units -= take_units

        if remaining_units > 1e-6:
            short = remaining_units * float(ideal.price_display)
            gaps.append(
                f"**{ideal.ticker}**: из идеальной покупки ~{ideal.amount:,.0f} "
                f"размещено ~{ideal.amount - short:,.0f}; остаток ~{short:,.0f} (лоты)."
            )

    return draft_buys, gaps


def _fund_buys_with_cash_routing(
    sells: Sequence[SuggestedSell],
    draft_buys: Sequence[SuggestedBuy],
    V_external: float,
    constraints: RebalanceConstraints,
) -> Tuple[List[SuggestedBuy], List[SuggestedBuy], List[str], float]:
    """Route proceeds and external V to fund storage-scoped buys."""
    proceeds_pool: Dict[int, float] = defaultdict(float)
    for s in sells:
        proceeds_pool[int(s.storage_id)] += float(s.implied_proceeds)

    v_pool: Dict[int, float] = defaultdict(float)
    deposit_ids = constraints.deposit_storage_ids
    withdraw_ids = constraints.withdraw_storage_ids

    draft_list = list(draft_buys)
    deposit_demand: Dict[int, float] = defaultdict(float)
    for b in draft_list:
        if b.storage_id is None:
            continue
        sid = int(b.storage_id)
        if deposit_ids is None or sid in deposit_ids:
            deposit_demand[sid] += float(b.implied_spend)

    if float(V_external) > 0:
        total_demand = sum(deposit_demand.values())
        if deposit_ids:
            if total_demand > 1e-6:
                for sid, demand in deposit_demand.items():
                    v_pool[sid] += float(V_external) * (demand / total_demand)
            else:
                each = float(V_external) / float(len(deposit_ids))
                for sid in deposit_ids:
                    v_pool[sid] += each
        elif total_demand > 1e-6:
            for sid, demand in deposit_demand.items():
                v_pool[sid] += float(V_external) * (demand / total_demand)
        elif draft_list:
            storages = {int(b.storage_id) for b in draft_list if b.storage_id is not None}
            if storages:
                each = float(V_external) / float(len(storages))
                for sid in storages:
                    v_pool[sid] += each

    gaps: List[str] = []
    funded: List[SuggestedBuy] = []
    ext_buys: List[SuggestedBuy] = []
    sorted_buys = sorted(
        draft_list,
        key=lambda b: -float(b.implied_spend),
    )

    for buy in sorted_buys:
        if buy.storage_id is None:
            gaps.append(
                f"**{buy.ticker}**: покупка ~{buy.implied_spend:,.0f} — счёт не назначен."
            )
            continue

        dst = int(buy.storage_id)
        cost = float(buy.implied_spend)
        from_local = min(cost, proceeds_pool.get(dst, 0.0))
        proceeds_pool[dst] -= from_local
        remaining = cost - from_local

        from_v = min(remaining, v_pool.get(dst, 0.0))
        v_pool[dst] -= from_v
        v_used = from_v
        remaining -= from_v

        if remaining > 1e-6:
            for src in sorted(withdraw_ids):
                if src == dst:
                    continue
                transfer = min(remaining, proceeds_pool.get(src, 0.0))
                if transfer > 1e-6:
                    proceeds_pool[src] -= transfer
                    remaining -= transfer
                if remaining <= 1e-6:
                    break

        if remaining > 1e-6 and dst not in withdraw_ids:
            for src, bal in sorted(
                proceeds_pool.items(), key=lambda x: -x[1]
            ):
                if src == dst or src in withdraw_ids:
                    continue
                if bal <= 1e-6:
                    continue
                if src not in withdraw_ids:
                    continue
                transfer = min(remaining, bal)
                proceeds_pool[src] -= transfer
                remaining -= transfer
                if remaining <= 1e-6:
                    break

        if remaining > 1e-6:
            gaps.append(
                f"**{buy.ticker}** ({buy.storage_name}): идеальная покупка ~{cost:,.0f} — "
                f"не хватает ~{remaining:,.0f} "
                "(включите **«Вывод денег»** или **«Ввод денег»** на нужном счёте)."
            )
            continue

        funded.append(buy)
        if v_used > 1e-6:
            ext_buys.append(
                SuggestedBuy(
                    ticker=buy.ticker,
                    asset_subclass_id=buy.asset_subclass_id,
                    subclass_name=buy.subclass_name,
                    spend_allocated=float(v_used),
                    units=float(buy.units) * (v_used / cost) if cost > 0 else 0.0,
                    implied_spend=float(v_used),
                    price_display=buy.price_display,
                    storage_id=buy.storage_id,
                    storage_name=buy.storage_name,
                )
            )

    unused_proceeds = sum(v for v in proceeds_pool.values() if v > 1e-6)
    if unused_proceeds > 1e-6:
        gaps.append(
            f"Неразмещённая выручка после маршрутизации: ~{unused_proceeds:,.0f}."
        )

    return funded, ext_buys, gaps, unused_proceeds


def assign_ideal_plan_to_storages(
    ideal: IdealPortfolioPlan,
    storage_rows: Sequence[StoragePositionValue],
    ticker_rows_initial: Sequence[TickerPositionValue],
    constraints: RebalanceConstraints,
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V_external: float,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    sellable_tickers: Optional[set[str]] = None,
) -> Tuple[
    List[SuggestedSell],
    List[SuggestedBuy],
    List[SuggestedBuy],
    List[str],
    float,
]:
    """Phase 2: map ideal trades to storages and route cash."""
    sellable_keys = _resolve_sellable_positions(
        sellable_positions, sellable_tickers, storage_rows
    )
    value_by_ticker = _aggregate_ticker_values(storage_rows)
    targets_sell, _ = _storage_targets(
        storage_rows,
        ticker_rows_initial,
        target_pct_by_sub,
        set(),
        0.0,
    )
    targets_buy, _ = _storage_targets(
        storage_rows,
        ticker_rows_initial,
        target_pct_by_sub,
        constraints.blocked_tickers,
        V_external,
    )
    targets_portfolio, _ = _storage_targets(
        storage_rows,
        ticker_rows_initial,
        target_pct_by_sub,
        constraints.blocked_tickers,
        V_external,
    )

    sells, sell_gaps = _split_ideal_sells_to_storages(
        ideal.sells,
        storage_rows,
        sellable_keys,
        targets_sell,
        value_by_ticker,
        subclass_names,
    )
    draft_buys, buy_gaps = _split_ideal_buys_to_storages(
        ideal.buys,
        storage_rows,
        constraints,
        targets_buy,
        value_by_ticker,
        subclass_names,
        sells,
        V_external,
    )
    funded_buys, ext_buys, route_gaps, _ = _fund_buys_with_cash_routing(
        sells, draft_buys, V_external, constraints
    )

    if not funded_buys and sells and float(V_external) <= 1e-6:
        total_proceeds = sum(float(s.implied_proceeds) for s in sells)
        if total_proceeds > 1e-6:
            sell_gaps.append(
                f"Продажи не выполнены: выручку (~{total_proceeds:,.0f}) некуда разместить "
                "(заблокированы покупки или нет счёта с вводом)."
            )
            sells = []

    constraint_gaps = sell_gaps + buy_gaps + route_gaps
    return (
        sells,
        _merge_suggested_buys(funded_buys),
        _merge_suggested_buys(ext_buys),
        constraint_gaps,
        compute_deviation_l1(
            _aggregate_ticker_values(
                _apply_trades_to_storage_copy(storage_rows, sells, funded_buys)
            ),
            targets_portfolio,
        ),
    )


def _apply_trades_to_storage_copy(
    storage_rows: Sequence[StoragePositionValue],
    sells: Sequence[SuggestedSell],
    buys: Sequence[SuggestedBuy],
) -> List[StoragePositionValue]:
    state = _copy_storage_rows(storage_rows)
    constraints = RebalanceConstraints()
    for s in sells:
        _apply_sell_to_storage(state, s)
    for b in buys:
        _apply_buy_to_storage(state, b, constraints)
    return state


def execute_two_phase_rebalance_plan(
    storage_rows: List[StoragePositionValue],
    ticker_rows_initial: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    blocked: set[str],
    constraints: RebalanceConstraints,
    *,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    sellable_tickers: Optional[set[str]] = None,
) -> Tuple[
    IdealPortfolioPlan,
    List[SuggestedSell],
    List[SuggestedBuy],
    List[SuggestedBuy],
    List[SubclassBudgetUnallocated],
    List[str],
    float,
    float,
    float,
    float,
    int,
    List[str],
]:
    """Two-phase rebalance: ideal portfolio plan, then storage assignment."""
    ideal = compute_ideal_portfolio_plan(
        ticker_rows_initial,
        w,
        subclass_names,
        target_pct_by_sub,
        V_external,
        blocked,
        constraints,
        sellable_tickers=sellable_tickers,
    )

    sells, buys, ext_buys, constraint_gaps, dev_after_actual = (
        assign_ideal_plan_to_storages(
            ideal,
            storage_rows,
            ticker_rows_initial,
            constraints,
            target_pct_by_sub,
            subclass_names,
            V_external,
            sellable_positions=sellable_positions,
            sellable_tickers=sellable_tickers,
        )
    )

    s_initial = sum(float(r.value_display or 0.0) for r in ticker_rows_initial)
    t_final = s_initial + float(V_external)
    _, skipped_low_pnl = compute_ideal_ticker_sells(
        ticker_rows_initial,
        target_pct_by_sub,
        subclass_names,
        _sellable_tickers_set(constraints, sellable_tickers),
        constraints.unrealized_pnl_pct_by_ticker,
        blocked,
        0.0,
    )

    return (
        ideal,
        sells,
        buys,
        ext_buys,
        [],
        skipped_low_pnl,
        s_initial,
        t_final,
        ideal.deviation_l1_before,
        dev_after_actual,
        ideal.iterations,
        constraint_gaps,
    )


def optimize_rebalance_plan(
    storage_rows: List[StoragePositionValue],
    ticker_rows_initial: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    target_pct_by_sub: Mapping[int, float],
    V_external: float,
    blocked: set[str],
    constraints: RebalanceConstraints,
    *,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    sellable_tickers: Optional[set[str]] = None,
) -> Tuple[
    List[SuggestedSell],
    List[SuggestedBuy],
    List[SuggestedBuy],
    List[SubclassBudgetUnallocated],
    List[str],
    float,
    float,
    float,
    float,
    int,
    List[str],
]:
    (
        _ideal,
        sells,
        buys,
        ext_buys,
        unallocated,
        skipped,
        s_final,
        t_final,
        dev_before,
        dev_after,
        iterations_run,
        constraint_gaps,
    ) = execute_two_phase_rebalance_plan(
        storage_rows,
        ticker_rows_initial,
        w,
        subclass_names,
        target_pct_by_sub,
        V_external,
        blocked,
        constraints,
        sellable_positions=sellable_positions,
        sellable_tickers=sellable_tickers,
    )
    return (
        sells,
        buys,
        ext_buys,
        unallocated,
        skipped,
        s_final,
        t_final,
        dev_before,
        dev_after,
        iterations_run,
        constraint_gaps,
    )


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


def split_ideal_sub_to_ticker_targets(
    ideal_sub: float,
    subclass_rows: Sequence[TickerPositionValue],
    blocked: set[str],
) -> Dict[str, float]:
    """
    Цели по тикерам внутри подкласса:
    заблокированным — текущая стоимость; остаток ideal_sub поровну между незаблокированными.
    """
    blocked_reserved = 0.0
    unblocked: List[Tuple[str, float]] = []
    targets: Dict[str, float] = {}

    for r in subclass_rows:
        if r.value_display is None or float(r.value_display) <= 0:
            continue
        t_up = r.ticker.upper()
        val = float(r.value_display)
        if t_up in blocked:
            targets[t_up] = val
            blocked_reserved += val
        else:
            unblocked.append((r.ticker, val))

    residual = float(ideal_sub) - blocked_reserved
    for tkr, tval in split_subclass_budget_to_tickers(residual, unblocked).items():
        targets[tkr.upper()] = float(tval)
    return targets


def _eligible_subclass_ticker_rows(
    subclass_rows: Sequence[TickerPositionValue],
    blocked: set[str],
    eligible_tickers: Optional[set[str]],
    excluded_tickers: Optional[set[str]] = None,
) -> List[TickerPositionValue]:
    out: List[TickerPositionValue] = []
    for r in subclass_rows:
        if r.value_display is None or r.price_display is None or float(r.value_display) <= 0:
            continue
        t_up = str(r.ticker).upper()
        if t_up in blocked:
            continue
        if excluded_tickers is not None and t_up in excluded_tickers:
            continue
        if eligible_tickers is not None and t_up not in eligible_tickers:
            continue
        out.append(r)
    return out


def _split_budget_equal_among_rows(
    budget: float,
    rows: Sequence[TickerPositionValue],
) -> Dict[str, float]:
    if budget <= 0 or not rows:
        return {}
    each = float(budget) / float(len(rows))
    return {str(r.ticker).upper(): each for r in rows}


def split_subclass_budget_by_ticker_gaps(
    budget: float,
    ideal_sub: float,
    subclass_rows: Sequence[TickerPositionValue],
    blocked: set[str],
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> Dict[str, float]:
    """
    Распределить бюджет покупок между незаблокированными пропорционально
    max(0, целевая_стоимость − текущая). Не покупать тикеры на/выше цели.
    """
    if budget <= 0:
        return {}

    eligible_rows = _eligible_subclass_ticker_rows(
        subclass_rows, blocked, eligible_tickers, excluded_tickers
    )
    if not eligible_rows:
        return {}

    targets = split_ideal_sub_to_ticker_targets(ideal_sub, subclass_rows, blocked)
    gaps: Dict[str, float] = {}
    for r in eligible_rows:
        t_up = str(r.ticker).upper()
        target = targets.get(t_up)
        if target is None:
            continue
        gap = max(0.0, float(target) - float(r.value_display))
        if gap > 0:
            gaps[t_up] = gap

    total_gap = sum(gaps.values())
    if total_gap <= 1e-12:
        return {}

    gap_phase = min(float(budget), total_gap)
    return {t: gap_phase * g / total_gap for t, g in gaps.items()}


def compute_ticker_target_values(
    rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    blocked_tickers: Optional[set[str]] = None,
    portfolio_total: Optional[float] = None,
) -> Tuple[float, Dict[str, float]]:
    """
    Целевая стоимость тикеров:
    ideal_sub = portfolio_total × w_sub;
    заблокированным — текущая стоимость;
    остаток ideal_sub − Σ(целевых заблокированных) поровну между незаблокированными.

    portfolio_total: база для ideal_sub (по умолчанию — текущая S из позиций).
    """
    blocked = {x.upper() for x in (blocked_tickers or set())}
    w, _, _ = normalize_subclass_weights(target_pct_by_sub)
    if not w:
        return 0.0, {}

    v_by_sub = aggregate_values_by_subclass(rows)
    for sid in w:
        v_by_sub.setdefault(sid, 0.0)
    s_total = sum(float(x) for x in v_by_sub.values())
    base_total = float(portfolio_total) if portfolio_total is not None else s_total
    if base_total <= 0:
        return s_total, {}

    rows_by_sub: Dict[int, List[TickerPositionValue]] = defaultdict(list)
    for r in rows:
        rows_by_sub[r.asset_subclass_id].append(r)

    targets: Dict[str, float] = {}
    for sid, weight in w.items():
        ideal_sub = base_total * float(weight)
        subclass_targets = split_ideal_sub_to_ticker_targets(
            ideal_sub, rows_by_sub.get(sid, []), blocked
        )
        targets.update(subclass_targets)

    return s_total, targets


def allocate_cash_to_subclasses(
    v_by_sub: Mapping[int, float],
    w_by_sub: Mapping[int, float],
    V: float,
) -> Tuple[Dict[int, float], float, float, float]:
    """
    Deploy V toward subclass targets at T = S + V.
    First fill gaps proportionally (up to min(V, total_gap)), then any remainder by weights.
    Returns (budget_by_sub, S, T, total_gap).
    """
    S = sum(float(x) for x in v_by_sub.values())
    T = S + float(V)
    gaps: Dict[int, float] = {}
    for sid, w in w_by_sub.items():
        v = float(v_by_sub.get(sid, 0.0))
        ideal = T * float(w)
        gaps[sid] = max(0.0, ideal - v)
    total_gap = sum(gaps.values())
    if V <= 0:
        return {}, S, T, total_gap
    if total_gap <= 1e-12:
        budget = {sid: float(V) * float(w) for sid, w in w_by_sub.items()}
        return budget, S, T, total_gap
    gap_phase = min(float(V), total_gap)
    budget = {sid: gap_phase * g / total_gap for sid, g in gaps.items()}
    remainder = float(V) - gap_phase
    if remainder > 1e-12:
        for sid, w in w_by_sub.items():
            budget[sid] = float(budget.get(sid, 0.0)) + remainder * float(w)
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


def _ticker_pnl_pct(
    ticker_up: str,
    unrealized_pnl_pct_by_ticker: Optional[Mapping[str, float]],
) -> Optional[float]:
    if not unrealized_pnl_pct_by_ticker:
        return None
    return unrealized_pnl_pct_by_ticker.get(ticker_up)


def _rows_after_sells(
    rows: Sequence[TickerPositionValue],
    sells: Sequence[SuggestedSell],
) -> List[TickerPositionValue]:
    proceeds_by_ticker = defaultdict(float)
    for s in sells:
        proceeds_by_ticker[str(s.ticker).upper()] += float(s.implied_proceeds)
    out: List[TickerPositionValue] = []
    for r in rows:
        t_up = str(r.ticker).upper()
        sold = float(proceeds_by_ticker.get(t_up, 0.0))
        if sold <= 0 or r.value_display is None:
            out.append(r)
            continue
        new_val = max(0.0, float(r.value_display) - sold)
        out.append(
            TickerPositionValue(
                ticker=r.ticker,
                asset_subclass_id=r.asset_subclass_id,
                value_display=new_val,
                price_display=r.price_display,
            )
        )
    return out


def _rows_after_buys(
    rows: Sequence[TickerPositionValue],
    buys: Sequence[SuggestedBuy],
) -> List[TickerPositionValue]:
    added_by_ticker = defaultdict(float)
    for b in buys:
        added_by_ticker[str(b.ticker).upper()] += float(b.implied_spend)
    out: List[TickerPositionValue] = []
    for r in rows:
        t_up = str(r.ticker).upper()
        added = float(added_by_ticker.get(t_up, 0.0))
        if added <= 0 or r.value_display is None:
            out.append(r)
            continue
        out.append(
            TickerPositionValue(
                ticker=r.ticker,
                asset_subclass_id=r.asset_subclass_id,
                value_display=float(r.value_display) + added,
                price_display=r.price_display,
            )
        )
    return out


def _legacy_storage_rows(
    rows: Sequence[TickerPositionValue],
) -> List[StoragePositionValue]:
    return [
        StoragePositionValue(
            ticker=r.ticker,
            storage_id=0,
            storage_name="",
            asset_subclass_id=int(r.asset_subclass_id),
            value_display=r.value_display,
            price_display=r.price_display,
        )
        for r in rows
    ]


def _resolve_sellable_positions(
    sellable_positions: Optional[set[tuple[str, int]]],
    sellable_tickers: Optional[set[str]],
    storage_rows: Sequence[StoragePositionValue],
) -> set[tuple[str, int]]:
    if sellable_positions:
        return {(str(t).upper(), int(sid)) for t, sid in sellable_positions}
    if sellable_tickers:
        sellable_up = {str(t).upper() for t in sellable_tickers}
        return {
            (str(r.ticker).upper(), int(r.storage_id))
            for r in storage_rows
            if str(r.ticker).upper() in sellable_up
        }
    return set()


def compute_suggested_sells(
    ticker_rows: Sequence[TickerPositionValue],
    storage_rows: Sequence[StoragePositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    *,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    sellable_tickers: Optional[set[str]] = None,
    unrealized_pnl_pct_by_ticker: Optional[Mapping[str, float]] = None,
) -> Tuple[List[SuggestedSell], List[str]]:
    """
    Sell overweight at sellable (ticker, storage) rows with unrealized P&L >= MIN threshold.
    Storage target = portfolio target × (storage value / ticker value).
    """
    sellable_keys = _resolve_sellable_positions(
        sellable_positions, sellable_tickers, storage_rows
    )
    if not sellable_keys:
        return [], []

    w, _, _ = normalize_subclass_weights(target_pct_by_sub)
    if not w:
        return [], []

    v_by_sub = aggregate_values_by_subclass(ticker_rows)
    for sid in w:
        v_by_sub.setdefault(sid, 0.0)
    S = sum(float(x) for x in v_by_sub.values())
    T = S + float(V)
    if T <= 0:
        return [], []

    _, targets = compute_ticker_target_values(
        ticker_rows, target_pct_by_sub, portfolio_total=T
    )
    value_by_ticker: Dict[str, float] = defaultdict(float)
    for r in storage_rows:
        if r.value_display is not None and float(r.value_display) > 0:
            value_by_ticker[str(r.ticker).upper()] += float(r.value_display)

    skipped: List[str] = []
    sells: List[SuggestedSell] = []

    for sr in storage_rows:
        if sr.value_display is None or sr.price_display is None:
            continue
        t_up = str(sr.ticker).upper()
        key = (t_up, int(sr.storage_id))
        if key not in sellable_keys:
            continue
        ticker_total = float(value_by_ticker.get(t_up, 0.0))
        if ticker_total <= 0:
            continue
        target = targets.get(t_up)
        if target is None:
            continue
        if float(target) >= ticker_total - 1e-12:
            continue
        pnl_pct = _ticker_pnl_pct(t_up, unrealized_pnl_pct_by_ticker)
        if pnl_pct is None or float(pnl_pct) < MIN_SELL_UNREALIZED_PNL_PCT:
            skipped.append(sr.ticker)
            continue
        storage_val = float(sr.value_display)
        storage_target = float(target) * (storage_val / ticker_total)
        if storage_val <= storage_target + 1e-12:
            continue
        sell_gap = storage_val - storage_target
        price = float(sr.price_display)
        units, proceeds = units_and_implied_spend(sr.ticker, sell_gap, price)
        if units <= 0 or proceeds <= 0:
            continue
        sid = int(sr.asset_subclass_id)
        sells.append(
            SuggestedSell(
                ticker=str(sr.ticker),
                asset_subclass_id=sid,
                subclass_name=subclass_names.get(sid, str(sid)),
                units=float(units),
                implied_proceeds=float(proceeds),
                price_display=price,
                storage_id=int(sr.storage_id),
                storage_name=str(sr.storage_name),
            )
        )

    return sells, sorted(set(skipped))


def build_rebalance_diagnostics(
    storage_rows: Sequence[StoragePositionValue],
    ticker_rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    constraints: RebalanceConstraints,
    *,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    sellable_tickers: Optional[set[str]] = None,
    blocked: set[str],
    executed_sells: Sequence[SuggestedSell],
    ideal: Optional[IdealPortfolioPlan] = None,
    constraint_gaps: Optional[Sequence[str]] = None,
) -> List[str]:
    """Human-readable gap between ideal portfolio plan and executable storage trades."""
    notes: List[str] = list(constraint_gaps or [])
    seen: set[str] = set(notes)

    def _add(msg: str) -> None:
        if msg not in seen:
            notes.append(msg)
            seen.add(msg)

    sellable_keys = _resolve_sellable_positions(
        sellable_positions, sellable_tickers, storage_rows
    )
    executed_keys = {
        (str(s.ticker).upper(), int(s.storage_id)) for s in executed_sells
    }

    if not sellable_keys:
        _add(
            "Нет позиций, помеченных как «продажные» (Инструменты → ребалансировка)."
        )
        return notes

    w, _, _ = normalize_subclass_weights(target_pct_by_sub)
    if not w:
        return notes

    _, targets = compute_ticker_target_values(
        ticker_rows, target_pct_by_sub, blocked_tickers=blocked
    )
    value_by_ticker: Dict[str, float] = defaultdict(float)
    for sr in storage_rows:
        if sr.value_display is not None and float(sr.value_display) > 0:
            value_by_ticker[str(sr.ticker).upper()] += float(sr.value_display)

    if ideal and (ideal.sells or ideal.buys):
        ideal_sell_amt = sum(float(s.amount) for s in ideal.sells)
        ideal_buy_amt = sum(float(b.amount) for b in ideal.buys)
        actual_sell_amt = sum(float(s.implied_proceeds) for s in executed_sells)
        _add(
            f"Фаза 1 (идеальный портфель): продажи ~{ideal_sell_amt:,.0f}, "
            f"покупки ~{ideal_buy_amt:,.0f}."
        )
        if ideal_sell_amt > actual_sell_amt + 1000.0:
            _add(
                f"Фаза 2 (брокеры): фактические продажи ~{actual_sell_amt:,.0f} "
                f"(разрыв ~{ideal_sell_amt - actual_sell_amt:,.0f})."
            )

    for sr in storage_rows:
        if sr.value_display is None or sr.price_display is None:
            continue
        t_up = str(sr.ticker).upper()
        key = (t_up, int(sr.storage_id))
        ticker_total = float(value_by_ticker.get(t_up, 0.0))
        if ticker_total <= 0:
            continue
        target = targets.get(t_up)
        if target is None:
            continue
        storage_val = float(sr.value_display)
        storage_name = str(sr.storage_name or sr.storage_id)
        storage_target = float(target) * (storage_val / ticker_total)
        overweight_storage = storage_val - storage_target
        overweight_ticker = ticker_total - float(target)

        if key in executed_keys:
            continue

        if float(target) >= ticker_total - 1e-6:
            if storage_val > storage_target + 1000.0 and key in sellable_keys:
                _add(
                    f"**{sr.ticker}** ({storage_name}): локальный перевес {overweight_storage:,.0f}, "
                    "но по портфелю в целом тикер уже на цели."
                )
            continue

        if key not in sellable_keys:
            if overweight_storage > 1000.0 or overweight_ticker > 1000.0:
                _add(
                    f"**{sr.ticker}** ({storage_name}): перевес ~{max(overweight_storage, 0):,.0f}, "
                    "но позиция не помечена как «продажная»."
                )
            continue

        pnl_pct = _ticker_pnl_pct(t_up, constraints.unrealized_pnl_pct_by_ticker)
        if pnl_pct is None:
            _add(
                f"**{sr.ticker}** ({storage_name}): перевес ~{overweight_storage:,.0f}, "
                "но нет данных о себестоимости — продажа заблокирована (нужен P&L ≥ 10%)."
            )
            continue
        if float(pnl_pct) < MIN_SELL_UNREALIZED_PNL_PCT:
            _add(
                f"**{sr.ticker}** ({storage_name}): перевес ~{overweight_storage:,.0f}, "
                f"но P&L {float(pnl_pct) * 100:.1f}% < 10% — продажа не предлагается."
            )
            continue

    if not executed_sells and not notes:
        _add("Сделки не требуются: портфель на целевых долях или нет доступных операций.")

    underweight: List[Tuple[str, float]] = []
    for t_up, ticker_total in value_by_ticker.items():
        target = targets.get(t_up)
        if target is None:
            continue
        gap = float(target) - float(ticker_total)
        if gap > 1000.0:
            underweight.append((t_up, gap))
    underweight.sort(key=lambda x: -x[1])

    sell_sources = {int(s.storage_id) for s in executed_sells}
    if not sell_sources and sellable_keys:
        sell_sources = {int(sid) for _, sid in sellable_keys}

    if underweight and _underweight_needs_withdraw_hint(
        underweight, constraints, sell_sources
    ):
        _add(_format_underweight_diagnostics(underweight, storage_rows, constraints))
        _add(
            "Для покупки недовесных инструментов на другом брокере включите "
            "**«Вывод денег»** на счёте с продажами."
        )

    return notes


def _spent_from_buys(buys: Sequence[SuggestedBuy]) -> float:
    return sum(float(b.implied_spend) for b in buys)


def _weights_to_target_pct(w: Mapping[int, float]) -> Dict[int, float]:
    return {int(sid): float(wv) * 100.0 for sid, wv in w.items()}


def _deploy_leftover_cash(
    rows: Sequence[TickerPositionValue],
    existing_buys: Sequence[SuggestedBuy],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    cash: float,
    blocked: set[str],
    *,
    buy_storage_id: Optional[int] = None,
    buy_storage_name: Optional[str] = None,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> List[SuggestedBuy]:
    """Spend remaining cash greedily toward largest positive gaps, then by weights."""
    extra: List[SuggestedBuy] = []
    working = _rows_after_buys(rows, existing_buys)
    remaining = float(cash)
    target_pct = _weights_to_target_pct(w)

    for _ in range(500):
        if remaining <= 1e-6:
            break
        s_now = sum(
            float(r.value_display)
            for r in working
            if r.value_display is not None
        )
        _, targets = compute_ticker_target_values(
            working,
            target_pct,
            blocked_tickers=blocked,
            portfolio_total=s_now + remaining,
        )

        best: Optional[TickerPositionValue] = None
        best_gap = 0.0
        for r in working:
            if r.value_display is None or r.price_display is None:
                continue
            t_up = str(r.ticker).upper()
            if t_up in blocked:
                continue
            if excluded_tickers is not None and t_up in excluded_tickers:
                continue
            if eligible_tickers is not None and t_up not in eligible_tickers:
                continue
            gap = float(targets.get(t_up, 0.0)) - float(r.value_display)
            if gap > best_gap + 1e-9:
                best_gap = gap
                best = r

        bought = False
        if best is not None and best_gap > 1e-9:
            price = float(best.price_display)
            units, implied = units_and_implied_spend(
                best.ticker, min(remaining, best_gap), price
            )
            if units > 0:
                sid = int(best.asset_subclass_id)
                buy = SuggestedBuy(
                    ticker=str(best.ticker),
                    asset_subclass_id=sid,
                    subclass_name=subclass_names.get(sid, str(sid)),
                    spend_allocated=float(min(remaining, best_gap)),
                    units=float(units),
                    implied_spend=float(implied),
                    price_display=price,
                    storage_id=buy_storage_id,
                    storage_name=buy_storage_name,
                )
                extra.append(buy)
                working = _rows_after_buys(working, [buy])
                remaining -= float(implied)
                bought = True

        if not bought:
            affordable = _try_affordable_lot_buy(
                working,
                remaining,
                targets,
                blocked,
                eligible_tickers=eligible_tickers,
                excluded_tickers=excluded_tickers,
            )
            if affordable is not None:
                r, units, implied = affordable
                sid = int(r.asset_subclass_id)
                buy = SuggestedBuy(
                    ticker=str(r.ticker),
                    asset_subclass_id=sid,
                    subclass_name=subclass_names.get(sid, str(sid)),
                    spend_allocated=float(implied),
                    units=float(units),
                    implied_spend=float(implied),
                    price_display=float(r.price_display),
                    storage_id=buy_storage_id,
                    storage_name=buy_storage_name,
                )
                extra.append(buy)
                working = _rows_after_buys(working, [buy])
                remaining -= float(implied)
                bought = True

        if bought:
            continue

        if best is not None and best_gap > 1e-9:
            break

        batch, _, _, _, _ = _compute_suggested_buys(
            working,
            w,
            subclass_names,
            remaining,
            blocked,
            buy_storage_id=buy_storage_id,
            buy_storage_name=buy_storage_name,
            eligible_tickers=eligible_tickers,
            excluded_tickers=excluded_tickers,
        )
        spent = _spent_from_buys(batch)
        if spent <= 1e-6:
            break
        extra.extend(batch)
        working = _rows_after_buys(working, batch)
        remaining -= spent

    return extra


def _allocate_buys_full_deploy(
    rows: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked: set[str],
    *,
    buy_storage_id: Optional[int] = None,
    buy_storage_name: Optional[str] = None,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> Tuple[List[SuggestedBuy], List[SubclassBudgetUnallocated], float, float, float]:
    buys, unallocated, S, T, total_gap = _compute_suggested_buys(
        rows,
        w,
        subclass_names,
        float(V),
        blocked,
        buy_storage_id=buy_storage_id,
        buy_storage_name=buy_storage_name,
        eligible_tickers=eligible_tickers,
        excluded_tickers=excluded_tickers,
    )
    pool = float(V) - _spent_from_buys(buys)
    if pool > 1e-6:
        extra = _deploy_leftover_cash(
            rows,
            buys,
            w,
            subclass_names,
            pool,
            blocked,
            buy_storage_id=buy_storage_id,
            buy_storage_name=buy_storage_name,
            eligible_tickers=eligible_tickers,
            excluded_tickers=excluded_tickers,
        )
        if extra:
            buys = list(buys) + extra
            unallocated = []
    return buys, unallocated, S, T, total_gap


def _compute_suggested_buys(
    rows: Sequence[TickerPositionValue],
    w: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked: set[str],
    *,
    buy_storage_id: Optional[int] = None,
    buy_storage_name: Optional[str] = None,
    eligible_tickers: Optional[set[str]] = None,
    excluded_tickers: Optional[set[str]] = None,
) -> Tuple[List[SuggestedBuy], List[SubclassBudgetUnallocated], float, float, float]:
    """Returns (buys, unallocated, S, T, total_gap)."""
    v_by_sub = aggregate_values_by_subclass(rows)
    for sid in w:
        v_by_sub.setdefault(sid, 0.0)

    budget_by_sub, S, T, total_gap = allocate_cash_to_subclasses(v_by_sub, w, V)
    if V <= 0:
        return [], [], S, T, total_gap

    by_ticker = {r.ticker.upper(): r for r in rows}
    rows_by_sub: Dict[int, List[TickerPositionValue]] = defaultdict(list)
    for r in rows:
        rows_by_sub[r.asset_subclass_id].append(r)

    buys: List[SuggestedBuy] = []
    unallocated: List[SubclassBudgetUnallocated] = []

    orphan_budget = 0.0
    sub_budgets: Dict[int, float] = {}
    sub_eligible: Dict[int, List[TickerPositionValue]] = {}
    for sid, bud in budget_by_sub.items():
        if bud <= 1e-12:
            continue
        subclass_rows = rows_by_sub.get(sid, [])
        eligible_rows = _eligible_subclass_ticker_rows(
            subclass_rows, blocked, eligible_tickers, excluded_tickers
        )
        if not eligible_rows:
            orphan_budget += float(bud)
            continue
        sub_budgets[sid] = float(bud)
        sub_eligible[sid] = eligible_rows

    if orphan_budget > 1e-12 and sub_budgets:
        receiver_weight = sum(float(w[sid]) for sid in sub_budgets)
        if receiver_weight > 0:
            for sid in sub_budgets:
                sub_budgets[sid] += orphan_budget * float(w[sid]) / receiver_weight
        else:
            each = orphan_budget / float(len(sub_budgets))
            for sid in sub_budgets:
                sub_budgets[sid] += each
    elif orphan_budget > 1e-12:
        fallback_eligible: Dict[int, List[TickerPositionValue]] = {}
        for sid in w:
            elig = _eligible_subclass_ticker_rows(
                rows_by_sub.get(sid, []), blocked, eligible_tickers, excluded_tickers
            )
            if elig:
                fallback_eligible[sid] = elig
        if fallback_eligible:
            receiver_weight = sum(float(w[sid]) for sid in fallback_eligible)
            for sid, elig in fallback_eligible.items():
                share = (
                    orphan_budget * float(w[sid]) / receiver_weight
                    if receiver_weight > 0
                    else orphan_budget / float(len(fallback_eligible))
                )
                sub_budgets[sid] = float(sub_budgets.get(sid, 0.0)) + share
                sub_eligible[sid] = elig
        else:
            unallocated.append(
                SubclassBudgetUnallocated(
                    subclass_id=-1,
                    subclass_name="—",
                    budget=orphan_budget,
                    reason="Нет доступных позиций для размещения средств",
                )
            )

    for sid, bud in sub_budgets.items():
        name = subclass_names.get(sid, str(sid))
        subclass_rows = rows_by_sub.get(sid, [])
        ideal_sub = T * float(w[sid])
        alloc = split_subclass_budget_by_ticker_gaps(
            bud, ideal_sub, subclass_rows, blocked, eligible_tickers, excluded_tickers
        )
        if not alloc:
            reason = (
                "Нет незаблокированных позиций с котировкой ниже цели на этом месте хранения"
                if eligible_tickers is not None
                else "Нет незаблокированных позиций с котировкой ниже цели в этом подклассе"
            )
            unallocated.append(
                SubclassBudgetUnallocated(
                    subclass_id=sid,
                    subclass_name=name,
                    budget=bud,
                    reason=reason,
                )
            )
            continue
        entry_rows = []
        for tkr, spend in alloc.items():
            r = by_ticker.get(tkr.upper())
            if r is None or r.price_display is None:
                continue
            price = float(r.price_display)
            units, implied = units_and_implied_spend(tkr, spend, price)
            entry_rows.append(
                {
                    "ticker": tkr,
                    "spend_allocated": float(spend),
                    "price": price,
                    "units": float(units),
                    "implied": float(implied),
                    "is_crypto": bool(is_crypto_ticker(tkr)),
                }
            )

        implied_sum = sum(float(e["implied"]) for e in entry_rows)
        if implied_sum <= 1e-6 and bud > 1e-6:
            targets_map = split_ideal_sub_to_ticker_targets(
                ideal_sub, subclass_rows, blocked
            )
            affordable = _try_affordable_lot_buy(
                sub_eligible.get(sid, []),
                float(bud),
                targets_map,
                blocked,
                eligible_tickers=eligible_tickers,
                excluded_tickers=excluded_tickers,
            )
            if affordable is not None:
                r, units, implied = affordable
                entry_rows = [
                    {
                        "ticker": str(r.ticker),
                        "spend_allocated": float(implied),
                        "price": float(r.price_display),
                        "units": float(units),
                        "implied": float(implied),
                        "is_crypto": bool(is_crypto_ticker(str(r.ticker))),
                    }
                ]
                implied_sum = float(implied)

        residual_sub = max(0.0, float(bud) - implied_sum)
        if residual_sub > 1e-12 and entry_rows:
            crypto_entries = [e for e in entry_rows if bool(e["is_crypto"]) and float(e["price"]) > 0]
            if crypto_entries:
                target = max(crypto_entries, key=lambda e: float(e["spend_allocated"]))
                p = float(target["price"])
                extra_units = math.floor((residual_sub / p) * 1e8) / 1e8
                if extra_units > 0:
                    extra_spend = extra_units * p
                    target["units"] = float(target["units"]) + float(extra_units)
                    target["implied"] = float(target["implied"]) + float(extra_spend)
                    residual_sub = max(0.0, residual_sub - float(extra_spend))

            stock_entries = [e for e in entry_rows if (not bool(e["is_crypto"])) and float(e["price"]) > 0]
            if stock_entries and residual_sub > 1e-12:
                cheapest = min(stock_entries, key=lambda e: float(e["price"]))
                cp = float(cheapest["price"])
                extra_lots = int(math.floor(residual_sub / cp))
                if extra_lots > 0:
                    extra_spend = float(extra_lots) * cp
                    cheapest["units"] = float(cheapest["units"]) + float(extra_lots)
                    cheapest["implied"] = float(cheapest["implied"]) + float(extra_spend)
                    residual_sub = max(0.0, residual_sub - float(extra_spend))

        for e in entry_rows:
            if float(e["units"]) <= 0 and float(e["spend_allocated"]) > 0:
                continue
            buys.append(
                SuggestedBuy(
                    ticker=str(e["ticker"]),
                    asset_subclass_id=sid,
                    subclass_name=name,
                    spend_allocated=float(e["spend_allocated"]),
                    units=float(e["units"]),
                    implied_spend=float(e["implied"]),
                    price_display=float(e["price"]),
                    storage_id=buy_storage_id,
                    storage_name=buy_storage_name,
                )
            )

    return buys, unallocated, S, T, total_gap


def _merge_suggested_buys(buys: Sequence[SuggestedBuy]) -> List[SuggestedBuy]:
    merged: Dict[Tuple[str, Optional[int]], SuggestedBuy] = {}
    for b in buys:
        key = (str(b.ticker).upper(), b.storage_id)
        prev = merged.get(key)
        if prev is None:
            merged[key] = b
            continue
        merged[key] = SuggestedBuy(
            ticker=str(prev.ticker),
            asset_subclass_id=int(prev.asset_subclass_id),
            subclass_name=str(prev.subclass_name),
            spend_allocated=float(prev.spend_allocated) + float(b.spend_allocated),
            units=float(prev.units) + float(b.units),
            implied_spend=float(prev.implied_spend) + float(b.implied_spend),
            price_display=float(prev.price_display),
            storage_id=prev.storage_id,
            storage_name=prev.storage_name,
        )
    return list(merged.values())


def _proceeds_by_storage(sells: Sequence[SuggestedSell]) -> Dict[int, Tuple[float, str]]:
    out: Dict[int, Tuple[float, str]] = {}
    for s in sells:
        sid = int(s.storage_id)
        prev = float(out.get(sid, (0.0, ""))[0])
        out[sid] = (prev + float(s.implied_proceeds), str(s.storage_name))
    return out


def _eligible_tickers_at_storages(
    storage_ids: set[int],
    unblocked_tickers_by_storage: Mapping[int, set[str]],
) -> set[str]:
    out: set[str] = set()
    for sid in storage_ids:
        out |= {str(t).upper() for t in unblocked_tickers_by_storage.get(int(sid), set())}
    return out


def compute_ideal_rebalance_plan(
    rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked_tickers: Optional[set[str]] = None,
    sellable_tickers: Optional[set[str]] = None,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    unrealized_pnl_pct_by_ticker: Optional[Mapping[str, float]] = None,
) -> RebalancePlan:
    """
    Phase-1 only: ideal portfolio rebalance as a single pool (no storage assignment).
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

    pnl_map = {
        str(k).upper(): float(v)
        for k, v in (unrealized_pnl_pct_by_ticker or {}).items()
    }
    constraints = RebalanceConstraints(
        blocked_tickers=blocked,
        sellable_positions=sellable_positions or set(),
        unrealized_pnl_pct_by_ticker=pnl_map,
    )

    ideal = compute_ideal_portfolio_plan(
        rows,
        w,
        subclass_names,
        target_pct_by_sub,
        float(V),
        blocked,
        constraints,
        sellable_tickers=sellable_tickers,
    )

    sells = [_ideal_to_suggested_sell(s) for s in ideal.sells]
    buys = [_ideal_to_suggested_buy(b) for b in ideal.buys]

    _, skipped = compute_ideal_ticker_sells(
        rows,
        target_pct_by_sub,
        subclass_names,
        _sellable_tickers_set(constraints, sellable_tickers),
        constraints.unrealized_pnl_pct_by_ticker,
        blocked,
        0.0,
    )

    working = _rows_after_sells(rows, sells)
    sold_tickers = {str(s.ticker).upper() for s in ideal.sells}
    ext_buys, _, _, _, _ = _allocate_buys_full_deploy(
        working,
        w,
        subclass_names,
        float(V),
        blocked,
        excluded_tickers=sold_tickers if sold_tickers else None,
    )

    plan.suggested_sells = sells
    plan.suggested_buys = buys
    plan.skipped_sells_low_pnl = skipped
    plan.skipped_sells_undeployable = []
    plan.ideal_sells = list(ideal.sells)
    plan.ideal_buys = list(ideal.buys)
    plan.constraint_gaps = []
    plan.rebalance_diagnostics = []
    plan.deviation_l1_before = float(ideal.deviation_l1_before)
    plan.deviation_l1_after = float(ideal.deviation_l1_after)
    plan.deviation_l1_after_ideal = float(ideal.deviation_l1_after)
    plan.optimizer_iterations = int(ideal.iterations)
    plan.total_sell_proceeds = sum(float(s.implied_proceeds) for s in sells)
    plan.V_effective = float(V)
    plan.unallocated = []

    s_initial = sum(float(r.value_display or 0.0) for r in rows)
    plan.S = s_initial
    plan.T = s_initial + float(V)
    _, _, _, total_gap = allocate_cash_to_subclasses(
        aggregate_values_by_subclass(rows), w, float(V)
    )
    plan.total_gap = total_gap
    plan.total_implied_spend = sum(x.implied_spend for x in buys)
    external_spend = sum(float(b.implied_spend) for b in ext_buys)
    plan.residual_vs_V = float(V) - external_spend
    proceed_spend = plan.total_implied_spend - external_spend
    plan.residual_sell_proceeds = max(
        0.0, float(plan.total_sell_proceeds) - proceed_spend
    )
    return plan


def _build_cash_sources(
    sells: Sequence[SuggestedSell],
    V_external: float,
    constraints: RebalanceConstraints,
) -> List[Dict[str, object]]:
    """Cash available for buys: per-storage sell proceeds + external V.

    Each source: {storage_id: int|None, amount: float, mobile: bool, is_external: bool}.
    mobile=True means cash may leave its storage toward deposit storages.
    """
    withdraw_ids = constraints.withdraw_storage_ids
    sources: List[Dict[str, object]] = []
    for sid, (amount, _name) in _proceeds_by_storage(sells).items():
        if float(amount) <= 1e-9:
            continue
        sources.append(
            {
                "storage_id": int(sid),
                "amount": float(amount),
                "mobile": int(sid) in withdraw_ids,
                "is_external": False,
            }
        )
    if float(V_external) > 1e-9:
        sources.append(
            {
                "storage_id": None,
                "amount": float(V_external),
                "mobile": True,
                "is_external": True,
            }
        )
    return sources


def _reachable_storages(
    source: Mapping[str, object],
    constraints: RebalanceConstraints,
    all_storage_ids: set[int],
) -> set[int]:
    """Storage ids where cash from this source can be spent on buys."""
    deposit_ids = constraints.deposit_storage_ids
    deposit_set = (
        {int(x) for x in deposit_ids} if deposit_ids is not None else set(all_storage_ids)
    )
    out: set[int] = set()
    sid = source.get("storage_id")
    if sid is not None:
        out.add(int(sid))
    if bool(source.get("mobile")) or bool(source.get("is_external")):
        out |= deposit_set
    return out


def _is_unblocked_at(ticker: str, sid: int, constraints: RebalanceConstraints) -> bool:
    allowed = constraints.unblocked_tickers_by_storage.get(int(sid))
    if not allowed:
        return False
    return str(ticker).upper() in {str(t).upper() for t in allowed}


def _choose_buy_destination(
    ticker: str,
    source: Mapping[str, object],
    reachable: set[int],
    constraints: RebalanceConstraints,
) -> Optional[int]:
    """Pick a storage to buy `ticker`, preferring the source's own storage."""
    src_sid = source.get("storage_id")
    if src_sid is not None and int(src_sid) in reachable and _is_unblocked_at(
        ticker, int(src_sid), constraints
    ):
        return int(src_sid)
    for sid in sorted(reachable):
        if _is_unblocked_at(ticker, sid, constraints):
            return int(sid)
    return None


def _is_deposit_inflow(source: Mapping[str, object], dst_sid: int) -> bool:
    """True when cash enters `dst_sid` from outside (external V or cross-storage)."""
    if bool(source.get("is_external")):
        return True
    src_sid = source.get("storage_id")
    if src_sid is None:
        return True
    if int(src_sid) == int(dst_sid):
        return False
    return bool(source.get("mobile"))


def _record_funding_flow(
    flows: Dict[int, StorageCashFlow],
    source: Mapping[str, object],
    dst_sid: int,
    amount: float,
) -> None:
    """Record how ``amount`` reached ``dst_sid`` to fund a buy."""
    if amount <= 1e-9:
        return
    dst = int(dst_sid)
    if dst not in flows:
        flows[dst] = StorageCashFlow()
    if bool(source.get("is_external")):
        flows[dst].external_inflow += float(amount)
        return
    src_sid = source.get("storage_id")
    if src_sid is None:
        flows[dst].external_inflow += float(amount)
        return
    src = int(src_sid)
    if src == dst:
        return
    if src not in flows:
        flows[src] = StorageCashFlow()
    flows[src].transfer_out += float(amount)
    flows[dst].transfer_in += float(amount)


def _unrecord_funding_flow(
    flows: Dict[int, StorageCashFlow],
    source: Mapping[str, object],
    dst_sid: int,
    amount: float,
) -> None:
    """Reverse ``_record_funding_flow`` when a buy is rejected."""
    if amount <= 1e-9:
        return
    dst = int(dst_sid)
    if bool(source.get("is_external")):
        if dst in flows:
            flows[dst].external_inflow = max(
                0.0, flows[dst].external_inflow - float(amount)
            )
        return
    src_sid = source.get("storage_id")
    if src_sid is None:
        if dst in flows:
            flows[dst].external_inflow = max(
                0.0, flows[dst].external_inflow - float(amount)
            )
        return
    src = int(src_sid)
    if src == dst:
        return
    if src in flows:
        flows[src].transfer_out = max(0.0, flows[src].transfer_out - float(amount))
    if dst in flows:
        flows[dst].transfer_in = max(0.0, flows[dst].transfer_in - float(amount))


def _populate_storage_trade_totals(
    flows: Dict[int, StorageCashFlow],
    sells: Sequence[SuggestedSell],
    buys: Sequence[SuggestedBuy],
) -> None:
    for s in sells:
        sid = int(s.storage_id)
        if sid not in flows:
            flows[sid] = StorageCashFlow()
        flows[sid].sell_proceeds += float(s.implied_proceeds)
    for b in buys:
        if b.storage_id is None:
            continue
        sid = int(b.storage_id)
        if sid not in flows:
            flows[sid] = StorageCashFlow()
        flows[sid].purchases += float(b.implied_spend)


def _allocate_external_v_to_storages(
    buys: Sequence[SuggestedBuy],
    V_external: float,
    deposit_ids: Optional[set[int]],
) -> Dict[int, float]:
    """Split external V across deposit-enabled storages proportional to buy demand there."""
    if float(V_external) <= 1e-9:
        return {}
    buy_list = list(buys)
    deposit_demand: Dict[int, float] = defaultdict(float)
    for b in buy_list:
        if b.storage_id is None:
            continue
        sid = int(b.storage_id)
        if deposit_ids is None or sid in deposit_ids:
            deposit_demand[sid] += float(b.implied_spend)

    allocation: Dict[int, float] = {}
    total_demand = sum(deposit_demand.values())
    if deposit_ids:
        if total_demand > 1e-6:
            for sid, demand in deposit_demand.items():
                allocation[int(sid)] = float(V_external) * (demand / total_demand)
        elif deposit_ids:
            each = float(V_external) / float(len(deposit_ids))
            for sid in deposit_ids:
                allocation[int(sid)] = each
    elif total_demand > 1e-6:
        for sid, demand in deposit_demand.items():
            allocation[int(sid)] = float(V_external) * (demand / total_demand)
    elif buy_list:
        storages = {int(b.storage_id) for b in buy_list if b.storage_id is not None}
        if storages:
            each = float(V_external) / float(len(storages))
            for sid in storages:
                allocation[int(sid)] = each
    return allocation


def compute_storage_cash_flows(
    sells: Sequence[SuggestedSell],
    buys: Sequence[SuggestedBuy],
    V_external: float,
    constraints: RebalanceConstraints,
) -> Dict[int, StorageCashFlow]:
    """Simulate cash routing for a rebalance plan (legacy two-phase path).

    Returns per-storage flows where
    ``продажи − вывод + ввод + перевод_в ≈ покупки``.
    """
    flows: Dict[int, StorageCashFlow] = defaultdict(StorageCashFlow)
    _populate_storage_trade_totals(flows, sells, buys)

    proceeds_pool: Dict[int, float] = defaultdict(float)
    for s in sells:
        proceeds_pool[int(s.storage_id)] += float(s.implied_proceeds)

    v_pool: Dict[int, float] = dict(
        _allocate_external_v_to_storages(
            buys,
            float(V_external),
            constraints.deposit_storage_ids,
        )
    )
    withdraw_ids = constraints.withdraw_storage_ids
    buy_list = list(buys)

    for buy in sorted(buy_list, key=lambda b: -float(b.implied_spend)):
        if buy.storage_id is None:
            continue
        dst = int(buy.storage_id)
        cost = float(buy.implied_spend)

        from_local = min(cost, proceeds_pool.get(dst, 0.0))
        proceeds_pool[dst] -= from_local
        remaining = cost - from_local

        from_v = min(remaining, v_pool.get(dst, 0.0))
        if from_v > 1e-6:
            flows[dst].external_inflow += from_v
        v_pool[dst] = max(0.0, v_pool.get(dst, 0.0) - from_v)
        remaining -= from_v

        if remaining > 1e-6:
            for src in sorted(withdraw_ids):
                if src == dst:
                    continue
                transfer = min(remaining, proceeds_pool.get(src, 0.0))
                if transfer > 1e-6:
                    flows[int(src)].transfer_out += transfer
                    flows[dst].transfer_in += transfer
                    proceeds_pool[src] -= transfer
                    remaining -= transfer
                if remaining <= 1e-6:
                    break

    return dict(flows)


def _purchase_meets_minimum(constraints: RebalanceConstraints, implied: float) -> bool:
    min_p = float(constraints.min_purchase_amount or 0.0)
    return min_p <= 1e-9 or float(implied) + 1e-9 >= min_p


def _filter_buys_by_amount_limits(
    buys_with_meta: Sequence[Tuple[SuggestedBuy, int, bool]],
    cash_sources: List[Dict[str, object]],
    constraints: RebalanceConstraints,
    flows: Optional[Dict[int, StorageCashFlow]] = None,
) -> List[SuggestedBuy]:
    """Drop buys for tickers whose total order is below min_purchase; drop deposit inflows
    into storages whose total inflow is below min_deposit."""
    min_p = float(constraints.min_purchase_amount or 0.0)
    min_d = float(constraints.min_deposit_amount or 0.0)

    # Aggregate totals per ticker and per deposit-destination storage.
    ticker_tot: Dict[str, float] = defaultdict(float)
    deposit_tot: Dict[int, float] = defaultdict(float)
    for buy, _src_idx, is_dep in buys_with_meta:
        ticker_tot[str(buy.ticker).upper()] += float(buy.implied_spend)
        if is_dep and buy.storage_id is not None:
            deposit_tot[int(buy.storage_id)] += float(buy.implied_spend)

    # Tickers whose *total* planned spend is below min_purchase are excluded.
    blocked_tickers: set[str] = set()
    if min_p > 1e-9:
        for t, tot in ticker_tot.items():
            if 1e-9 < tot + 1e-9 < min_p:
                blocked_tickers.add(t)

    blocked_dsts: set[int] = set()
    if min_d > 1e-9:
        for sid, tot in deposit_tot.items():
            if 1e-9 < tot + 1e-9 < min_d:
                blocked_dsts.add(int(sid))

    kept: List[SuggestedBuy] = []
    for buy, src_idx, is_dep in buys_with_meta:
        implied = float(buy.implied_spend)
        sid = buy.storage_id
        if str(buy.ticker).upper() in blocked_tickers:
            cash_sources[src_idx]["amount"] = float(cash_sources[src_idx]["amount"]) + implied
            if flows is not None and buy.storage_id is not None:
                _unrecord_funding_flow(
                    flows, cash_sources[src_idx], int(buy.storage_id), implied
                )
            continue
        if is_dep and sid is not None and int(sid) in blocked_dsts:
            cash_sources[src_idx]["amount"] = float(cash_sources[src_idx]["amount"]) + implied
            if flows is not None and buy.storage_id is not None:
                _unrecord_funding_flow(
                    flows, cash_sources[src_idx], int(buy.storage_id), implied
                )
            continue
        kept.append(buy)
    return kept


def _try_buy_one_lot(
    t_up: str,
    gap: Dict[str, float],
    ticker_info: Mapping[str, Tuple[float, int, str]],
    constraints: RebalanceConstraints,
    cash_sources: List[Dict[str, object]],
    source_order: Sequence[int],
    storage_names: Mapping[int, str],
    buys_with_meta: List[Tuple[SuggestedBuy, int, bool]],
    *,
    single_lot: bool = False,
    flows: Optional[Dict[int, StorageCashFlow]] = None,
) -> bool:
    """Buy up to one batch (or full gap) for `t_up` from the first viable source.

    When *single_lot* is True (proportional mode) the spend per step is capped
    at ``max(price, min_purchase_amount)`` to keep steps comparable across
    tickers with very different lot sizes (e.g. TMOS ~7 ₽ vs BTC ~8 M ₽).
    The per-step min-purchase check is skipped because lot-rounding can shrink
    the spend by a few kopecks — the step was already sized at ≥ min_purchase.
    """
    price, sub_id, sub_name = ticker_info[t_up]
    if gap.get(t_up, 0.0) <= 1e-6 or price <= 0:
        return False
    for i in source_order:
        src = cash_sources[i]
        cash = float(src["amount"])
        if cash <= 1e-9:
            continue
        reach = src["_reach"]  # type: ignore[assignment]
        dst = _choose_buy_destination(t_up, src, reach, constraints)
        if dst is None:
            continue
        if single_lot:
            # One proportional step: budget at least one lot but never below
            # min_purchase_amount so that most steps are meaningfully sized.
            # We skip the min-purchase filter here because lot-rounding can
            # shave a few units off the step – the intent is already satisfied.
            min_purch = float(getattr(constraints, "min_purchase_amount", 0) or 0)
            step = max(float(price), min_purch)
            spend_cap = min(step, gap[t_up], cash)
        else:
            spend_cap = min(gap[t_up], cash)
        units, implied = units_and_implied_spend(t_up, spend_cap, price)
        if units <= 0 or implied <= 1e-9:
            continue
        if not single_lot and not _purchase_meets_minimum(constraints, implied):
            continue
        buys_with_meta.append(
            (
                SuggestedBuy(
                    ticker=t_up,
                    asset_subclass_id=int(sub_id),
                    subclass_name=str(sub_name),
                    spend_allocated=float(implied),
                    units=float(units),
                    implied_spend=float(implied),
                    price_display=float(price),
                    storage_id=int(dst),
                    storage_name=storage_names.get(int(dst), str(dst)),
                ),
                i,
                _is_deposit_inflow(src, int(dst)),
            )
        )
        src["amount"] = cash - implied
        gap[t_up] -= implied
        if flows is not None:
            _record_funding_flow(flows, src, int(dst), float(implied))
        return True
    return False


def _allocate_buys_constrained(
    targets: Mapping[str, float],
    value_by_ticker: Mapping[str, float],
    ticker_info: Mapping[str, Tuple[float, int, str]],
    constraints: RebalanceConstraints,
    cash_sources: List[Dict[str, object]],
    sold_tickers: set[str],
    storage_names: Mapping[int, str],
    all_storage_ids: set[int],
    flows: Optional[Dict[int, StorageCashFlow]] = None,
) -> Tuple[List[SuggestedBuy], List[Dict[str, object]]]:
    """Reachability-aware buy allocation minimizing ticker-level L1.

    ``buy_allocation_mode`` in constraints:
    - ``max_gap``: always fund the largest remaining underweight first.
    - ``proportional``: round-robin by lowest ``allocated/gap`` (fair share).

    Returns (buys, remaining_sources_with_leftover_cash).
    """
    gap: Dict[str, float] = {}
    for t_up, (price, _sid, _name) in ticker_info.items():
        if t_up in sold_tickers or price <= 0:
            continue
        g = float(targets.get(t_up, 0.0)) - float(value_by_ticker.get(t_up, 0.0))
        if g > 1e-6:
            gap[t_up] = g

    for src in cash_sources:
        src["_reach"] = _reachable_storages(src, constraints, all_storage_ids)

    source_order = sorted(
        range(len(cash_sources)),
        key=lambda i: (
            bool(cash_sources[i].get("mobile")),
            bool(cash_sources[i].get("is_external")),
            cash_sources[i].get("storage_id")
            if cash_sources[i].get("storage_id") is not None
            else 1 << 30,
        ),
    )

    buys_with_meta: List[Tuple[SuggestedBuy, int, bool]] = []
    mode = str(constraints.buy_allocation_mode or "max_gap").strip().lower()
    proportional = mode == "proportional"

    if proportional:
        # initial_gap captures the full underweight before any purchase so the
        # ratio `allocated / initial_gap` stays comparable across tickers with
        # very different lot sizes (e.g. TMOS ~7 ₽ vs BTC ~8 M ₽).
        initial_gap: Dict[str, float] = dict(gap)
        allocated: Dict[str, float] = defaultdict(float)
        exhausted: set[str] = set()
        while True:
            candidates = [t for t in gap if gap[t] > 1e-6 and t not in exhausted]
            if not candidates:
                break
            # Pick the ticker that has covered the smallest fraction of its
            # original underweight.  Break ties by largest initial gap so that
            # large underweights aren't starved when everything starts at 0/x.
            t_up = min(
                candidates,
                key=lambda t: (
                    allocated[t] / initial_gap[t],
                    -initial_gap[t],
                ),
            )
            if _try_buy_one_lot(
                t_up,
                gap,
                ticker_info,
                constraints,
                cash_sources,
                source_order,
                storage_names,
                buys_with_meta,
                single_lot=True,
                flows=flows,
            ):
                allocated[t_up] += float(buys_with_meta[-1][0].implied_spend)
            else:
                exhausted.add(t_up)
    else:
        exhausted: set[str] = set()
        while True:
            candidates = [t for t in gap if gap[t] > 1e-6 and t not in exhausted]
            if not candidates:
                break
            t_up = max(candidates, key=lambda t: gap[t])
            if _try_buy_one_lot(
                t_up,
                gap,
                ticker_info,
                constraints,
                cash_sources,
                source_order,
                storage_names,
                buys_with_meta,
                flows=flows,
            ):
                pass
            else:
                exhausted.add(t_up)

    buys = _filter_buys_by_amount_limits(
        buys_with_meta, cash_sources, constraints, flows=flows
    )
    leftovers = [s for s in cash_sources if float(s["amount"]) > 1e-6]
    return _merge_suggested_buys(buys), leftovers


def _polish_leftover_cash(
    buys: List[SuggestedBuy],
    targets: Mapping[str, float],
    value_by_ticker: Mapping[str, float],
    ticker_info: Mapping[str, Tuple[float, int, str]],
    constraints: RebalanceConstraints,
    leftovers: List[Dict[str, object]],
    sold_tickers: set[str],
    storage_names: Mapping[int, str],
    all_storage_ids: set[int],
    flows: Optional[Dict[int, StorageCashFlow]] = None,
) -> List[SuggestedBuy]:
    # post-allocation value per ticker (display)
    post: Dict[str, float] = {t: float(v) for t, v in value_by_ticker.items()}
    for b in buys:
        post[str(b.ticker).upper()] = post.get(str(b.ticker).upper(), 0.0) + float(
            b.implied_spend
        )

    extra_with_meta: List[Tuple[SuggestedBuy, int, bool]] = []
    for src in leftovers:
        src["_reach"] = _reachable_storages(src, constraints, all_storage_ids)

    while True:
        best = None  # (improvement, src_idx, t_up, dst, units, implied)
        for idx, src in enumerate(leftovers):
            cash = float(src["amount"])
            if cash <= 1e-9:
                continue
            reach = src["_reach"]  # type: ignore[assignment]
            for t_up, (price, sub_id, sub_name) in ticker_info.items():
                if t_up in sold_tickers or price <= 0 or is_crypto_ticker(t_up):
                    continue
                if cash < price:
                    continue
                dst = _choose_buy_destination(t_up, src, reach, constraints)
                if dst is None:
                    continue
                # Buy enough lots to meet min_purchase if one lot doesn't reach it.
                min_p = float(constraints.min_purchase_amount or 0.0)
                step_lots = max(1, math.ceil(min_p / price)) if min_p > price else 1
                implied = step_lots * price
                if implied > cash:
                    implied = math.floor(cash / price) * price
                    step_lots = int(round(implied / price))
                if step_lots < 1 or implied < price - 1e-9:
                    continue
                cur = post.get(t_up, 0.0)
                tgt = float(targets.get(t_up, 0.0))
                before = abs(cur - tgt)
                after = abs(cur + implied - tgt)
                improvement = before - after
                if improvement <= 1e-6:
                    continue
                if best is None or improvement > best[0]:
                    best = (improvement, idx, t_up, int(dst), float(step_lots), float(implied))
        if best is None:
            break
        _imp, idx, t_up, dst, units, implied = best
        price, sub_id, sub_name = ticker_info[t_up]
        buy = SuggestedBuy(
            ticker=t_up,
            asset_subclass_id=int(sub_id),
            subclass_name=str(sub_name),
            spend_allocated=float(implied),
            units=float(units),
            implied_spend=float(implied),
            price_display=float(price),
            storage_id=int(dst),
            storage_name=storage_names.get(int(dst), str(dst)),
        )
        extra_with_meta.append((buy, idx, _is_deposit_inflow(leftovers[idx], int(dst))))
        leftovers[idx]["amount"] = float(leftovers[idx]["amount"]) - implied
        post[t_up] = post.get(t_up, 0.0) + implied
        if flows is not None:
            _record_funding_flow(flows, leftovers[idx], int(dst), float(implied))

    extra = _filter_buys_by_amount_limits(
        extra_with_meta, leftovers, constraints, flows=flows
    )
    return _merge_suggested_buys(list(buys) + extra)


def _reduce_sells_for_trapped_cash(
    sells: Sequence[SuggestedSell],
    trapped_by_sid: Mapping[int, float],
) -> Tuple[List[SuggestedSell], bool]:
    """Trim sells whose proceeds cannot be deployed (would become idle cash).

    For each storage with idle proceeds `excess`, cut whole lots of the sold
    tickers there (crypto fractionally) so the remaining proceeds fully deploy.
    """
    by_sid: Dict[int, List[SuggestedSell]] = defaultdict(list)
    for s in sells:
        by_sid[int(s.storage_id)].append(s)

    result: List[SuggestedSell] = []
    changed = False
    for sid, group in by_sid.items():
        excess = float(trapped_by_sid.get(int(sid), 0.0))
        if excess <= 1e-6:
            result.extend(group)
            continue
        for s in sorted(group, key=lambda x: -float(x.implied_proceeds)):
            price = float(s.price_display)
            if excess <= 1e-6 or price <= 0:
                result.append(s)
                continue
            if is_crypto_ticker(s.ticker):
                cut_value = min(excess, float(s.implied_proceeds))
                new_units = max(
                    0.0, round((float(s.implied_proceeds) - cut_value) / price, 8)
                )
            else:
                cur_lots = int(round(float(s.units)))
                lots_to_cut = min(cur_lots, math.ceil(excess / price - 1e-9))
                new_units = float(cur_lots - lots_to_cut)
            new_proceeds = new_units * price
            cut_value = float(s.implied_proceeds) - new_proceeds
            if cut_value > 1e-6:
                changed = True
                excess -= cut_value
            if new_units > 1e-9:
                result.append(
                    SuggestedSell(
                        ticker=s.ticker,
                        asset_subclass_id=int(s.asset_subclass_id),
                        subclass_name=str(s.subclass_name),
                        units=new_units,
                        implied_proceeds=new_proceeds,
                        price_display=price,
                        storage_id=int(s.storage_id),
                        storage_name=str(s.storage_name),
                    )
                )
    return result, changed


def _force_deploy_cash(
    buys: List[SuggestedBuy],
    leftovers: List[Dict[str, object]],
    targets: Mapping[str, float],
    value_by_ticker: Mapping[str, float],
    ticker_info: Mapping[str, Tuple[float, int, str]],
    constraints: RebalanceConstraints,
    sold_tickers: set[str],
    storage_names: Mapping[int, str],
    all_storage_ids: set[int],
    flows: Optional[Dict[int, StorageCashFlow]] = None,
) -> Tuple[List[SuggestedBuy], float]:
    """Deploy ALL remaining cash, even past target, minimizing the L1 increase.

    Whole lots first (pick the lot with the smallest resulting L1 delta), then
    any sub-lot remainder fractionally into a reachable crypto ticker.
    """
    post: Dict[str, float] = {t: float(v) for t, v in value_by_ticker.items()}
    for b in buys:
        post[str(b.ticker).upper()] = post.get(str(b.ticker).upper(), 0.0) + float(
            b.implied_spend
        )

    for src in leftovers:
        src["_reach"] = _reachable_storages(src, constraints, all_storage_ids)

    extra_with_meta: List[Tuple[SuggestedBuy, int, bool]] = []
    max_iters = 1_000_000
    iters = 0
    while iters < max_iters:
        iters += 1
        best = None  # (delta_l1, src_idx, t_up, dst, price, sub_id, sub_name)
        for idx, src in enumerate(leftovers):
            cash = float(src["amount"])
            if cash <= 1e-9:
                continue
            reach = src["_reach"]  # type: ignore[assignment]
            for t_up, (price, sub_id, sub_name) in ticker_info.items():
                if t_up in sold_tickers or price <= 0 or is_crypto_ticker(t_up):
                    continue
                if cash < price:
                    continue
                dst = _choose_buy_destination(t_up, src, reach, constraints)
                if dst is None:
                    continue
                # Buy enough lots to meet min_purchase if one lot doesn't reach it.
                min_p = float(constraints.min_purchase_amount or 0.0)
                step_lots = max(1, math.ceil(min_p / price)) if min_p > price else 1
                step_spend = step_lots * price
                if step_spend > cash:
                    step_lots = max(1, int(math.floor(cash / price)))
                    step_spend = step_lots * price
                if step_spend > cash + 1e-9:
                    continue
                cur = post.get(t_up, 0.0)
                tgt = float(targets.get(t_up, 0.0))
                delta = abs(cur + step_spend - tgt) - abs(cur - tgt)
                if best is None or delta < best[0]:
                    best = (delta, idx, t_up, int(dst), float(price), sub_id, sub_name,
                            step_lots, step_spend)
        if best is None:
            break
        _delta, idx, t_up, dst, price, sub_id, sub_name, step_lots, step_spend = best
        buy = SuggestedBuy(
            ticker=t_up,
            asset_subclass_id=int(sub_id),
            subclass_name=str(sub_name),
            spend_allocated=float(step_spend),
            units=float(step_lots),
            implied_spend=float(step_spend),
            price_display=float(price),
            storage_id=int(dst),
            storage_name=storage_names.get(int(dst), str(dst)),
        )
        extra_with_meta.append((buy, idx, _is_deposit_inflow(leftovers[idx], int(dst))))
        leftovers[idx]["amount"] = float(leftovers[idx]["amount"]) - step_spend
        post[t_up] = post.get(t_up, 0.0) + step_spend
        if flows is not None:
            _record_funding_flow(flows, leftovers[idx], int(dst), float(step_spend))

    # sub-lot remainder: spend fractionally on a reachable crypto ticker
    for idx, src in enumerate(leftovers):
        cash = float(src["amount"])
        if cash <= 1e-6:
            continue
        reach = src["_reach"]  # type: ignore[assignment]
        chosen = None
        best_dev = None
        for t_up, (price, sub_id, sub_name) in ticker_info.items():
            if t_up in sold_tickers or price <= 0 or not is_crypto_ticker(t_up):
                continue
            dst = _choose_buy_destination(t_up, src, reach, constraints)
            if dst is None:
                continue
            # prefer the most underweight reachable crypto
            dev = float(targets.get(t_up, 0.0)) - post.get(t_up, 0.0)
            if best_dev is None or dev > best_dev:
                best_dev = dev
                chosen = (t_up, float(price), sub_id, sub_name, int(dst))
        if chosen is None:
            continue
        t_up, price, sub_id, sub_name, dst = chosen
        units, implied = units_and_implied_spend(t_up, cash, price)
        if units <= 0 or implied <= 1e-9:
            continue
        if not _purchase_meets_minimum(constraints, implied):
            continue
        buy = SuggestedBuy(
            ticker=t_up,
            asset_subclass_id=int(sub_id),
            subclass_name=str(sub_name),
            spend_allocated=float(implied),
            units=float(units),
            implied_spend=float(implied),
            price_display=float(price),
            storage_id=int(dst),
            storage_name=storage_names.get(int(dst), str(dst)),
        )
        extra_with_meta.append((buy, idx, _is_deposit_inflow(src, int(dst))))
        src["amount"] = cash - implied
        post[t_up] = post.get(t_up, 0.0) + implied
        if flows is not None:
            _record_funding_flow(flows, src, int(dst), float(implied))

    extra = _filter_buys_by_amount_limits(
        extra_with_meta, leftovers, constraints, flows=flows
    )
    remaining = sum(
        float(s["amount"]) for s in leftovers if float(s["amount"]) > 1e-6
    )
    return _merge_suggested_buys(list(buys) + extra), remaining


def compute_constrained_rebalance_plan(
    rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked_tickers: Optional[set[str]] = None,
    sellable_tickers: Optional[set[str]] = None,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    storage_rows: Optional[Sequence[StoragePositionValue]] = None,
    unblocked_tickers_by_storage: Optional[Mapping[int, set[str]]] = None,
    deposit_storage_ids: Optional[set[int]] = None,
    withdraw_storage_ids: Optional[set[int]] = None,
    unrealized_pnl_pct_by_ticker: Optional[Mapping[str, float]] = None,
    min_purchase_amount: float = 0.0,
    min_deposit_amount: float = 0.0,
    buy_allocation_mode: str = "max_gap",
) -> RebalancePlan:
    """
    Phase-2 (new logic): reach the ideal end-state under storage constraints.

    Sells come from the ideal plan; buys are rebuilt from per-ticker underweight
    and routed to reachable, unblocked storages, minimizing ticker-level L1.
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

    storage_state = (
        list(storage_rows) if storage_rows is not None else _legacy_storage_rows(rows)
    )
    deposit_ids = (
        {int(x) for x in deposit_storage_ids}
        if deposit_storage_ids is not None
        else None
    )
    withdraw_ids = (
        {int(x) for x in withdraw_storage_ids}
        if withdraw_storage_ids is not None
        else set()
    )
    unblocked_by_storage = {
        int(k): {str(t).upper() for t in v}
        for k, v in (unblocked_tickers_by_storage or {}).items()
    }
    pnl_map = {
        str(k).upper(): float(v)
        for k, v in (unrealized_pnl_pct_by_ticker or {}).items()
    }
    constraints = RebalanceConstraints(
        blocked_tickers=blocked,
        sellable_positions=sellable_positions or set(),
        unblocked_tickers_by_storage=unblocked_by_storage,
        deposit_storage_ids=deposit_ids,
        withdraw_storage_ids=withdraw_ids,
        unrealized_pnl_pct_by_ticker=pnl_map,
        min_purchase_amount=float(min_purchase_amount or 0.0),
        min_deposit_amount=float(min_deposit_amount or 0.0),
        buy_allocation_mode=(
            "proportional"
            if str(buy_allocation_mode or "max_gap").strip().lower() == "proportional"
            else "max_gap"
        ),
    )

    s_initial = sum(float(r.value_display or 0.0) for r in rows)
    t_final = s_initial + float(V)

    ideal = compute_ideal_portfolio_plan(
        rows,
        w,
        subclass_names,
        target_pct_by_sub,
        float(V),
        blocked,
        constraints,
        sellable_tickers=sellable_tickers,
    )

    _, targets = compute_ticker_target_values(
        rows, target_pct_by_sub, blocked_tickers=blocked, portfolio_total=t_final
    )

    sellable_keys = _resolve_sellable_positions(
        sellable_positions, sellable_tickers, storage_state
    )
    value_by_ticker = _aggregate_ticker_values(storage_state)
    targets_storage_sell, _ = _storage_targets(
        storage_state, rows, target_pct_by_sub, blocked, 0.0
    )
    sells, sell_gaps = _split_ideal_sells_to_storages(
        ideal.sells,
        storage_state,
        sellable_keys,
        targets_storage_sell,
        value_by_ticker,
        subclass_names,
    )

    ticker_info: Dict[str, Tuple[float, int, str]] = {}
    for r in rows:
        if r.price_display is None:
            continue
        t_up = str(r.ticker).upper()
        sid = int(r.asset_subclass_id)
        ticker_info[t_up] = (
            float(r.price_display),
            sid,
            subclass_names.get(sid, str(sid)),
        )

    all_storage_ids = {int(sr.storage_id) for sr in storage_state}
    storage_names = _storage_id_to_name(storage_state)
    current_by_ticker = _ticker_values_map(rows)

    initial_sell_proceeds = sum(float(s.implied_proceeds) for s in sells)

    # Iteratively trim sells whose proceeds cannot be deployed (idle cash),
    # settling `sells` until no storage-bound proceeds remain unplaced.
    for _ in range(20):
        sold_tickers = {str(s.ticker).upper() for s in sells}
        cash_sources = _build_cash_sources(sells, float(V), constraints)
        _, probe_leftovers = _allocate_buys_constrained(
            targets,
            current_by_ticker,
            ticker_info,
            constraints,
            cash_sources,
            sold_tickers,
            storage_names,
            all_storage_ids,
        )
        trapped_by_sid: Dict[int, float] = {}
        for src in probe_leftovers:
            if bool(src.get("is_external")):
                continue
            sid = src.get("storage_id")
            amt = float(src.get("amount", 0.0))
            if sid is not None and amt > 1e-6:
                trapped_by_sid[int(sid)] = trapped_by_sid.get(int(sid), 0.0) + amt
        if not trapped_by_sid:
            break
        sells, changed = _reduce_sells_for_trapped_cash(sells, trapped_by_sid)
        if not changed:
            break

    # Final allocation consistent with the settled `sells`.
    sold_tickers = {str(s.ticker).upper() for s in sells}
    cash_sources = _build_cash_sources(sells, float(V), constraints)
    cash_flows: Dict[int, StorageCashFlow] = defaultdict(StorageCashFlow)
    buys, leftovers = _allocate_buys_constrained(
        targets,
        current_by_ticker,
        ticker_info,
        constraints,
        cash_sources,
        sold_tickers,
        storage_names,
        all_storage_ids,
        flows=cash_flows,
    )
    buys = _polish_leftover_cash(
        buys,
        targets,
        current_by_ticker,
        ticker_info,
        constraints,
        leftovers,
        sold_tickers,
        storage_names,
        all_storage_ids,
        flows=cash_flows,
    )
    # Force-deploy any remaining cash (external V and residuals) fully, even
    # past target, so no idle cash is left.
    buys, unplaced_cash = _force_deploy_cash(
        buys,
        leftovers,
        targets,
        current_by_ticker,
        ticker_info,
        constraints,
        sold_tickers,
        storage_names,
        all_storage_ids,
        flows=cash_flows,
    )

    constraint_gaps = list(sell_gaps)
    final_sell_proceeds = sum(float(s.implied_proceeds) for s in sells)
    sells_trimmed = initial_sell_proceeds - final_sell_proceeds
    if sells_trimmed > 1.0:
        constraint_gaps.append(
            f"Продажи уменьшены на ~{sells_trimmed:,.0f}: эту выручку некуда вложить "
            "(нет достижимого счёта с разрешённой покупкой) — актив оставлен."
        )
    if unplaced_cash > 1.0:
        constraint_gaps.append(
            f"Неразмещаемый кэш: ~{unplaced_cash:,.0f} "
            "(нет ни одного достижимого тикера с разрешённой покупкой)."
        )

    plan.suggested_sells = sells
    plan.suggested_buys = buys
    _, skipped = compute_ideal_ticker_sells(
        rows,
        target_pct_by_sub,
        subclass_names,
        _sellable_tickers_set(constraints, sellable_tickers),
        constraints.unrealized_pnl_pct_by_ticker,
        blocked,
        0.0,
    )
    plan.skipped_sells_low_pnl = skipped
    plan.skipped_sells_undeployable = []
    plan.ideal_sells = list(ideal.sells)
    plan.ideal_buys = list(ideal.buys)
    plan.constraint_gaps = constraint_gaps
    plan.rebalance_diagnostics = []
    plan.deviation_l1_before = float(ideal.deviation_l1_before)
    plan.deviation_l1_after_ideal = float(ideal.deviation_l1_after)
    plan.deviation_l1_after = compute_deviation_l1(
        _aggregate_ticker_values(
            _apply_trades_to_storage_copy(storage_state, sells, buys)
        ),
        targets,
    )
    plan.optimizer_iterations = int(ideal.iterations)
    plan.total_sell_proceeds = sum(float(s.implied_proceeds) for s in sells)
    plan.V_effective = float(V)
    plan.unallocated = []
    plan.S = s_initial
    plan.T = t_final
    _, _, _, total_gap = allocate_cash_to_subclasses(
        aggregate_values_by_subclass(rows), w, float(V)
    )
    plan.total_gap = total_gap
    plan.total_implied_spend = sum(float(b.implied_spend) for b in buys)
    # external spend = buys funded beyond sell proceeds
    total_proceeds = plan.total_sell_proceeds
    proceeds_spent = min(plan.total_implied_spend, total_proceeds)
    external_spend = max(0.0, plan.total_implied_spend - proceeds_spent)
    plan.residual_vs_V = float(V) - external_spend
    plan.residual_sell_proceeds = max(0.0, total_proceeds - proceeds_spent)
    for cf in cash_flows.values():
        cf.sell_proceeds = 0.0
        cf.purchases = 0.0
    _populate_storage_trade_totals(cash_flows, sells, buys)
    plan.storage_cash_flows = dict(cash_flows)
    return plan


def compute_rebalance_plan(
    rows: Sequence[TickerPositionValue],
    target_pct_by_sub: Mapping[int, float],
    subclass_names: Mapping[int, str],
    V: float,
    blocked_tickers: Optional[set[str]] = None,
    sellable_tickers: Optional[set[str]] = None,
    sellable_positions: Optional[set[tuple[str, int]]] = None,
    storage_rows: Optional[Sequence[StoragePositionValue]] = None,
    unblocked_tickers_by_storage: Optional[Mapping[int, set[str]]] = None,
    deposit_storage_ids: Optional[set[int]] = None,
    withdraw_storage_ids: Optional[set[int]] = None,
    unrealized_pnl_pct_by_ticker: Optional[Mapping[str, float]] = None,
) -> RebalancePlan:
    """
    rows: all held tickers with optional values/prices in display currency.
    storage_rows: per (ticker, storage) values; required for storage-scoped sells.

    Two-phase rebalance: ideal portfolio plan (phase 1), then storage assignment (phase 2).
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

    storage_state = (
        list(storage_rows) if storage_rows is not None else _legacy_storage_rows(rows)
    )
    deposit_ids = (
        {int(x) for x in deposit_storage_ids}
        if deposit_storage_ids is not None
        else None
    )
    withdraw_ids = (
        {int(x) for x in withdraw_storage_ids}
        if withdraw_storage_ids is not None
        else set()
    )
    unblocked_by_storage = {
        int(k): {str(t).upper() for t in v}
        for k, v in (unblocked_tickers_by_storage or {}).items()
    }
    pnl_map = {
        str(k).upper(): float(v)
        for k, v in (unrealized_pnl_pct_by_ticker or {}).items()
    }
    constraints = RebalanceConstraints(
        blocked_tickers=blocked,
        sellable_positions=sellable_positions or set(),
        unblocked_tickers_by_storage=unblocked_by_storage,
        deposit_storage_ids=deposit_ids,
        withdraw_storage_ids=withdraw_ids,
        unrealized_pnl_pct_by_ticker=pnl_map,
    )

    (
        ideal,
        sells,
        buys,
        ext_buys,
        unallocated,
        skipped,
        _s_after,
        t_final,
        dev_before,
        dev_after,
        iterations_run,
        constraint_gaps,
    ) = execute_two_phase_rebalance_plan(
        storage_state,
        rows,
        w,
        subclass_names,
        target_pct_by_sub,
        float(V),
        blocked,
        constraints,
        sellable_positions=sellable_positions,
        sellable_tickers=sellable_tickers,
    )

    plan.suggested_sells = sells
    plan.skipped_sells_low_pnl = skipped
    plan.skipped_sells_undeployable = []
    plan.ideal_sells = list(ideal.sells)
    plan.ideal_buys = list(ideal.buys)
    plan.constraint_gaps = list(constraint_gaps)
    plan.deviation_l1_after_ideal = float(ideal.deviation_l1_after)
    plan.rebalance_diagnostics = build_rebalance_diagnostics(
        storage_state,
        rows,
        target_pct_by_sub,
        constraints,
        sellable_positions=sellable_positions,
        sellable_tickers=sellable_tickers,
        blocked=blocked,
        executed_sells=sells,
        ideal=ideal,
        constraint_gaps=constraint_gaps,
    )
    plan.total_sell_proceeds = sum(float(s.implied_proceeds) for s in sells)
    plan.V_effective = float(V)
    plan.suggested_buys = buys
    plan.unallocated = unallocated
    plan.deviation_l1_before = dev_before
    plan.deviation_l1_after = dev_after
    plan.optimizer_iterations = iterations_run

    s_initial = sum(float(r.value_display or 0.0) for r in rows)
    plan.S = s_initial
    plan.T = t_final
    _, _, _, total_gap = allocate_cash_to_subclasses(
        aggregate_values_by_subclass(rows), w, float(V)
    )
    plan.total_gap = total_gap
    plan.total_implied_spend = sum(x.implied_spend for x in plan.suggested_buys)
    external_spend = sum(float(b.implied_spend) for b in ext_buys)
    plan.residual_vs_V = float(V) - external_spend
    proceed_spend = plan.total_implied_spend - external_spend
    plan.residual_sell_proceeds = max(
        0.0, float(plan.total_sell_proceeds) - proceed_spend
    )
    plan.storage_cash_flows = compute_storage_cash_flows(
        plan.suggested_sells,
        plan.suggested_buys,
        float(V),
        constraints,
    )
    return plan
