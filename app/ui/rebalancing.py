"""Ребалансировка: покупки и опциональные продажи по настройкам."""

from collections import defaultdict
from typing import Sequence

import pandas as pd
import streamlit as st

from app.db import (
    get_app_setting,
    list_buy_blocked_tickers,
    list_asset_classes,
    list_asset_subclasses,
    list_portfolio_blocks,
    list_positions,
    list_positions_by_ticker,
    list_storages,
    set_app_setting,
    set_portfolio_blocked,
    set_portfolio_sellable,
    set_storage_rebalance_flags,
    set_storage_taxable_flag,
)
from app.services.fx import convert_amount, format_money, refresh_fx_cache
from app.services.performance import compute_ticker_unrealized_pnl_pct
from app.services.portfolio_order import (
    build_portfolio_ticker_order,
    portfolio_ticker_sort_key,
)
from app.services.price_currency import resolve_quote_currency
from app.services.prices import (
    get_app_quotes,
    is_crypto_ticker,
    normalize_quote_price_for_valuation,
    request_quotes_refresh,
)

from app.services.rebalancing import (
    StoragePositionValue,
    TickerPositionValue,
    compute_constrained_rebalance_plan,
    compute_ticker_target_values,
)
from app.services.tax import compute_rebalance_tax_summary

_REBALANCE_MIN_PURCHASE_SETTING = "rebalance_min_purchase_amount"
_REBALANCE_MIN_DEPOSIT_SETTING = "rebalance_min_deposit_amount"
_REBALANCE_BUY_ALLOCATION_SETTING = "rebalance_buy_allocation_mode"
_REBALANCE_TAX_RATE_SETTING = "rebalance_tax_rate"
_BUY_ALLOCATION_OPTIONS = ("max_gap", "proportional")


def _read_rebalance_limits() -> tuple[float, float, str]:
    stored_min_purchase = get_app_setting(_REBALANCE_MIN_PURCHASE_SETTING)
    min_purchase = (
        float(stored_min_purchase) if stored_min_purchase not in (None, "") else 0.0
    )
    stored_min_deposit = get_app_setting(_REBALANCE_MIN_DEPOSIT_SETTING)
    min_deposit = (
        float(stored_min_deposit) if stored_min_deposit not in (None, "") else 0.0
    )
    stored_mode = get_app_setting(_REBALANCE_BUY_ALLOCATION_SETTING)
    buy_mode = stored_mode if stored_mode in _BUY_ALLOCATION_OPTIONS else "max_gap"
    return min_purchase, min_deposit, buy_mode


def _persist_rebalance_limits(
    min_purchase: float, min_deposit: float, buy_mode: str
) -> None:
    set_app_setting(_REBALANCE_MIN_PURCHASE_SETTING, str(min_purchase))
    set_app_setting(_REBALANCE_MIN_DEPOSIT_SETTING, str(min_deposit))
    if buy_mode in _BUY_ALLOCATION_OPTIONS:
        set_app_setting(_REBALANCE_BUY_ALLOCATION_SETTING, str(buy_mode))


def _read_tax_settings() -> tuple[float, dict[int, bool]]:
    stored_rate = get_app_setting(_REBALANCE_TAX_RATE_SETTING)
    rate = float(stored_rate) if stored_rate not in (None, "") else 0.13
    taxable_by_id = {int(s.id): bool(s.taxable) for s in list_storages()}
    return rate, taxable_by_id


def _persist_tax_settings(rate_pct: float, edited_rows: list[dict], current: list[dict]) -> None:
    rate = max(0.0, float(rate_pct)) / 100.0
    set_app_setting(_REBALANCE_TAX_RATE_SETTING, str(rate))
    id_by_name = {str(r["name"]): int(r["id"]) for r in current}
    for row in edited_rows:
        storage_id = id_by_name.get(str(row["Место хранения"]))
        if storage_id is None:
            continue
        set_storage_taxable_flag(storage_id, taxable=bool(row["Облагается налогом"]))


_REBALANCE_TABLE_COLUMNS = (
    "Подкласс",
    "Тикер",
    "Кол-во",
    "Сумма",
    "Цель",
    "Откл до",
    "Откл после",
)
_REBALANCE_PORTFOLIO_COLUMNS = (
    "Класс",
    "Подкласс",
    "Тикер",
    "Стоимость",
    "Стоимость после",
    "Откл до",
    "Откл после",
)


def _buy_entry_key(entry: dict) -> tuple[str, int | None]:
    t_up = str(entry["ticker"]).upper()
    sid = entry.get("storage_id")
    return t_up, int(sid) if sid is not None else None


def _entry_has_fixed_storage(entry: dict) -> bool:
    if entry.get("storage_id") is None:
        return False
    storage_name = str(entry.get("storage_name") or "").strip()
    return bool(storage_name) and storage_name != "—"


def _sort_trade_entries(
    entries: list[dict],
    *,
    ticker_order: dict[str, int],
) -> list[dict]:
    """Same row order as the main portfolio summary table (Tickers view)."""
    return sorted(
        entries,
        key=lambda x: (
            ticker_order.get(str(x["ticker"]).upper(), 10**9),
            str(x["ticker"]).upper(),
        ),
    )


def _sort_buy_entries(
    buy_entries: list[dict],
    *,
    ticker_order: dict[str, int],
) -> list[dict]:
    return _sort_trade_entries(buy_entries, ticker_order=ticker_order)


def _render_storage_cash_flow_table(
    *,
    cash_flows: dict[int, object],
    all_storages,
    display_ccy: str,
) -> None:
    rows_out: list[dict[str, str]] = []
    for s in sorted(
        all_storages,
        key=lambda x: (int(x.sort_order), str(x.name).casefold()),
    ):
        sid = int(s.id)
        cf = cash_flows.get(sid)
        if cf is None:
            continue
        sells = float(getattr(cf, "sell_proceeds", 0.0))
        external = float(getattr(cf, "external_inflow", 0.0))
        transfer_in = float(getattr(cf, "transfer_in", 0.0))
        transfer_out = float(getattr(cf, "transfer_out", 0.0))
        purchases = float(getattr(cf, "purchases", 0.0))
        if (
            sells <= 1e-6
            and external <= 1e-6
            and transfer_in <= 1e-6
            and transfer_out <= 1e-6
            and purchases <= 1e-6
        ):
            continue
        rows_out.append(
            {
                "Место хранения": str(s.name),
                "Продажи": format_money(sells, display_ccy),
                "Ввод": format_money(external, display_ccy),
                "Перевод в": format_money(transfer_in, display_ccy),
                "Вывод": format_money(transfer_out, display_ccy),
                "Покупки": format_money(purchases, display_ccy),
            }
        )
    if not rows_out:
        st.caption("Нет движения денег между счетами.")
        return
    st.dataframe(
        pd.DataFrame(rows_out),
        hide_index=True,
        width="stretch",
    )


def _format_signed_money(value: float, currency: str) -> str:
    sym = {"RUB": "₽", "USD": "$", "EUR": "€"}.get(currency.upper(), currency)
    if value >= 0:
        return f"+{value:,.2f} {sym}"
    return f"{value:,.2f} {sym}"


def _format_trade_qty(ticker: str, units: float) -> str:
    sign = "-" if float(units) < 0 else ""
    abs_units = abs(float(units))
    if abs_units == int(abs_units) and not is_crypto_ticker(ticker):
        return f"{sign}{int(abs_units)}"
    if is_crypto_ticker(ticker):
        return f"{sign}{round(abs_units, 8)}"
    if abs_units == int(abs_units):
        return f"{sign}{int(abs_units)}"
    return f"{sign}{abs_units:.4f}"


def _format_buy_qty(ticker: str, units: float) -> str:
    return _format_trade_qty(ticker, units)


def _aggregate_ticker_values_from_storage(
    storage_rows: Sequence[StoragePositionValue],
) -> dict[str, float]:
    out: dict[str, float] = defaultdict(float)
    for sr in storage_rows:
        if sr.value_display is None:
            continue
        sn = str(sr.storage_name or "").strip()
        if not sn:
            continue
        out[str(sr.ticker).upper()] += float(sr.value_display)
    return dict(out)


def _apply_trade_to_storage_values(
    values_by_storage: dict[tuple[str, str], float],
    entry: dict,
    amount: float,
    storages_by_ticker: dict[str, list[str]],
) -> None:
    """amount: positive for buys, negative for sells."""
    if abs(float(amount)) <= 1e-12:
        return
    t_up = str(entry["ticker"]).upper()
    if _entry_has_fixed_storage(entry):
        sn = str(entry.get("storage_name") or "").strip()
        if not sn:
            return
        key = (t_up, sn)
        values_by_storage[key] = float(values_by_storage.get(key, 0.0)) + float(amount)
        return
    storage_names = storages_by_ticker.get(t_up, [])
    if not storage_names:
        return
    per_storage = float(amount) / float(len(storage_names))
    for sn in storage_names:
        key = (t_up, str(sn).strip())
        values_by_storage[key] = float(values_by_storage.get(key, 0.0)) + per_storage


def _build_rebalance_value_state(
    portfolio_rows: Sequence[TickerPositionValue],
    storage_rows: Sequence[StoragePositionValue],
    sell_entries: list[dict],
    buy_entries: list[dict],
    storages_by_ticker_unblocked: dict[str, list[str]],
    storages_by_ticker_sellable: dict[str, list[str]],
) -> tuple[dict[str, float], dict[str, float], float, float]:
    """Ticker values aggregated across all storages; after-state applies all trades."""
    values_by_storage: dict[tuple[str, str], float] = {}
    for sr in storage_rows:
        if sr.value_display is None:
            continue
        sn = str(sr.storage_name or "").strip()
        if not sn:
            continue
        t_up = str(sr.ticker).upper()
        key = (t_up, sn)
        values_by_storage[key] = float(values_by_storage.get(key, 0.0)) + float(
            sr.value_display
        )

    value_by_ticker = _aggregate_ticker_values_from_storage(storage_rows)
    for r in portfolio_rows:
        t_up = str(r.ticker).upper()
        if t_up in value_by_ticker or r.value_display is None:
            continue
        value_by_ticker[t_up] = float(r.value_display)

    after_by_storage = dict(values_by_storage)
    for e in sell_entries:
        _apply_trade_to_storage_values(
            after_by_storage,
            e,
            -float(e["implied_proceeds"]),
            storages_by_ticker_sellable,
        )
    for e in buy_entries:
        _apply_trade_to_storage_values(
            after_by_storage,
            e,
            float(e["implied_spend"]),
            storages_by_ticker_unblocked,
        )

    value_after_by_ticker: dict[str, float] = defaultdict(float)
    for (t_up, _), val in after_by_storage.items():
        value_after_by_ticker[t_up] += float(val)
    for t_up, val in value_by_ticker.items():
        if t_up not in value_after_by_ticker:
            value_after_by_ticker[t_up] = float(val)

    total_before = sum(
        float(value_by_ticker.get(t_up, 0.0)) for t_up in value_by_ticker
    )
    total_after = sum(
        float(value_after_by_ticker.get(t_up, 0.0)) for t_up in value_after_by_ticker
    )
    return dict(value_by_ticker), dict(value_after_by_ticker), total_before, total_after


def _ticker_rows_for_targets(
    portfolio_rows: Sequence[TickerPositionValue],
    value_by_ticker: dict[str, float],
) -> list[TickerPositionValue]:
    out: list[TickerPositionValue] = []
    for r in portfolio_rows:
        t_up = str(r.ticker).upper()
        val = value_by_ticker.get(t_up)
        if val is None:
            continue
        out.append(
            TickerPositionValue(
                ticker=r.ticker,
                asset_subclass_id=int(r.asset_subclass_id),
                value_display=float(val),
                price_display=r.price_display,
            )
        )
    return out


def _ticker_deviation_cells(
    *,
    cur_value: float,
    post_value: float,
    target_before: float | None,
    target_after: float | None,
    total_before: float,
    total_after: float,
    display_ccy: str,
    percent_mode: bool,
) -> tuple[str, str, str]:
    if target_before is None or total_before <= 0:
        target_cell = "—"
        dev_before = "—"
    elif percent_mode:
        target_cell = f"{target_before / total_before * 100.0:.1f}%"
        dev_before = f"{(cur_value / total_before * 100.0 - target_before / total_before * 100.0):+.1f}"
    else:
        target_cell = f"{target_before / total_before * 100.0:.1f}%"
        dev_before = _format_signed_money(cur_value - target_before, display_ccy)

    if target_after is None or total_after <= 0:
        dev_after = "—"
    elif percent_mode:
        dev_after = f"{(post_value / total_after * 100.0 - target_after / total_after * 100.0):+.1f}"
    else:
        dev_after = _format_signed_money(post_value - target_after, display_ccy)

    return target_cell, dev_before, dev_after


def _build_rebalance_table_row(
    *,
    ticker: str,
    subclass_name: str,
    units: float,
    group_spend_display: float,
    quote_ccy: str,
    price_native: float,
    display_ccy: str,
    rub: float,
    eur: float,
    cur_value: float,
    value_delta: float,
    post_value_override: float | None,
    target_before: float | None,
    target_after: float | None,
    total_before: float,
    total_after: float,
    percent_mode: bool,
) -> dict:
    if price_native > 0:
        native_spend = float(units) * float(price_native)
    else:
        native_spend = convert_amount(
            float(group_spend_display), display_ccy, quote_ccy, rub, eur
        )
    post_value = (
        float(post_value_override)
        if post_value_override is not None
        else float(cur_value) + float(value_delta)
    )
    target_cell, dev_before, dev_after = _ticker_deviation_cells(
        cur_value=float(cur_value),
        post_value=post_value,
        target_before=target_before,
        target_after=target_after,
        total_before=total_before,
        total_after=total_after,
        display_ccy=display_ccy,
        percent_mode=percent_mode,
    )
    return {
        "Подкласс": subclass_name,
        "Тикер": ticker,
        "Кол-во": _format_trade_qty(ticker, units),
        "Сумма": format_money(native_spend, quote_ccy),
        "Цель": target_cell,
        "Откл до": dev_before,
        "Откл после": dev_after,
    }


def _build_storage_buy_table_rows(
    buy_entries: list[dict],
    *,
    storage_name: str,
    ticker_order: dict[str, int],
    storages_by_ticker_unblocked: dict[str, list[str]],
    quote_ccy_by_ticker: dict[str, str],
    price_native_by_ticker: dict[str, float],
    value_by_ticker: dict[str, float],
    value_after_by_ticker: dict[str, float],
    target_at_s: dict[str, float],
    target_at_t: dict[str, float],
    total_before: float,
    total_after: float,
    display_ccy: str,
    rub: float,
    eur: float,
    percent_mode: bool,
) -> list[dict]:
    rows: list[dict] = []
    storage_name = str(storage_name).strip()
    for b in _sort_buy_entries(buy_entries, ticker_order=ticker_order):
        ticker = str(b["ticker"])
        t_up = ticker.upper()
        if _entry_has_fixed_storage(b):
            if str(b.get("storage_name") or "").strip() != storage_name:
                continue
            storage_spend = float(b["implied_spend"])
            units = float(b["units"])
        else:
            ticker_storages = storages_by_ticker_unblocked.get(t_up, [])
            if storage_name not in ticker_storages:
                continue
            total_spend = float(b["implied_spend"])
            storage_spend = total_spend / float(len(ticker_storages))
            units = float(b["units"])
            if total_spend > 0 and abs(storage_spend - total_spend) > 1e-9:
                units = units * (storage_spend / total_spend)
        rows.append(
            _build_rebalance_table_row(
                ticker=ticker,
                subclass_name=str(b["subclass_name"]),
                units=units,
                group_spend_display=storage_spend,
                quote_ccy=quote_ccy_by_ticker.get(t_up, display_ccy),
                price_native=float(price_native_by_ticker.get(t_up, 0.0)),
                display_ccy=display_ccy,
                rub=rub,
                eur=eur,
                cur_value=float(value_by_ticker.get(t_up, 0.0)),
                value_delta=storage_spend,
                post_value_override=float(value_after_by_ticker.get(t_up, 0.0)),
                target_before=target_at_s.get(t_up),
                target_after=target_at_t.get(t_up),
                total_before=total_before,
                total_after=total_after,
                percent_mode=percent_mode,
            )
        )
    return rows


def _build_storage_sell_table_rows(
    sell_entries: list[dict],
    *,
    storage_name: str,
    ticker_order: dict[str, int],
    storages_by_ticker_sellable: dict[str, list[str]],
    quote_ccy_by_ticker: dict[str, str],
    price_native_by_ticker: dict[str, float],
    value_by_ticker: dict[str, float],
    value_after_by_ticker: dict[str, float],
    target_at_s: dict[str, float],
    target_at_t: dict[str, float],
    total_before: float,
    total_after: float,
    display_ccy: str,
    rub: float,
    eur: float,
    percent_mode: bool,
) -> list[dict]:
    rows: list[dict] = []
    storage_name = str(storage_name).strip()
    for s in _sort_trade_entries(sell_entries, ticker_order=ticker_order):
        ticker = str(s["ticker"])
        t_up = ticker.upper()
        if _entry_has_fixed_storage(s):
            if str(s.get("storage_name") or "").strip() != storage_name:
                continue
            storage_proceeds = float(s["implied_proceeds"])
            units = float(s["units"])
        else:
            ticker_storages = storages_by_ticker_sellable.get(t_up, [])
            if storage_name not in ticker_storages:
                continue
            total_proceeds = float(s["implied_proceeds"])
            storage_proceeds = total_proceeds / float(len(ticker_storages))
            units = float(s["units"])
            if total_proceeds > 0 and abs(storage_proceeds - total_proceeds) > 1e-9:
                units = units * (storage_proceeds / total_proceeds)
        rows.append(
            _build_rebalance_table_row(
                ticker=ticker,
                subclass_name=str(s["subclass_name"]),
                units=-abs(units),
                group_spend_display=storage_proceeds,
                quote_ccy=quote_ccy_by_ticker.get(t_up, display_ccy),
                price_native=float(price_native_by_ticker.get(t_up, 0.0)),
                display_ccy=display_ccy,
                rub=rub,
                eur=eur,
                cur_value=float(value_by_ticker.get(t_up, 0.0)),
                value_delta=-storage_proceeds,
                post_value_override=float(value_after_by_ticker.get(t_up, 0.0)),
                target_before=target_at_s.get(t_up),
                target_after=target_at_t.get(t_up),
                total_before=total_before,
                total_after=total_after,
                percent_mode=percent_mode,
            )
        )
    return rows


def _storages_with_trades(
    sell_entries: list[dict],
    buy_entries: list[dict],
    storages_by_ticker_unblocked: dict[str, list[str]],
    storages_by_ticker_sellable: dict[str, list[str]],
    storage_sort_order: dict[str, int],
) -> list[str]:
    names: set[str] = set()
    for s in sell_entries:
        if _entry_has_fixed_storage(s):
            sn = str(s.get("storage_name") or "").strip()
            if sn:
                names.add(sn)
        else:
            names.update(storages_by_ticker_sellable.get(str(s["ticker"]).upper(), []))
    for b in buy_entries:
        if _entry_has_fixed_storage(b):
            sn = str(b.get("storage_name") or "").strip()
            if sn:
                names.add(sn)
        else:
            names.update(storages_by_ticker_unblocked.get(str(b["ticker"]).upper(), []))
    return sorted(
        names,
        key=lambda n: (int(storage_sort_order.get(n, 10**9)), str(n).casefold()),
    )


def _build_portfolio_result_rows(
    portfolio_rows: list[TickerPositionValue],
    *,
    classes,
    subclasses,
    ticker_order: dict[str, int],
    value_by_ticker: dict[str, float],
    value_after_by_ticker: dict[str, float],
    target_at_s: dict[str, float],
    target_at_t: dict[str, float],
    total_before: float,
    total_after: float,
    display_ccy: str,
    percent_mode: bool,
) -> list[dict]:
    class_by_id = {int(c.id): c for c in classes}
    subclass_by_id = {int(s.id): s for s in subclasses}
    sorted_rows = sorted(
        portfolio_rows,
        key=lambda r: (
            ticker_order.get(str(r.ticker).upper(), 10**9),
            str(r.ticker).upper(),
        ),
    )
    out: list[dict] = []
    for r in sorted_rows:
        t_up = str(r.ticker).upper()
        cur = float(value_by_ticker.get(t_up, 0.0))
        post = float(value_after_by_ticker.get(t_up, cur))
        sub = subclass_by_id.get(int(r.asset_subclass_id))
        ac = class_by_id.get(int(sub.asset_class_id)) if sub else None
        _, dev_before, dev_after = _ticker_deviation_cells(
            cur_value=cur,
            post_value=post,
            target_before=target_at_s.get(t_up),
            target_after=target_at_t.get(t_up),
            total_before=total_before,
            total_after=total_after,
            display_ccy=display_ccy,
            percent_mode=percent_mode,
        )
        has_price = r.value_display is not None
        out.append(
            {
                "Класс": ac.name if ac else "—",
                "Подкласс": sub.name if sub else "—",
                "Тикер": r.ticker,
                "Стоимость": format_money(cur, display_ccy) if has_price else "—",
                "Стоимость после": (
                    format_money(post, display_ccy) if has_price else "—"
                ),
                "Откл до": dev_before,
                "Откл после": dev_after,
            }
        )
    return out


def _as_rebalance_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(_REBALANCE_TABLE_COLUMNS))
    return pd.DataFrame(rows)[list(_REBALANCE_TABLE_COLUMNS)]


def _as_portfolio_dataframe(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=list(_REBALANCE_PORTFOLIO_COLUMNS))
    return pd.DataFrame(rows)[list(_REBALANCE_PORTFOLIO_COLUMNS)]


def _persist_rebalance_settings(edited_rows: list[dict], current: list[dict]) -> None:
    id_by_key = {
        (str(r["ticker"]).upper(), str(r["storage_name"])): int(r["storage_id"])
        for r in current
    }
    for row in edited_rows:
        key = (str(row["Тикер"]).upper(), str(row["Место хранения"]))
        storage_id = id_by_key.get(key)
        if storage_id is None:
            continue
        set_portfolio_blocked(
            str(row["Тикер"]).upper(),
            storage_id,
            bool(row["Блокировать покупку"]),
        )
        set_portfolio_sellable(
            str(row["Тикер"]).upper(),
            storage_id,
            bool(row["Допускать продажу"]),
        )


def _sorted_portfolio_blocks_for_settings(
    blocks: list[dict],
) -> list[dict]:
    """Same ticker order as the summary table (Tickers view); storages alphabetically within ticker."""
    positions = list_positions_by_ticker(main_only=True)
    subclass_by_id = {s.id: s for s in list_asset_subclasses()}
    class_sort_by_id = {c.id: int(c.sort_order) for c in list_asset_classes()}
    subclass_by_ticker = {
        str(p.ticker).upper(): int(p.asset_subclass_id) for p in positions
    }

    def _sort_key(row: dict) -> tuple:
        t_up = str(row["ticker"]).upper()
        sid = subclass_by_ticker.get(t_up)
        return (
            *portfolio_ticker_sort_key(
                str(row["ticker"]),
                asset_subclass_id=sid,
                subclass_by_id=subclass_by_id,
                class_sort_by_id=class_sort_by_id,
            ),
            str(row["storage_name"]),
        )

    return sorted(blocks, key=_sort_key)


def _persist_storage_cash_flow_settings(
    edited_rows: list[dict], current: list[dict]
) -> None:
    id_by_name = {str(r["name"]): int(r["id"]) for r in current}
    for row in edited_rows:
        storage_id = id_by_name.get(str(row["Место хранения"]))
        if storage_id is None:
            continue
        set_storage_rebalance_flags(
            storage_id,
            deposit=bool(row["Ввод денег"]),
            withdraw=bool(row["Вывод денег"]),
        )


@st.dialog("Настройки ребалансировки", width="medium")
def _render_rebalance_settings_dialog() -> None:
    display_ccy = st.session_state.get("display_currency", "RUB")
    tab_instruments, tab_storages, tab_limits, tab_taxes = st.tabs(
        ["Инструменты", "Места хранения", "Ограничения", "Налоги"]
    )

    portfolio_blocks = _sorted_portfolio_blocks_for_settings(
        list_portfolio_blocks(main_only=True)
    )
    storages = list_storages()
    min_purchase_default, min_deposit_default, buy_mode_default = (
        _read_rebalance_limits()
    )
    tax_rate_default, _taxable_by_id = _read_tax_settings()

    with tab_instruments:
        st.caption(
            "Блокировка покупки и допуск продажи по каждому инструменту и месту хранения."
        )
        instruments_df = pd.DataFrame(
            [
                {
                    "Тикер": r["ticker"],
                    "Место хранения": r["storage_name"],
                    "Блокировать покупку": bool(r["blocked"]),
                    "Допускать продажу": bool(r["sellable"]),
                }
                for r in portfolio_blocks
            ]
        )
        instruments_height = min(720, max(360, len(instruments_df) * 38 + 48))
        edited_instruments = st.data_editor(
            instruments_df,
            width="stretch",
            height=instruments_height,
            hide_index=True,
            disabled=["Тикер", "Место хранения"],
            column_config={
                "Тикер": st.column_config.TextColumn("Тикер"),
                "Место хранения": st.column_config.TextColumn("Место хранения"),
                "Блокировать покупку": st.column_config.CheckboxColumn(
                    "Блокировать покупку", default=False
                ),
                "Допускать продажу": st.column_config.CheckboxColumn(
                    "Допускать продажу", default=False
                ),
            },
            key="rebalance_settings_dialog_instruments",
        )

    with tab_storages:
        st.caption(
            "Ввод денег — сюда можно направить внешний ввод и выручку от продаж "
            "(если на источнике включён вывод). "
            "Вывод денег — выручка с продаж на этом месте может быть перенаправлена "
            "на покупки в других местах с включённым вводом; иначе остаётся здесь."
        )
        storages_current = [
            {"id": int(s.id), "name": str(s.name), "sort_order": int(s.sort_order)}
            for s in storages
        ]
        storages_df = pd.DataFrame(
            [
                {
                    "Место хранения": s.name,
                    "Ввод денег": bool(s.rebalance_deposit),
                    "Вывод денег": bool(s.rebalance_withdraw),
                }
                for s in storages
            ]
        )
        storages_height = min(480, max(220, len(storages_df) * 38 + 48))
        edited_storages = st.data_editor(
            storages_df,
            width="stretch",
            height=storages_height,
            hide_index=True,
            disabled=["Место хранения"],
            column_config={
                "Место хранения": st.column_config.TextColumn("Место хранения"),
                "Ввод денег": st.column_config.CheckboxColumn(
                    "Ввод денег", default=True
                ),
                "Вывод денег": st.column_config.CheckboxColumn(
                    "Вывод денег", default=False
                ),
            },
            key="rebalance_settings_dialog_storages",
        )

    with tab_limits:
        st.caption("Минимальные суммы и стратегия распределения покупок.")
        if "rebalance_dialog_min_purchase" not in st.session_state:
            st.session_state["rebalance_dialog_min_purchase"] = min_purchase_default
        if "rebalance_dialog_min_deposit" not in st.session_state:
            st.session_state["rebalance_dialog_min_deposit"] = min_deposit_default
        if "rebalance_dialog_buy_mode" not in st.session_state:
            st.session_state["rebalance_dialog_buy_mode"] = buy_mode_default
        min_purchase = float(
            st.number_input(
                f"Мин. сумма покупки ({display_ccy})",
                min_value=0.0,
                step=100.0,
                format="%.2f",
                key="rebalance_dialog_min_purchase",
                help=(
                    "Итоговая покупка по одному тикеру должна быть не меньше этой суммы; "
                    "меньшие сделки не попадают в план. 0 — без ограничения."
                ),
            )
        )
        min_deposit = float(
            st.number_input(
                f"Мин. сумма ввода ({display_ccy})",
                min_value=0.0,
                step=100.0,
                format="%.2f",
                key="rebalance_dialog_min_deposit",
                help=(
                    "Внешний ввод на место хранения учитывается только если суммарный ввод "
                    "на этот счёт не меньше порога. 0 — без ограничения."
                ),
            )
        )
        buy_mode = st.segmented_control(
            "Стратегия покупок",
            options=list(_BUY_ALLOCATION_OPTIONS),
            format_func=lambda x: "Макс. недовес" if x == "max_gap" else "Пропорционально",
            key="rebalance_dialog_buy_mode",
            width="stretch",
            help=(
                "Макс. недовес — каждый следующий лот идёт в тикер с наибольшим оставшимся "
                "недовесом до цели. Пропорционально — лоты чередуются по тикерам с "
                "наименьшей долей уже закрытого недовеса (равномернее при разных лотах)."
            ),
        )
        if buy_mode not in _BUY_ALLOCATION_OPTIONS:
            buy_mode = "max_gap"

    with tab_taxes:
        st.caption(
            "Отметьте места хранения, где продажи облагаются налогом. "
            "Расчёт ведётся по методу FIFO с учётом переводов между счетами; "
            "себестоимость и выручка пересчитываются в рубли по курсу на дату операции."
        )
        if "rebalance_dialog_tax_rate_pct" not in st.session_state:
            st.session_state["rebalance_dialog_tax_rate_pct"] = tax_rate_default * 100.0
        tax_rate_pct = float(
            st.number_input(
                "Ставка налога, %",
                min_value=0.0,
                max_value=100.0,
                step=1.0,
                format="%.1f",
                key="rebalance_dialog_tax_rate_pct",
                help="Применяется к чистой налогооблагаемой базе (прибыль минус убытки).",
            )
        )
        taxes_df = pd.DataFrame(
            [
                {
                    "Место хранения": s.name,
                    "Облагается налогом": bool(s.taxable),
                }
                for s in storages
            ]
        )
        taxes_height = min(480, max(220, len(taxes_df) * 38 + 48))
        edited_taxes = st.data_editor(
            taxes_df,
            width="stretch",
            height=taxes_height,
            hide_index=True,
            disabled=["Место хранения"],
            column_config={
                "Место хранения": st.column_config.TextColumn("Место хранения"),
                "Облагается налогом": st.column_config.CheckboxColumn(
                    "Облагается налогом", default=False
                ),
            },
            key="rebalance_settings_dialog_taxes",
        )

    c1, c2 = st.columns(2)
    with c1:
        if st.button(
            "Сохранить",
            type="primary",
            key="rebalance_settings_dialog_save",
            width="stretch",
        ):
            _persist_rebalance_settings(
                edited_instruments.to_dict("records"), portfolio_blocks
            )
            _persist_storage_cash_flow_settings(
                edited_storages.to_dict("records"), storages_current
            )
            _persist_rebalance_limits(min_purchase, min_deposit, str(buy_mode))
            _persist_tax_settings(
                tax_rate_pct,
                edited_taxes.to_dict("records"),
                storages_current,
            )
            st.rerun()
    with c2:
        if st.button(
            "Отмена",
            key="rebalance_settings_dialog_cancel",
            width="stretch",
        ):
            st.rerun()


def _render_rebalancing_controls() -> None:
    st.header(
        "Ребалансировка",
        help=(
            "Учитываются только инструменты с флагом main = 1. "
            "Сумма новых средств распределяется по недовложенным подклассам пропорционально "
            "пробелу до цели; внутри подкласса — к целевым долям незаблокированных "
            "(заблокированным цель = текущая стоимость, остаток поровну между остальными). "
            "Продажи возможны для отмеченных тикеров с нереализованной P&L ≥ 10%; "
            "выручка идёт на покупки. В настройках мест хранения задаётся ввод и вывод денег."
        ),
    )

    display_ccy = st.session_state.get("display_currency", "RUB")
    V = st.number_input(
        f"Сумма к инвестированию ({display_ccy})",
        min_value=0.0,
        value=0.0,
        step=100.0,
        format="%.2f",
        key="rebalance_invest_amount",
    )

    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button(
            "Рассчитать ребалансировку",
            type="primary",
            key="rebalance_compute",
            width="stretch",
        ):
            request_quotes_refresh()
            refresh_fx_cache()
            st.session_state["rebalance_last_V"] = float(V)
            st.rerun()
    with btn_col2:
        if st.button(
            "Настройки",
            key="rebalance_open_settings_dialog",
            width="stretch",
        ):
            _render_rebalance_settings_dialog()


def _render_rebalancing_results() -> None:
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    process_messages_intro: list[str] = []
    process_messages_warnings: list[str] = []
    process_messages_residuals: list[str] = []
    warnings: list[str] = []

    if "rebalance_last_V" not in st.session_state:
        return

    run_v = float(st.session_state["rebalance_last_V"])
    min_purchase_amount, min_deposit_amount, buy_allocation_mode = (
        _read_rebalance_limits()
    )

    positions = list_positions_by_ticker(main_only=True)
    if not positions:
        st.info("В основном портфеле нет позиций (`main = 1`).")
        return

    classes = list_asset_classes()
    subclasses = list_asset_subclasses()
    target_pct = {s.id: float(s.target_pct) for s in subclasses}
    sub_names = {s.id: s.name for s in subclasses}

    storage_block_rows = list_portfolio_blocks(main_only=True)
    all_storages = list_storages()
    deposit_storage_ids = {int(s.id) for s in all_storages if bool(s.rebalance_deposit)}
    withdraw_storage_ids = {
        int(s.id) for s in all_storages if bool(s.rebalance_withdraw)
    }
    blocked = {t.upper() for t in list_buy_blocked_tickers(main_only=True)}
    sellable_positions = {
        (str(r["ticker"]).upper(), int(r["storage_id"]))
        for r in storage_block_rows
        if bool(r["sellable"])
    }
    tickers = [p.ticker for p in positions]
    main_block_keys = {
        (str(r["ticker"]).upper(), int(r["storage_id"])) for r in storage_block_rows
    }

    storages_by_ticker_unblocked: dict[str, list[str]] = defaultdict(list)
    storages_by_ticker_sellable: dict[str, list[str]] = defaultdict(list)
    unblocked_tickers_by_storage: dict[int, set[str]] = defaultdict(set)
    for r in storage_block_rows:
        t = str(r["ticker"]).upper()
        sid = int(r["storage_id"])
        sn = str(r["storage_name"] or "").strip()
        if not sn:
            continue
        if not bool(r["blocked"]):
            unblocked_tickers_by_storage[sid].add(t)
            if sid in deposit_storage_ids:
                storages_by_ticker_unblocked[t].append(sn)
        if bool(r["sellable"]):
            storages_by_ticker_sellable[t].append(sn)

    price_tickers = sorted({t.upper() for t in tickers} | {t.upper() for t in blocked})
    quotes = get_app_quotes(price_tickers) if price_tickers else {}
    quote_ccy_by_ticker: dict[str, str] = {}
    price_native_by_ticker: dict[str, float] = {}

    rows: list[TickerPositionValue] = []
    storage_rows: list[StoragePositionValue] = []
    for p in positions:
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = resolve_quote_currency(p.ticker, q.currency if q else None)
        t_up = str(p.ticker).upper()
        quote_ccy_by_ticker[t_up] = quote_ccy
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_native_by_ticker[t_up] = float(price)
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
            rows.append(
                TickerPositionValue(
                    ticker=p.ticker,
                    asset_subclass_id=p.asset_subclass_id,
                    value_display=float(value_disp),
                    price_display=float(price_disp),
                )
            )
        else:
            rows.append(
                TickerPositionValue(
                    ticker=p.ticker,
                    asset_subclass_id=p.asset_subclass_id,
                    value_display=None,
                    price_display=None,
                )
            )

    for p in list_positions():
        key = (str(p.ticker).upper(), int(p.storage_id))
        if key not in main_block_keys:
            continue
        q = quotes.get(p.ticker)
        raw_price = q.price if q else None
        quote_ccy = resolve_quote_currency(p.ticker, q.currency if q else None)
        price = normalize_quote_price_for_valuation(p.ticker, raw_price, quote_ccy)
        if price is not None:
            price_disp = convert_amount(price, quote_ccy, display_ccy, rub, eur)
            value_native = price * p.amount
            value_disp = convert_amount(value_native, quote_ccy, display_ccy, rub, eur)
            storage_rows.append(
                StoragePositionValue(
                    ticker=p.ticker,
                    storage_id=int(p.storage_id),
                    storage_name=str(p.storage_name or "—"),
                    asset_subclass_id=int(p.asset_subclass_id),
                    value_display=float(value_disp),
                    price_display=float(price_disp),
                )
            )
        else:
            storage_rows.append(
                StoragePositionValue(
                    ticker=p.ticker,
                    storage_id=int(p.storage_id),
                    storage_name=str(p.storage_name or "—"),
                    asset_subclass_id=int(p.asset_subclass_id),
                    value_display=None,
                    price_display=None,
                )
            )

    unrealized_pnl_pct_by_ticker: dict[str, float] = {}
    for r in rows:
        if r.value_display is None or float(r.value_display) <= 0:
            continue
        t_up = str(r.ticker).upper()
        pnl_pct = compute_ticker_unrealized_pnl_pct(
            r.ticker, float(r.value_display), display_ccy, rub, eur
        )
        if pnl_pct is not None:
            unrealized_pnl_pct_by_ticker[t_up] = float(pnl_pct)

    raw_target_sum = sum(target_pct.values())
    if abs(raw_target_sum - 100.0) > 0.05:
        process_messages_intro.append(
            f"Сумма целевых долей подклассов: {raw_target_sum:.3f}%. "
            "Для расчёта веса выполнена нормализация до 100%."
        )

    plan = compute_constrained_rebalance_plan(
        rows,
        target_pct,
        sub_names,
        run_v,
        blocked_tickers=blocked,
        sellable_positions=sellable_positions,
        storage_rows=storage_rows,
        unblocked_tickers_by_storage=dict(unblocked_tickers_by_storage),
        deposit_storage_ids=deposit_storage_ids,
        withdraw_storage_ids=withdraw_storage_ids,
        unrealized_pnl_pct_by_ticker=unrealized_pnl_pct_by_ticker,
        min_purchase_amount=min_purchase_amount,
        min_deposit_amount=min_deposit_amount,
        buy_allocation_mode=buy_allocation_mode,
    )

    if plan.total_sell_proceeds > 0 or plan.suggested_sells or plan.suggested_buys:
        process_messages_intro.append(
            f"Продажи {format_money(plan.total_sell_proceeds, display_ccy)}, "
            f"покупки {format_money(plan.total_implied_spend, display_ccy)}."
        )
        if withdraw_storage_ids and plan.total_sell_proceeds > 0:
            process_messages_intro.append(
                "Выручка с мест с выводом перенаправляется на покупки в местах с вводом; "
                "остальная — в том же месте хранения."
            )
        elif plan.total_sell_proceeds > 0:
            process_messages_intro.append(
                "Выручка направляется на покупки в том же месте хранения."
            )
        if run_v > 0:
            process_messages_intro.append(
                f"Внешний ввод {format_money(run_v, display_ccy)} размещается на счетах с вводом."
            )
    if plan.residual_sell_proceeds > 0.01:
        process_messages_residuals.append(
            "Неразмещённая выручка от продаж (кратность лотов): "
            f"{format_money(plan.residual_sell_proceeds, display_ccy)}."
        )
    if abs(plan.residual_vs_V) > 0.01:
        process_messages_residuals.append(
            "Неразмещённый внешний ввод (лотность/округление): "
            f"{format_money(plan.residual_vs_V, display_ccy)}."
        )
    mode_label = (
        "пропорционально"
        if buy_allocation_mode == "proportional"
        else "макс. недовес"
    )
    process_messages_intro.append(f"Стратегия покупок: {mode_label}.")
    process_messages_intro.append(
        f"Отклонение L1 до: {format_money(plan.deviation_l1_before, display_ccy)}; "
        f"после: {format_money(plan.deviation_l1_after, display_ccy)}."
    )
    if plan.skipped_sells_low_pnl:
        unknown_pnl = [
            t
            for t in plan.skipped_sells_low_pnl
            if unrealized_pnl_pct_by_ticker.get(str(t).upper()) is None
        ]
        low_pnl = [
            t
            for t in plan.skipped_sells_low_pnl
            if unrealized_pnl_pct_by_ticker.get(str(t).upper()) is not None
        ]
        if unknown_pnl:
            process_messages_warnings.append(
                "Не проданы (нет себестоимости для P&L): "
                + ", ".join(unknown_pnl)
                + "."
            )
        if low_pnl:
            process_messages_warnings.append(
                "Не проданы (P&L < 10%): " + ", ".join(low_pnl) + "."
            )
    if plan.rebalance_diagnostics:
        process_messages_warnings.extend(plan.rebalance_diagnostics)

    process_messages_intro.append(
        f"После ввода {format_money(run_v, display_ccy)} целевая капитализация основного портфеля ~{format_money(plan.T, display_ccy)}."
    )

    if plan.unpriced_tickers:
        process_messages_warnings.append(
            "Без котировки (в расчёт не вошли): "
            + ", ".join(plan.unpriced_tickers)
            + "."
        )

    if plan.weights_were_normalized:
        process_messages_intro.append(
            "Целевые веса подклассов нормализованы, потому что исходная сумма долей отличалась от 100%."
        )

    if plan.unallocated:
        for u in plan.unallocated:
            warnings.append(
                f"**{u.subclass_name}**: нужно разместить **{format_money(u.budget, display_ccy)}** — "
                f"{u.reason}. Добавьте позицию с ценой или выберите тикер вручную."
            )

    if warnings:
        process_messages_warnings.extend(warnings)

    if run_v <= 0 and not plan.suggested_sells and not plan.suggested_buys:
        if not plan.rebalance_diagnostics:
            process_messages_warnings.append(
                "Нет сделок: перевесов среди продажных позиций не найдено или выручку некуда разместить."
            )
    elif plan.total_gap <= 0 and not plan.suggested_sells:
        process_messages_warnings.append(
            "После увеличения капитала нет подклассов ниже цели; дополнительные покупки могут усилить текущий перекос."
        )

    subclass_by_id = {s.id: s for s in subclasses}
    class_sort_by_id = {c.id: int(c.sort_order) for c in classes}
    ticker_order = build_portfolio_ticker_order(
        [(r.ticker, int(r.asset_subclass_id)) for r in rows],
        subclass_by_id=subclass_by_id,
        class_sort_by_id=class_sort_by_id,
    )

    buy_entries = [
        {
            "ticker": b.ticker,
            "asset_subclass_id": int(b.asset_subclass_id),
            "subclass_name": b.subclass_name,
            "units": float(b.units),
            "implied_spend": float(b.implied_spend),
            "price_display": float(b.price_display),
            "storage_id": b.storage_id,
            "storage_name": b.storage_name,
        }
        for b in plan.suggested_buys
    ]
    sell_entries = [
        {
            "ticker": s.ticker,
            "asset_subclass_id": int(s.asset_subclass_id),
            "subclass_name": s.subclass_name,
            "units": float(s.units),
            "implied_proceeds": float(s.implied_proceeds),
            "price_display": float(s.price_display),
            "storage_id": int(s.storage_id),
            "storage_name": str(s.storage_name),
        }
        for s in plan.suggested_sells
    ]

    value_by_ticker, value_after_by_ticker, total_before, total_after = (
        _build_rebalance_value_state(
            rows,
            storage_rows,
            sell_entries,
            buy_entries,
            storages_by_ticker_unblocked,
            storages_by_ticker_sellable,
        )
    )
    target_rows = _ticker_rows_for_targets(rows, value_by_ticker)
    _, target_at_s = compute_ticker_target_values(target_rows, target_pct, blocked)
    _, target_at_t = compute_ticker_target_values(
        target_rows,
        target_pct,
        blocked,
        portfolio_total=max(float(total_after), 1e-9),
    )
    storage_sort_order = {str(s.name): int(s.sort_order) for s in all_storages}
    storage_id_by_name = {str(s.name): int(s.id) for s in all_storages}

    process_messages = (
        process_messages_intro
        + process_messages_warnings
        + process_messages_residuals
    )
    constraint_gaps = list(plan.constraint_gaps)
    if constraint_gaps or process_messages:
        with st.expander("Инфо по расчёту ребалансировки", expanded=False):
            if constraint_gaps:
                st.markdown("**Ограничения**")
                for msg in constraint_gaps:
                    st.markdown(f"- {msg}")
            if process_messages:
                if constraint_gaps:
                    st.markdown("**Детали расчёта**")
                for msg in process_messages:
                    st.markdown(f"- {msg}")

    st.subheader(
        "Ввод и вывод по местам хранения",
        help=(
            "По каждому счёту: Продажи − Вывод + Ввод + Перевод в = Покупки. "
            "Ввод — новые средства (V), использованные на этом счёте; "
            "не доля от V «на бумаге», а реальная потребность в пополнении."
        ),
    )
    _render_storage_cash_flow_table(
        cash_flows=plan.storage_cash_flows,
        all_storages=all_storages,
        display_ccy=display_ccy,
    )

    if (
        plan.rebalance_diagnostics
        and not plan.suggested_sells
        and not plan.suggested_buys
    ):
        with st.expander("Почему нет сделок", expanded=True):
            for msg in plan.rebalance_diagnostics:
                st.markdown(f"- {msg}")

    results_col, deviation_col = st.columns([2, 1])
    with results_col:
        results_mode = st.segmented_control(
            "Результаты",
            options=["Trades", "Portfolio"],
            format_func=lambda x: (
                "Продажи и покупки" if x == "Trades" else "Портфель"
            ),
            default="Trades",
            key="rebalance_results_mode",
            width="stretch",
        )
    with deviation_col:
        deviation_mode = st.segmented_control(
            "Отклонение",
            options=["Percent", "Absolute"],
            format_func=lambda x: "%" if x == "Percent" else "Абс.",
            default="Percent",
            key="rebalance_deviation_mode",
            width="content",
        )
    percent_mode = deviation_mode == "Percent"
    storages_with_trades = _storages_with_trades(
        sell_entries,
        buy_entries,
        storages_by_ticker_unblocked,
        storages_by_ticker_sellable,
        storage_sort_order,
    )
    sell_table_rows_by_storage = {
        sn: _build_storage_sell_table_rows(
            sell_entries,
            storage_name=sn,
            ticker_order=ticker_order,
            storages_by_ticker_sellable=storages_by_ticker_sellable,
            quote_ccy_by_ticker=quote_ccy_by_ticker,
            price_native_by_ticker=price_native_by_ticker,
            value_by_ticker=value_by_ticker,
            value_after_by_ticker=value_after_by_ticker,
            target_at_s=target_at_s,
            target_at_t=target_at_t,
            total_before=total_before,
            total_after=total_after,
            display_ccy=display_ccy,
            rub=rub,
            eur=eur,
            percent_mode=percent_mode,
        )
        for sn in storages_with_trades
    }
    table_rows_by_storage = {
        sn: _build_storage_buy_table_rows(
            buy_entries,
            storage_name=sn,
            ticker_order=ticker_order,
            storages_by_ticker_unblocked=storages_by_ticker_unblocked,
            quote_ccy_by_ticker=quote_ccy_by_ticker,
            price_native_by_ticker=price_native_by_ticker,
            value_by_ticker=value_by_ticker,
            value_after_by_ticker=value_after_by_ticker,
            target_at_s=target_at_s,
            target_at_t=target_at_t,
            total_before=total_before,
            total_after=total_after,
            display_ccy=display_ccy,
            rub=rub,
            eur=eur,
            percent_mode=percent_mode,
        )
        for sn in storages_with_trades
    }
    portfolio_table_rows = _build_portfolio_result_rows(
        rows,
        classes=classes,
        subclasses=subclasses,
        ticker_order=ticker_order,
        value_by_ticker=value_by_ticker,
        value_after_by_ticker=value_after_by_ticker,
        target_at_s=target_at_s,
        target_at_t=target_at_t,
        total_before=total_before,
        total_after=total_after,
        display_ccy=display_ccy,
        percent_mode=percent_mode,
    )

    if results_mode == "Portfolio":
        st.dataframe(
            _as_portfolio_dataframe(portfolio_table_rows),
            width="stretch",
            hide_index=True,
            column_order=list(_REBALANCE_PORTFOLIO_COLUMNS),
            key=f"rebalance_portfolio_{deviation_mode}_df",
        )
    elif not storages_with_trades:
        st.info("Нет доступных сделок по текущим условиям ребалансировки.")
    else:
        for storage_name in storages_with_trades:
            sell_rows = sell_table_rows_by_storage.get(storage_name, [])
            buy_rows = table_rows_by_storage.get(storage_name, [])
            storage_key = storage_id_by_name.get(storage_name, storage_name)
            st.subheader(storage_name)
            if sell_rows:
                st.markdown("**Продажи**")
                st.dataframe(
                    _as_rebalance_dataframe(sell_rows),
                    width="stretch",
                    hide_index=True,
                    column_order=list(_REBALANCE_TABLE_COLUMNS),
                    key=f"rebalance_sell_{storage_key}_{deviation_mode}_df",
                )
            if buy_rows:
                st.markdown("**Покупки**")
                st.dataframe(
                    _as_rebalance_dataframe(buy_rows),
                    width="stretch",
                    hide_index=True,
                    column_order=list(_REBALANCE_TABLE_COLUMNS),
                    key=f"rebalance_buy_{storage_key}_{deviation_mode}_df",
                )
            if not sell_rows and not buy_rows:
                st.caption("Нет сделок для этого места хранения.")

    _render_rebalance_tax_section(
        plan=plan,
        display_ccy=display_ccy,
        rub=rub,
        eur=eur,
    )


def _render_rebalance_tax_section(
    *,
    plan,
    display_ccy: str,
    rub: float,
    eur: float,
) -> None:
    if not plan.suggested_sells:
        return

    tax_rate, taxable_by_id = _read_tax_settings()
    taxable_storage_ids = {sid for sid, flag in taxable_by_id.items() if flag}
    summary = compute_rebalance_tax_summary(
        plan.suggested_sells,
        taxable_storage_ids=taxable_storage_ids,
        tax_rate=tax_rate,
        rub_per_usd=rub,
        eur_per_usd=eur,
        display_currency=display_ccy,
    )

    st.subheader(
        "Налоги",
        help=(
            "Оценка налога на плановые продажи по методу FIFO в рублях. "
            "Себестоимость — по курсу на дату покупки, выручка — по курсу на дату продажи. "
            "Не влияет на расчёт ребалансировки."
        ),
    )

    if not taxable_storage_ids:
        st.info(
            "Ни одно место хранения не отмечено как облагаемое. "
            "Настройте это во вкладке «Налоги» в настройках ребалансировки."
        )
    else:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Прибыль", format_money(summary.total_gain_rub, "RUB"))
        with c2:
            st.metric("Убытки", format_money(summary.total_loss_rub, "RUB"))
        with c3:
            st.metric("Налоговая база", format_money(summary.net_taxable_base_rub, "RUB"))
        with c4:
            st.metric(
                f"Налог ({summary.tax_rate * 100:.0f}%)",
                format_money(summary.estimated_tax_rub, "RUB"),
            )

    if summary.dispositions:
        lot_rows = [
            {
                "Тикер": d.ticker,
                "Место хранения": d.storage_name,
                "Кол-во": d.sell_qty,
                "Дата покупки": d.acquired_date,
                "Себестоимость": format_money(d.cost_rub, "RUB"),
                "Выручка": format_money(d.proceeds_rub, "RUB"),
                "Прибыль": format_money(d.gain_rub, "RUB"),
            }
            for d in summary.dispositions
        ]
        st.dataframe(
            pd.DataFrame(lot_rows),
            width="stretch",
            hide_index=True,
            key="rebalance_tax_lots_df",
        )

    if summary.exempt_sells:
        exempt_parts = [
            f"{s.ticker} ({s.storage_name}, {s.units:g} шт.)"
            for s in summary.exempt_sells
        ]
        st.caption(f"Не облагается: {', '.join(exempt_parts)}")

    for msg in summary.warnings:
        st.warning(msg)


def render_rebalancing():
    @st.fragment
    def _controls_fragment():
        _render_rebalancing_controls()

    @st.fragment
    def _results_fragment():
        _render_rebalancing_results()

    _controls_fragment()
    _results_fragment()
