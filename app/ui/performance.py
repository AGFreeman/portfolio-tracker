"""Portfolio performance UI (TWR + historical backfill)."""
import bisect
import colorsys
import json
from collections import defaultdict
from dataclasses import dataclass

import plotly.graph_objects as go
import pandas as pd
import streamlit as st
from plotly.subplots import make_subplots
from pathlib import Path

from app.db import (
    get_app_setting,
    get_instrument_provider,
    get_instrument_subclass_id_map,
    list_asset_classes,
    list_asset_subclasses,
    list_cash_flows,
    list_positions_by_ticker,
)
from app.services.fx import format_money
from app.services.prices import (
    get_app_quotes,
    get_quotes_cache_meta,
    normalize_quote_price_for_valuation,
)
from app.services.performance import (
    compute_benchmark_period_returns,
    compute_period_returns,
    compute_portfolio_performance,
)
from app.services.price_currency import (
    bucket_diversification_currency,
    resolve_quote_currency,
)
from app.services.policy_rates import (
    synthetic_policy_label,
    uses_synthetic_policy_benchmark,
)


def _fmt_pct(x: float) -> str:
    return f"{x * 100.0:+.2f}%"


def _pnl_return_delta(pnl: float, net_invested: float) -> str | None:
    if net_invested <= 0:
        return None
    return _fmt_pct(pnl / net_invested)


def _synthetic_benchmark_help_note(result, display_ccy: str) -> str:
    if (
        result.benchmark_ticker
        and uses_synthetic_policy_benchmark(display_ccy)
        and result.benchmark_first_quote_date
    ):
        return (
            f" Для benchmark `{result.benchmark_ticker}` на период до "
            f"`{result.benchmark_first_quote_date}` используется синтетическая "
            f"оценка по {synthetic_policy_label(display_ccy)}."
        )
    return ""


_PORTFOLIO_LINE = {"width": 2, "color": "#636EFA"}
_BENCHMARK_LINE = {"width": 2, "dash": "dot", "color": "#EF553B"}
_CASHFLOW_VLINE_IN = "rgba(46, 125, 50, 0.55)"
_CASHFLOW_VLINE_OUT = "rgba(198, 40, 40, 0.55)"


def _add_subplot_line_traces(
    fig: go.Figure,
    df: pd.DataFrame,
    *,
    row: int,
    col: int,
    y_col: str,
    benchmark_y_col: str,
    is_percent: bool,
    hover_label: str,
    y_tick_prefix: str,
    benchmark_label: str,
    show_legend: bool,
) -> bool:
    plot_df = df[df[y_col].notna()]
    if plot_df.empty:
        return False

    y_values = plot_df[y_col].astype(float)
    custom_vals = y_values * 100.0 if is_percent else y_values
    hover_value_suffix = "%" if is_percent else ""
    hover_template = (
        "Date: %{x|%m-%Y}<br>"
        f"{hover_label}: "
        + f"{y_tick_prefix}%{{customdata:.2f}}{hover_value_suffix}"
        + "<extra></extra>"
    )
    fig.add_trace(
        go.Scatter(
            x=plot_df["date"],
            y=y_values,
            mode="lines",
            line=_PORTFOLIO_LINE,
            customdata=custom_vals,
            hovertemplate=hover_template,
            name="Портфель",
            legendgroup="portfolio",
            showlegend=show_legend,
        ),
        row=row,
        col=col,
    )

    show_benchmark = bool(benchmark_y_col and benchmark_y_col in df.columns)
    if show_benchmark:
        benchmark_df = df[df[benchmark_y_col].notna()]
        if not benchmark_df.empty:
            benchmark_vals = benchmark_df[benchmark_y_col].astype(float)
            benchmark_custom_vals = (
                benchmark_vals * 100.0 if is_percent else benchmark_vals
            )
            fig.add_trace(
                go.Scatter(
                    x=benchmark_df["date"],
                    y=benchmark_vals,
                    mode="lines",
                    line=_BENCHMARK_LINE,
                    customdata=benchmark_custom_vals,
                    hovertemplate=(
                        "Date: %{x|%m-%Y}<br>"
                        f"{benchmark_label}: "
                        + f"{y_tick_prefix}%{{customdata:.2f}}{hover_value_suffix}"
                        + "<extra></extra>"
                    ),
                    name=benchmark_label,
                    legendgroup="benchmark",
                    showlegend=show_legend,
                ),
                row=row,
                col=col,
            )
    return True


def _cash_flow_vline_shape(
    ts: pd.Timestamp,
    kind: str,
    *,
    xref: str,
    yref: str,
) -> dict:
    color = _CASHFLOW_VLINE_IN if kind == "in" else _CASHFLOW_VLINE_OUT
    x = pd.to_datetime(ts).isoformat()
    return {
        "type": "line",
        "x0": x,
        "x1": x,
        "y0": 0,
        "y1": 1,
        "xref": xref,
        "yref": yref,
        "line": {"color": color, "width": 1, "dash": "dot"},
        "layer": "below",
    }


def _subplot_axis_refs(col: int) -> tuple[str, str]:
    if col <= 1:
        return "x", "y domain"
    return f"x{col}", f"y{col} domain"


@st.cache_data(show_spinner=False)
def _cash_flow_chart_markers_cached(db_mtime: float) -> tuple[tuple[str, str], ...]:
    _ = db_mtime
    net_by_day: dict[str, float] = defaultdict(float)
    for flow in list_cash_flows():
        day = str(flow.flow_date or "")[:10]
        if not day:
            continue
        net_by_day[day] += float(flow.amount)
    markers: list[tuple[str, str]] = []
    for day in sorted(net_by_day):
        net = float(net_by_day[day])
        if abs(net) <= 1e-12:
            continue
        markers.append((day, "in" if net > 0 else "out"))
    return tuple(markers)


def _cash_flow_chart_markers(db_mtime: float) -> list[tuple[pd.Timestamp, str]]:
    return [
        (pd.to_datetime(day), kind)
        for day, kind in _cash_flow_chart_markers_cached(db_mtime)
    ]


def _cash_flow_markers_in_df_range(
    markers: list[tuple[pd.Timestamp, str]],
    df: pd.DataFrame,
) -> list[tuple[pd.Timestamp, str]]:
    if df.empty or "date" not in df.columns:
        return []
    dmin = pd.to_datetime(df["date"]).min()
    dmax = pd.to_datetime(df["date"]).max()
    return [(ts, kind) for ts, kind in markers if dmin <= ts <= dmax]


def _cash_flow_vline_shapes(
    markers: list[tuple[pd.Timestamp, str]],
    *,
    subplot_cols: int = 1,
) -> list[dict]:
    if not markers:
        return []
    shapes: list[dict] = []
    for col_idx in range(1, subplot_cols + 1):
        xref, yref = _subplot_axis_refs(col_idx)
        for ts, kind in markers:
            shapes.append(_cash_flow_vline_shape(ts, kind, xref=xref, yref=yref))
    return shapes


def _render_performance_charts(
    df: pd.DataFrame,
    *,
    display_ccy: str,
    benchmark_label: str,
    cash_flow_markers: list[tuple[pd.Timestamp, str]] | None = None,
) -> None:
    panels = (
        {
            "title": "Кривая стоимости",
            "y_col": "portfolio_value",
            "benchmark_y_col": "benchmark_value",
            "is_percent": False,
            "hover_label": "Портфель",
            "y_tick_prefix": f"{display_ccy} ",
        },
        {
            "title": "Кумулятивная доходность",
            "y_col": "twr_cum_return",
            "benchmark_y_col": "benchmark_cum_return",
            "is_percent": True,
            "hover_label": "Портфель",
            "y_tick_prefix": "",
        },
        {
            "title": "Кумулятивная MWR",
            "y_col": "mwr_cum_return",
            "benchmark_y_col": "benchmark_mwr_cum_return",
            "is_percent": True,
            "hover_label": "Портфель",
            "y_tick_prefix": "",
        },
    )
    fig = make_subplots(
        rows=1,
        cols=3,
        subplot_titles=[panel["title"] for panel in panels],
        horizontal_spacing=0.06,
    )
    rendered_any = False
    for col_idx, panel in enumerate(panels, start=1):
        if _add_subplot_line_traces(
            fig,
            df,
            row=1,
            col=col_idx,
            y_col=panel["y_col"],
            benchmark_y_col=panel["benchmark_y_col"],
            is_percent=panel["is_percent"],
            hover_label=panel["hover_label"],
            y_tick_prefix=panel["y_tick_prefix"],
            benchmark_label=benchmark_label,
            show_legend=col_idx == 1,
        ):
            rendered_any = True
        if panel["is_percent"]:
            fig.update_yaxes(
                title=None, tickformat=".1%", fixedrange=True, row=1, col=col_idx
            )
        else:
            fig.update_yaxes(
                title=None,
                tickprefix=panel["y_tick_prefix"],
                tickformat=",.0f",
                fixedrange=True,
                row=1,
                col=col_idx,
            )
        fig.update_xaxes(
            title=None,
            tickformat="%m-%Y",
            hoverformat="%m-%Y",
            rangeslider={"visible": False},
            fixedrange=False,
            row=1,
            col=col_idx,
        )

    if not rendered_any:
        st.info("Недостаточно данных для графика.")
        return

    vline_markers = _cash_flow_markers_in_df_range(cash_flow_markers or [], df)
    vline_shapes = _cash_flow_vline_shapes(vline_markers, subplot_cols=3)

    fig.update_layout(
        margin={"l": 8, "r": 8, "t": 48, "b": 72},
        height=300,
        hovermode="x unified",
        dragmode="zoom",
        shapes=vline_shapes,
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": -0.18,
            "xanchor": "center",
            "x": 0.5,
        },
    )
    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displaylogo": False,
            "scrollZoom": True,
            "modeBarButtonsToRemove": [
                "pan2d",
                "select2d",
                "lasso2d",
                "toImage",
            ],
        },
    )


def _value_series_col(name: str) -> str:
    return f"value::{name}"


_NON_US_YF_SUFFIXES = {
    ".AS",
    ".AT",
    ".AX",
    ".BE",
    ".BK",
    ".BR",
    ".CO",
    ".DE",
    ".DU",
    ".F",
    ".HE",
    ".HK",
    ".IR",
    ".JK",
    ".JO",
    ".KQ",
    ".KS",
    ".L",
    ".LS",
    ".MC",
    ".ME",
    ".MI",
    ".MX",
    ".NS",
    ".NZ",
    ".OL",
    ".PA",
    ".PR",
    ".SA",
    ".SG",
    ".SI",
    ".SN",
    ".SR",
    ".SS",
    ".ST",
    ".SW",
    ".SZ",
    ".T",
    ".TA",
    ".TLV",
    ".TO",
    ".TSX",
    ".TW",
    ".VI",
    ".WA",
}


def _is_us_exchange_ticker(ticker: str) -> bool:
    """Same heuristic as portfolio summary table."""
    up = (ticker or "").upper().strip()
    if not up:
        return False
    row = get_instrument_provider(up)
    provider = (row[0] if row else None) or ""
    if provider in ("moex_iss", "tbank", "coingecko"):
        return False
    if up.endswith("-EUR") or up.endswith("-RUB"):
        return False
    return not any(up.endswith(sfx) for sfx in _NON_US_YF_SUFFIXES)


@dataclass(frozen=True)
class BreakdownSeriesSpec:
    name: str
    legend_group: str
    legend_group_title: str | None = None


def _asset_taxonomy_maps():
    subclass_by_id = {s.id: s for s in list_asset_subclasses()}
    class_by_id = {c.id: c for c in list_asset_classes()}
    return subclass_by_id, class_by_id


def _label_from_subclass_id(
    subclass_id: int,
    group_mode: str,
    *,
    subclass_by_id: dict,
    class_by_id: dict,
) -> str:
    sub = subclass_by_id.get(subclass_id)
    if group_mode == "subclasses":
        return sub.name if sub else "—"
    if group_mode == "classes":
        if sub:
            asset_class = class_by_id.get(sub.asset_class_id)
            return asset_class.name if asset_class else "—"
        return "—"
    return "—"


def _collect_breakdown_tickers(points) -> set[str]:
    tickers: set[str] = set()
    for p in points:
        ticker_vals = dict(p.ticker_values or {})
        if not ticker_vals:
            ticker_vals = dict(p.main_ticker_values or {})
        tickers.update(str(k).upper() for k in ticker_vals)
    return tickers


@st.cache_data(show_spinner=False)
def _ticker_group_label_cache(
    tickers_key: tuple[str, ...],
    group_mode: str,
    db_mtime: float,
) -> dict[str, str]:
    _ = db_mtime
    subclass_by_id, class_by_id = _asset_taxonomy_maps()
    subclass_ids = get_instrument_subclass_id_map(tickers_key)
    return {
        ticker: _label_from_subclass_id(
            subclass_ids[ticker],
            group_mode,
            subclass_by_id=subclass_by_id,
            class_by_id=class_by_id,
        )
        for ticker in tickers_key
    }


@st.cache_data(show_spinner=False)
def _ticker_currency_bucket_cache(
    tickers_key: tuple[str, ...],
    db_mtime: float,
    quotes_cache_ts: int,
) -> dict[str, str]:
    _ = (db_mtime, quotes_cache_ts)
    quotes = get_app_quotes(list(tickers_key)) if tickers_key else {}
    out: dict[str, str] = {}
    for ticker in tickers_key:
        q = quotes.get(ticker) or quotes.get(ticker.upper())
        live_ccy = q.currency if q is not None else None
        out[ticker] = bucket_diversification_currency(
            resolve_quote_currency(ticker, live_ccy)
        )
    return out


def _subclass_sort_key(
    subclass_name: str,
    *,
    subclass_by_id: dict,
    class_by_id: dict,
    subclass_name_to_id: dict[str, int],
) -> tuple:
    sid = subclass_name_to_id.get(subclass_name)
    sub = subclass_by_id.get(sid) if sid is not None else None
    asset_class = class_by_id.get(sub.asset_class_id) if sub else None
    return (
        int(asset_class.sort_order) if asset_class else 10**9,
        int(sub.sort_order) if sub else 10**9,
        subclass_name,
    )


def _ticker_sort_key(
    ticker: str,
    *,
    ticker_subclass_ids: dict[str, int],
    subclass_by_id: dict,
    class_by_id: dict,
) -> tuple:
    up = str(ticker).upper()
    sid = ticker_subclass_ids.get(up)
    sub = subclass_by_id.get(sid) if sid is not None else None
    asset_class = class_by_id.get(sub.asset_class_id) if sub else None
    return (
        int(asset_class.sort_order) if asset_class else 10**9,
        int(sub.sort_order) if sub else 10**9,
        0 if _is_us_exchange_ticker(up) else 1,
        up,
    )


def _subclass_name_for_ticker(
    ticker: str,
    *,
    ticker_subclass_ids: dict[str, int],
    subclass_by_id: dict,
) -> str:
    up = str(ticker).upper()
    sid = ticker_subclass_ids.get(up)
    sub = subclass_by_id.get(sid) if sid is not None else None
    return sub.name if sub else "—"


def _class_name_for_subclass(
    subclass_name: str,
    *,
    subclass_by_id: dict,
    class_by_id: dict,
    subclass_name_to_id: dict[str, int],
) -> str:
    sid = subclass_name_to_id.get(subclass_name)
    sub = subclass_by_id.get(sid) if sid is not None else None
    if sub:
        asset_class = class_by_id.get(sub.asset_class_id)
        return asset_class.name if asset_class else "—"
    return "—"


def _ordered_breakdown_series_specs(
    group_mode: str,
    seen: set[str],
    *,
    subclass_by_id: dict,
    class_by_id: dict,
    ticker_subclass_ids: dict[str, int] | None = None,
) -> list[BreakdownSeriesSpec]:
    subclass_name_to_id = {s.name: s.id for s in subclass_by_id.values()}
    specs: list[BreakdownSeriesSpec] = []

    if group_mode == "tickers":
        ticker_subclass_ids = ticker_subclass_ids or {}
        tickers = sorted(
            (t for t in seen if t != "Прочие активы"),
            key=lambda t: _ticker_sort_key(
                t,
                ticker_subclass_ids=ticker_subclass_ids,
                subclass_by_id=subclass_by_id,
                class_by_id=class_by_id,
            ),
        )
        prev_group: str | None = None
        for ticker in tickers:
            group = _subclass_name_for_ticker(
                ticker,
                ticker_subclass_ids=ticker_subclass_ids,
                subclass_by_id=subclass_by_id,
            )
            specs.append(
                BreakdownSeriesSpec(
                    name=ticker,
                    legend_group=group,
                    legend_group_title=group if group != prev_group else None,
                )
            )
            prev_group = group
        if "Прочие активы" in seen:
            specs.append(
                BreakdownSeriesSpec(
                    name="Прочие активы",
                    legend_group="Прочие активы",
                    legend_group_title="Прочие активы",
                )
            )
        return specs

    if group_mode == "subclasses":
        subclass_names = sorted(
            seen,
            key=lambda name: _subclass_sort_key(
                name,
                subclass_by_id=subclass_by_id,
                class_by_id=class_by_id,
                subclass_name_to_id=subclass_name_to_id,
            ),
        )
        prev_group = None
        for subclass_name in subclass_names:
            group = _class_name_for_subclass(
                subclass_name,
                subclass_by_id=subclass_by_id,
                class_by_id=class_by_id,
                subclass_name_to_id=subclass_name_to_id,
            )
            specs.append(
                BreakdownSeriesSpec(
                    name=subclass_name,
                    legend_group=group,
                    legend_group_title=group if group != prev_group else None,
                )
            )
            prev_group = group
        return specs

    if group_mode == "currencies":
        currency_order = {"RUB": 0, "USD": 1, "EUR": 2}
        currency_names = sorted(
            seen,
            key=lambda name: (currency_order.get(name, 99), name),
        )
        for currency_name in currency_names:
            specs.append(
                BreakdownSeriesSpec(
                    name=currency_name,
                    legend_group=currency_name,
                    legend_group_title=None,
                )
            )
        return specs

    class_name_to_sort = {c.name: int(c.sort_order) for c in class_by_id.values()}
    class_names = sorted(
        seen,
        key=lambda name: (class_name_to_sort.get(name, 10**9), name),
    )
    for class_name in class_names:
        specs.append(
            BreakdownSeriesSpec(
                name=class_name,
                legend_group=class_name,
                legend_group_title=None,
            )
        )
    return specs


def _apply_breakdown_percent_view(
    df: pd.DataFrame,
    series_specs: list[BreakdownSeriesSpec],
) -> pd.DataFrame:
    out = df.copy()
    total = out["portfolio_value"].astype(float).replace(0, pd.NA)
    for spec in series_specs:
        col = _value_series_col(spec.name)
        if col in out.columns:
            out[col] = out[col].astype(float) / total
    return out


def _build_breakdown_chart_df(
    points,
    group_mode: str,
    *,
    db_mtime: float = 0.0,
    quotes_cache_ts: int = 0,
) -> tuple[pd.DataFrame, list[BreakdownSeriesSpec]]:
    subclass_by_id, class_by_id = _asset_taxonomy_maps()
    label_cache: dict[str, str] = {}
    ticker_subclass_ids: dict[str, int] = {}
    if group_mode == "tickers":
        main_tickers_key = tuple(
            sorted(
                {str(k).upper() for p in points for k in (p.main_ticker_values or {})}
            )
        )
        if main_tickers_key:
            ticker_subclass_ids = get_instrument_subclass_id_map(main_tickers_key)
    elif group_mode in ("subclasses", "classes"):
        tickers_key = tuple(sorted(_collect_breakdown_tickers(points)))
        label_cache = _ticker_group_label_cache(tickers_key, group_mode, db_mtime)
    elif group_mode == "currencies":
        tickers_key = tuple(sorted(_collect_breakdown_tickers(points)))
        label_cache = _ticker_currency_bucket_cache(
            tickers_key, db_mtime, quotes_cache_ts
        )
    seen_series: set[str] = set()
    rows: list[dict] = []

    for p in points:
        if group_mode == "tickers":
            main_vals = dict(p.main_ticker_values or {})
            group_vals = {
                str(k).upper(): float(v) for k, v in main_vals.items() if float(v) > 0
            }
            other_value = float(p.other_assets_value or 0.0)
            if other_value > 0:
                group_vals["Прочие активы"] = other_value
        else:
            group_vals: dict[str, float] = {}
            ticker_vals = dict(p.ticker_values or {})
            if not ticker_vals:
                ticker_vals = dict(p.main_ticker_values or {})
            for ticker, value in ticker_vals.items():
                amount = float(value)
                if amount <= 0:
                    continue
                up = str(ticker).upper()
                label = label_cache.get(up, "—")
                group_vals[label] = group_vals.get(label, 0.0) + amount

        seen_series.update(group_vals.keys())
        rows.append(
            {
                "date": p.date,
                "portfolio_value": p.portfolio_value,
                "_group_vals": group_vals,
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df, []
    df["date"] = pd.to_datetime(df["date"])
    series_specs = _ordered_breakdown_series_specs(
        group_mode,
        seen_series,
        subclass_by_id=subclass_by_id,
        class_by_id=class_by_id,
        ticker_subclass_ids=ticker_subclass_ids,
    )
    for spec in series_specs:
        df[_value_series_col(spec.name)] = [
            float(row["_group_vals"].get(spec.name) or 0.0) for row in rows
        ]
    return df.drop(columns=["_group_vals"]), series_specs


def _rgba(red: float, green: float, blue: float, alpha: float) -> str:
    return f"rgba({int(red * 255)},{int(green * 255)},{int(blue * 255)},{alpha})"


def _breakdown_series_colors(
    series_specs: list[BreakdownSeriesSpec],
) -> dict[str, tuple[str, str]]:
    """
    Distinct hue per legend group; shades of that hue within the group.
    Returns fill/line rgba tuples (fill is more transparent).
    """
    groups: list[tuple[str, list[str]]] = []
    group_index: dict[str, int] = {}
    for spec in series_specs:
        if spec.legend_group not in group_index:
            group_index[spec.legend_group] = len(groups)
            groups.append((spec.legend_group, []))
        groups[group_index[spec.legend_group]][1].append(spec.name)

    color_by_name: dict[str, tuple[str, str]] = {}
    golden_ratio = 0.618033988749895
    for gi, (_group, names) in enumerate(groups):
        hue = (gi * golden_ratio) % 1.0
        count = len(names)
        for ni, name in enumerate(names):
            if count == 1:
                lightness = 0.52
                saturation = 0.72
            else:
                lightness = 0.36 + (ni / (count - 1)) * 0.34
                saturation = 0.68
            red, green, blue = colorsys.hls_to_rgb(hue, lightness, saturation)
            color_by_name[name] = (
                _rgba(red, green, blue, 0.55),
                _rgba(red, green, blue, 0.9),
            )
    return color_by_name


def _add_stacked_breakdown_trace(
    fig: go.Figure,
    df: pd.DataFrame,
    *,
    y_col: str,
    name: str,
    display_ccy: str,
    hover_label: str,
    legend_group: str,
    legend_group_title: str | None = None,
    percent_mode: bool = False,
    fill_color: str | None = None,
    line_color: str | None = None,
) -> None:
    y_values = df[y_col].astype(float).fillna(0.0)
    if percent_mode:
        if float(y_values.sum()) <= 0:
            return
    elif float(y_values.sum()) <= 0:
        return
    if percent_mode:
        hover_template = (
            "Date: %{x|%m-%Y}<br>"
            f"{hover_label}: %{{customdata:.1%}}"
            "<extra></extra>"
        )
    else:
        hover_template = (
            "Date: %{x|%m-%Y}<br>"
            f"{hover_label}: {display_ccy} %{{customdata:,.0f}}"
            "<extra></extra>"
        )
    default_fill = "rgba(99,110,250,0.55)"
    default_line = "rgba(99,110,250,0.9)"
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=y_values,
            mode="lines",
            stackgroup="portfolio",
            line={"width": 0.5, "color": line_color or default_line},
            fillcolor=fill_color or default_fill,
            customdata=y_values,
            hovertemplate=hover_template,
            name=name,
            legendgroup=legend_group,
            legendgrouptitle_text=legend_group_title,
        )
    )


_HISTORICAL_FX_SETTING_KEY = "historical_fx_v1"


def _build_fx_to_rub_df(
    chart_dates: pd.Series,
    *,
    rub_per_usd_spot: float,
    eur_per_usd_spot: float,
) -> pd.DataFrame:
    """Align USD/RUB and EUR/RUB to chart dates (last FX on or before each day)."""
    if chart_dates.empty:
        return pd.DataFrame(columns=["date", "usd_rub", "eur_rub"])

    fx_exact: dict[str, tuple[float, float]] = {}
    raw = get_app_setting(_HISTORICAL_FX_SETTING_KEY)
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {}
        if isinstance(data, dict):
            for day, pair in data.items():
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    continue
                rub, eur = float(pair[0]), float(pair[1])
                if rub > 0 and eur > 0:
                    fx_exact[str(day)] = (rub, eur)

    default = (float(rub_per_usd_spot), float(eur_per_usd_spot))
    sorted_fx_days = sorted(fx_exact.keys())

    def _fx_pair_as_of(day: str) -> tuple[float, float]:
        if not sorted_fx_days:
            return default
        idx = bisect.bisect_right(sorted_fx_days, day) - 1
        if idx < 0:
            return default
        return fx_exact[sorted_fx_days[idx]]

    rows: list[dict[str, object]] = []
    for ts in chart_dates:
        day = pd.Timestamp(ts).strftime("%Y-%m-%d")
        rub, eur = _fx_pair_as_of(day)
        eur_rub = rub / eur if eur > 0 else float("nan")
        rows.append(
            {
                "date": pd.Timestamp(ts),
                "usd_rub": float(rub),
                "eur_rub": float(eur_rub),
            }
        )
    return pd.DataFrame(rows)


def _fx_secondary_axis_range(
    fx_df: pd.DataFrame,
    *,
    currencies: set[str],
) -> list[float] | None:
    values: list[float] = []
    if "USD" in currencies and "usd_rub" in fx_df.columns:
        values.extend(
            float(v) for v in fx_df["usd_rub"].tolist() if v is not None and pd.notna(v)
        )
    if "EUR" in currencies and "eur_rub" in fx_df.columns:
        values.extend(
            float(v) for v in fx_df["eur_rub"].tolist() if v is not None and pd.notna(v)
        )
    if not values:
        return None
    lo = min(values)
    hi = max(values)
    if lo == hi:
        pad = max(1.0, abs(lo) * 0.02)
        return [lo - pad, hi + pad]
    span = hi - lo
    pad = max(0.5, span * 0.05)
    return [lo - pad, hi + pad]


_FX_USD_LINE_COLOR = "#06B6D4"
_FX_EUR_LINE_COLOR = "#C026D3"


def _add_fx_to_rub_secondary_axis(
    fig: go.Figure,
    fx_df: pd.DataFrame,
    *,
    currencies: set[str],
) -> bool:
    if fx_df.empty or not currencies:
        return False
    added = False
    if "USD" in currencies:
        fig.add_trace(
            go.Scatter(
                x=fx_df["date"],
                y=fx_df["usd_rub"],
                mode="lines",
                name="USD/RUB",
                yaxis="y2",
                line={"width": 2.5, "dash": "dash", "color": _FX_USD_LINE_COLOR},
                hovertemplate="USD/RUB: %{y:,.2f} ₽<extra></extra>",
                legendgroup="fx_rates",
                legendgrouptitle_text="Курс к ₽" if not added else None,
            )
        )
        added = True
    if "EUR" in currencies:
        fig.add_trace(
            go.Scatter(
                x=fx_df["date"],
                y=fx_df["eur_rub"],
                mode="lines",
                name="EUR/RUB",
                yaxis="y2",
                line={"width": 2.5, "dash": "dot", "color": _FX_EUR_LINE_COLOR},
                hovertemplate="EUR/RUB: %{y:,.2f} ₽<extra></extra>",
                legendgroup="fx_rates",
                legendgrouptitle_text="Курс к ₽" if not added else None,
            )
        )
        added = True
    return added


def _render_value_breakdown_chart(
    df: pd.DataFrame,
    series_specs: list[BreakdownSeriesSpec],
    *,
    display_ccy: str,
    height: int = 420,
    percent_mode: bool = False,
    cash_flow_markers: list[tuple[pd.Timestamp, str]] | None = None,
    rub_per_usd: float = 95.0,
    eur_per_usd: float = 0.92,
    fx_currencies: set[str] | None = None,
) -> None:
    if df.empty or df["portfolio_value"].notna().sum() == 0:
        st.info("Недостаточно данных для графика.")
        return

    fig = go.Figure()
    color_by_name = _breakdown_series_colors(series_specs)
    has_any_layer = False
    for spec in series_specs:
        col = _value_series_col(spec.name)
        if col not in df.columns:
            continue
        before = len(fig.data)
        fill_color, line_color = color_by_name.get(spec.name, (None, None))
        _add_stacked_breakdown_trace(
            fig,
            df,
            y_col=col,
            name=spec.name,
            display_ccy=display_ccy,
            hover_label=spec.name,
            legend_group=spec.legend_group,
            legend_group_title=spec.legend_group_title,
            percent_mode=percent_mode,
            fill_color=fill_color,
            line_color=line_color,
        )
        if len(fig.data) > before:
            has_any_layer = True

    if not has_any_layer:
        st.info("Недостаточно данных для графика.")
        return

    fx_selection = {str(c).upper() for c in (fx_currencies or set())}
    show_fx_secondary = not percent_mode and bool(fx_selection)
    has_fx_secondary = False
    fx_df = pd.DataFrame()
    if show_fx_secondary:
        fx_df = _build_fx_to_rub_df(
            df["date"],
            rub_per_usd_spot=rub_per_usd,
            eur_per_usd_spot=eur_per_usd,
        )
        has_fx_secondary = _add_fx_to_rub_secondary_axis(
            fig,
            fx_df,
            currencies=fx_selection,
        )

    if percent_mode:
        fig.update_layout(
            yaxis={
                "title": None,
                "tickformat": ".0%",
                "ticksuffix": "",
                "fixedrange": True,
            }
        )
    else:
        fig.update_layout(
            yaxis={
                "title": None,
                "tickprefix": f"{display_ccy} ",
                "tickformat": ",.0f",
                "fixedrange": True,
            }
        )
    if has_fx_secondary:
        yaxis2: dict[str, object] = {
            "title": None,
            "overlaying": "y",
            "side": "right",
            "tickformat": ",.1f",
            "ticksuffix": " ₽",
            "showgrid": False,
            "fixedrange": True,
            "automargin": True,
            "ticklabelposition": "outside",
        }
        fx_range = _fx_secondary_axis_range(fx_df, currencies=fx_selection)
        if fx_range is not None:
            yaxis2["range"] = fx_range
        fig.update_layout(yaxis2=yaxis2)
    fig.update_xaxes(
        title=None,
        tickformat="%m-%Y",
        hoverformat="%m-%Y",
        rangeslider={"visible": False},
        fixedrange=False,
    )
    vline_shapes = _cash_flow_vline_shapes(
        _cash_flow_markers_in_df_range(cash_flow_markers or [], df),
        subplot_cols=1,
    )
    base_legend_margin = 180 if height >= 600 else 140
    if has_fx_secondary:
        legend_right_margin = base_legend_margin + 88
        legend_x = 1.14
    else:
        legend_right_margin = base_legend_margin
        legend_x = 1.02
    fig.update_layout(
        margin={"l": 8, "r": legend_right_margin, "t": 24, "b": 24},
        height=height,
        hovermode="x unified",
        dragmode="zoom",
        shapes=vline_shapes,
        legend={
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": legend_x,
            "tracegroupgap": 8,
            "itemsizing": "constant",
            "groupclick": "toggleitem",
        },
    )
    st.plotly_chart(
        fig,
        width="stretch",
        config={
            "displaylogo": False,
            "scrollZoom": True,
            "modeBarButtonsToRemove": [
                "pan2d",
                "select2d",
                "lasso2d",
                "toImage",
            ],
        },
    )


def _filter_chart_df_by_frequency(
    df: pd.DataFrame, chart_frequency: str
) -> pd.DataFrame:
    freq = str(chart_frequency or "daily").strip().lower()
    if df.empty:
        return df
    if freq == "monthly":
        month_end_mask = df["date"].dt.to_period("M") != df["date"].shift(
            -1
        ).dt.to_period("M")
        filtered = df[month_end_mask]
    elif freq == "weekly":
        curr_week = (
            df["date"].dt.isocalendar().year.astype(str)
            + "-"
            + df["date"].dt.isocalendar().week.astype(str)
        )
        next_week = curr_week.shift(-1)
        week_end_mask = curr_week != next_week
        filtered = df[week_end_mask]
    else:
        filtered = df
    last_row = df.iloc[[-1]]
    if filtered.empty or filtered["date"].iloc[-1] != last_row["date"].iloc[0]:
        filtered = pd.concat([filtered, last_row], ignore_index=True)
    return filtered.drop_duplicates(subset=["date"], keep="last").sort_values("date")


@st.cache_data(show_spinner=False)
def _compute_portfolio_performance_cached(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    mwr_curve_frequency: str,
    db_mtime: float,
    quotes_cache_ts: int,
):
    _ = (db_mtime, quotes_cache_ts)  # cache invalidation on DB / live quotes updates
    return compute_portfolio_performance(
        display_currency=display_currency,
        rub_per_usd=rub_per_usd,
        eur_per_usd=eur_per_usd,
        mwr_curve_frequency=mwr_curve_frequency,
    )


def _perf_session_cache_key(
    display_currency: str,
    mwr_curve_frequency: str,
    db_mtime: float,
    quotes_cache_ts: int,
) -> str:
    """Flat string key — tuple keys in nested dicts are unreliable in Streamlit session_state."""
    ccy = str(display_currency or "RUB").upper()
    freq = str(mwr_curve_frequency or "daily").strip().lower()
    mtime = int(float(db_mtime))
    qts = int(quotes_cache_ts)
    return f"portfolio_perf:{ccy}:{freq}:{mtime}:{qts}"


def _quotes_cache_ts() -> int:
    meta = get_quotes_cache_meta()
    return int(float(meta.get("ts") or 0))


def _get_portfolio_performance(
    display_currency: str,
    rub_per_usd: float,
    eur_per_usd: float,
    mwr_curve_frequency: str,
    db_mtime: float,
):
    """One heavy compute per rerun per parameter set (Streamlit tabs call this twice)."""
    quotes_cache_ts = _quotes_cache_ts()
    session_key = _perf_session_cache_key(
        display_currency, mwr_curve_frequency, db_mtime, quotes_cache_ts
    )
    cached = st.session_state.get(session_key)
    if cached is not None:
        return cached
    result = _compute_portfolio_performance_cached(
        display_currency=display_currency,
        rub_per_usd=rub_per_usd,
        eur_per_usd=eur_per_usd,
        mwr_curve_frequency=mwr_curve_frequency,
        db_mtime=db_mtime,
        quotes_cache_ts=quotes_cache_ts,
    )
    st.session_state[session_key] = result
    return result


def render_performance_top_metrics() -> None:
    """Render key performance metrics above main tabs."""
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
    db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0
    result = _get_portfolio_performance(
        display_currency=display_ccy,
        rub_per_usd=rub,
        eur_per_usd=eur,
        mwr_curve_frequency="monthly",
        db_mtime=db_mtime,
    )
    if not result.points:
        return

    m1, m2, m3 = st.columns(3)
    m1.metric(
        "MWR (все время)",
        (
            _fmt_pct(result.points[-1].mwr_cum_return)
            if (result.points and result.points[-1].mwr_cum_return is not None)
            else "—"
        ),
        help=f"Прибыль на каждый вложенный {display_ccy}.",
    )
    m2.metric(
        "MWR (XIRR)",
        (
            _fmt_pct(result.mwr_xirr_annualized)
            if result.mwr_xirr_annualized is not None
            else "—"
        ),
        help="MWR в % годовых",
    )
    m3.metric(
        "P&L",
        format_money(result.total_pnl, display_ccy),
        delta=_pnl_return_delta(result.total_pnl, result.net_invested),
        help=(f"Простая доходность (стоимость портфеля - инвестированный капитал)."),
    )


def render_performance() -> None:
    display_ccy = st.session_state.get("display_currency", "RUB")
    fx = st.session_state.get("fx_cache") or {}
    rub = float(fx.get("rub") or 95.0)
    eur = float(fx.get("eur") or 0.92)
    # Keep behavior consistent with top summary metrics: if no current quotes are
    # available for any active instrument, do not show historical performance charts.
    current_positions = list_positions_by_ticker()
    active_tickers = sorted(
        {
            str(p.ticker or "").upper().strip()
            for p in current_positions
            if str(p.ticker or "").strip() and float(p.amount or 0) > 0
        }
    )
    if active_tickers:
        live_quotes = get_app_quotes(active_tickers)
        live_priced_count = 0
        for ticker in active_tickers:
            q = live_quotes.get(ticker)
            raw_price = q.price if q is not None else None
            quote_ccy = q.currency if q is not None else None
            norm_price = normalize_quote_price_for_valuation(
                ticker=ticker,
                price=raw_price,
                currency=quote_ccy,
            )
            if norm_price is not None:
                live_priced_count += 1
        if live_priced_count == 0:
            st.warning(
                "Нет актуальных котировок по текущим позициям. "
                "Доходность скрыта, пока не появится хотя бы одна текущая цена."
            )
            return

    with st.container(horizontal=True, gap="small"):
        view_label = st.segmented_control(
            "Вид",
            options=["Overview", "Breakdown"],
            format_func=lambda x: "Общая" if x == "Overview" else "Разбивка",
            default="Overview",
            key="perf_view_mode",
            width="content",
        )
        freq_label = st.segmented_control(
            "Частота",
            options=["Months", "Weeks", "Days"],
            format_func=lambda x: (
                "Месяцы" if x == "Months" else "Недели" if x == "Weeks" else "Дни"
            ),
            default="Months",
            key="perf_chart_frequency",
            width="content",
        )
        cash_flow_label = st.segmented_control(
            "Ввод/Вывод",
            options=["Off", "On"],
            format_func=lambda x: "Выкл." if x == "Off" else "Вкл.",
            default="Off",
            key="perf_cash_flow_lines",
            width="content",
        )

        is_breakdown = view_label == "Breakdown"
        if is_breakdown:
            group_label = st.segmented_control(
                "Группировка",
                options=["Tickers", "Subclasses", "Classes", "Currencies"],
                format_func=lambda x: (
                    "Тикеры"
                    if x == "Tickers"
                    else (
                        "Подклассы"
                        if x == "Subclasses"
                        else "Классы" if x == "Classes" else "Валюты"
                    )
                ),
                default="Tickers",
                key="perf_breakdown_group_mode",
                width="content",
            )
            value_label = st.segmented_control(
                "Отображение",
                options=["Absolute", "Percent"],
                format_func=lambda x: "Абс." if x == "Absolute" else "%",
                default="Percent",
                key="perf_breakdown_value_mode",
                width="content",
            )
            percent_mode = value_label == "Percent"
            if not percent_mode:
                fx_raw = st.segmented_control(
                    "Курс к ₽",
                    options=["USD", "EUR"],
                    selection_mode="multi",
                    default=["USD"],
                    key="perf_breakdown_fx_currencies",
                    width="content",
                )
                fx_currencies = {str(c).upper() for c in (fx_raw or [])}
            else:
                fx_currencies = set()
        else:
            group_label = st.session_state.get("perf_breakdown_group_mode", "Tickers")
            value_label = st.session_state.get("perf_breakdown_value_mode", "Percent")
            percent_mode = value_label == "Percent"
            fx_raw = st.session_state.get("perf_breakdown_fx_currencies", ["USD"])
            fx_currencies = {str(c).upper() for c in (fx_raw or [])}
    chart_frequency = (
        "monthly"
        if freq_label == "Months"
        else ("weekly" if freq_label == "Weeks" else "daily")
    )
    show_cash_flow_lines = cash_flow_label == "On"
    group_mode = (
        "tickers"
        if group_label == "Tickers"
        else (
            "subclasses"
            if group_label == "Subclasses"
            else "classes" if group_label == "Classes" else "currencies"
        )
    )
    db_path = Path(__file__).resolve().parents[2] / "data" / "portfolio.db"
    db_mtime = float(db_path.stat().st_mtime) if db_path.exists() else 0.0

    with st.spinner("Расчёт доходности…"):
        result = _get_portfolio_performance(
            display_currency=display_ccy,
            rub_per_usd=rub,
            eur_per_usd=eur,
            mwr_curve_frequency=chart_frequency,
            db_mtime=db_mtime,
        )
    if not result.points:
        st.info("Недостаточно данных: добавьте хотя бы одну сделку.")
        return

    df = pd.DataFrame(
        {
            "date": [p.date for p in result.points],
            "portfolio_value": [p.portfolio_value for p in result.points],
            "twr_cum_return": [p.twr_cum_return for p in result.points],
            "mwr_cum_return": [p.mwr_cum_return for p in result.points],
            "priced_ratio": [p.priced_ratio for p in result.points],
            "benchmark_value": [p.benchmark_value for p in result.points],
            "benchmark_cum_return": [p.benchmark_cum_return for p in result.points],
            "benchmark_mwr_cum_return": [
                p.benchmark_mwr_cum_return for p in result.points
            ],
        }
    )
    df["date"] = pd.to_datetime(df["date"])
    chart_df = _filter_chart_df_by_frequency(df, chart_frequency)
    cash_flow_markers = (
        _cash_flow_chart_markers(db_mtime) if show_cash_flow_lines else []
    )
    benchmark_help_note = _synthetic_benchmark_help_note(result, display_ccy)

    if view_label == "Overview":
        _render_performance_charts(
            chart_df,
            display_ccy=display_ccy,
            benchmark_label=f"Бенчмарк ({result.benchmark_ticker})",
            cash_flow_markers=cash_flow_markers,
        )

        st.divider()

        # Row 1: P&L + benchmark comparison
        benchmark_pnl = (
            float(result.benchmark_current_value) - float(result.net_invested)
            if result.benchmark_current_value is not None
            else None
        )
        r1c1, r1c2, r1c3 = st.columns(3)
        r1c1.metric(
            "P&L",
            format_money(result.total_pnl, display_ccy),
            delta=_pnl_return_delta(result.total_pnl, result.net_invested),
            help=(
                f"Простая доходность (стоимость портфеля - инвестированный капитал)."
            ),
        )
        r1c2.metric(
            f"P&L Бенчмарк",
            (
                format_money(benchmark_pnl, display_ccy)
                if benchmark_pnl is not None
                else "—"
            ),
            delta=_pnl_return_delta(benchmark_pnl, result.net_invested),
            help=(
                f"Простая доходность бенчмарка ({result.benchmark_ticker}) "
                f"при тех же вводах/выводах.{benchmark_help_note}"
                if result.benchmark_ticker
                else f"Простая доходность бенчмарка при тех же вводах/выводах.{benchmark_help_note}"
            ),
        )
        r1c3.metric(
            f"Дельта vs Бенчмарк",
            (
                format_money(result.benchmark_delta_value, display_ccy)
                if result.benchmark_delta_value is not None
                else "—"
            ),
            delta=_pnl_return_delta(result.benchmark_delta_value, result.total_pnl),
            help=(
                f"Разница текущей стоимости портфеля и фонда-бенчмарка ({result.benchmark_ticker}) "
                f"денежного рынка в {display_ccy}.{benchmark_help_note}"
            ),
        )

        st.divider()

        # Row 2: MWR (XIRR + all-time cumulative MWR)
        benchmark_all_time_mwr = next(
            (
                p.benchmark_mwr_cum_return
                for p in reversed(result.points)
                if p.benchmark_mwr_cum_return is not None
            ),
            None,
        )
        r2c1, r2c2, r2c3, r2c4 = st.columns(4)
        r2c1.metric(
            "MWR (XIRR)",
            (
                _fmt_pct(result.mwr_xirr_annualized)
                if result.mwr_xirr_annualized is not None
                else "—"
            ),
            help="MWR в % годовых",
        )
        r2c2.metric(
            "MWR (XIRR) Бенчмарк",
            (
                _fmt_pct(result.benchmark_mwr_xirr_annualized)
                if result.benchmark_mwr_xirr_annualized is not None
                else "—"
            ),
            help=(
                f"MWR в % годовых если бы инвестировали в бенчмарк ({result.benchmark_ticker})."
                f"{benchmark_help_note}"
                if result.benchmark_ticker
                else f"MWR бенчмарка в % годовых если бы инвестировали в бенчмарк.{benchmark_help_note}"
            ),
        )
        r2c3.metric(
            "MWR (все время)",
            (
                _fmt_pct(result.points[-1].mwr_cum_return)
                if result.points[-1].mwr_cum_return is not None
                else "—"
            ),
            help=f"Прибыль на каждый вложенный {display_ccy}.",
        )
        r2c4.metric(
            "MWR Бенчмарк (все время)",
            (
                _fmt_pct(benchmark_all_time_mwr)
                if benchmark_all_time_mwr is not None
                else "—"
            ),
            help=(
                f"Прибыль на каждый вложенный {display_ccy} если бы инвестировали в бенчмарк ({result.benchmark_ticker})."
                f"{benchmark_help_note}"
                if result.benchmark_ticker
                else f"Прибыль на каждый вложенный {display_ccy} если бы инвестировали в бенчмарк.{benchmark_help_note}"
            ),
        )

        st.divider()

        # Row 3: Portfolio simple return by period
        period = compute_period_returns(result.points, net_invested=result.net_invested)
        r3c1, r3c2, r3c3, r3c4, r3c5, r3c6 = st.columns(6)
        r3c1.metric("P&L - 1M", _fmt_pct(period["1M"]))
        r3c2.metric("P&L - 3M", _fmt_pct(period["3M"]))
        r3c3.metric("P&L - 6M", _fmt_pct(period["6M"]))
        r3c4.metric("P&L - 1Y", _fmt_pct(period["1Y"]))
        r3c5.metric("P&L - YTD", _fmt_pct(period["YTD"]))
        r3c6.metric("P&L - ALL", _fmt_pct(period["ALL"]))

        # Row 4: Benchmark simple return by period
        benchmark_period = compute_benchmark_period_returns(
            result.points, net_invested=result.net_invested
        )
        r4c1, r4c2, r4c3, r4c4, r4c5, r4c6 = st.columns(6)
        r4c1.metric("P&L Бенчмарк - 1M", _fmt_pct(benchmark_period["1M"]))
        r4c2.metric("P&L Бенчмарк - 3M", _fmt_pct(benchmark_period["3M"]))
        r4c3.metric("P&L Бенчмарк - 6M", _fmt_pct(benchmark_period["6M"]))
        r4c4.metric("P&L Бенчмарк - 1Y", _fmt_pct(benchmark_period["1Y"]))
        r4c5.metric("P&L Бенчмарк - YTD", _fmt_pct(benchmark_period["YTD"]))
        r4c6.metric("P&L Бенчмарк - ALL", _fmt_pct(benchmark_period["ALL"]))

        low_coverage_days = int((df["priced_ratio"] < 1.0).sum())
        recent_low_coverage = int((df.tail(7)["priced_ratio"] < 1.0).sum())
        if result.missing_price_tickers or recent_low_coverage > 0:
            warn = []
            if result.missing_price_tickers:
                warn.append(
                    "Нет исторических котировок для: "
                    + ", ".join(sorted(result.missing_price_tickers))
                )
            if recent_low_coverage > 0:
                warn.append(
                    f"Дней с неполным покрытием цен (последние 7): {recent_low_coverage}"
                )
            st.warning(" | ".join(warn))

    else:
        breakdown_df, series_specs = _build_breakdown_chart_df(
            result.points,
            group_mode,
            db_mtime=db_mtime,
            quotes_cache_ts=_quotes_cache_ts(),
        )
        breakdown_chart_df = _filter_chart_df_by_frequency(
            breakdown_df, chart_frequency
        )
        if percent_mode:
            breakdown_chart_df = _apply_breakdown_percent_view(
                breakdown_chart_df,
                series_specs,
            )
        _render_value_breakdown_chart(
            breakdown_chart_df,
            series_specs,
            display_ccy=display_ccy,
            height=840,
            percent_mode=percent_mode,
            cash_flow_markers=cash_flow_markers,
            rub_per_usd=rub,
            eur_per_usd=eur,
            fx_currencies=fx_currencies,
        )
