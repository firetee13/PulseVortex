#!/usr/bin/env python3
"""Chart helpers using Plotly for the Dash UI.

Functions:
- get_ohlc_for_setup(meta) -> dict | None
- candlestick_figure_from_ohlc(ohlc_data, title) -> plotly.graph_objs.Figure
- pnl_category_figures(series_dict) -> dict[str, Figure]

Relies on monitor.mt5_client for tick fetching and monitor.web_db for series data.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import plotly.graph_objects as go

from monitor import mt5_client

UTC = timezone.utc
DISPLAY_TZ = timezone(timedelta(hours=3))


def _tz_label(dt: datetime) -> str:
    """Return a compact UTC offset label for aware datetimes (e.g., UTC+03)."""
    if dt.tzinfo is None:
        return "UTC"
    offset = dt.utcoffset()
    if offset is None:
        return "UTC"
    total_minutes = int(offset.total_seconds() // 60)
    sign = "+" if total_minutes >= 0 else "-"
    total_minutes = abs(total_minutes)
    hours, minutes = divmod(total_minutes, 60)
    if minutes:
        return f"UTC{sign}{hours:02d}:{minutes:02d}"
    return f"UTC{sign}{hours:02d}"


def _ensure_mt5(timeout: int = 30, retries: int = 1, portable: bool = False) -> bool:
    try:
        mt5_client.init_mt5(timeout=timeout, retries=retries, portable=portable)
        return True
    except Exception:
        return mt5_client.has_mt5()



def _tick_field(tk: Any, key: str) -> Any:
    """Safely extract field values from MetaTrader tick objects, dicts, or numpy record arrays."""
    try:
        val = getattr(tk, key)
        if val is not None:
            return val
    except Exception:
        pass
    try:
        if isinstance(tk, dict):
            val = tk.get(key)
            if val is not None:
                return val
    except Exception:
        pass
    try:
        if hasattr(tk, '__getitem__'):
            val = tk[key]
            return val
    except Exception:
        pass
    return None

def _get_tick_price(tk: Any, direction: str) -> Optional[float]:
    """Return price from tick object according to direction (buy -> use bid, sell -> use ask)."""
    bid = _tick_field(tk, "bid")
    ask = _tick_field(tk, "ask")
    try:
        if direction.lower() == "buy":
            return float(bid) if bid is not None else None
        return float(ask) if ask is not None else None
    except Exception:
        return None


def _get_tick_time_utc(tk: Any, server_offset_hours: int) -> Optional[datetime]:
    """Return tick timestamp as timezone-aware UTC datetime."""
    tms = _tick_field(tk, "time_msc")
    dt_raw = None
    if tms not in (None, 0):
        try:
            dt_raw = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
        except Exception:
            dt_raw = None
    if dt_raw is None:
        tse = _tick_field(tk, "time")
        if tse in (None, 0):
            return None
        try:
            dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)
        except Exception:
            return None
    try:
        dt_utc = dt_raw - timedelta(hours=server_offset_hours)
    except Exception:
        dt_utc = dt_raw
    return dt_utc


def get_ohlc_for_setup(
    meta: Dict[str, Any],
    lookback_minutes_before: int = 20,
    max_after_minutes: int = 20,
    mt5_init: bool = True,
) -> Optional[Dict[str, Any]]:
    """
    Fetch ticks for a setup and aggregate into 1m OHLC suitable for Plotly.

    meta should contain keys produced by [`monitor/web_db.py`](monitor/web_db.py:1) `_meta`:
      - symbol, direction, entry_utc_str, entry_price, tp, sl, hit_kind, hit_time_utc_str, hit_price

    Returns:
      dict with keys: times, opens, highs, lows, closes, entry_utc, entry_price, tp, sl, hit_dt, hit_price, symbol
    """
    if meta is None:
        return None
    symbol = meta.get("symbol")
    direction = (meta.get("direction") or "buy").lower()
    mt5_symbol = symbol
    entry_utc = None
    try:
        s = meta.get("entry_utc_str") or ""
        if isinstance(s, str) and s:
            entry_utc = datetime.fromisoformat(s)
            entry_utc = entry_utc.replace(tzinfo=UTC)
    except Exception:
        entry_utc = None
    if entry_utc is None:
        return None

    now_utc = datetime.now(UTC)
    start_utc = entry_utc - timedelta(minutes=lookback_minutes_before)
    hit_dt = None
    hit_price = None
    hit_kind = meta.get("hit_kind")
    try:
        if meta.get("hit_time_utc_str"):
            hit_dt = datetime.fromisoformat(str(meta.get("hit_time_utc_str")))
            try:
                hit_dt = hit_dt.replace(tzinfo=UTC)
            except Exception:
                pass
    except Exception:
        hit_dt = None
    if meta.get("hit_price") is not None:
        try:
            hit_price = float(meta.get("hit_price"))
        except Exception:
            hit_price = None

    # Limit fetch_end to now or hit_dt + max_after_minutes
    fetch_end_utc = now_utc
    if hit_dt is not None and hit_kind in ("TP", "SL"):
        candidate = hit_dt + timedelta(minutes=max_after_minutes, seconds=30)
        if candidate < fetch_end_utc:
            fetch_end_utc = candidate

    # Initialize MT5 if requested
    if mt5_init:
        try:
            _ensure_mt5()
        except Exception:
            pass
    if mt5_symbol and hasattr(mt5_client, "resolve_symbol"):
        try:
            resolved = mt5_client.resolve_symbol(mt5_symbol)
            if resolved:
                mt5_symbol = resolved
        except Exception:
            pass

    # Compute server naive times for mt5 fetch
    try:
        offset_h = mt5_client.get_server_offset_hours(mt5_symbol) if hasattr(mt5_client, "get_server_offset_hours") else 0
    except Exception:
        offset_h = 0

    try:
        start_server = mt5_client.to_server_naive(start_utc, offset_h)
        end_server = mt5_client.to_server_naive(fetch_end_utc, offset_h)
    except Exception:
        start_server = start_utc.replace(tzinfo=None)
        end_server = fetch_end_utc.replace(tzinfo=None)

    # Fetch ticks (paged to avoid MT5 per-call limits). Fall back to UTC-naive range if needed.
    try:
        # Use a large page size to minimize calls but ensure we bypass any copy_ticks_range cap.
        ticks, stats = mt5_client.ticks_paged(
            mt5_symbol,
            start_server,
            end_server,
            page=200000,
            server_offset_hours=offset_h,
        )
    except Exception:
        ticks = []
    # Coerce numpy-like arrays to plain Python list to avoid ambiguous truth-value checks
    if ticks is not None:
        try:
            ticks = list(ticks)
        except Exception:
            pass
    # Fallback: if no ticks, try using UTC-naive range (some brokers interpret naive times as UTC)
    if ticks is None or len(ticks) == 0:
        try:
            start_naive = start_utc.replace(tzinfo=None)
            end_naive = fetch_end_utc.replace(tzinfo=None)
            ticks, _ = mt5_client.ticks_range_all(mt5_symbol, start_naive, end_naive)
            try:
                ticks = list(ticks) if ticks is not None else []
            except Exception:
                pass
        except Exception:
            ticks = []

    if ticks is None or len(ticks) == 0:
        return None

    # Aggregate into 1m OHLC
    minute_data: Dict[datetime, List[float]] = defaultdict(list)
    for tk in ticks:
        price = _get_tick_price(tk, direction)
        if price is None:
            continue
        dt_utc = _get_tick_time_utc(tk, offset_h)
        if dt_utc is None:
            continue
        minute = dt_utc.replace(second=0, microsecond=0)
        minute_data[minute].append(price)

    times: List[datetime] = []
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for minute in sorted(minute_data):
        prices = minute_data[minute]
        if not prices:
            continue
        times.append(minute)
        opens.append(prices[0])
        highs.append(max(prices))
        lows.append(min(prices))
        closes.append(prices[-1])

    if not times:
        return None

    # Trim arrays to include at most max_after_minutes after hit if hit present
    if hit_dt is not None and hit_kind in ("TP", "SL") and times:
        cutoff = hit_dt + timedelta(minutes=max_after_minutes, seconds=30)
        end_idx = 0
        for i, t in enumerate(times):
            if t <= cutoff:
                end_idx = i
            else:
                break
        times = times[: end_idx + 1]
        opens = opens[: end_idx + 1]
        highs = highs[: end_idx + 1]
        lows = lows[: end_idx + 1]
        closes = closes[: end_idx + 1]

    return {
        "symbol": symbol,
        "mt5_symbol": mt5_symbol,
        "times": times,
        "opens": opens,
        "highs": highs,
        "lows": lows,
        "closes": closes,
        "entry_utc": entry_utc,
        "entry_price": float(meta.get("entry_price")) if meta.get("entry_price") is not None else None,
        "tp": float(meta.get("tp")) if meta.get("tp") is not None else None,
        "sl": float(meta.get("sl")) if meta.get("sl") is not None else None,
        "hit_dt": hit_dt,
        "hit_price": hit_price,
        "direction": direction,
    }


def candlestick_figure_from_ohlc(ohlc: Dict[str, Any], title: Optional[str] = None) -> go.Figure:
    """Build a Plotly candlestick figure with overlays from aggregated OHLC dict (see get_ohlc_for_setup)."""
    if not ohlc or not ohlc.get("times"):
        fig = go.Figure()
        fig.update_layout(title=title or "No data")
        return fig

    times = ohlc["times"]
    opens = ohlc["opens"]
    highs = ohlc["highs"]
    lows = ohlc["lows"]
    closes = ohlc["closes"]
    entry_utc = ohlc.get("entry_utc")
    entry_price = ohlc.get("entry_price")
    tp = ohlc.get("tp")
    sl = ohlc.get("sl")
    hit_dt = ohlc.get("hit_dt")
    hit_price = ohlc.get("hit_price")
    symbol = ohlc.get("symbol") or ohlc.get("mt5_symbol", "")

    # Convert times to display timezone (UTC+3) for plotting
    times_disp = []
    for t in times:
        try:
            times_disp.append(t.astimezone(DISPLAY_TZ))
        except Exception:
            times_disp.append(t + timedelta(hours=3))

    # Convert overlay timestamps to display timezone
    entry_utc_disp = None
    if entry_utc is not None:
        try:
            entry_utc_disp = entry_utc.astimezone(DISPLAY_TZ)
        except Exception:
            entry_utc_disp = entry_utc + timedelta(hours=3)
    hit_dt_disp = None
    if hit_dt is not None:
        try:
            hit_dt_disp = hit_dt.astimezone(DISPLAY_TZ)
        except Exception:
            hit_dt_disp = hit_dt + timedelta(hours=3)

    # Candlestick using display times
    fig = go.Figure(data=[go.Candlestick(x=times_disp, open=opens, high=highs, low=lows, close=closes, name="OHLC")])

    # Add entry annotation (arrow) if available
    if entry_utc_disp is not None and entry_price is not None:
        # find a point to place arrow body (a time slightly to the right)
        next_time = None
        for t in times_disp:
            if t > entry_utc_disp:
                next_time = t
                break
        if next_time is None:
            next_time = entry_utc_disp + timedelta(minutes=3)
        try:
            fig.add_annotation(
                x=entry_utc_disp,
                y=entry_price,
                ax=next_time,
                ay=entry_price,
                xref="x",
                yref="y",
                axref="x",
                ayref="y",
                showarrow=True,
                arrowhead=3,
                arrowsize=1,
                arrowwidth=1.2,
                arrowcolor="blue",
                standoff=2,
                startstandoff=2,
            )
            # Add a visible entry marker (in addition to annotation)
            fig.add_trace(
                go.Scatter(
                    x=[entry_utc_disp],
                    y=[entry_price],
                    mode="markers",
                    marker=dict(symbol="diamond", color="blue", size=8),
                    name="Entry",
                )
            )
        except Exception:
            pass

    # Ensure x-range can include hit by adding an invisible "extent" point
    try:
        if hit_dt_disp is not None:
            y_ext = None
            for v in (hit_price, entry_price, tp, sl, (max(highs) if highs else None)):
                if v is not None:
                    y_ext = float(v)
                    break
            if y_ext is None and lows:
                y_ext = float(min(lows))
            if y_ext is not None:
                fig.add_trace(
                    go.Scatter(
                        x=[hit_dt_disp],
                        y=[y_ext],
                        mode="markers",
                        opacity=0.0,
                        marker=dict(size=0.1, color="rgba(0,0,0,0)"),
                        showlegend=False,
                        hoverinfo="skip",
                        name="_extent",
                    )
                )
    except Exception:
        pass

    # SL / TP / Entry horizontal lines (values are in price space, independent of timezone)
    shapes = []
    y_min = min(lows) if lows else None
    y_max = max(highs) if highs else None
    x0 = times_disp[0] if times_disp else None
    x1 = times_disp[-1] if times_disp else None
    if sl is not None and x0 is not None and x1 is not None:
        shapes.append(dict(type="line", x0=x0, x1=x1, y0=sl, y1=sl, yref="y", line=dict(color="red", width=1.0, dash="dash")))
    if tp is not None and x0 is not None and x1 is not None:
        shapes.append(dict(type="line", x0=x0, x1=x1, y0=tp, y1=tp, yref="y", line=dict(color="green", width=1.0, dash="dash")))
    # Entry horizontal price line to make entry clearly visible
    if entry_price is not None and x0 is not None and x1 is not None:
        shapes.append(dict(type="line", x0=x0, x1=x1, y0=entry_price, y1=entry_price, yref="y", line=dict(color="blue", width=1.0, dash="dot")))

    fig.update_layout(shapes=shapes)

    # Hit marker
    if hit_dt_disp is not None and hit_price is not None:
        try:
            color = "skyblue"
            fig.add_trace(go.Scatter(x=[hit_dt_disp], y=[hit_price], mode="markers", marker=dict(size=10, color=color), name="Hit"))
        except Exception:
            pass

    # Expand plot ranges to include entry/hit/TP/SL/entry_price if they sit outside OHLC bounds
    try:
        # Include overlay Y values in y-range computation
        candidates_y = []
        for v in (entry_price, tp, sl, hit_price):
            try:
                if v is not None:
                    candidates_y.append(float(v))
            except Exception:
                pass
        if candidates_y:
            try:
                y_min = min([y_min] + candidates_y) if y_min is not None else min(candidates_y)
                y_max = max([y_max] + candidates_y) if y_max is not None else max(candidates_y)
            except Exception:
                pass

        # Include entry/hit times in x-range computation (display tz)
        x_min = x0
        x_max = x1
        try:
            if entry_utc_disp is not None:
                x_min = entry_utc_disp if x_min is None else min(x_min, entry_utc_disp)
                x_max = entry_utc_disp if x_max is None else max(x_max, entry_utc_disp)
        except Exception:
            pass
        try:
            if hit_dt_disp is not None:
                x_min = hit_dt_disp if x_min is None else min(x_min, hit_dt_disp)
                x_max = hit_dt_disp if x_max is None else max(x_max, hit_dt_disp)
        except Exception:
            pass

        if title is None:
            time_suffix = ""
            if entry_utc_disp is not None:
                formatted = entry_utc_disp.strftime("%Y-%m-%d %H:%M:%S")
                time_suffix = f" | inserted {formatted} {_tz_label(entry_utc_disp)}"
            title = f"{symbol} | 1m{time_suffix}" if symbol else f"1m{time_suffix}"

        fig.update_layout(
            title=title,
            xaxis_title="Time (UTC+3)",
            yaxis_title="Price",
            legend=dict(orientation="h"),
            xaxis=dict(rangeslider=dict(visible=False))
        )
        if x_min is not None and x_max is not None:
            pad = timedelta(minutes=5)
            fig.update_xaxes(range=[x_min - pad, x_max + pad], autorange=False)
        if y_min is not None and y_max is not None and y_max > y_min:
            pad_y = (y_max - y_min) * 0.05
            fig.update_yaxes(range=[y_min - pad_y, y_max + pad_y])
    except Exception:
        pass

    return fig


# --- PnL charting helpers ---


def _classify_symbol(sym: str) -> str:
    s = (sym or "").upper()
    crypto_tickers = [
        "BTC",
        "ETH",
        "XRP",
        "ADA",
        "SOL",
        "DOGE",
        "BNB",
        "DOT",
        "AVAX",
        "LINK",
        "LNK",
        "LTC",
        "BCH",
        "XLM",
        "TRX",
        "ETC",
        "UNI",
        "ATOM",
        "APT",
        "SHIB",
        "PEPE",
    ]
    if any(t in s for t in crypto_tickers):
        return "crypto"
    index_keys = [
        "US30",
        "US100",
        "US500",
        "SP500",
        "SPX",
        "NDX",
        "NAS100",
        "USTEC",
        "DAX",
        "DE30",
        "FTSE",
        "JP225",
    ]
    if any(k in s for k in index_keys):
        return "indices"
    # Forex heuristics
    iso_ccy = {
        "USD",
        "EUR",
        "JPY",
        "GBP",
        "AUD",
        "NZD",
        "CAD",
        "CHF",
        "NOK",
        "SEK",
        "DKK",
        "ZAR",
        "TRY",
        "MXN",
        "PLN",
        "CZK",
        "HUF",
        "CNH",
        "CNY",
        "HKD",
        "SGD",
    }
    def is_pair(x: str) -> bool:
        if len(x) >= 6:
            a = x[:3]; b = x[3:6]
            if (a in iso_ccy or a in {"XAU","XAG","XPT","XPD"}) and (b in iso_ccy):
                return True
        return False
    if is_pair(s):
        return "forex"
    if s.endswith("USD") and any(t in s for t in crypto_tickers):
        return "crypto"
    if any(ch.isdigit() for ch in s):
        return "indices"
    return "forex"


def pnl_category_figure(times: List[datetime], returns: List[float], cum: List[float], avg: List[float], symbols: List[str], title: str) -> go.Figure:
    """Create a PnL figure with smooth curved lines (spline) and markers for wins/losses."""
    fig = go.Figure()
    if not times:
        fig.update_layout(title=title)
        return fig
    # Convert times to display tz for labels (preserve datetimes for x axis)
    times_disp = []
    for t in times:
        try:
            times_disp.append(t.astimezone(DISPLAY_TZ))
        except Exception:
            times_disp.append(t + timedelta(hours=3))

    # Curved lines (spline) — plot directly without step-leading baseline
    fig.add_trace(
        go.Scatter(
            x=times_disp,
            y=cum,
            mode="lines",
            line=dict(shape="spline", smoothing=1.05, color="#1f77b4", width=2),
            name="Cumulative",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=times_disp,
            y=avg if avg else [c / (i + 1) for i, c in enumerate(cum)],
            mode="lines",
            line=dict(shape="spline", smoothing=1.0, color="#ff7f0e", width=1.5, dash="dash"),
            name="Avg per trade",
        )
    )

    # markers for wins/losses at end of each trade
    wins_x = [times_disp[i] for i, v in enumerate(returns) if v > 0]
    wins_y = [cum[i] for i, v in enumerate(returns) if v > 0]
    losses_x = [times_disp[i] for i, v in enumerate(returns) if v < 0]
    losses_y = [cum[i] for i, v in enumerate(returns) if v < 0]
    if wins_x:
        fig.add_trace(
            go.Scatter(
                x=wins_x,
                y=wins_y,
                mode="markers",
                marker=dict(symbol="triangle-up", color="green", size=10),
                name="TP",
            )
        )
    if losses_x:
        fig.add_trace(
            go.Scatter(
                x=losses_x,
                y=losses_y,
                mode="markers",
                marker=dict(symbol="triangle-down", color="red", size=10),
                name="SL",
            )
        )

    fig.update_layout(title=title, xaxis_title="Time (UTC+3)", yaxis_title="PnL ($)", legend=dict(orientation="h"))
    return fig


def pnl_figures_from_series(series: Dict[str, Any]) -> Dict[str, go.Figure]:
    """
    Given a series dict (as returned by monitor.web_db.compute_pnl_series),
    split by category and return figures for forex/crypto/indices.

    Important corrections:
    - Use notional returns for plotting when available to match the "10k" title.
    - Recompute cumulative/average per category; do NOT slice a global cumulative,
      which produces incorrect category curves.
    """
    times: List[datetime] = series.get("times", [])
    symbols: List[str] = series.get("symbols", [])
    norm_returns: List[float] = series.get("norm_returns", [])
    notional_returns: List[float] = series.get("notional_returns", [])

    # Prefer notional returns (10k) when present and aligned; otherwise fall back to normalized/price returns
    use_notional = len(notional_returns) == len(times) and len(times) > 0
    base_returns: List[float] = notional_returns if use_notional else norm_returns

    # Build indices per category
    idxs: Dict[str, List[int]] = {"forex": [], "crypto": [], "indices": []}
    for i, s in enumerate(symbols):
        cls = _classify_symbol(s)
        if cls not in idxs:
            cls = "forex"
        idxs[cls].append(i)

    def build_category(cat: str, title: str) -> go.Figure:
        cat_idxs = idxs.get(cat, [])
        if not cat_idxs:
            return pnl_category_figure([], [], [], [], [], title)

        t_cat = [times[i] for i in cat_idxs]
        r_cat = [base_returns[i] for i in cat_idxs]
        s_cat = [symbols[i] for i in cat_idxs]

        # Recompute cumulative and average per category
        cum_cat: List[float] = []
        total = 0.0
        for v in r_cat:
            total += v
            cum_cat.append(total)
        avg_cat: List[float] = [c / (i + 1) for i, c in enumerate(cum_cat)] if cum_cat else []

        return pnl_category_figure(t_cat, r_cat, cum_cat, avg_cat, s_cat, title)

    fx_fig = build_category("forex", "PnL (10k - Forex)")
    crypto_fig = build_category("crypto", "PnL (10k - Crypto)")
    indices_fig = build_category("indices", "PnL (10k - Indices)")

    return {"forex": fx_fig, "crypto": crypto_fig, "indices": indices_fig}