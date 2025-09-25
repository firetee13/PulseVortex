#!/usr/bin/env python3
"""Tick-level backtest harness driven by CSV data and MT5 context.

This tool mirrors the entry filtering logic from timelapse_setups and the
resolution rules from realtime_check_tp_sl_hits while sourcing tick prices from
an offline CSV. Use it to replay historical ticks against the live MT5 terminal
for higher-timeframe context (4H/1D/1W pivots, ATR, etc.).
"""

from __future__ import annotations

import argparse
import atexit
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

from timelapse_setups import (
    Snapshot,
    canonicalize_key,
    normalize_spread_pct,
    spread_class,
    HEADER_SYMBOL,
    INPUT_TZ,
    UTC,
    UTC3,
    _atr,
    analyze,
)
from monitor import mt5_client

# Constants -----------------------------------------------------------------
CSV_TIMEZONE = timezone(timedelta(hours=3))  # Ticks stored as UTC+3
DEFAULT_NOTIONAL = 10_000.0
DEFAULT_MIN_PROX_SL = 0.33
DEFAULT_MAX_PROX_SL = 0.49

# Data models ----------------------------------------------------------------
@dataclass
class Trade:
    symbol: str
    direction: str
    entry_time: datetime  # UTC
    entry_price: float
    sl: float
    tp: float
    quantity: float
    explanation: str
    as_of: datetime  # UTC naive converted to aware
    exit_time: Optional[datetime] = None  # UTC
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None  # 'TP' or 'SL'
    pnl: float = 0.0
    unrealized: float = 0.0


@dataclass
class TimeframeState:
    name: str
    seconds: int
    bars: List[Dict[str, float]] = field(default_factory=list)
    current_bar: Optional[Dict[str, float]] = None


# Helper utilities -----------------------------------------------------------
def _coerce_float(value: object) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            if isinstance(value, float) and np.isnan(value):
                return None
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _percent_change(bars: List[Dict[str, float]], idx: int) -> Optional[float]:
    if idx <= 0 or idx >= len(bars):
        return None
    prev_close = bars[idx - 1]["close"]
    curr_close = bars[idx]["close"]
    if prev_close is None or prev_close == 0:
        return None
    try:
        return ((curr_close - prev_close) / prev_close) * 100.0
    except Exception:
        return None


def _previous_percent_change(bars: List[Dict[str, float]], idx: int) -> Optional[float]:
    # Percent change for the prior bar (idx-1 vs idx-2)
    prev_idx = idx - 1
    if prev_idx <= 0:
        return None
    return _percent_change(bars, prev_idx)


def _pivots_from_previous_bar(bars: List[Dict[str, float]], idx: int) -> Tuple[Optional[float], Optional[float]]:
    if idx <= 0 or idx >= len(bars):
        return (None, None)
    prev = bars[idx - 1]
    try:
        h = float(prev["high"])
        l = float(prev["low"])
        c = float(prev["close"])
        p = (h + l + c) / 3.0
        s1 = 2.0 * p - h
        r1 = 2.0 * p - l
        return (s1, r1)
    except Exception:
        return (None, None)


def _compute_atr_and_pct(d1_bars: List[Dict[str, float]], idx: int) -> Tuple[Optional[float], Optional[float]]:
    # Need at least 15 bars (current + previous 14)
    if idx < 14 or idx >= len(d1_bars):
        return (None, None)
    window = d1_bars[idx - 14 : idx + 1]
    try:
        values = [(float(b["high"]), float(b["low"]), float(b["close"])) for b in window]
    except Exception:
        return (None, None)
    atr = _atr(values, 14)
    if atr is None:
        return (None, None)
    d1_close = window[-1]["close"]
    try:
        atrp = (atr / d1_close) * 100.0 if d1_close else None
    except Exception:
        atrp = None
    return atr, atrp


def _build_row(symbol: str, data: Dict[str, object]) -> Dict[str, object]:
    row: Dict[str, object] = {}
    for key, value in data.items():
        row[canonicalize_key(key)] = value
    row[HEADER_SYMBOL] = symbol
    return row


def _datetime_from_csv(date_str: str, time_str: str) -> datetime:
    date_str = str(date_str).strip()
    time_str = str(time_str).strip()
    fmt_variants = ["%Y.%m.%d %H:%M:%S.%f", "%Y.%m.%d %H:%M:%S"]
    combined = f"{date_str} {time_str}"
    for fmt in fmt_variants:
        try:
            dt = datetime.strptime(combined, fmt)
            return dt.replace(tzinfo=CSV_TIMEZONE)
        except ValueError:
            continue
    raise ValueError(f"Unable to parse timestamp: {combined}")


def _mt5_to_list(rates: Optional[Iterable[object]]) -> List[Dict[str, float]]:
    if rates is None:
        return []
    out: List[Dict[str, float]] = []
    for row in rates:
        try:
            time_val = getattr(row, "time", None)
            if time_val is None:
                time_val = row["time"]  # type: ignore[index]
            open_val = getattr(row, "open", None)
            if open_val is None:
                open_val = row["open"]  # type: ignore[index]
            high_val = getattr(row, "high", None)
            if high_val is None:
                high_val = row["high"]  # type: ignore[index]
            low_val = getattr(row, "low", None)
            if low_val is None:
                low_val = row["low"]  # type: ignore[index]
            close_val = getattr(row, "close", None)
            if close_val is None:
                close_val = row["close"]  # type: ignore[index]
        except Exception:
            continue
        out.append(
            {
                "time": float(time_val),
                "open": float(open_val),
                "high": float(high_val),
                "low": float(low_val),
                "close": float(close_val),
            }
        )
    return sorted(out, key=lambda r: r["time"])


# Backtest engine ------------------------------------------------------------
class BacktestEngine:
    def __init__(
        self,
        symbol: str,
        csv_path: Optional[str],
        download_ticks: bool,
        start_date: Optional[str],
        end_date: Optional[str],
        min_prox_sl: float,
        max_prox_sl: float,
        notional: float,
        plot_path: str,
        verbose: bool,
        mt5_timeout: int,
        mt5_retries: int,
        mt5_portable: bool,
        mt5_path: Optional[str],
    ) -> None:
        self.symbol = symbol
        self.csv_path = csv_path
        self.download_ticks = download_ticks
        self.start_date = start_date
        self.end_date = end_date
        self.min_prox_sl = min_prox_sl
        self.max_prox_sl = max_prox_sl
        self.notional = notional
        self.plot_path = plot_path
        self.verbose = verbose
        self.mt5_timeout = mt5_timeout
        self.mt5_retries = mt5_retries
        self.mt5_portable = mt5_portable
        self.mt5_path = mt5_path

        self.ticks: pd.DataFrame = pd.DataFrame()
        self.timeframes: Dict[str, TimeframeState] = {}
        self.current_trade: Optional[Trade] = None
        self.closed_trades: List[Trade] = []
        self.realized_pnl: float = 0.0
        self.equity_curve: List[Tuple[datetime, float]] = []
        self.price_series: List[Tuple[datetime, Optional[float]]] = []
        self.seen_signals: Dict[Tuple[str, float, float], datetime] = {}

    # ----- Lifecycle -----------------------------------------------------
    def run(self) -> None:
        self._ensure_mt5()
        try:
            self._load_ticks()
            if self.ticks.empty:
                raise RuntimeError("No tick data loaded from CSV")
            self._prime_timeframes()
            self._simulate()
            self._render_plot()
        finally:
            mt5_client.shutdown_mt5()

    # ----- MT5 -----------------------------------------------------------
    def _ensure_mt5(self) -> None:
        if not mt5_client.has_mt5():
            raise RuntimeError("MetaTrader5 package is not available; install requirements first")
        init_kwargs = {
            "path": self.mt5_path,
            "timeout": self.mt5_timeout,
            "retries": self.mt5_retries,
            "portable": self.mt5_portable,
            "verbose": self.verbose,
        }
        mt5_client.init_mt5(**init_kwargs)
        atexit.register(mt5_client.shutdown_mt5)

    # ----- Data ingestion ------------------------------------------------
    def _load_ticks(self) -> None:
        if self.download_ticks:
            self._download_ticks_from_mt5()
        else:
            self._load_ticks_from_csv()

    def _load_ticks_from_csv(self) -> None:
        if not self.csv_path:
            raise RuntimeError("CSV path is required for CSV loading")

        if self.verbose:
            print(f"Loading CSV: {self.csv_path}")

        chunksize = 1_000_000
        chunks = []
        total_rows = 0
        if tqdm is not None:
            pbar = tqdm(desc="Loading CSV chunks", unit="rows")
        else:
            pbar = None
        usecols = ['<DATE>', '<TIME>', '<BID>', '<ASK>']
        dtype_spec = {'<BID>': 'float32', '<ASK>': 'float32', '<DATE>': 'string', '<TIME>': 'string'}
        for chunk in pd.read_csv(self.csv_path, sep="\t", engine="c", header=0, usecols=usecols, dtype=dtype_spec, chunksize=chunksize):
            chunks.append(chunk)
            total_rows += len(chunk)
            if pbar is not None:
                pbar.update(len(chunk))
        if pbar is not None:
            pbar.close()
        df = pd.concat(chunks, ignore_index=True)
        if self.verbose:
            print(f"Concatenated {len(chunks)} chunks into DataFrame with {len(df)} rows")

        # Rename columns to lowercase
        df.columns = ['date', 'time', 'bid', 'ask']

        # Vectorized datetime parsing (much faster than Python loops)
        df['datetime_str'] = df['date'] + ' ' + df['time']
        df['timestamp'] = pd.to_datetime(df['datetime_str'], format='%Y.%m.%d %H:%M:%S.%f', utc=True, errors='coerce')
        df['timestamp'] = df['timestamp'].dt.tz_convert('Europe/Moscow')  # CSV timezone (UTC+3)

        # Filter out invalid timestamps and missing bid/ask in one operation
        valid_mask = df['timestamp'].notna() & df['bid'].notna() & df['ask'].notna()
        df = df[valid_mask].reset_index(drop=True)

        if df.empty:
            raise RuntimeError("No valid data found in CSV after filtering")

        # Convert timezones using vectorized operations
        df['timestamp_utc'] = df['timestamp'].dt.tz_convert('UTC')
        df['timestamp_input'] = df['timestamp_utc'].dt.tz_convert(str(INPUT_TZ))

        # Memory optimization: use float32 for bid/ask
        df['bid'] = df['bid'].astype('float32')
        df['ask'] = df['ask'].astype('float32')

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        self.ticks = df

        # Precompute tick counts per minute for fast volume checks (optimized)
        self.tick_counts_per_min = df.groupby(df['timestamp_utc'].dt.floor('min'), sort=False).size()
        if self.verbose:
            first_ts = df["timestamp"].iloc[0]
            last_ts = df["timestamp"].iloc[-1]
            print(f"Loaded {len(df)} ticks from {self.csv_path} spanning {first_ts} to {last_ts}")

    def _download_ticks_from_mt5(self) -> None:
        if self.verbose:
            print(f"Downloading ticks from MT5: {self.symbol} from {self.start_date} to {self.end_date}")

        # Parse dates
        start_dt = datetime.strptime(self.start_date, "%Y-%m-%d").replace(tzinfo=UTC)
        end_dt = datetime.strptime(self.end_date, "%Y-%m-%d").replace(tzinfo=UTC)
        end_dt = end_dt.replace(hour=23, minute=59, second=59)  # Include full end date

        # Get server offset for timezone conversion
        server_offset = mt5_client.get_server_offset_hours(self.symbol)

        # Convert to server-naive datetime for MT5 API
        start_server = mt5_client.to_server_naive(start_dt, server_offset)
        end_server = mt5_client.to_server_naive(end_dt, server_offset)

        # Download ticks from MT5
        ticks, stats = mt5_client.ticks_range_all(
            symbol=self.symbol,
            start_server_naive=start_server,
            end_server_naive=end_server,
            trace=self.verbose
        )

        if self.verbose:
            print(f"Downloaded {len(ticks)} ticks in {stats.elapsed_s:.2f}s")

        if len(ticks) == 0:
            raise RuntimeError(f"No ticks downloaded from MT5 for {self.symbol}")

        # Convert MT5 tick objects to DataFrame
        tick_data = []
        processed_count = 0
        skipped_count = 0

        # Debug: check the first few ticks to understand their structure
        if len(ticks) > 0:
            first_tick = ticks[0]
            if self.verbose:
                print(f"First tick attributes: {dir(first_tick)}")
                print(f"First tick type: {type(first_tick)}")
                # Try to print some key attributes
                for attr in ['bid', 'ask', 'time', 'time_msc', 'last', 'volume']:
                    value = getattr(first_tick, attr, 'NOT_FOUND')
                    print(f"  {attr}: {value}")

        for tick in ticks:
            # Extract bid/ask prices (handle numpy structured arrays)
            bid = None
            ask = None

            # Try different access methods for numpy structured arrays
            try:
                # Method 1: numpy structured array access
                bid = tick['bid'] if 'bid' in tick.dtype.names else None
                ask = tick['ask'] if 'ask' in tick.dtype.names else None
            except (TypeError, ValueError, KeyError):
                try:
                    # Method 2: attribute access
                    bid = getattr(tick, 'bid', None)
                    ask = getattr(tick, 'ask', None)
                except:
                    pass

            # For crypto symbols, might use 'last' instead of bid/ask
            if bid is None and ask is None:
                try:
                    last_price = tick['last'] if 'last' in tick.dtype.names else getattr(tick, 'last', None)
                    if last_price is not None:
                        bid = last_price
                        ask = last_price
                except:
                    pass

            if bid is None and ask is None:
                skipped_count += 1
                continue

            # Extract timestamp (prefer time_msc for millisecond precision)
            tms = None
            try:
                tms = tick['time_msc'] if 'time_msc' in tick.dtype.names else None
            except:
                tms = getattr(tick, 'time_msc', None)

            if tms is not None:
                dt_utc = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
            else:
                try:
                    tse = tick['time'] if 'time' in tick.dtype.names else None
                except:
                    tse = getattr(tick, 'time', None)

                if tse is None:
                    skipped_count += 1
                    continue
                dt_utc = datetime.fromtimestamp(float(tse), tz=UTC)

            # Convert from server time to UTC
            dt_utc = dt_utc - timedelta(hours=server_offset)

            tick_data.append({
                'timestamp': dt_utc,
                'timestamp_utc': dt_utc,
                'timestamp_input': dt_utc.astimezone(INPUT_TZ),
                'bid': float(bid) if bid is not None else None,
                'ask': float(ask) if ask is not None else None,
            })
            processed_count += 1

        if self.verbose:
            print(f"Processed {processed_count} ticks, skipped {skipped_count} ticks")

        if not tick_data:
            raise RuntimeError("No valid tick data extracted from MT5 ticks")

        # Create DataFrame with explicit column order
        df = pd.DataFrame(tick_data, columns=['timestamp', 'timestamp_utc', 'timestamp_input', 'bid', 'ask'])

        # Filter out rows with missing bid/ask
        df = df.dropna(subset=['bid', 'ask']).reset_index(drop=True)

        if df.empty:
            raise RuntimeError("No valid tick data after filtering")

        # Memory optimization
        df['bid'] = df['bid'].astype('float32')
        df['ask'] = df['ask'].astype('float32')

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        self.ticks = df

        # Precompute tick counts per minute for fast volume checks
        self.tick_counts_per_min = df.groupby(df['timestamp_utc'].dt.floor('min'), sort=False).size()

        if self.verbose:
            first_ts = df["timestamp"].iloc[0]
            last_ts = df["timestamp"].iloc[-1]
            print(f"Processed {len(df)} ticks from MT5 spanning {first_ts} to {last_ts}")
    def _prime_timeframes(self) -> None:
        first_tick_utc = self.ticks["timestamp_utc"].iloc[0]
        history_start = first_tick_utc - timedelta(days=30)
        tf_defs = {
            "M15": (getattr(mt5_client.mt5, "TIMEFRAME_M15", 7), 15 * 60),
            "H1": (getattr(mt5_client.mt5, "TIMEFRAME_H1", 8), 60 * 60),
            "H4": (getattr(mt5_client.mt5, "TIMEFRAME_H4", 9), 4 * 60 * 60),
            "D1": (getattr(mt5_client.mt5, "TIMEFRAME_D1", 86400), 24 * 60 * 60),
            "W1": (getattr(mt5_client.mt5, "TIMEFRAME_W1", 7 * 86400), 7 * 24 * 60 * 60),
        }
        first_tick_epoch = int(first_tick_utc.timestamp())
        for name, (tf_id, seconds) in tf_defs.items():
            rates = mt5_client.mt5.copy_rates_range(  # type: ignore[union-attr]
                self.symbol,
                tf_id,
                history_start,
                first_tick_utc,
            )
            bars = _mt5_to_list(rates)
            if self.verbose:
                print(f"Fetched {len(bars)} {name} bars from MT5")
            if not bars:
                raise RuntimeError(f"No {name} bars available for {self.symbol}; cannot continue")

            bar_start = (first_tick_epoch // seconds) * seconds
            historical: List[Dict[str, float]] = []
            current_bar: Optional[Dict[str, float]] = None
            for bar in bars:
                bar_time = int(bar.get("time", 0))
                if bar_time < bar_start:
                    historical.append(bar)
                elif bar_time == bar_start:
                    current_bar = bar
                else:
                    # Bars beyond the first tick timestamp represent future information; ignore.
                    break

            if current_bar is None:
                prev_close = historical[-1]["close"] if historical else None
                if prev_close is None:
                    raise RuntimeError(f"Insufficient historical data to seed {name} timeframe for {self.symbol}")
                current_bar = {
                    "time": float(bar_start),
                    "open": float(prev_close),
                    "high": float(prev_close),
                    "low": float(prev_close),
                    "close": float(prev_close),
                }
            self.timeframes[name] = TimeframeState(
                name=name,
                seconds=seconds,
                bars=historical[-600:],  # keep recent history only
                current_bar=current_bar,
            )

    def _trim_history(self, state: TimeframeState, limit: int = 600) -> None:
        if len(state.bars) > limit:
            state.bars = state.bars[-limit:]

    def _start_new_bar(self, state: TimeframeState, bar_start: int, price: float) -> None:
        state.current_bar = {
            "time": float(bar_start),
            "open": price,
            "high": price,
            "low": price,
            "close": price,
        }

    def _finalize_current_bar(self, state: TimeframeState) -> None:
        if state.current_bar is not None:
            state.bars.append(state.current_bar)
            self._trim_history(state)
            state.current_bar = None

    def _update_timeframes(self, tick_time_utc: datetime, price: Optional[float]) -> None:
        if price is None:
            return
        tick_epoch = int(tick_time_utc.timestamp())
        for state in self.timeframes.values():
            bar_start = (tick_epoch // state.seconds) * state.seconds
            current = state.current_bar
            if current is None:
                self._start_new_bar(state, bar_start, price)
                continue

            current_start = int(current.get("time", bar_start))
            if bar_start == current_start:
                current["high"] = max(current["high"], price)
                current["low"] = min(current["low"], price)
                current["close"] = price
                continue

            if bar_start < current_start:
                # Historical tick out of order; ignore to preserve chronological consistency.
                continue

            # Close current bar and optionally fill gaps if we skipped timeframe intervals.
            prev_close = current["close"]
            self._finalize_current_bar(state)

            next_start = current_start + state.seconds
            while next_start < bar_start:
                filler = {
                    "time": float(next_start),
                    "open": prev_close,
                    "high": prev_close,
                    "low": prev_close,
                    "close": prev_close,
                }
                state.bars.append(filler)
                self._trim_history(state)
                prev_close = filler["close"]
                next_start += state.seconds

            self._start_new_bar(state, bar_start, price)

    # ----- Signal handling & Trade updates -------------------------------
    def _handle_signal(self, signal: dict, entry_time: datetime) -> None:
        # Dedup signals by symbol, sl, tp tuple to avoid reopening on same setup
        signal_key = (
            signal["symbol"],
            round(signal["sl"], 4),  # Precision for float comparison
            round(signal["tp"], 4),
        )
        if signal_key in self.seen_signals:
            # Signal repeated; skip
            return
        self.seen_signals[signal_key] = entry_time

        if self.current_trade is not None:
            # Already in trade; skip or close? For backtest, allow only one at a time
            return

        direction = signal["direction"]
        entry_price = signal["price"]
        sl = signal["sl"]
        tp = signal["tp"]
        quantity = self.notional / entry_price if entry_price != 0 else 0.0  # Simple sizing assuming pip value ~1
        explanation = signal.get("explain", "Generated setup")
        as_of = signal.get("as_of", entry_time)

        self.current_trade = Trade(
            symbol=self.symbol,
            direction=direction,
            entry_time=entry_time,
            entry_price=entry_price,
            sl=sl,
            tp=tp,
            quantity=quantity,
            explanation=explanation,
            as_of=as_of,
            pnl=0.0,
            unrealized=0.0,
        )
        if self.verbose:
            print(f"Opened {direction} for {self.symbol} at {entry_price} -> SL:{sl} TP:{tp}")

    def _update_trade(self, tick_time: datetime, bid: Optional[float], ask: Optional[float]) -> None:
        if self.current_trade is None:
            return

        direction = self.current_trade.direction
        entry_price = self.current_trade.entry_price
        sl = self.current_trade.sl
        tp = self.current_trade.tp
        quantity = self.current_trade.quantity
        hit = None
        exit_price = None
        dir_mult = 1.0 if direction == "Buy" else -1.0

        if direction == "Buy":
            if bid is not None:
                self.current_trade.unrealized = dir_mult * (bid - entry_price) * quantity
                if bid <= sl:
                    hit = "SL"
                    exit_price = bid
                elif bid >= tp:
                    hit = "TP"
                    exit_price = bid
        else:  # Sell
            if ask is not None:
                self.current_trade.unrealized = dir_mult * (entry_price - ask) * quantity
                if ask >= sl:
                    hit = "SL"
                    exit_price = ask
                elif ask <= tp:
                    hit = "TP"
                    exit_price = ask

        if hit is not None:
            pnl_delta = dir_mult * (exit_price - entry_price) * quantity if entry_price and exit_price else 0.0
            self.current_trade.exit_time = tick_time
            self.current_trade.exit_price = exit_price
            self.current_trade.exit_reason = hit
            self.current_trade.pnl = pnl_delta
            self.realized_pnl += pnl_delta
            self.closed_trades.append(self.current_trade)
            if self.verbose:
                print(
                    f"Closed {direction} for {self.symbol} at {tick_time}: {hit} @ {exit_price}, PnL: {pnl_delta:.2f} "
                    f"(total realized: {self.realized_pnl:.2f})"
                )
            self.current_trade = None

    # ----- Simulation & Recording ----------------------------------------
    def _simulate(self) -> None:
        prev_date = None
        total_ticks = len(self.ticks)
        if self.verbose:
            print(f"Starting simulation of {total_ticks} ticks")
        # Optimized iteration using only needed columns
        tick_data = self.ticks[['bid', 'ask', 'timestamp_utc', 'timestamp_input']]
        tick_iter = tqdm(tick_data.itertuples(index=False), desc="Simulating ticks", unit="ticks", total=total_ticks) if tqdm else tick_data.itertuples(index=False)
        for row in tick_iter:
            bid = _coerce_float(row.bid)
            ask = _coerce_float(row.ask)
            if bid is None and ask is None:
                continue
            tick_time_utc: datetime = row.timestamp_utc
            current_date = tick_time_utc.date()
            if self.verbose and prev_date is not None and current_date != prev_date:
                print(f"SIM: Processing new day: {current_date} (UTC)")
            prev_date = current_date
            tick_time_input: datetime = row.timestamp_input
            price_for_bars = None
            if bid is not None and ask is not None:
                price_for_bars = (bid + ask) / 2.0
            elif bid is not None:
                price_for_bars = bid
            elif ask is not None:
                price_for_bars = ask
            self._update_timeframes(tick_time_utc, price_for_bars)
            metrics = self._compute_metrics()
            if metrics is None:
                continue
            # Check historical tick volume for the last 2 bars
            volume_check = self._check_tick_volume_last_2_bars(tick_time_utc)
            if volume_check is False:
                continue
            series = self._build_series(tick_time_input, tick_time_utc, bid, ask, metrics)
            # Temporarily override the volume check to use historical data
            import timelapse_setups
            original_volume_check = timelapse_setups._get_tick_volume_last_2_bars
            timelapse_setups._get_tick_volume_last_2_bars = lambda sym: volume_check
            try:
                results, _ = analyze(
                    series=series,
                    min_rrr=0.0,
                    min_prox_sl=self.min_prox_sl,
                    max_prox_sl=self.max_prox_sl,
                    min_sl_pct=0.0,
                    as_of_ts=tick_time_input,
                    debug=False,
                )
            finally:
                timelapse_setups._get_tick_volume_last_2_bars = original_volume_check
            signal = next((r for r in results if r.get("symbol") == self.symbol), None)
            if signal is not None:
                self._handle_signal(signal, tick_time_utc)
            self._update_trade(tick_time_utc, bid, ask)
            # Record points
            unrealized_current = self.current_trade.unrealized if self.current_trade else 0.0
            self._record_equity_point(tick_time_utc, unrealized_current)
            self._record_price_point(tick_time_utc, bid, ask)

    def _record_equity_point(self, dt: datetime, unrealized: float = 0.0) -> None:
        equity = self.realized_pnl + unrealized
        self.equity_curve.append((dt, equity))

    def _record_price_point(self, dt: datetime, bid: Optional[float], ask: Optional[float]) -> None:
        price = None
        if bid is not None and ask is not None:
            price = (bid + ask) / 2.0
        elif bid is not None:
            price = bid
        elif ask is not None:
            price = ask
        self.price_series.append((dt, price))

    def _build_series(
        self,
        tick_time_input: datetime,
        tick_time_utc: datetime,
        bid: Optional[float],
        ask: Optional[float],
        metrics: Dict[str, Optional[float]],
    ) -> Dict[str, List[Snapshot]]:
        spread_pct = None
        if bid is not None and ask is not None and bid > 0 and ask > 0 and ask > bid:
            mid = (ask + bid) / 2.0
            spread_pct = ((ask - bid) / mid) * 100.0
        # Add missing keys from metrics or None
        d1_high = metrics.get("d1_high")
        d1_low = metrics.get("d1_low")
        m15_close = metrics.get("m15_close")
        h1_close = metrics.get("h1_close")
        r1 = metrics.get("r1")
        s1 = metrics.get("s1")
        d1_close = metrics.get("d1_close")
        first_row = _build_row(
            self.symbol,
            {
                "D1 Close": metrics.get("first_d1_close"),
                "Strength 4H": metrics.get("ss_4h_prev"),
            },
        )
        last_row = _build_row(
            self.symbol,
            {
                "Bid": bid,
                "Ask": ask,
                "Spread%": spread_pct,
                "Backfilled": 1,  # Historical replay treated as backfilled
                "Strength 4H": metrics.get("ss_4h"),
                "Strength 1D": metrics.get("ss_1d"),
                "Strength 1W": metrics.get("ss_1w"),
                "ATR D1": metrics.get("atr_d1"),
                "ATR (%) D1": metrics.get("atr_percent"),
                "S1 Level M5": s1,
                "R1 Level M5": r1,
                "D1 Close": d1_close,
                "D1 High": d1_high,
                "D1 Low": d1_low,
                "M15 Close": m15_close,
                "H1 Close": h1_close,
                "Recent Tick": 1,  # Assume valid for sim
            },
        )
        return {self.symbol: [Snapshot(ts=tick_time_input, row=first_row),
                              Snapshot(ts=tick_time_input, row=last_row)]}

    def _check_tick_volume_last_2_bars(self, tick_time_utc: datetime) -> Optional[bool]:
        """Check tick count for the last 2 completed M1 bars before tick_time_utc using precomputed counts."""
        current_min = tick_time_utc.replace(second=0, microsecond=0)
        prev_min1 = current_min - timedelta(minutes=1)
        prev_min2 = current_min - timedelta(minutes=2)
        # Get counts for the last 2 completed minutes
        count1 = self.tick_counts_per_min.get(prev_min1, 0)
        count2 = self.tick_counts_per_min.get(prev_min2, 0)
        return count1 >= 10 and count2 >= 10

    def _compute_metrics(self) -> Optional[Dict[str, Optional[float]]]:
        d1_state = self.timeframes.get("D1")
        h4_state = self.timeframes.get("H4")
        w1_state = self.timeframes.get("W1")
        h1_state = self.timeframes.get("H1")
        m15_state = self.timeframes.get("M15")
        if not all([d1_state, h4_state, w1_state, h1_state, m15_state]):
            return None

        def _bars_with_current(state: TimeframeState) -> List[Dict[str, float]]:
            bars = list(state.bars)
            if state.current_bar is not None:
                bars.append(state.current_bar)
            return bars

        d1_bars = _bars_with_current(d1_state)
        if not d1_bars:
            return None
        d1_idx = len(d1_bars) - 1
        d1_bar = d1_bars[d1_idx]

        metrics: Dict[str, Optional[float]] = {}
        metrics["d1_close"] = d1_bar.get("close")
        metrics["d1_high"] = d1_bar.get("high")
        metrics["d1_low"] = d1_bar.get("low")
        first_d1_close = d1_bars[d1_idx - 1].get("close") if d1_idx > 0 else d1_bar.get("close")
        metrics["first_d1_close"] = first_d1_close

        s1, r1 = _pivots_from_previous_bar(d1_bars, d1_idx)
        metrics["s1"] = s1
        metrics["r1"] = r1
        metrics["ss_1d"] = _percent_change(d1_bars, d1_idx)

        h4_bars = _bars_with_current(h4_state)
        h4_idx = len(h4_bars) - 1 if h4_bars else None
        metrics["ss_4h"] = _percent_change(h4_bars, h4_idx) if h4_idx is not None else None
        metrics["ss_4h_prev"] = _previous_percent_change(h4_bars, h4_idx) if h4_idx is not None else None

        w1_bars = _bars_with_current(w1_state)
        w1_idx = len(w1_bars) - 1 if w1_bars else None
        metrics["ss_1w"] = _percent_change(w1_bars, w1_idx) if w1_idx is not None else None

        atr, atrp = _compute_atr_and_pct(d1_bars, d1_idx)
        metrics["atr_d1"] = atr
        metrics["atr_percent"] = atrp

        m15_bars = _bars_with_current(m15_state)
        metrics["m15_close"] = m15_bars[-1].get("close") if m15_bars else None

        h1_bars = _bars_with_current(h1_state)
        metrics["h1_close"] = h1_bars[-1].get("close") if h1_bars else None

        return metrics

    def _render_plot(self) -> None:
        if not self.equity_curve and self.verbose:
            print("No points to plot.")
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if self.plot_path:
            plot_file = f"{self.plot_path}_{self.symbol}_{timestamp}.png"
        else:
            plot_file = f"backtest_{self.symbol}_{timestamp}.png"

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        times, equities = zip(*self.equity_curve)
        ax1.plot(times, equities)
        ax1.set_title(f"{self.symbol} Equity Curve")
        ax1.set_ylabel("PnL")
        ax1.grid(True)

        if self.price_series:
            p_data = [(t, p) for t, p in self.price_series if p is not None]
            if p_data:
                pt, ps = zip(*p_data)
                ax2.plot(pt, ps, label="Mid", lw=1)
                ax2.set_title(f"{self.symbol} Price Series")
                ax2.set_ylabel("Price")
                ax2.grid(True)
        else:
            ax2.set_title(f"{self.symbol} (No Price Data)")

        fig.autofmt_xdate()
        plt.tight_layout()
        plt.savefig(plot_file, dpi=100)
        plt.close(fig)

        if self.verbose:
            print(f"Plot saved: {plot_file}")
            print(f"Backtest summary: {len(self.closed_trades)} trades, Final PnL: {self.realized_pnl:.2f}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run tick-level backtest from CSV with MT5 context")
    p.add_argument("--symbol", required=True, help="Trading symbol (e.g., EURUSD)")
    p.add_argument("--csv-path", help="Path to tab-separated tick CSV (mutually exclusive with --download)")
    p.add_argument("--download", action="store_true", help="Download ticks from MT5 instead of reading CSV")
    p.add_argument("--start-date", help="Start date for MT5 download (YYYY-MM-DD format, required with --download)")
    p.add_argument("--end-date", help="End date for MT5 download (YYYY-MM-DD format, required with --download)")
    p.add_argument("--min-prox-sl", type=float, default=DEFAULT_MIN_PROX_SL, help="Min SL proximity fraction (default 0.33)")
    p.add_argument("--max-prox-sl", type=float, default=DEFAULT_MAX_PROX_SL, help="Max SL proximity fraction (default 0.49)")
    p.add_argument("--notional", type=float, default=DEFAULT_NOTIONAL, help="Notional amount for position sizing (default 10000)")
    p.add_argument("--plot-prefix", default="", help="Prefix for plot file names (default empty)")
    p.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    # MT5 options
    p.add_argument("--mt5-timeout", type=int, default=30, help="MT5 initialization timeout (default 30s)")
    p.add_argument("--mt5-retries", type=int, default=3, help="MT5 init retries (default 3)")
    p.add_argument("--mt5-portable", action="store_true", help="MT5 portable mode")
    p.add_argument("--mt5-path", type=str, default=None, help="Custom path to terminal64.exe")
    return p.parse_args()


def validate_args(args):
    """Validate command-line arguments."""
    if args.download:
        if not args.start_date or not args.end_date:
            raise ValueError("--start-date and --end-date are required when using --download")
        if args.csv_path:
            raise ValueError("--csv-path and --download are mutually exclusive")
        # Validate date format
        try:
            datetime.strptime(args.start_date, "%Y-%m-%d")
            datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYY-MM-DD: {e}")
    else:
        if not args.csv_path:
            raise ValueError("Either --csv-path or --download must be specified")


if __name__ == "__main__":
    args = parse_args()
    validate_args(args)

    engine = BacktestEngine(
        symbol=args.symbol,
        csv_path=getattr(args, 'csv_path', None),
        download_ticks=args.download,
        start_date=getattr(args, 'start_date', None),
        end_date=getattr(args, 'end_date', None),
        min_prox_sl=args.min_prox_sl,
        max_prox_sl=args.max_prox_sl,
        notional=args.notional,
        plot_path=args.plot_prefix,
        verbose=args.verbose,
        mt5_timeout=args.mt5_timeout,
        mt5_retries=args.mt5_retries,
        mt5_portable=args.mt5_portable,
        mt5_path=args.mt5_path,
    )
    engine.run()
