from __future__ import annotations

import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from .domain import Hit, Setup
from .db import ensure_hits_table_sqlite, load_setups_sqlite, record_hit_sqlite
from .mt5_client import (
    get_server_offset_hours,
    resolve_symbol,
    ticks_range_all,
    to_server_naive,
)
from .redis_ticks import LAST_TICK_KEY_FMT, TICKS_KEY_FMT

UTC = timezone.utc


@dataclass
class SetupFilters:
    since_hours: Optional[int]
    ids: Optional[Sequence[int]]
    symbols: Optional[Sequence[str]]


@dataclass
class TradeCacheEntry:
    setup: Setup
    server_offset_hours: int
    mt5_symbol: str


class RedisRealtimeExitManager:
    """Maintain Redis-backed tick buffers and resolve TP/SL hits in real time."""

    def __init__(
        self,
        *,
        redis_url: str,
        db_path: str,
        filters: SetupFilters,
        tick_window_seconds: int = 10,
        poll_interval_ms: int = 500,
        fallback_interval_s: int = 30,
        dry_run: bool = False,
        verbose: bool = False,
        redis_prefix: str = "monitor",
    ) -> None:
        if redis is None:
            raise RuntimeError("redis package is required for real-time mode")
        self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self._db_path = db_path
        self._filters = filters
        self._tick_window_ms = max(1, tick_window_seconds) * 1000
        self._poll_interval = max(50, poll_interval_ms) / 1000.0
        self._fallback_interval = max(1, fallback_interval_s)
        self._dry_run = dry_run
        self._verbose = verbose
        self._prefix = redis_prefix.rstrip(":")

        self._setups: Dict[int, TradeCacheEntry] = {}
        self._setups_lock = threading.Lock()
        self._mt5_lock = threading.Lock()
        self._running = False
        self._fallback_gate = threading.Event()
        self._fallback_gate.set()
        self._fallback_stop = threading.Event()
        self._last_tick_ms: Dict[str, int] = {}
        self._conn: Optional[sqlite3.Connection] = None

    # Redis key helpers -------------------------------------------------

    def _key_trade(self, setup_id: int) -> str:
        return f"{self._prefix}:trade:{setup_id}"

    def _key_active(self) -> str:
        return f"{self._prefix}:trades:active"

    def _key_ticks(self, symbol: str) -> str:
        return TICKS_KEY_FMT.format(prefix=self._prefix, symbol=symbol)

    def _key_last_tick(self, symbol: str) -> str:
        return LAST_TICK_KEY_FMT.format(prefix=self._prefix, symbol=symbol)

    # Lifecycle ---------------------------------------------------------

    def start(self) -> None:
        self._running = True
        self._fallback_stop.clear()
        self._conn = sqlite3.connect(self._db_path)
        ensure_hits_table_sqlite(self._conn)
        self._bootstrap_trades()

        fallback_thread = threading.Thread(target=self._fallback_loop, daemon=True)
        fallback_thread.start()

        try:
            while self._running:
                loop_start = time.perf_counter()
                if not self._fallback_gate.is_set():
                    # Fallback is rebuilding state; wait briefly
                    time.sleep(self._poll_interval)
                    continue
                try:
                    self._poll_and_evaluate()
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    if self._verbose:
                        print(f"[rt] error during poll/eval: {exc}")
                elapsed = time.perf_counter() - loop_start
                delay = self._poll_interval - elapsed
                if delay > 0:
                    time.sleep(delay)
        finally:
            self._running = False
            self._fallback_stop.set()
            fallback_thread.join(timeout=2.0)
            if self._conn is not None:
                try:
                    self._conn.close()
                except Exception:
                    pass
                self._conn = None

    def stop(self) -> None:
        self._running = False

    # Bootstrap ---------------------------------------------------------

    def _bootstrap_trades(self) -> None:
        setups = self._load_setups()
        if self._verbose:
            print(f"[rt] bootstrap {len(setups)} setup(s)")
        self._sync_trades(setups, replace=True)
        symbols = sorted({entry.mt5_symbol for entry in self._setups.values()})
        self._reseed_ticks(symbols)

    # Core loops --------------------------------------------------------

    def _poll_and_evaluate(self) -> None:
        with self._setups_lock:
            setups_snapshot = dict(self._setups)
        if not setups_snapshot:
            time.sleep(self._poll_interval)
            return
        symbols = {entry.mt5_symbol for entry in setups_snapshot.values()}
        for symbol in symbols:
            self._fetch_incremental_ticks(symbol)
        for entry in setups_snapshot.values():
            self._evaluate_setup(entry)

    # Tick handling -----------------------------------------------------

    def _reseed_ticks(self, symbols: Iterable[str]) -> None:
        now_utc = datetime.now(UTC)
        for symbol in symbols:
            entry = self._entry_for_symbol(symbol)
            offset = entry.server_offset_hours if entry else 0
            start_utc = now_utc - timedelta(milliseconds=self._tick_window_ms)
            start_server = to_server_naive(start_utc, offset)
            end_server = to_server_naive(now_utc, offset)
            with self._mt5_lock:
                ticks, _ = ticks_range_all(symbol, start_server, end_server, trace=self._verbose)
            self._store_ticks(symbol, ticks, replace=True)

    def _fetch_incremental_ticks(self, symbol: str) -> None:
        entry = self._entry_for_symbol(symbol)
        if entry is None:
            return
        offset = entry.server_offset_hours
        last_ms = self._last_tick_ms.get(symbol) or self._read_last_tick_ms(symbol)
        if last_ms is None:
            last_ms = 0
        if last_ms <= 0:
            start_utc = datetime.now(UTC) - timedelta(milliseconds=self._tick_window_ms)
        else:
            start_utc = datetime.fromtimestamp(last_ms / 1000.0, tz=UTC) - timedelta(milliseconds=1)
        end_utc = datetime.now(UTC)
        start_server = to_server_naive(start_utc, offset)
        end_server = to_server_naive(end_utc, offset)
        with self._mt5_lock:
            ticks, _ = ticks_range_all(symbol, start_server, end_server, trace=False)
        self._store_ticks(symbol, ticks, replace=False)

    def _store_ticks(self, symbol: str, ticks: Sequence[object], *, replace: bool) -> None:
        key_ticks = self._key_ticks(symbol)
        key_last = self._key_last_tick(symbol)
        if ticks is None:
            if replace:
                self._redis.delete(key_ticks)
                self._redis.delete(key_last)
                self._last_tick_ms.pop(symbol, None)
            return
        tick_list = list(ticks)
        if not tick_list:
            if replace:
                self._redis.delete(key_ticks)
                self._redis.delete(key_last)
                self._last_tick_ms.pop(symbol, None)
            return
        pipe = self._redis.pipeline()
        if replace:
            pipe.delete(key_ticks)
        max_ts = self._last_tick_ms.get(symbol, 0)
        added = 0
        for obj in tick_list:
            tick_payload = self._extract_tick(obj)
            if tick_payload is None:
                continue
            t_ms = tick_payload["time_msc"]
            if not replace and t_ms <= max_ts:
                continue
            payload = json.dumps(tick_payload, separators=(",", ":"))
            pipe.zadd(key_ticks, {payload: t_ms}, nx=True)
            if t_ms > max_ts:
                max_ts = t_ms
            added += 1
        if added:
            trim_before = max(0, max_ts - self._tick_window_ms)
            pipe.zremrangebyscore(key_ticks, 0, trim_before)
            pipe.set(key_last, max_ts)
            self._last_tick_ms[symbol] = max_ts
        pipe.execute()

    def _extract_tick(self, tick: object) -> Optional[Dict[str, float]]:
        try:
            bid = getattr(tick, "bid", None)
            if bid is None and isinstance(tick, dict):
                bid = tick.get("bid")
            ask = getattr(tick, "ask", None)
            if ask is None and isinstance(tick, dict):
                ask = tick.get("ask")
            tms = getattr(tick, "time_msc", None)
            if tms is None and isinstance(tick, dict):
                tms = tick.get("time_msc")
            if tms is None:
                tse = getattr(tick, "time", None)
                if tse is None and isinstance(tick, dict):
                    tse = tick.get("time")
                if tse is None:
                    return None
                tms = int(float(tse) * 1000.0)
            else:
                tms = int(tms)
            payload: Dict[str, float] = {"time_msc": int(tms)}
            if bid is not None:
                payload["bid"] = float(bid)
            if ask is not None:
                payload["ask"] = float(ask)
            return payload
        except Exception:
            return None

    def _read_last_tick_ms(self, symbol: str) -> Optional[int]:
        value = self._redis.get(self._key_last_tick(symbol))
        if value is None:
            return None
        try:
            ms = int(float(value))
        except Exception:
            return None
        self._last_tick_ms[symbol] = ms
        return ms

    # Evaluation --------------------------------------------------------

    def _evaluate_setup(self, entry: TradeCacheEntry) -> None:
        key = self._key_trade(entry.setup.id)
        data = self._redis.hgetall(key)
        if not data:
            return
        symbol = data.get("mt5_symbol") or entry.mt5_symbol
        ticks_key = self._key_ticks(symbol)
        entry_ts = self._parse_int(data.get("entry_ts")) or int(entry.setup.as_of_utc.timestamp() * 1000)
        last_eval = self._parse_int(data.get("last_eval_ts")) or 0
        tick = self._redis.zrevrangebyscore(ticks_key, "+inf", entry_ts, start=0, num=1, withscores=True)
        if not tick:
            return
        payload_str, tick_ms = tick[0]
        tick_ms = int(tick_ms)
        if tick_ms <= last_eval:
            return
        try:
            payload = json.loads(payload_str)
        except Exception:
            payload = {}
        bid = payload.get("bid")
        ask = payload.get("ask")
        direction = (data.get("direction") or entry.setup.direction).lower()
        hit: Optional[Hit] = None
        if direction == "buy":
            if bid is not None and bid <= entry.setup.sl:
                hit = self._make_hit("SL", tick_ms, entry.server_offset_hours, bid)
            elif bid is not None and bid >= entry.setup.tp:
                hit = self._make_hit("TP", tick_ms, entry.server_offset_hours, bid)
        else:
            if ask is not None and ask >= entry.setup.sl:
                hit = self._make_hit("SL", tick_ms, entry.server_offset_hours, ask)
            elif ask is not None and ask <= entry.setup.tp:
                hit = self._make_hit("TP", tick_ms, entry.server_offset_hours, ask)
        self._redis.hset(key, mapping={"last_eval_ts": tick_ms})
        if hit is None:
            return
        if self._conn is None:
            raise RuntimeError("database connection not initialized")
        record_hit_sqlite(self._conn, entry.setup, hit, self._dry_run, self._verbose)
        self._redis.srem(self._key_active(), entry.setup.id)
        self._redis.delete(key)
        with self._setups_lock:
            self._setups.pop(entry.setup.id, None)
        if self._verbose:
            print(f"[rt] recorded {hit.kind} for setup #{entry.setup.id}")

    def _make_hit(self, kind: str, tick_ms: int, offset_hours: int, price: float) -> Hit:
        dt_raw = datetime.fromtimestamp(tick_ms / 1000.0, tz=UTC)
        dt_utc = dt_raw - timedelta(hours=offset_hours)
        return Hit(kind=kind, time_utc=dt_utc, price=price)

    # Fallback ----------------------------------------------------------

    def _fallback_loop(self) -> None:
        while not self._fallback_stop.wait(self._fallback_interval):
            if not self._running:
                break
            self._fallback_gate.clear()
            try:
                setups = self._load_setups()
                self._sync_trades(setups, replace=False)
                symbols = sorted({entry.mt5_symbol for entry in self._setups.values()})
                self._reseed_ticks(symbols)
            except Exception as exc:
                if self._verbose:
                    print(f"[rt] fallback error: {exc}")
            finally:
                self._fallback_gate.set()

    # Trade sync --------------------------------------------------------

    def _load_setups(self) -> List[Setup]:
        conn = sqlite3.connect(self._db_path)
        try:
            ensure_hits_table_sqlite(conn)
            setups = load_setups_sqlite(
                conn,
                "timelapse_setups",
                self._filters.since_hours,
                self._filters.ids,
                self._filters.symbols,
            )
            return setups
        finally:
            conn.close()

    def _sync_trades(self, setups: Sequence[Setup], *, replace: bool) -> None:
        existing_ids = {setup_id for setup_id in self._redis.smembers(self._key_active())}
        new_ids = {str(setup.id) for setup in setups}
        to_remove = existing_ids - new_ids
        pipe = self._redis.pipeline()
        for setup_id in to_remove:
            key = self._key_trade(int(setup_id))
            pipe.delete(key)
            pipe.srem(self._key_active(), setup_id)
        prepared: List[Tuple[Setup, str, int]] = []
        for setup in setups:
            with self._mt5_lock:
                resolved = resolve_symbol(setup.symbol)
                mt5_symbol = resolved or setup.symbol
                offset = get_server_offset_hours(mt5_symbol)
            prepared.append((setup, mt5_symbol, offset))

        for setup, mt5_symbol, _ in prepared:
            key = self._key_trade(setup.id)
            last_eval = None
            if str(setup.id) in existing_ids:
                try:
                    last_eval = self._redis.hget(key, "last_eval_ts")
                except Exception:
                    last_eval = None
            mapping = {
                "id": setup.id,
                "symbol": setup.symbol,
                "mt5_symbol": mt5_symbol,
                "direction": setup.direction.lower(),
                "sl": setup.sl,
                "tp": setup.tp,
                "entry_ts": int(setup.as_of_utc.timestamp() * 1000),
                "entry_iso": setup.as_of_utc.isoformat(),
                "entry_price": setup.entry_price if setup.entry_price is not None else "",
                "last_eval_ts": int(last_eval) if last_eval is not None else 0,
            }
            pipe.hset(key, mapping={k: self._encode_redis_value(v) for k, v in mapping.items()})
            pipe.sadd(self._key_active(), setup.id)
        pipe.execute()
        with self._setups_lock:
            self._setups.clear()
            for setup, mt5_symbol, offset in prepared:
                self._setups[setup.id] = TradeCacheEntry(
                    setup=setup,
                    server_offset_hours=offset,
                    mt5_symbol=mt5_symbol,
                )
        if replace:
            self._last_tick_ms.clear()

    # Helpers -----------------------------------------------------------

    def _parse_int(self, value: Optional[str]) -> Optional[int]:
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    def _entry_for_symbol(self, symbol: str) -> Optional[TradeCacheEntry]:
        with self._setups_lock:
            for entry in self._setups.values():
                if entry.mt5_symbol == symbol:
                    return entry
        return None

    def _encode_redis_value(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)
