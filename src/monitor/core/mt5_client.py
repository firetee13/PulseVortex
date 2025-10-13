from __future__ import annotations

import glob
import os
import time
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .domain import Hit, TickFetchStats

try:
    import MetaTrader5 as mt5
except Exception:  # pragma: no cover
    mt5 = None

UTC = timezone.utc


def _coerce_price(value: Any) -> Optional[float]:
    """Best-effort float conversion that ignores missing values."""
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


_TIMEFRAME_SECOND_MAP: Dict[int, int] = {}


def _register_timeframe_seconds() -> None:
    if mt5 is None:
        return
    mapping = [
        ("TIMEFRAME_M1", 60),
        ("TIMEFRAME_M2", 120),
        ("TIMEFRAME_M3", 180),
        ("TIMEFRAME_M4", 240),
        ("TIMEFRAME_M5", 300),
        ("TIMEFRAME_M6", 360),
        ("TIMEFRAME_M10", 600),
        ("TIMEFRAME_M12", 720),
        ("TIMEFRAME_M15", 900),
        ("TIMEFRAME_M20", 1200),
        ("TIMEFRAME_M30", 1800),
        ("TIMEFRAME_H1", 3600),
        ("TIMEFRAME_H2", 7200),
        ("TIMEFRAME_H3", 10800),
        ("TIMEFRAME_H4", 14400),
        ("TIMEFRAME_H6", 21600),
        ("TIMEFRAME_H8", 28800),
        ("TIMEFRAME_H12", 43200),
        ("TIMEFRAME_D1", 86400),
        ("TIMEFRAME_W1", 604800),
        ("TIMEFRAME_MN1", 2592000),
    ]
    for name, seconds in mapping:
        try:
            value = getattr(mt5, name)
        except Exception:
            value = None
        if value is None:
            continue
        try:
            _TIMEFRAME_SECOND_MAP[int(value)] = int(seconds)
        except Exception:
            continue


_register_timeframe_seconds()


def has_mt5() -> bool:
    return mt5 is not None


def timeframe_m1() -> int:
    if mt5 is None:
        return 1
    try:
        return int(getattr(mt5, "TIMEFRAME_M1"))
    except Exception:
        return 1


def timeframe_seconds(timeframe: int) -> int:
    try:
        key = int(timeframe)
    except Exception:
        return 60
    return _TIMEFRAME_SECOND_MAP.get(key, 60)


def timeframe_from_code(code: str) -> Optional[int]:
    if not code:
        return None
    name = f"TIMEFRAME_{code.strip().upper()}"
    if mt5 is None:
        return None
    try:
        value = getattr(mt5, name)
    except Exception:
        value = None
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _candidate_terminal_paths(
    user_hint: Optional[str],
) -> List[Optional[str]]:
    """Return candidate terminal64.exe paths to try.

    Includes None (auto) first.
    """
    candidates: List[Optional[str]] = [None]
    if user_hint:
        candidates.append(user_hint)
    if os.name == "nt":
        pf = os.environ.get("PROGRAMFILES") or r"C:\\Program Files"
        pfx = os.environ.get("PROGRAMFILES(X86)") or r"C:\\Program Files (x86)"
        base_path = os.path.expanduser("~")
        roaming = os.path.join(
            base_path, "AppData", "Roaming", "MetaQuotes", "Terminal"
        )
        patterns = [
            os.path.join(pf, "MetaTrader 5", "terminal64.exe"),
            os.path.join(pf, "MetaTrader 5 *", "terminal64.exe"),
            os.path.join(pfx, "MetaTrader 5", "terminal64.exe"),
            os.path.join(roaming, "*", "terminal64.exe"),
        ]
        for pat in patterns:
            for path in glob.glob(pat):
                if os.path.isfile(path):
                    candidates.append(path)
    seen = set()
    uniq: List[Optional[str]] = []
    for cand in candidates:
        key = cand or "<auto>"
        if key not in seen:
            seen.add(key)
            uniq.append(cand)
    return uniq


def init_mt5(
    path: Optional[str] = None,
    *,
    timeout: int = 90,
    retries: int = 2,
    portable: bool = False,
    verbose: bool = False,
) -> None:
    """Initialize MetaTrader5 with retries and optional terminal path."""
    if mt5 is None:
        raise RuntimeError("MetaTrader5 package is not installed")

    login = os.environ.get("MT5_LOGIN")
    password = os.environ.get("MT5_PASSWORD")
    server = os.environ.get("MT5_SERVER")
    candidates = _candidate_terminal_paths(path)
    last_err: Optional[Tuple[int, str]] = None

    for attempt in range(1, max(1, retries) + 1):
        for cand in candidates:
            if verbose:
                where = cand or "<auto>"
                print(
                    f"[mt5] initialize attempt {attempt} path={where} "
                    f"timeout={timeout}s portable={portable}"
                )
            ok = False
            try:
                kwargs: Dict[str, object] = {"timeout": timeout}
                if portable:
                    kwargs["portable"] = True
                if login and password and server:
                    try:
                        kwargs["login"] = int(login)
                        kwargs["password"] = password
                        kwargs["server"] = server
                    except Exception:
                        pass
                if cand is None:
                    ok = mt5.initialize(**kwargs)
                else:
                    ok = mt5.initialize(cand, **kwargs)
            except Exception:
                ok = False
            if ok:
                try:
                    mt5.version()
                except Exception:
                    pass
                return
            try:
                last_err = mt5.last_error()
            except Exception:
                last_err = None
            if verbose and last_err:
                print(
                    f"[mt5] initialize failed at path={cand or '<auto>'}: "
                    f"{last_err}"
                )
            if last_err and last_err[0] in (-10004, -10005, -10006):
                time.sleep(1.0)
            try:
                mt5.shutdown()
            except Exception:
                pass
        time.sleep(1.0)
    raise RuntimeError(f"mt5.initialize failed after retries: {last_err}")


def shutdown_mt5() -> None:
    if mt5 is None:
        return
    try:
        mt5.shutdown()
    except Exception:
        pass


def resolve_symbol(base: str) -> Optional[str]:
    if mt5 is None:
        return None
    if mt5.symbol_select(base, True):
        return base
    try:
        candidates = mt5.symbols_get(f"{base}*") or []
    except Exception:
        candidates = []
    best: Optional[Tuple[int, str]] = None
    for symbol_info in candidates:
        name = getattr(symbol_info, "name", None)
        if not name:
            continue
        score = 0
        if getattr(symbol_info, "visible", False):
            score -= 10
        score += len(name)
        if best is None or score < best[0]:
            best = (score, name)
    if best is not None:
        chosen = best[1]
        if mt5.symbol_select(chosen, True):
            return chosen
    return None


def get_symbol_info(symbol: str) -> Any | None:
    if mt5 is None:
        return None
    try:
        return mt5.symbol_info(symbol)
    except Exception:
        return None


def get_server_offset_hours(symbol_for_probe: str) -> int:
    """Infer whole-hour server offset using latest tick time vs now UTC."""
    if mt5 is None:
        return 0
    tick = mt5.symbol_info_tick(symbol_for_probe)
    if tick is None:
        return 0
    try:
        ts = float(getattr(tick, "time_msc", 0) or 0) / 1000.0
        if ts == 0:
            ts = float(getattr(tick, "time", 0) or 0)
        dt_raw = datetime.fromtimestamp(ts, tz=UTC)
        now_utc = datetime.now(UTC)
        diff_hours = (dt_raw - now_utc).total_seconds() / 3600.0
        if abs(diff_hours) <= (10.0 / 60.0):
            return 0
        est = int(round(diff_hours))
        if -12 <= est <= 12:
            return est
    except Exception:
        pass
    return 0


def to_server_naive(dt_utc: datetime, offset_hours: int) -> datetime:
    target_epoch = dt_utc.timestamp() + (offset_hours * 3600.0)
    return datetime.fromtimestamp(target_epoch)


def epoch_to_server_naive(epoch_seconds: float, offset_hours: int) -> datetime:
    return datetime.fromtimestamp(epoch_seconds + (offset_hours * 3600.0))


def from_server_naive(dt_naive: datetime, offset_hours: int) -> datetime:
    epoch = dt_naive.timestamp() - (offset_hours * 3600.0)
    return datetime.fromtimestamp(epoch, tz=UTC)


def rates_range_utc(
    symbol: str,
    timeframe: int,
    start_utc: datetime,
    end_utc: datetime,
    offset_hours: int,
    trace: bool = False,
):
    """Fetch bars for [start_utc, end_utc] and return them as a list."""

    if mt5 is None or start_utc >= end_utc:
        return []
    start_server = to_server_naive(start_utc, offset_hours)
    end_server = to_server_naive(end_utc, offset_hours)
    call_t0 = perf_counter()
    rates = mt5.copy_rates_range(symbol, timeframe, start_server, end_server)
    elapsed = perf_counter() - call_t0
    if trace:
        count = 0 if rates is None else len(rates)
        print(
            f"    [rates-range] {symbol} tf={timeframe} bars={count} "
            f"in {elapsed * 1000:.1f} ms"
        )
    return [] if rates is None else list(rates)


def ticks_paged(
    symbol: str,
    start_server_naive: datetime,
    end_server_naive: datetime,
    page: int,
    trace: bool = False,
    server_offset_hours: int = 0,
) -> Tuple[List[object], TickFetchStats]:
    """Fetch ticks from start..end (server-local naive) using copy_ticks_from."""
    if mt5 is None:
        return [], TickFetchStats(
            pages=0,
            total_ticks=0,
            elapsed_s=0.0,
            fetch_s=0.0,
            early_stop=False,
        )
    t0 = perf_counter()
    all_ticks: List[object] = []
    cur = start_server_naive
    pages = 0
    fetch_s = 0.0
    while True:
        call_t0 = perf_counter()
        chunk = mt5.copy_ticks_from(symbol, cur, page, mt5.COPY_TICKS_ALL)
        call_dt = perf_counter() - call_t0
        fetch_s += call_dt
        n = 0 if chunk is None else len(chunk)
        if trace:
            cur_str = cur.isoformat(sep=" ", timespec="seconds")
            print(
                f"    [ticks] page {pages+1} start={cur_str} -> "
                f"got {n} ticks in {call_dt * 1000:.1f} ms"
            )
        if chunk is None or n == 0:
            break
        all_ticks.extend(chunk)
        pages += 1
        last = chunk[-1]
        try:
            tms = getattr(last, "time_msc", None)
            if tms is None:
                tms = (
                    int(last["time_msc"])
                    if isinstance(last, dict)
                    else last["time_msc"]
                )
            next_ts = (int(tms) + 1) / 1000.0
        except Exception:
            try:
                tse = getattr(last, "time", None)
                if tse is None:
                    tse = int(last["time"]) if isinstance(last, dict) else last["time"]
                next_ts = int(tse) + 1
            except Exception:
                break
        cur = epoch_to_server_naive(next_ts, server_offset_hours)
        if cur > end_server_naive:
            break
    elapsed = perf_counter() - t0
    return all_ticks, TickFetchStats(
        pages=pages,
        total_ticks=len(all_ticks),
        elapsed_s=elapsed,
        fetch_s=fetch_s,
        early_stop=False,
    )


def ticks_range_all(
    symbol: str,
    start_server_naive: datetime,
    end_server_naive: datetime,
    trace: bool = False,
) -> Tuple[List[object], TickFetchStats]:
    """Fetch all ticks for [start, end] using copy_ticks_range."""
    if mt5 is None:
        return [], TickFetchStats(
            pages=0,
            total_ticks=0,
            elapsed_s=0.0,
            fetch_s=0.0,
            early_stop=False,
        )
    t0 = perf_counter()
    call_t0 = perf_counter()
    ticks = mt5.copy_ticks_range(
        symbol, start_server_naive, end_server_naive, mt5.COPY_TICKS_ALL
    )
    call_dt = perf_counter() - call_t0
    n = 0 if ticks is None else len(ticks)
    if trace:
        print(f"    [ticks-range] {n} ticks in {call_dt * 1000:.1f} ms")
    elapsed = perf_counter() - t0
    ticks_out = ticks if ticks is not None else []
    pages = 1 if n > 0 else 0
    return ticks_out, TickFetchStats(
        pages=pages,
        total_ticks=n,
        elapsed_s=elapsed,
        fetch_s=call_dt,
        early_stop=False,
    )


def scan_ticks_paged_for_hit(
    symbol: str,
    start_server_naive: datetime,
    end_server_naive: datetime,
    page: int,
    direction: str,
    sl: float,
    tp: float,
    server_offset_hours: int,
    trace: bool = False,
) -> Tuple[Optional[Hit], TickFetchStats]:
    """Fetch ticks page-by-page and stop as soon as a hit is detected."""
    if mt5 is None:
        return None, TickFetchStats(
            pages=0,
            total_ticks=0,
            elapsed_s=0.0,
            fetch_s=0.0,
            early_stop=False,
        )
    pages = 0
    total_ticks = 0
    t0 = perf_counter()
    fetch_s = 0.0
    cur = start_server_naive
    while True:
        call_t0 = perf_counter()
        chunk = mt5.copy_ticks_from(symbol, cur, page, mt5.COPY_TICKS_ALL)
        call_dt = perf_counter() - call_t0
        fetch_s += call_dt
        n = 0 if chunk is None else len(chunk)
        if trace:
            cur_str = cur.isoformat(sep=" ", timespec="seconds")
            print(
                f"    [ticks] page {pages+1} start={cur_str} -> "
                f"got {n} ticks in {call_dt * 1000:.1f} ms"
            )
        if chunk is None or n == 0:
            break
        pages += 1
        total_ticks += n
        hit = earliest_hit_from_ticks(chunk, direction, sl, tp, server_offset_hours)
        if hit is not None:
            elapsed = perf_counter() - t0
            return hit, TickFetchStats(
                pages=pages,
                total_ticks=total_ticks,
                elapsed_s=elapsed,
                fetch_s=fetch_s,
                early_stop=True,
            )
        last = chunk[-1]
        try:
            tms = getattr(last, "time_msc", None)
            if tms is None:
                tms = (
                    int(last["time_msc"])
                    if isinstance(last, dict)
                    else last["time_msc"]
                )
            next_ts = (int(tms) + 1) / 1000.0
        except Exception:
            try:
                tse = getattr(last, "time", None)
                if tse is None:
                    tse = int(last["time"]) if isinstance(last, dict) else last["time"]
                next_ts = int(tse) + 1
            except Exception:
                break
        cur = epoch_to_server_naive(next_ts, server_offset_hours)
        if cur > end_server_naive:
            break
    elapsed = perf_counter() - t0
    return None, TickFetchStats(
        pages=pages,
        total_ticks=total_ticks,
        elapsed_s=elapsed,
        fetch_s=fetch_s,
        early_stop=False,
    )


def earliest_hit_from_ticks(
    ticks: Sequence[object],
    direction: str,
    sl: float,
    tp: float,
    server_offset_hours: int,
    entry_price: Optional[float] = None,
) -> Optional[Hit]:
    if ticks is None:
        return None
    try:
        n = len(ticks)
    except Exception:
        try:
            n = int(getattr(ticks, "size", 0))
        except Exception:
            n = 0
    if n == 0:
        return None
    last_bid: Optional[float] = None
    last_ask: Optional[float] = None
    adverse_price: Optional[float] = entry_price

    def _calc_drawdown(
        hit_direction: str,
        entry: Optional[float],
        adverse: Optional[float],
    ) -> Tuple[Optional[float], Optional[float]]:
        if entry is None or adverse is None:
            return None, None
        try:
            if hit_direction.lower() == "buy":
                adverse_move = max(0.0, float(entry) - float(adverse))
                target_span = max(0.0, float(tp) - float(entry))
            else:
                adverse_move = max(0.0, float(adverse) - float(entry))
                target_span = max(0.0, float(entry) - float(tp))
        except Exception:
            return None, None
        if target_span <= 0.0:
            return adverse_move, None
        return adverse_move, adverse_move / target_span

    for i in range(n):
        tk = ticks[i]
        bid = getattr(tk, "bid", None)
        ask = getattr(tk, "ask", None)
        if bid is None:
            try:
                bid = tk["bid"]  # type: ignore[index]
            except Exception:
                if isinstance(tk, dict):
                    bid = tk.get("bid")
        if ask is None:
            try:
                ask = tk["ask"]  # type: ignore[index]
            except Exception:
                if isinstance(tk, dict):
                    ask = tk.get("ask")
        bid_val = _coerce_price(bid)
        ask_val = _coerce_price(ask)
        if bid_val is not None:
            last_bid = bid_val
        else:
            bid_val = last_bid
        if ask_val is not None:
            last_ask = ask_val
        else:
            ask_val = last_ask
        if bid_val is None and ask_val is None:
            continue
        tms = getattr(tk, "time_msc", None)
        if tms is None:
            try:
                tms = tk["time_msc"]  # type: ignore[index]
            except Exception:
                if isinstance(tk, dict):
                    tms = tk.get("time_msc")
        dt_raw = None
        if tms is not None:
            dt_raw = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
        else:
            tse = getattr(tk, "time", None)
            if tse is None:
                try:
                    tse = tk["time"]  # type: ignore[index]
                except Exception:
                    if isinstance(tk, dict):
                        tse = tk.get("time")
            if tse is None:
                continue
            dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)
        if dt_raw is None:
            continue
        dt_utc = dt_raw - timedelta(hours=server_offset_hours)
        lower_direction = direction.lower()
        if lower_direction == "buy":
            if bid_val is None:
                continue
            if adverse_price is None:
                baseline = entry_price if entry_price is not None else bid_val
                adverse_price = min(baseline, bid_val)
            else:
                adverse_price = min(adverse_price, bid_val)
            if bid_val <= sl:
                adverse_move, drawdown_ratio = _calc_drawdown(
                    lower_direction, entry_price, adverse_price
                )
                return Hit(
                    kind="SL",
                    time_utc=dt_utc,
                    price=bid_val,
                    adverse_price=adverse_price,
                    adverse_move=adverse_move,
                    drawdown_to_target=drawdown_ratio,
                )
            if bid_val >= tp:
                adverse_move, drawdown_ratio = _calc_drawdown(
                    lower_direction, entry_price, adverse_price
                )
                return Hit(
                    kind="TP",
                    time_utc=dt_utc,
                    price=bid_val,
                    adverse_price=adverse_price,
                    adverse_move=adverse_move,
                    drawdown_to_target=drawdown_ratio,
                )
        else:
            if ask_val is None:
                continue
            if adverse_price is None:
                baseline = entry_price if entry_price is not None else ask_val
                adverse_price = max(baseline, ask_val)
            else:
                adverse_price = max(adverse_price, ask_val)
            if ask_val >= sl:
                adverse_move, drawdown_ratio = _calc_drawdown(
                    lower_direction, entry_price, adverse_price
                )
                return Hit(
                    kind="SL",
                    time_utc=dt_utc,
                    price=ask_val,
                    adverse_price=adverse_price,
                    adverse_move=adverse_move,
                    drawdown_to_target=drawdown_ratio,
                )
            if ask_val <= tp:
                adverse_move, drawdown_ratio = _calc_drawdown(
                    lower_direction, entry_price, adverse_price
                )
                return Hit(
                    kind="TP",
                    time_utc=dt_utc,
                    price=ask_val,
                    adverse_price=adverse_price,
                    adverse_move=adverse_move,
                    drawdown_to_target=drawdown_ratio,
                )
    return None
