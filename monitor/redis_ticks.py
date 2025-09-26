from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Optional, Sequence

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


TICKS_KEY_FMT = "{prefix}:ticks:{symbol}"
LAST_TICK_KEY_FMT = "{prefix}:state:last_tick:{symbol}"


class CacheUnavailable(RuntimeError):
    """Raised when Redis-based tick cache cannot be reached."""


@dataclass
class CachedTick:
    time_msc: int
    bid: Optional[float]
    ask: Optional[float]

    def as_dict(self) -> dict[str, float | int | None]:
        data: dict[str, float | int | None] = {"time_msc": self.time_msc}
        if self.bid is not None:
            data["bid"] = self.bid
        if self.ask is not None:
            data["ask"] = self.ask
        return data


class RedisTickCache:
    """Read-only helper for ticks stored by RedisRealtimeExitManager."""

    def __init__(
        self,
        *,
        redis_url: str,
        prefix: str = "monitor",
        client=None,
        test_connection: bool = True,
    ) -> None:
        if client is not None:
            self._redis = client
        else:
            if redis is None:
                raise CacheUnavailable("redis package not installed")
            try:
                self._redis = redis.Redis.from_url(redis_url, decode_responses=True)
            except Exception as exc:  # pragma: no cover - connection parsing failure
                raise CacheUnavailable(str(exc)) from exc
        self._prefix = prefix.rstrip(":")
        if test_connection:
            try:
                self._redis.ping()
            except Exception as exc:
                raise CacheUnavailable(str(exc)) from exc

    # Redis key helpers -------------------------------------------------

    def _ticks_key(self, symbol: str) -> str:
        return TICKS_KEY_FMT.format(prefix=self._prefix, symbol=symbol)

    def _last_tick_key(self, symbol: str) -> str:
        return LAST_TICK_KEY_FMT.format(prefix=self._prefix, symbol=symbol)

    # Public API --------------------------------------------------------

    def latest(self, symbol: str) -> Optional[CachedTick]:
        try:
            result = self._redis.zrevrange(self._ticks_key(symbol), 0, 0, withscores=True)
        except Exception as exc:
            raise CacheUnavailable(str(exc)) from exc
        if not result:
            return None
        payload, score = result[0]
        tick = self._decode_tick(payload)
        if tick is None:
            return None
        tick.time_msc = int(score)
        return tick

    def window(self, symbol: str, start_ms: int, end_ms: int) -> List[CachedTick]:
        if end_ms < start_ms:
            return []
        try:
            raw = self._redis.zrangebyscore(
                self._ticks_key(symbol),
                min=start_ms,
                max=end_ms,
                withscores=True,
            )
        except Exception as exc:
            raise CacheUnavailable(str(exc)) from exc
        ticks: List[CachedTick] = []
        for payload, score in raw:
            tick = self._decode_tick(payload)
            if tick is None:
                continue
            tick.time_msc = int(score)
            ticks.append(tick)
        return ticks

    def last_timestamp(self, symbol: str) -> Optional[int]:
        try:
            value = self._redis.get(self._last_tick_key(symbol))
        except Exception as exc:
            raise CacheUnavailable(str(exc)) from exc
        if not value:
            return None
        try:
            return int(float(value))
        except Exception:
            return None

    # Helpers -----------------------------------------------------------

    @staticmethod
    def _decode_tick(payload: str) -> Optional[CachedTick]:
        try:
            data = json.loads(payload)
        except Exception:
            return None
        try:
            t_ms = int(float(data.get("time_msc")))
        except Exception:
            return None
        bid = data.get("bid")
        ask = data.get("ask")
        try:
            bid_f = float(bid) if bid is not None else None
        except Exception:
            bid_f = None
        try:
            ask_f = float(ask) if ask is not None else None
        except Exception:
            ask_f = None
        return CachedTick(time_msc=t_ms, bid=bid_f, ask=ask_f)


def ticks_to_dicts(ticks: Sequence[CachedTick]) -> List[dict[str, float | int]]:
    """Convert cached tick objects into dicts compatible with existing consumers."""
    return [tick.as_dict() for tick in ticks]


__all__ = [
    "CacheUnavailable",
    "CachedTick",
    "RedisTickCache",
    "ticks_to_dicts",
    "TICKS_KEY_FMT",
    "LAST_TICK_KEY_FMT",
]
