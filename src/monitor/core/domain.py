from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Setup:
    """Canonical representation of a timelapse setup stored in SQLite."""

    id: int
    symbol: str
    direction: str
    sl: float
    tp: float
    entry_price: Optional[float]
    as_of_utc: datetime


@dataclass
class Hit:
    """Represents a resolved TP/SL event for a setup."""

    kind: str  # 'TP' or 'SL'
    time_utc: datetime
    price: float


@dataclass
class TickFetchStats:
    """Execution statistics for MT5 tick retrieval."""

    pages: int
    total_ticks: int
    elapsed_s: float
    fetch_s: float
    early_stop: bool = False
