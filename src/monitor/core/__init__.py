"""
Core modules for the monitor package.

This package contains the core business logic and utilities for the
trade setup analyzer and monitor.
"""

from .config import (
    db_path_str,
    default_db_path,
    project_root,
    resolve_db_path,
)
from .domain import (
    Hit,
    Setup,
    TickFetchStats,
)
from .mt5_client import (
    earliest_hit_from_ticks,
    init_mt5,
)
from .quiet_hours import (
    is_quiet_time,
    iter_active_utc_ranges,
    iter_quiet_utc_ranges,
    next_quiet_transition,
)
from .symbols import (
    classify_symbol,
    is_crypto_symbol,
)

__all__ = [
    # Configuration
    "db_path_str",
    "default_db_path",
    "project_root",
    "resolve_db_path",
    # Domain models
    "Setup",
    "Hit",
    "TickFetchStats",
    # MT5 client
    "earliest_hit_from_ticks",
    "init_mt5",
    # Quiet hours
    "is_quiet_time",
    "iter_active_utc_ranges",
    "iter_quiet_utc_ranges",
    "next_quiet_transition",
    # Symbol classification
    "classify_symbol",
    "is_crypto_symbol",
]
