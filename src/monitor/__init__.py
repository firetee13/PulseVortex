"""
Monitor: Trade Setup Analyzer & Monitor

A comprehensive Python application suite for automated trade setup detection
and monitoring via MetaTrader 5 (MT5) integration.
"""

__version__ = "0.1.0"
__author__ = "PulseVortex Team"
__email__ = "support@pulsevortex.com"
__license__ = "MIT"

from .core.config import db_path_str, default_db_path, project_root, resolve_db_path
from .core.domain import Hit, Setup, TickFetchStats
from .core.symbols import classify_symbol, is_crypto_symbol

__all__ = [
    # Version info
    "__version__",
    "__author__",
    "__email__",
    "__license__",
    # Core functionality
    "db_path_str",
    "default_db_path",
    "project_root",
    "resolve_db_path",
    # Domain models
    "Setup",
    "Hit",
    "TickFetchStats",
    # Symbol utilities
    "classify_symbol",
    "is_crypto_symbol",
]
