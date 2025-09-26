from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_DEFAULT_REDIS_PREFIX = "monitor"
_DEFAULT_REDIS_MAX_AGE_MS = 1500

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_DB_FILENAME = "timelapse.db"


def project_root() -> Path:
    """Return the project root directory (one level above the monitor package)."""
    return _PROJECT_ROOT


def _resolve_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return _PROJECT_ROOT / path


def default_db_path() -> Path:
    """Resolve the default SQLite database path, honoring TIMELAPSE_DB_PATH if set."""
    env_override = os.environ.get("TIMELAPSE_DB_PATH")
    if env_override:
        return _resolve_path(env_override)
    return _resolve_path(_DEFAULT_DB_FILENAME)


def resolve_db_path(candidate: Optional[str]) -> Path:
    """Resolve a user-supplied database path against the project root."""
    if candidate:
        return _resolve_path(candidate)
    return default_db_path()


def db_path_str(candidate: Optional[str] = None) -> str:
    """Return the resolved database path as a string for sqlite3.connect and CLI args."""
    return str(resolve_db_path(candidate))


def redis_url(default: Optional[str] = None) -> Optional[str]:
    """Return the Redis URL for shared tick cache if configured."""
    value = os.environ.get("TIMELAPSE_REDIS_URL")
    if value:
        return value
    return default if default is not None else _DEFAULT_REDIS_URL


def redis_prefix(default: Optional[str] = None) -> str:
    """Return the Redis key prefix (`monitor` by default)."""
    value = os.environ.get("TIMELAPSE_REDIS_PREFIX")
    if value:
        return value
    return default if default is not None else _DEFAULT_REDIS_PREFIX


def redis_tick_max_age_ms(default: Optional[int] = None) -> int:
    """Maximum tick staleness (milliseconds) accepted from Redis cache."""
    value = os.environ.get("TIMELAPSE_REDIS_MAX_AGE_MS")
    if value:
        try:
            parsed = int(float(value))
            if parsed > 0:
                return parsed
        except Exception:
            pass
    if default is not None and default > 0:
        return default
    return _DEFAULT_REDIS_MAX_AGE_MS
