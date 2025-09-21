#!/usr/bin/env python3
"""In-memory log buffers for Dash UI

Provides thread-safe LogBuffer and registry helpers. Designed to be attached to ProcController.log_put
to capture real-time stdout lines for display via web UI.
"""
from __future__ import annotations

import threading
from collections import deque
from typing import Dict, List, Tuple, Optional, Callable

DEFAULT_MAX_LINES = 10000


class LogBuffer:
    def __init__(self, max_lines: int = DEFAULT_MAX_LINES) -> None:
        self._lock = threading.Lock()
        self._lines: deque[Tuple[int, str]] = deque(maxlen=max_lines)
        self._seq = 0

    def append(self, line: str) -> int:
        """Append a line and return its sequence number."""
        with self._lock:
            self._seq += 1
            self._lines.append((self._seq, line))
            return self._seq

    def tail_lines(self, n: int = 200) -> List[str]:
        """Return last n lines (as strings) from the buffer."""
        with self._lock:
            return [ln for (_, ln) in list(self._lines)[-n:]]

    def get_from(self, seq: int = 0) -> Tuple[List[str], int]:
        """Return (lines, new_seq) for entries with sequence > seq."""
        with self._lock:
            out = [ln for (s, ln) in self._lines if s > seq]
            new_seq = self._lines[-1][0] if self._lines else seq
            return out, new_seq

    def get_text(self) -> str:
        """Return whole buffer as a single string."""
        with self._lock:
            return "".join(ln for (_, ln) in self._lines)

    def clear(self) -> None:
        """Clear the buffer and reset sequence counter."""
        with self._lock:
            self._lines.clear()
            self._seq = 0

    def last_seq(self) -> int:
        with self._lock:
            return self._lines[-1][0] if self._lines else 0


# Global registry of log buffers by name
_BUFFERS: Dict[str, LogBuffer] = {}
_REG_LOCK = threading.Lock()


def get_buffer(name: str) -> LogBuffer:
    """Get or create a named LogBuffer."""
    with _REG_LOCK:
        buf = _BUFFERS.get(name)
        if buf is None:
            buf = LogBuffer()
            _BUFFERS[name] = buf
        return buf


def append(name: str, line: str) -> int:
    """Append line to named buffer; returns sequence number."""
    return get_buffer(name).append(line)


def tail(name: str, n: int = 200) -> List[str]:
    return get_buffer(name).tail_lines(n)


def get_since(name: str, seq: int = 0) -> Tuple[List[str], int]:
    """Get lines since sequence number `seq` and return (lines, new_seq)."""
    return get_buffer(name).get_from(seq)


def get_text(name: str) -> str:
    return get_buffer(name).get_text()


def clear(name: str) -> None:
    get_buffer(name).clear()


def last_seq(name: str) -> int:
    return get_buffer(name).last_seq()


def names() -> List[str]:
    with _REG_LOCK:
        return list(_BUFFERS.keys())


def attach_proc_controller(ctrl) -> None:
    """Attach a ProcController or any object with a `log_put` attribute to write to buffers.

    The ProcController will call log_put(name, line). We set a function that forwards
    to the buffer with the given name.
    """
    try:
        def _forward(name: str, line: str) -> None:
            try:
                append(name, line)
            except Exception:
                # swallow to avoid breaking the process controller
                pass

        setattr(ctrl, "log_put", _forward)
    except Exception:
        pass


def attach_named(name: str) -> Callable[[str, str], None]:
    """Return a log_put callable bound to a specific named buffer."""
    def _fn(_name: str, line: str) -> None:
        try:
            append(name, line)
        except Exception:
            pass

    return _fn


__all__ = [
    "LogBuffer",
    "get_buffer",
    "append",
    "tail",
    "get_since",
    "get_text",
    "clear",
    "last_seq",
    "names",
    "attach_proc_controller",
    "attach_named",
]