#!/usr/bin/env python3
"""Lightweight subprocess controller utilities.

This module provides a ProcController class (extracted and simplified from the
desktop GUI) suitable for use by a server (Dash) to start/stop the existing CLI
tools and stream their stdout lines to a callback.

Usage example:
    from monitor.proc import create_controller, get_controller
    def log_put(name, line): print(f"[{name}] {line}", end='')
    ctrl = create_controller("timelapse", [sys.executable, "-u", "timelapse_setups.py", "--watch"], log_put=log_put)
    ctrl.start()
"""
from __future__ import annotations

import os
import subprocess
import threading
import time
import signal
import atexit
from typing import Callable, Optional, List, Dict

LogCallback = Callable[[str, str], None]


class ProcController:
    """Controller for running a subprocess and streaming its stdout.

    - start(): launches the process if not already running.
    - stop(): attempts graceful termination, then kills if necessary.
    - is_running(): True when the child process is alive.
    - log_put(name, line): optional callback invoked for each stdout line.
    """

    def __init__(self, name: str, cmd: List[str], cwd: Optional[str] = None, log_put: Optional[LogCallback] = None) -> None:
        self.name = name
        self.cmd = cmd
        self.cwd = cwd or os.getcwd()
        self.log_put = log_put
        self.proc: Optional[subprocess.Popen] = None
        self._reader_thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self) -> None:
        if self.is_running():
            if self.log_put:
                self.log_put(self.name, f"Already running: {' '.join(self.cmd)}\n")
            return
        self._stop_evt.clear()
        try:
            creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            self.proc = subprocess.Popen(
                self.cmd,
                cwd=self.cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                text=True,
                creationflags=creationflags,
            )
        except Exception as e:
            if self.log_put:
                self.log_put(self.name, f"Failed to start: {e}\n")
            self.proc = None
            return
        if self.log_put:
            self.log_put(self.name, f"Started: {' '.join(self.cmd)}\n")
        self._reader_thread = threading.Thread(target=self._reader_loop, name=f"{self.name}-reader", daemon=True)
        self._reader_thread.start()

    def _reader_loop(self) -> None:
        if self.proc is None:
            return
        out = self.proc.stdout
        if out is None:
            return
        try:
            for line in out:
                if self._stop_evt.is_set():
                    break
                if self.log_put:
                    self.log_put(self.name, line)
        except Exception as e:
            if self.log_put:
                self.log_put(self.name, f"[reader] error: {e}\n")
        finally:
            try:
                out.close()
            except Exception:
                pass
            code = self.proc.poll() if self.proc is not None else None
            if self.log_put:
                self.log_put(self.name, f"Exited with code {code}.\n")

    def stop(self, timeout: float = 3.0) -> None:
        """Stop the process, waiting up to `timeout` seconds for graceful exit."""
        if not self.proc or self.proc.poll() is not None:
            if self.log_put:
                self.log_put(self.name, "Not running.\n")
            return
        self._stop_evt.set()
        try:
            if os.name == "nt":
                try:
                    self.proc.terminate()
                except Exception:
                    pass
            else:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
                except Exception:
                    try:
                        self.proc.terminate()
                    except Exception:
                        pass
        except Exception as e:
            if self.log_put:
                self.log_put(self.name, f"Stop error: {e}\n")
        # Wait for process to exit
        start = time.time()
        while self.proc is not None and self.proc.poll() is None and (time.time() - start) < timeout:
            time.sleep(0.1)
        # If still alive, force kill
        if self.proc is not None and self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        # Join reader thread
        if self._reader_thread is not None:
            try:
                self._reader_thread.join(timeout=0.5)
            except Exception:
                pass
            self._reader_thread = None
        self.proc = None


# Registry helpers for server usage
_CONTROLLERS: Dict[str, ProcController] = {}


def create_controller(name: str, cmd: List[str], cwd: Optional[str] = None, log_put: Optional[LogCallback] = None) -> ProcController:
    """Create or update a named controller in the global registry."""
    if name in _CONTROLLERS:
        ctrl = _CONTROLLERS[name]
        ctrl.cmd = cmd
        ctrl.cwd = cwd or ctrl.cwd
        if log_put is not None:
            ctrl.log_put = log_put
        return ctrl
    ctrl = ProcController(name=name, cmd=cmd, cwd=cwd, log_put=log_put)
    _CONTROLLERS[name] = ctrl
    return ctrl


def get_controller(name: str) -> Optional[ProcController]:
    return _CONTROLLERS.get(name)


def stop_all() -> None:
    for c in list(_CONTROLLERS.values()):
        try:
            c.stop()
        except Exception:
            pass


atexit.register(stop_all)