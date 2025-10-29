#!/usr/bin/env python3
"""
PulseVortex GUI Launcher for:
  - monitor-setup --watch
  - monitor-hits --watch

Provides Start/Stop buttons and a shared log output.

Usage:
  monitor-gui

Notes:
  - Uses the CLI entry points for proper package integration.
  - Runs both child scripts with unbuffered output (-u) so logs stream live.
  - Stops processes via terminate() when pressing Stop or closing the window.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
import tkinter as tk
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from tkinter import ttk
from typing import Sequence

from monitor.core.config import db_path_str, default_db_path
from monitor.core.mt5_client import get_server_offset_hours as _GET_OFFS
from monitor.core.mt5_client import init_mt5 as _INIT_MT5
from monitor.core.mt5_client import normalize_terminal_path as _NORMALIZE_MT5_PATH
from monitor.core.mt5_client import rates_range_utc as _RATES_RANGE
from monitor.core.mt5_client import resolve_symbol as _RESOLVE
from monitor.core.mt5_client import timeframe_m1 as _TIMEFRAME_M1
from monitor.core.mt5_client import timeframe_seconds as _TIMEFRAME_SECONDS
from monitor.core.mt5_client import to_server_naive as _TO_SERVER
from monitor.core.quiet_hours import (
    is_quiet_time,
    iter_active_utc_ranges,
    iter_quiet_utc_ranges,
    next_quiet_transition,
)
from monitor.core.symbols import classify_symbol

# Plotting
try:
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt  # noqa: F401
    from matplotlib.backends.backend_tkagg import (
        FigureCanvasTkAgg,
        NavigationToolbar2Tk,
    )
    from matplotlib.figure import Figure
except Exception:
    FigureCanvasTkAgg = None  # type: ignore
    NavigationToolbar2Tk = None  # type: ignore
    Figure = None  # type: ignore
    mdates = None  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))

_MT5_PATH_OVERRIDE = _NORMALIZE_MT5_PATH(
    os.environ.get("TIMELAPSE_MT5_TERMINAL_PATH") or os.environ.get("MT5_TERMINAL_PATH")
)


def _as_float(value: object | None) -> float | None:
    """Safely coerce MT5 numeric fields to float."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the monitor GUI.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="PulseVortex GUI")
    parser.add_argument(
        "--restore-timelapse-log",
        help="Path to timelapse log file to restore on startup",
    )
    parser.add_argument(
        "--restore-hits-log", help="Path to hits log file to restore on startup"
    )
    return parser.parse_args()


# MT5 imports (optional at module import; initialized lazily when needed)
_MT5_IMPORTED = False
try:
    import MetaTrader5 as mt5  # type: ignore

    _MT5_IMPORTED = True
except Exception:
    mt5 = None  # type: ignore
    _MT5_IMPORTED = False

    # MT5 helper functions shared with CLI scripts
    pass

UTC = timezone.utc
DISPLAY_TZ = timezone(timedelta(hours=3))  # UTC+3 for chart display
QUIET_CHART_MESSAGE = (
    "Charts paused during quiet hours (23:45-00:59 UTC+3; weekends for non-crypto)."
)
TOP_EXPECTANCY_MIN_EDGE = 0.05
TOP_SCORE_MIN = 0.35
WORST_EXPECTANCY_MAX_EDGE = -TOP_EXPECTANCY_MIN_EDGE
WORST_SCORE_MAX = -0.1
PROX_SYMBOL_ALL_LABEL = "(All symbols)"


class ProcController:
    def __init__(self, name: str, cmd: list[str], log_put):
        self.name = name
        self.cmd = cmd
        self.log_put = log_put
        self.proc: subprocess.Popen | None = None
        self._reader_thread: threading.Thread | None = None
        self._stop_evt = threading.Event()

    def is_running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.log_put(self.name, f"Already running: {' '.join(self.cmd)}\n")
            return
        self._stop_evt.clear()
        try:
            from monitor.core.config import project_root

            self.proc = subprocess.Popen(
                self.cmd,
                cwd=str(project_root()),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                universal_newlines=True,
                creationflags=(
                    subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
                ),
            )
        except Exception as e:
            self.log_put(self.name, f"Failed to start: {e}\n")
            self.proc = None
            return

        self.log_put(self.name, f"Started: {' '.join(self.cmd)}\n")
        self._reader_thread = threading.Thread(
            target=self._reader_loop, name=f"{self.name}-reader", daemon=True
        )
        self._reader_thread.start()

    def _reader_loop(self) -> None:
        assert self.proc is not None
        f = self.proc.stdout
        if f is None:
            return
        try:
            for line in f:
                if self._stop_evt.is_set():
                    break
                self.log_put(self.name, line)
        except Exception as e:
            self.log_put(self.name, f"[reader] error: {e}\n")
        finally:
            try:
                f.close()
            except Exception:
                pass
            code = self.proc.poll()
            self.log_put(self.name, f"Exited with code {code}.\n")

    def stop(self) -> None:
        if not self.proc or self.proc.poll() is not None:
            self.log_put(self.name, "Not running.\n")
            return
        self._stop_evt.set()
        # Attempt graceful termination
        try:
            if os.name == "nt":
                # Best-effort graceful stop; child may not handle CTRL_BREAK, so terminate as fallback
                try:
                    self.proc.terminate()
                except Exception:
                    pass
            else:
                try:
                    os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
                except Exception:
                    pass
        except Exception as e:
            self.log_put(self.name, f"Stop error: {e}\n")
        # Wait for process to exit
        for _ in range(20):
            if self.proc.poll() is not None:
                break
            time.sleep(0.1)
        # If still alive, force kill
        if self.proc.poll() is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        # Ensure reader thread ends cleanly
        if self._reader_thread is not None:
            try:
                self._reader_thread.join(timeout=0.5)
            except Exception:
                pass
            self._reader_thread = None
        self.proc = None


class App(tk.Tk):
    def __init__(
        self,
        restore_timelapse_log: str | None = None,
        restore_hits_log: str | None = None,
    ) -> None:
        super().__init__()
        self.title("PulseVortex")
        self.geometry("1000x600")
        self.minsize(800, 400)
        self._set_initial_window_state()

        # Store restore log paths for later cleanup
        self.restore_timelapse_log = restore_timelapse_log
        self.restore_hits_log = restore_hits_log

        # Notebook with tabs
        self.nb = ttk.Notebook(self)
        self.nb.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.tab_mon = ttk.Frame(self.nb)
        self.nb.add(self.tab_mon, text="Monitors")

        self.tab_db = ttk.Frame(self.nb)
        self.nb.add(self.tab_db, text="DB Results")

        self.tab_prox = ttk.Frame(self.nb)
        self.nb.add(self.tab_prox, text="SL Proximity")

        self.tab_top = ttk.Frame(self.nb)
        self.nb.add(self.tab_top, text="Top Performers")

        # Set DB Results tab as default active tab
        self.nb.select(self.tab_db)

        # User-configurable exclude list for timelapse setups (comma-separated symbols)
        self.var_exclude_symbols = tk.StringVar(value="")
        # Min prox SL for timelapse setups
        # Max prox SL for timelapse setups
        # DB tab variables
        self.var_db_name = tk.StringVar(value=str(default_db_path()))
        self.var_since_hours = tk.IntVar(value=168)
        self.var_auto = tk.BooleanVar(value=True)
        self.var_interval = tk.IntVar(value=60)
        self.var_symbol_filter = tk.StringVar(value="")
        # Proximity stats tab variables
        self.var_prox_since_hours = tk.IntVar(value=336)
        self.var_prox_min_trades = tk.IntVar(value=5)
        self.var_prox_symbol_filter = tk.StringVar(value="")
        self.var_prox_symbol_choice = tk.StringVar(value=PROX_SYMBOL_ALL_LABEL)
        self.var_prox_category = tk.StringVar(value="All")
        self.var_prox_auto = tk.BooleanVar(value=False)
        self.var_prox_interval = tk.IntVar(value=300)
        # Top Performers tab variables
        self.var_top_since_hours = tk.IntVar(value=168)
        self.var_top_min_trades = tk.IntVar(value=10)
        self.var_top_view = tk.StringVar(value="Top performers")
        self.var_top_auto = tk.BooleanVar(value=True)
        self.var_top_interval = tk.IntVar(value=300)
        # Load persisted settings (if any) before building controls
        try:
            self._load_settings()
        except Exception:
            pass
        try:
            self._sync_prox_symbol_choice_from_filter()
        except Exception:
            pass
        # Persist on any change
        try:
            self.var_exclude_symbols.trace_add("write", self._on_exclude_changed)
            self.var_symbol_filter.trace_add("write", self._on_filter_changed)
            self.var_prox_symbol_filter.trace_add(
                "write", self._on_prox_setting_changed
            )
            self.var_prox_symbol_choice.trace_add(
                "write", self._on_prox_symbol_choice_changed
            )
            self.var_prox_category.trace_add("write", self._on_prox_category_changed)
            self.var_prox_min_trades.trace_add("write", self._on_prox_setting_changed)
            self.var_prox_since_hours.trace_add("write", self._on_prox_setting_changed)
            self.var_prox_interval.trace_add("write", self._on_prox_setting_changed)
            # Top Performers settings
            self.var_top_since_hours.trace_add("write", self._on_top_setting_changed)
            self.var_top_min_trades.trace_add("write", self._on_top_setting_changed)
            self.var_top_interval.trace_add("write", self._on_top_setting_changed)
            self.var_top_view.trace_add("write", self._on_top_view_changed)
        except Exception:
            pass

        # UI elements in Monitors tab
        self._make_controls(self.tab_mon)
        self._make_logs(self.tab_mon)

        # Restore logs if provided
        self._restore_logs()

        # UI elements in DB tab
        self._make_db_tab(self.tab_db)
        # UI elements in SL proximity tab
        self._make_prox_tab(self.tab_prox)
        # UI elements in Top Performers tab
        self._make_top_tab(self.tab_top)
        # Ensure DB results refresh once at startup and auto-refresh is active
        try:
            self.var_auto.set(True)
        except Exception:
            pass
        self._db_refresh()
        # Prime proximity stats view
        try:
            self._prox_refresh()
        except Exception:
            pass
        # Prime top performers view
        try:
            self._top_refresh()
        except Exception:
            pass

        # Log queue for thread-safe updates
        self.log_q: queue.Queue[tuple[str, str]] = queue.Queue()
        self.after(50, self._drain_log)

        setup_cmd = ["monitor-setup", "--watch"]
        hits_cmd = ["monitor-hits", "--watch", "--interval", "1"]
        if _MT5_PATH_OVERRIDE:
            setup_cmd += ["--mt5-path", _MT5_PATH_OVERRIDE]
            hits_cmd += ["--mt5-path", _MT5_PATH_OVERRIDE]
        self.timelapse = ProcController(
            name="timelapse",
            cmd=setup_cmd,
            log_put=self._enqueue_log,
        )
        self.hits = ProcController(
            name="hits",
            cmd=hits_cmd,
            log_put=self._enqueue_log,
        )

        self._hits_should_run = True
        self._hits_quiet_paused = False
        try:
            self.after(1000, self._hits_quiet_guard)
        except Exception:
            pass

        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Autostart both services shortly after UI loads
        self.after(300, self._auto_start)

    def _make_controls(self, parent) -> None:
        frm = ttk.Frame(parent)
        frm.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        # PulseVortex controls
        tl = ttk.LabelFrame(frm, text="PulseVortex Timelapse Setups --watch")
        tl.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.Y, expand=True)
        self.btn_tl_toggle = ttk.Button(
            tl, text="Start", command=self._toggle_timelapse
        )
        self.btn_tl_toggle.pack(side=tk.TOP, padx=4, pady=6)
        # Exclude symbols input (comma-separated)
        ex_frame = ttk.Frame(tl)
        ex_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=2)
        ttk.Label(ex_frame, text="Exclude (comma):").pack(side=tk.LEFT)
        ent_ex = ttk.Entry(ex_frame, textvariable=self.var_exclude_symbols)
        ent_ex.pack(side=tk.LEFT, padx=(4, 0), fill=tk.X, expand=True)
        # TP/SL Hits controls
        ht = ttk.LabelFrame(frm, text="TP/SL Hits --watch")
        ht.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.X, expand=True)
        self.btn_hits_toggle = ttk.Button(ht, text="Start", command=self._toggle_hits)
        self.btn_hits_toggle.pack(side=tk.LEFT, padx=4, pady=6)

        # Misc
        misc = ttk.Frame(frm)
        misc.pack(side=tk.RIGHT)
        ttk.Button(misc, text="Clear Log", command=self._clear_log).pack(
            side=tk.TOP, padx=4, pady=4
        )

    def _make_logs(self, parent) -> None:
        # Two side-by-side log panes
        paned = ttk.Panedwindow(parent, orient=tk.HORIZONTAL)
        paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Timelapse pane
        lf_tl = ttk.LabelFrame(paned, text="Timelapse Log")
        frm_tl = ttk.Frame(lf_tl)
        frm_tl.pack(fill=tk.BOTH, expand=True)
        self.txt_tl = tk.Text(
            frm_tl, wrap=tk.NONE, state=tk.DISABLED, font=("Consolas", 10)
        )
        self.txt_tl.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs_tl = ttk.Scrollbar(frm_tl, orient=tk.VERTICAL, command=self.txt_tl.yview)
        vs_tl.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt_tl.configure(yscrollcommand=vs_tl.set)

        # Hits pane
        lf_hits = ttk.LabelFrame(paned, text="TP/SL Hits Log")
        frm_hits = ttk.Frame(lf_hits)
        frm_hits.pack(fill=tk.BOTH, expand=True)
        self.txt_hits = tk.Text(
            frm_hits, wrap=tk.NONE, state=tk.DISABLED, font=("Consolas", 10)
        )
        self.txt_hits.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs_hits = ttk.Scrollbar(
            frm_hits, orient=tk.VERTICAL, command=self.txt_hits.yview
        )
        vs_hits.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt_hits.configure(yscrollcommand=vs_hits.set)

        paned.add(lf_tl, weight=1)
        paned.add(lf_hits, weight=1)

    def _restore_logs(self) -> None:
        """Restore logs from files if provided."""
        # Restore timelapse log
        if self.restore_timelapse_log and os.path.exists(self.restore_timelapse_log):
            try:
                with open(self.restore_timelapse_log, "r", encoding="utf-8") as f:
                    content = f.read()
                    if content:
                        self._append_text(self.txt_tl, content)
            except Exception:
                pass  # Ignore errors during restore

        # Restore hits log
        if self.restore_hits_log and os.path.exists(self.restore_hits_log):
            try:
                with open(self.restore_hits_log, "r", encoding="utf-8") as f:
                    content = f.read()
                    if content:
                        self._append_text(self.txt_hits, content)
            except Exception:
                pass  # Ignore errors during restore

        # Clean up temporary files after restore
        self._cleanup_restore_files()

    def _cleanup_restore_files(self) -> None:
        """Clean up temporary log files after restoring."""
        files_to_clean = []
        if self.restore_timelapse_log and os.path.exists(self.restore_timelapse_log):
            files_to_clean.append(self.restore_timelapse_log)
        if self.restore_hits_log and os.path.exists(self.restore_hits_log):
            files_to_clean.append(self.restore_hits_log)

        # Clean up in a separate thread to avoid blocking UI
        if files_to_clean:

            def cleanup():
                for file_path in files_to_clean:
                    try:
                        os.remove(file_path)
                    except Exception:
                        pass

            threading.Thread(target=cleanup, daemon=True).start()

    # --- DB TAB ---
    def _make_db_tab(self, parent) -> None:
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        # DB config (variables already created in __init__)
        # Add filter variables
        self.var_symbol_category = tk.StringVar(value="All")
        self.var_hit_status = tk.StringVar(value="All")
        # Trace changes to filter variables to trigger refresh
        try:
            self.var_symbol_category.trace_add("write", self._on_filter_changed)
            self.var_hit_status.trace_add("write", self._on_filter_changed)
        except Exception:
            pass

        def add_labeled(parent, label, widget):
            f = ttk.Frame(parent)
            ttk.Label(f, text=label).pack(side=tk.LEFT)
            widget.pack(side=tk.LEFT, padx=6)
            return f

        row1 = ttk.Frame(top)
        row1.pack(side=tk.TOP, fill=tk.X)
        add_labeled(
            row1,
            "Since(h):",
            ttk.Spinbox(
                row1, from_=1, to=24 * 365, textvariable=self.var_since_hours, width=6
            ),
        ).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row1, text="Refresh", command=self._db_refresh).pack(side=tk.LEFT)
        ttk.Checkbutton(
            row1, text="Auto", variable=self.var_auto, command=self._db_auto_toggle
        ).pack(side=tk.LEFT, padx=(10, 4))
        add_labeled(
            row1,
            "Every(s):",
            ttk.Spinbox(
                row1, from_=5, to=3600, textvariable=self.var_interval, width=6
            ),
        ).pack(side=tk.LEFT)
        ttk.Button(row1, text="Restart", command=self._restart_monitors).pack(
            side=tk.RIGHT, padx=(10, 0)
        )

        # Add filter row
        row2 = ttk.Frame(top)
        row2.pack(side=tk.TOP, fill=tk.X, pady=(4, 0))
        add_labeled(
            row2,
            "Category:",
            ttk.Combobox(
                row2,
                textvariable=self.var_symbol_category,
                values=["All", "Forex", "Crypto", "Indices"],
                state="readonly",
                width=10,
            ),
        ).pack(side=tk.LEFT, padx=(0, 10))
        add_labeled(
            row2,
            "Status:",
            ttk.Combobox(
                row2,
                textvariable=self.var_hit_status,
                values=["All", "TP", "SL", "Running", "Hits"],
                state="readonly",
                width=10,
            ),
        ).pack(side=tk.LEFT, padx=(0, 10))
        add_labeled(
            row2,
            "Symbol:",
            ttk.Entry(row2, textvariable=self.var_symbol_filter, width=12),
        ).pack(side=tk.LEFT)

        # Tree (table)
        # Splitter: top table, bottom chart
        splitter = ttk.Panedwindow(parent, orient=tk.VERTICAL)
        splitter.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Top: table container
        mid = ttk.Frame(splitter)
        cols = (
            "symbol",
            "direction",
            "entry_utc3",
            "hit_time_utc3",
            "hit",
            "tp",
            "sl",
            "entry_price",
            "proximity_to_sl",
            "proximity_bin",
        )
        self.db_tree = ttk.Treeview(mid, columns=cols, show="headings", height=12)
        self.db_tree.heading("symbol", text="Symbol")
        self.db_tree.heading("direction", text="Direction")
        self.db_tree.heading("entry_utc3", text="Inserted UTC+3")
        self.db_tree.heading("hit_time_utc3", text="Hit Time UTC+3")
        self.db_tree.heading("hit", text="Hit")
        self.db_tree.heading("tp", text="TP")
        self.db_tree.heading("sl", text="SL")
        self.db_tree.heading("entry_price", text="Entry Price")
        self.db_tree.heading("proximity_to_sl", text="Prox to SL")
        self.db_tree.heading("proximity_bin", text="Prox Bin")
        self.db_tree.column("symbol", width=120, anchor=tk.W)
        self.db_tree.column("direction", width=80, anchor=tk.W)
        self.db_tree.column("entry_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit_time_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit", width=80, anchor=tk.W)
        self.db_tree.column("tp", width=100, anchor=tk.E)
        self.db_tree.column("sl", width=100, anchor=tk.E)
        self.db_tree.column("entry_price", width=120, anchor=tk.E)
        self.db_tree.column("proximity_to_sl", width=100, anchor=tk.E)
        self.db_tree.column("proximity_bin", width=90, anchor=tk.W)

        vs = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.db_tree.yview)
        self.db_tree.configure(yscrollcommand=vs.set)
        self.db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs.pack(side=tk.RIGHT, fill=tk.Y)

        splitter.add(mid, weight=3)

        # Bottom: chart container
        chart_wrap = ttk.Frame(splitter)
        self.chart_status = ttk.Label(
            chart_wrap, text="Select a row to render 1m chart (Inserted±) with SL/TP."
        )
        self.chart_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(4, 0))
        try:
            self.chart_spinner = ttk.Progressbar(chart_wrap, mode="indeterminate")
        except Exception:
            self.chart_spinner = None
        self._chart_spinner_visible = False
        self._chart_spinner_req_id: int | None = None
        self.chart_frame = ttk.Frame(chart_wrap)
        self.chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        splitter.add(chart_wrap, weight=2)

        # Matplotlib setup (if available)
        self._chart_fig = None
        self._chart_ax = None
        self._chart_canvas = None
        self._chart_toolbar = None
        self._init_chart_widgets()

        # Row metadata by item iid
        self._db_row_meta: dict[str, dict] = {}

        # Bind selection handler
        try:
            self.db_tree.bind("<<TreeviewSelect>>", self._on_db_row_selected)
        except Exception:
            pass

        # Row tags for coloring
        self.db_tree.tag_configure("tp", background="#d8f3dc")  # greenish
        self.db_tree.tag_configure("sl", background="#f8d7da")  # reddish

        # Status bar
        bot = ttk.Frame(parent)
        bot.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0, 8))
        self.db_status = ttk.Label(bot, text="Ready.")
        self.db_status.pack(side=tk.LEFT)
        # Delete button for selected row (works for all entries)
        ttk.Button(bot, text="Delete Selected", command=self._db_delete_selected).pack(
            side=tk.RIGHT
        )

        self._db_loading = False
        self._db_auto_job: str | None = None
        self._ohlc_loading = False
        self._chart_req_id = 0
        self._chart_active_req_id: int | None = None
        self._mt5_inited = False
        self._chart_quiet_paused = False
        self._chart_last_symbol: str | None = None
        # Proximity chart state
        self._prox_fig = None
        self._prox_ax_bins = None
        self._prox_ax_symbols = None
        self._prox_canvas = None
        self._prox_toolbar = None
        self.prox_status = None
        self.prox_table = None
        self.prox_symbol_combo = None
        self.prox_chart_frame = None
        self._prox_loading = False
        self._prox_auto_job: str | None = None
        self._prox_refresh_job: str | None = None
        # Filter refresh job
        self._filter_refresh_job = None
        # Top Performers state
        self._top_fig = None
        self._top_ax = None
        self._top_canvas = None
        self._top_toolbar = None
        self._top_loading = False
        self.top_status = None
        self.top_table = None
        self.top_chart_frame = None
        self._top_last_data: dict[str, object] | None = None
        self._top_auto_job: str | None = None
        self._top_refresh_job: str | None = None

        # Guard to blank charts during quiet hours even without new selections
        try:
            self.after(30000, self._chart_quiet_guard)
        except Exception:
            pass

    def _make_prox_tab(self, parent) -> None:
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        row1 = ttk.Frame(top)
        row1.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(row1, text="Since(h):").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=1, to=24 * 365, textvariable=self.var_prox_since_hours, width=6
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(row1, text="Min trades:").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=1, to=500, textvariable=self.var_prox_min_trades, width=4
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Button(row1, text="Refresh", command=self._prox_refresh).pack(side=tk.LEFT)
        ttk.Checkbutton(
            row1,
            text="Auto",
            variable=self.var_prox_auto,
            command=self._prox_auto_toggle,
        ).pack(side=tk.LEFT, padx=(10, 4))
        ttk.Label(row1, text="Every(s):").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=15, to=3600, textvariable=self.var_prox_interval, width=6
        ).pack(side=tk.LEFT, padx=(4, 10))

        row2 = ttk.Frame(top)
        row2.pack(side=tk.TOP, fill=tk.X, pady=(4, 0))
        ttk.Label(row2, text="Category:").pack(side=tk.LEFT)
        ttk.Combobox(
            row2,
            textvariable=self.var_prox_category,
            values=["All", "Forex", "Crypto", "Indices"],
            state="readonly",
            width=10,
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(row2, text="Symbol:").pack(side=tk.LEFT)
        self.prox_symbol_combo = ttk.Combobox(
            row2,
            textvariable=self.var_prox_symbol_choice,
            values=[PROX_SYMBOL_ALL_LABEL],
            state="readonly",
            width=16,
        )
        self.prox_symbol_combo.pack(side=tk.LEFT, padx=(4, 10))
        try:
            initial_symbol = (
                self.var_prox_symbol_filter.get().strip()
                if self.var_prox_symbol_filter is not None
                else ""
            )
            symbols_for_init = [initial_symbol] if initial_symbol else []
            self._prox_update_symbol_dropdown(symbols_for_init)
        except Exception:
            pass

        chart_wrap = ttk.Frame(parent)
        chart_wrap.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        self.prox_status = ttk.Label(
            chart_wrap, text="Proximity stats pending refresh…"
        )
        self.prox_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(0, 4))

        table_frame = ttk.Frame(chart_wrap)
        table_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(0, 6))
        cols = (
            "category",
            "bin",
            "completed",
            "total",
            "pending",
            "tp_pct",
            "avg_rrr",
            "expectancy",
        )
        self.prox_table = ttk.Treeview(
            table_frame, columns=cols, show="headings", height=5
        )
        headings = {
            "category": "Type",
            "bin": "Sweet Spot Bin",
            "completed": "Done",
            "total": "Total",
            "pending": "Pending",
            "tp_pct": "TP%",
            "avg_rrr": "Avg RRR",
            "expectancy": "Edge (R)",
        }
        for col in cols:
            self.prox_table.heading(col, text=headings[col])
        self.prox_table.column("category", width=90, anchor=tk.W)
        self.prox_table.column("bin", width=120, anchor=tk.W)
        self.prox_table.column("completed", width=80, anchor=tk.E)
        self.prox_table.column("total", width=70, anchor=tk.E)
        self.prox_table.column("pending", width=70, anchor=tk.E)
        self.prox_table.column("tp_pct", width=70, anchor=tk.E)
        self.prox_table.column("avg_rrr", width=80, anchor=tk.E)
        self.prox_table.column("expectancy", width=90, anchor=tk.E)
        vs_table = ttk.Scrollbar(
            table_frame, orient=tk.VERTICAL, command=self.prox_table.yview
        )
        self.prox_table.configure(yscrollcommand=vs_table.set)
        self.prox_table.pack(side=tk.LEFT, fill=tk.X, expand=True)
        vs_table.pack(side=tk.RIGHT, fill=tk.Y)

        self.prox_chart_frame = ttk.Frame(chart_wrap)
        self.prox_chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        if FigureCanvasTkAgg is None or Figure is None:
            try:
                self.prox_status.config(
                    text="Matplotlib not available; charts disabled."
                )
            except Exception:
                pass

    def _init_prox_chart_widgets(self) -> None:
        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self.prox_chart_frame is None:
            return
        for w in self.prox_chart_frame.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 4), dpi=100)
        gs = fig.add_gridspec(2, 1, height_ratios=[1, 1.2], hspace=0.32)
        ax_bins = fig.add_subplot(gs[0])
        ax_symbols = fig.add_subplot(gs[1])
        ax_bins.set_ylabel("Expectancy (R)")
        ax_bins.set_ylim(-1.5, 2.5)
        ax_bins.grid(True, axis="y", linestyle="--", alpha=0.3)
        ax_symbols.set_xlabel("Average proximity to SL at entry")
        ax_symbols.set_ylabel("Expectancy (R multiples)")
        ax_symbols.set_ylim(-1.5, 2.5)
        ax_symbols.grid(True, linestyle="--", alpha=0.3)
        canvas = FigureCanvasTkAgg(fig, master=self.prox_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(
                canvas, self.prox_chart_frame, pack_toolbar=False
            )
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
        except Exception:
            toolbar = None
        self._prox_fig = fig
        self._prox_ax_bins = ax_bins
        self._prox_ax_symbols = ax_symbols
        self._prox_canvas = canvas
        self._prox_toolbar = toolbar

    def _prox_auto_toggle(self) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        if self.var_prox_auto.get():
            self._prox_schedule_next(soon=True)
        else:
            if self._prox_auto_job is not None:
                try:
                    self.after_cancel(self._prox_auto_job)
                except Exception:
                    pass
                self._prox_auto_job = None

    def _prox_update_symbol_dropdown(self, symbols: Sequence[str] | None) -> None:
        if self.prox_symbol_combo is None:
            return
        clean_symbols: list[str] = []
        seen = set()
        if symbols:
            for sym in symbols:
                if not isinstance(sym, str):
                    continue
                sym_s = sym.strip()
                if not sym_s or sym_s in seen:
                    continue
                seen.add(sym_s)
                clean_symbols.append(sym_s)
        try:
            clean_symbols.sort()
        except Exception:
            clean_symbols = list(clean_symbols)

        current_filter = ""
        if self.var_prox_symbol_filter is not None:
            try:
                current_filter = self.var_prox_symbol_filter.get().strip()
            except Exception:
                current_filter = ""
        preserve_current = bool(current_filter and not clean_symbols)
        if preserve_current and current_filter not in clean_symbols:
            clean_symbols.append(current_filter)
            try:
                clean_symbols.sort()
            except Exception:
                pass
        values = [PROX_SYMBOL_ALL_LABEL] + clean_symbols
        try:
            self.prox_symbol_combo.configure(values=values)
        except Exception:
            return

        display_value = PROX_SYMBOL_ALL_LABEL if not current_filter else current_filter
        if display_value not in values:
            display_value = PROX_SYMBOL_ALL_LABEL
        if self.var_prox_symbol_choice.get() != display_value:
            self.var_prox_symbol_choice.set(display_value)
        else:
            try:
                self.prox_symbol_combo.set(display_value)
            except Exception:
                pass
        if display_value == PROX_SYMBOL_ALL_LABEL and current_filter:
            if self.var_prox_symbol_filter is not None:
                try:
                    self.var_prox_symbol_filter.set("")
                except Exception:
                    pass

    def _sync_prox_symbol_choice_from_filter(self) -> None:
        current_filter = ""
        if self.var_prox_symbol_filter is not None:
            try:
                current_filter = self.var_prox_symbol_filter.get().strip()
            except Exception:
                current_filter = ""
        display_value = PROX_SYMBOL_ALL_LABEL if not current_filter else current_filter
        if self.var_prox_symbol_choice.get() != display_value:
            self.var_prox_symbol_choice.set(display_value)
        elif self.prox_symbol_combo is not None:
            try:
                self.prox_symbol_combo.set(display_value)
            except Exception:
                pass

    def _on_prox_symbol_choice_changed(self, *args) -> None:
        if self.var_prox_symbol_filter is None:
            return
        choice = self.var_prox_symbol_choice.get().strip()
        actual = "" if choice == PROX_SYMBOL_ALL_LABEL else choice
        try:
            current = self.var_prox_symbol_filter.get()
        except Exception:
            current = ""
        if current == actual:
            return
        try:
            self.var_prox_symbol_filter.set(actual)
        except Exception:
            pass

    def _on_prox_category_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        if self.var_prox_symbol_filter is not None:
            try:
                self.var_prox_symbol_filter.set("")
            except Exception:
                pass
        try:
            self._sync_prox_symbol_choice_from_filter()
        except Exception:
            pass
        self._schedule_prox_refresh()

    def _prox_schedule_next(self, soon: bool = False) -> None:
        if not self.var_prox_auto.get():
            return
        delay = 1000 if soon else max(5, int(self.var_prox_interval.get())) * 1000
        if self._prox_auto_job is not None:
            try:
                self.after_cancel(self._prox_auto_job)
            except Exception:
                pass
            self._prox_auto_job = None
        self._prox_auto_job = self.after(delay, self._prox_refresh)

    def _schedule_prox_refresh(self, delay_ms: int = 350) -> None:
        if self._prox_refresh_job is not None:
            try:
                self.after_cancel(self._prox_refresh_job)
            except Exception:
                pass
            self._prox_refresh_job = None
        self._prox_refresh_job = self.after(delay_ms, self._prox_refresh)

    def _prox_refresh(self) -> None:
        if self._prox_loading:
            return
        if self._prox_refresh_job is not None:
            try:
                self.after_cancel(self._prox_refresh_job)
            except Exception:
                pass
            self._prox_refresh_job = None
        if self.prox_status is not None:
            try:
                self.prox_status.config(text="Loading proximity stats…")
            except Exception:
                pass
        self._prox_loading = True
        threading.Thread(target=self._prox_fetch_thread, daemon=True).start()

    def _prox_fetch_thread(self) -> None:
        dbname = self.var_db_name.get().strip()
        hours = max(1, int(self.var_prox_since_hours.get()))
        min_trades = max(1, int(self.var_prox_min_trades.get()))
        symbol_filter = self.var_prox_symbol_filter.get().strip()
        category_filter = self.var_prox_category.get()

        payload: dict[str, object] = {
            "error": None,
            "since_hours": hours,
            "min_trades": min_trades,
        }
        rows: list[dict[str, object]] = []
        max_prox = 0.0
        available_symbols: set[str] = set()

        try:
            try:
                import sqlite3  # type: ignore
            except Exception as exc:
                raise RuntimeError(f"sqlite3 not available: {exc}")
            db_path = db_path_str(dbname)
            conn = sqlite3.connect(db_path, timeout=12)
            try:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_setups_inserted_at ON timelapse_setups(inserted_at)"
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_hits_setup_id ON timelapse_hits(setup_id)"
                    )
                except Exception:
                    pass
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_setups'"
                )
                if cur.fetchone() is None:
                    payload["rows"] = []
                else:
                    thr = (
                        datetime.now(timezone.utc) - timedelta(hours=hours)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    sql = """
                        SELECT s.symbol, s.proximity_to_sl, s.rrr, h.hit, h.hit_time
                        FROM timelapse_setups s
                        LEFT JOIN timelapse_hits h ON h.setup_id = s.id
                        WHERE s.inserted_at >= ?
                        ORDER BY s.inserted_at DESC
                        """
                    cur.execute(sql, (thr,))
                    raw_rows = cur.fetchmany(100000) or []
                    max_prox = 0.0
                    for sym, prox_raw, rrr_raw, hit, hit_time in raw_rows:
                        sym_s = str(sym) if sym is not None else ""
                        category = self._classify_symbol(sym_s).title()
                        if category_filter != "All" and category != category_filter:
                            continue
                        if sym_s:
                            available_symbols.add(sym_s)
                        if symbol_filter and symbol_filter.upper() not in sym_s.upper():
                            continue
                        if prox_raw is None:
                            continue
                        try:
                            prox_val = float(prox_raw)
                        except Exception:
                            continue
                        rrr_val = None
                        if rrr_raw is not None:
                            try:
                                rrr_val = float(rrr_raw)
                            except Exception:
                                rrr_val = None
                        max_prox = max(max_prox, prox_val)
                        hit_str = hit or ""
                        outcome = None
                        if isinstance(hit_str, str):
                            u = hit_str.upper()
                            if u == "TP":
                                outcome = "win"
                            elif u == "SL":
                                outcome = "loss"
                        rows.append(
                            {
                                "symbol": sym_s,
                                "category": category,
                                "proximity": prox_val,
                                "rrr": rrr_val,
                                "outcome": outcome,
                            }
                        )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as exc:
            payload["error"] = str(exc)

        payload["rows"] = rows
        payload["max_prox"] = max_prox
        payload["category_filter_used"] = category_filter
        payload["symbol_options"] = sorted(available_symbols)

        self.after(0, lambda: self._prox_apply_result(payload))

    def _prox_apply_result(self, payload: dict[str, object]) -> None:
        self._prox_loading = False
        error = payload.get("error")
        if error:
            if self.prox_status is not None:
                try:
                    self.prox_status.config(text=f"Error: {error}")
                except Exception:
                    pass
            self._prox_schedule_next()
            return
        symbol_options = payload.get("symbol_options")
        if isinstance(symbol_options, (list, tuple)):
            try:
                self._prox_update_symbol_dropdown(list(symbol_options))
            except Exception:
                pass
        rows = payload.get("rows")
        if not isinstance(rows, list):
            rows = []
        try:
            processed = self._prox_compute_stats(rows, payload)
        except Exception as exc:
            if self.prox_status is not None:
                try:
                    self.prox_status.config(text=f"Error during compute: {exc}")
                except Exception:
                    pass
            print(f"[prox_apply_result] compute error: {exc}", file=sys.stderr)
            self._prox_schedule_next()
            return
        try:
            self._prox_render(processed)
        except Exception as exc:
            if self.prox_status is not None:
                try:
                    self.prox_status.config(text=f"Error during render: {exc}")
                except Exception:
                    pass
            print(f"[prox_apply_result] render error: {exc}", file=sys.stderr)
            self._prox_schedule_next()
            return
        if self.prox_status is not None:
            try:
                self.prox_status.config(text="Proximity stats ready.")
            except Exception:
                pass
        self._prox_schedule_next()

    def _prox_compute_stats(
        self, rows: list[dict[str, object]], payload: dict[str, object]
    ) -> dict[str, object]:
        hours = payload.get("since_hours", 0)
        min_trades = payload.get("min_trades", 1)
        try:
            min_trades_int = max(1, int(min_trades))
        except Exception:
            min_trades_int = 1

        proximities = [
            float(r["proximity"])
            for r in rows
            if isinstance(r.get("proximity"), (int, float))
        ]
        max_prox = float(
            payload.get("max_prox") or (max(proximities) if proximities else 0.0)
        )
        bucket = 0.1
        if max_prox <= 0:
            upper = bucket
        else:
            upper = max(bucket, math.ceil(max_prox / bucket) * bucket)
        bins: list[dict[str, object]] = []
        edge_steps = int(round(upper / bucket + 1e-9))
        edges = [round(i * bucket, 4) for i in range(edge_steps + 1)]
        if not edges or edges[-1] < upper - 1e-6:
            edges.append(round(upper, 4))
        for idx in range(len(edges) - 1):
            start = edges[idx]
            end = edges[idx + 1]
            label = f"{start:.1f}-{end:.1f}"
            bins.append(
                {
                    "start": start,
                    "end": end,
                    "label": label,
                    "midpoint": (start + end) / 2.0,
                    "count": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_rrr_wins": 0.0,
                }
            )
        if not bins:
            bins.append(
                {
                    "start": 0.0,
                    "end": bucket,
                    "label": f"0.0-{bucket:.1f}",
                    "midpoint": bucket / 2.0,
                    "count": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_rrr_wins": 0.0,
                }
            )

        def pick_bin(value: float) -> dict[str, object]:
            for i, b in enumerate(bins):
                if value < b["end"] or i == len(bins) - 1:
                    return b
            return bins[-1]

        symbol_stats: dict[str, dict[str, object]] = {}
        category_bins: dict[str, dict[str, dict[str, object]]] = {}
        wins_total = 0
        losses_total = 0
        global_rrr_sum = 0.0

        for row in rows:
            prox = row.get("proximity")
            if not isinstance(prox, (int, float)):
                continue
            outcome = row.get("outcome")
            symbol = str(row.get("symbol") or "")
            category = str(row.get("category") or "Forex")
            rrr_val = row.get("rrr")
            rrr_float: float | None
            if isinstance(rrr_val, (int, float)):
                rrr_float = float(rrr_val)
            else:
                rrr_float = None
            bin_item = pick_bin(float(prox))
            bin_item["count"] = int(bin_item.get("count", 0)) + 1
            if outcome == "win":
                bin_item["wins"] = int(bin_item.get("wins", 0)) + 1
                wins_total += 1
            elif outcome == "loss":
                bin_item["losses"] = int(bin_item.get("losses", 0)) + 1
                losses_total += 1

            stat = symbol_stats.setdefault(
                symbol,
                {
                    "symbol": symbol,
                    "category": category,
                    "trades": 0,
                    "completed": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_prox": 0.0,
                    "sum_prox_completed": 0.0,
                    "sum_rrr_wins": 0.0,
                },
            )
            stat["trades"] = int(stat["trades"]) + 1
            stat["sum_prox"] = float(stat["sum_prox"]) + float(prox)
            if outcome == "win":
                stat["completed"] = int(stat["completed"]) + 1
                stat["wins"] = int(stat["wins"]) + 1
                stat["sum_prox_completed"] = float(stat["sum_prox_completed"]) + float(
                    prox
                )
                if rrr_float is not None:
                    bin_item["sum_rrr_wins"] = (
                        float(bin_item.get("sum_rrr_wins", 0.0)) + rrr_float
                    )
                    stat["sum_rrr_wins"] = (
                        float(stat.get("sum_rrr_wins", 0.0)) + rrr_float
                    )
                    global_rrr_sum += rrr_float
            elif outcome == "loss":
                stat["completed"] = int(stat["completed"]) + 1
                stat["losses"] = int(stat["losses"]) + 1
                stat["sum_prox_completed"] = float(stat["sum_prox_completed"]) + float(
                    prox
                )

            cat_bins = category_bins.setdefault(category, {})
            bin_label = bin_item.get("label")
            cat_entry = cat_bins.setdefault(
                bin_label,
                {
                    "label": bin_label,
                    "midpoint": bin_item.get("midpoint"),
                    "count": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_rrr_wins": 0.0,
                },
            )
            cat_entry["count"] = int(cat_entry.get("count", 0)) + 1
            if outcome == "win":
                cat_entry["wins"] = int(cat_entry.get("wins", 0)) + 1
                if rrr_float is not None:
                    cat_entry["sum_rrr_wins"] = (
                        float(cat_entry.get("sum_rrr_wins", 0.0)) + rrr_float
                    )
            elif outcome == "loss":
                cat_entry["losses"] = int(cat_entry.get("losses", 0)) + 1

        for b in bins:
            wins_b = int(b.get("wins", 0))
            losses_b = int(b.get("losses", 0))
            completed_b = wins_b + losses_b
            b["completed"] = completed_b
            total_b = int(b.get("count", 0))
            b["pending"] = max(0, total_b - completed_b)
            if completed_b:
                b["success_rate"] = wins_b / completed_b
                sum_rrr_wins = float(b.get("sum_rrr_wins", 0.0))
                avg_rrr = (
                    (sum_rrr_wins / wins_b) if (wins_b and sum_rrr_wins > 0) else None
                )
                b["avg_rrr"] = avg_rrr
                if avg_rrr is not None:
                    success = b["success_rate"]
                    b["expectancy"] = success * avg_rrr - (1 - success)
                else:
                    b["expectancy"] = None
            else:
                b["success_rate"] = None
                b["avg_rrr"] = None
                b["expectancy"] = None

        symbol_entries: list[dict[str, object]] = []
        category_summary: dict[str, dict[str, float]] = {}
        for stat in symbol_stats.values():
            trades = int(stat["trades"])
            completed = int(stat["completed"])
            wins_s = int(stat["wins"])
            losses_s = int(stat["losses"])
            avg_all = float(stat["sum_prox"]) / trades if trades else 0.0
            avg_completed = (
                (float(stat["sum_prox_completed"]) / completed)
                if completed
                else avg_all
            )
            success = (wins_s / completed) if completed else None
            sum_rrr_wins = float(stat.get("sum_rrr_wins", 0.0)) if wins_s else 0.0
            avg_rrr_completed = (
                (sum_rrr_wins / wins_s) if (wins_s and sum_rrr_wins > 0) else None
            )
            expectancy = None
            if success is not None and avg_rrr_completed is not None:
                expectancy = success * avg_rrr_completed - (1 - success)
            entry = {
                "symbol": stat["symbol"],
                "category": stat["category"],
                "trades": trades,
                "completed": completed,
                "wins": wins_s,
                "losses": losses_s,
                "avg_prox": avg_all,
                "avg_prox_completed": avg_completed,
                "success_rate": success,
                "avg_rrr": avg_rrr_completed,
                "expectancy": expectancy,
            }
            symbol_entries.append(entry)
            if completed:
                cat_data = category_summary.setdefault(
                    stat["category"], {"wins": 0, "completed": 0, "sum_rrr_wins": 0.0}
                )
                cat_data["wins"] += wins_s
                cat_data["completed"] += completed
                cat_data["sum_rrr_wins"] += sum_rrr_wins

        eligible_symbols = [
            s
            for s in symbol_entries
            if s.get("success_rate") is not None
            and s.get("expectancy") is not None
            and int(s.get("completed", 0)) >= min_trades_int
        ]
        eligible_symbols.sort(
            key=lambda s: (
                s.get("expectancy") or 0.0,
                s.get("success_rate") or 0.0,
                s.get("completed") or 0,
            ),
            reverse=True,
        )
        best_symbols = eligible_symbols[:3]

        global_completed = wins_total + losses_total
        global_rate = (wins_total / global_completed) if global_completed else None
        global_avg_rrr = (
            (global_rrr_sum / wins_total) if wins_total and global_rrr_sum > 0 else None
        )
        global_expectancy = None
        if global_rate is not None and global_avg_rrr is not None:
            global_expectancy = global_rate * global_avg_rrr - (1 - global_rate)

        sweet_bin = None
        for b in bins:
            completed_b = b.get("completed", 0)
            expectancy = b.get("expectancy")
            if not completed_b or completed_b < max(3, min_trades_int):
                continue
            prev_expectancy = None if sweet_bin is None else sweet_bin.get("expectancy")
            if sweet_bin is None or (
                expectancy is not None
                and (prev_expectancy is None or expectancy > prev_expectancy)
            ):
                sweet_bin = {
                    "label": b["label"],
                    "success_rate": b.get("success_rate"),
                    "completed": completed_b,
                    "avg_rrr": b.get("avg_rrr"),
                    "expectancy": expectancy,
                    "midpoint": b["midpoint"],
                }

        category_sweet_spots: list[dict[str, object]] = []
        cat_summary_fmt = []
        for cat, data in category_summary.items():
            completed_cat = data.get("completed", 0)
            if completed_cat:
                rate_cat = data.get("wins", 0) / completed_cat
                wins_cat = data.get("wins", 0)
                sum_rrr_cat = data.get("sum_rrr_wins", 0.0)
                avg_rrr_cat = (
                    (sum_rrr_cat / wins_cat) if (wins_cat and sum_rrr_cat > 0) else None
                )
                expectancy_cat = None
                if avg_rrr_cat is not None:
                    expectancy_cat = rate_cat * avg_rrr_cat - (1 - rate_cat)
                cat_summary_fmt.append(
                    {
                        "category": cat,
                        "success_rate": rate_cat,
                        "expectancy": expectancy_cat,
                    }
                )

            bins_map = category_bins.get(cat, {})
            best_bin = None
            for bin_label, bin_stats in bins_map.items():
                wins_cat = int(bin_stats.get("wins", 0))
                losses_cat = int(bin_stats.get("losses", 0))
                total_cat = int(bin_stats.get("count", 0))
                completed_cat_bin = wins_cat + losses_cat
                pending_cat = max(0, total_cat - completed_cat_bin)
                success_cat = (
                    (wins_cat / completed_cat_bin) if completed_cat_bin else None
                )
                avg_rrr_cat_bin = None
                if completed_cat_bin:
                    sum_rrr_cat_bin = float(bin_stats.get("sum_rrr_wins", 0.0))
                    if wins_cat and sum_rrr_cat_bin > 0:
                        avg_rrr_cat_bin = sum_rrr_cat_bin / wins_cat
                expectancy_cat_bin = None
                if success_cat is not None and avg_rrr_cat_bin is not None:
                    expectancy_cat_bin = success_cat * avg_rrr_cat_bin - (
                        1 - success_cat
                    )
                bin_stats["success_rate"] = success_cat
                bin_stats["avg_rrr"] = avg_rrr_cat_bin
                bin_stats["expectancy"] = expectancy_cat_bin
                bin_stats["completed"] = completed_cat_bin
                bin_stats["pending"] = pending_cat
                if (
                    completed_cat_bin >= max(3, min_trades_int)
                ) and expectancy_cat_bin is not None:
                    prev_best_expectancy = (
                        None if best_bin is None else best_bin.get("expectancy")
                    )
                    if (
                        best_bin is None
                        or prev_best_expectancy is None
                        or expectancy_cat_bin > prev_best_expectancy
                    ):
                        best_bin = {
                            "category": cat,
                            "label": bin_label,
                            "completed": completed_cat_bin,
                            "total": total_cat,
                            "pending": pending_cat,
                            "success_rate": success_cat,
                            "avg_rrr": avg_rrr_cat_bin,
                            "expectancy": expectancy_cat_bin,
                        }
            if best_bin is not None:
                category_sweet_spots.append(best_bin)

        result = {
            "since_hours": hours,
            "min_trades": min_trades_int,
            "bin_stats": bins,
            "symbol_stats": eligible_symbols,
            "best_symbols": best_symbols,
            "global_success_rate": global_rate,
            "global_avg_rrr": global_avg_rrr,
            "global_expectancy": global_expectancy,
            "completed_trades": global_completed,
            "pending_trades": max(0, len(rows) - global_completed),
            "symbols_seen": len(symbol_stats),
            "sweet_bin": sweet_bin,
            "category_summary": cat_summary_fmt,
            "category_sweet_spots": category_sweet_spots,
        }
        return result

    def _prox_render(self, data: dict[str, object]) -> None:
        if self.prox_status is None:
            return

        status_parts: list[str] = []
        completed = data.get("completed_trades") or 0
        pending = data.get("pending_trades") or 0
        symbols_seen = data.get("symbols_seen") or 0
        since_hours = data.get("since_hours") or 0
        status_parts.append(
            f"{completed} completed / {pending} open across {symbols_seen} symbols (last {since_hours}h)"
        )

        sweet = data.get("sweet_bin") or None
        if sweet and isinstance(sweet, dict) and sweet.get("expectancy") is not None:
            pieces = []
            try:
                sr = sweet.get("success_rate")
                if sr is not None:
                    pieces.append(f"{float(sr) * 100:.1f}% TP")
            except Exception:
                pass
            try:
                avg_rrr = sweet.get("avg_rrr")
                if avg_rrr is not None:
                    pieces.append(f"avg RRR {float(avg_rrr):.2f}")
            except Exception:
                pass
            pieces.append(f"edge {float(sweet['expectancy']):+.2f}R")
            status_parts.append(
                f"Sweet spot {sweet.get('label')} → "
                + ", ".join(pieces)
                + f" on {int(sweet['completed'])} trades"
            )

        best_symbols = data.get("best_symbols") or []
        if isinstance(best_symbols, list) and best_symbols:
            best_bits = []
            for entry in best_symbols:
                try:
                    sym = entry.get("symbol")
                    rate = entry.get("success_rate")
                    expectancy = entry.get("expectancy")
                    avg_rrr = entry.get("avg_rrr")
                    cnt = int(entry.get("completed") or 0)
                    bit = f"{sym}"
                    if expectancy is not None:
                        bit += f" {float(expectancy):+.2f}R"
                    if rate is not None:
                        bit += f" ({float(rate) * 100:.0f}%"
                        if avg_rrr is not None:
                            bit += f" @ {float(avg_rrr):.2f}R"
                        bit += f", {cnt})"
                    else:
                        bit += f" ({cnt})"
                    best_bits.append(bit)
                except Exception:
                    continue
            if best_bits:
                status_parts.append("Leaders: " + ", ".join(best_bits))

        cat_summary = data.get("category_summary") or []
        if isinstance(cat_summary, list) and cat_summary:
            cat_bits = []
            for entry in cat_summary:
                try:
                    cat = entry.get("category")
                    rate = entry.get("success_rate")
                    expectancy = entry.get("expectancy")
                    snippet = f"{cat}"
                    if expectancy is not None:
                        snippet += f" {float(expectancy):+.2f}R"
                    if rate is not None:
                        snippet += f" ({float(rate) * 100:.0f}% TP)"
                    cat_bits.append(snippet)
                except Exception:
                    continue
            if cat_bits:
                status_parts.append("By category: " + ", ".join(cat_bits))

        global_expectancy = data.get("global_expectancy")
        global_avg_rrr = data.get("global_avg_rrr")
        if isinstance(global_expectancy, (int, float)):
            extra = f"Global edge {float(global_expectancy):+.2f}R"
            if isinstance(global_avg_rrr, (int, float)):
                extra += f" @ avg RRR {float(global_avg_rrr):.2f}"
            status_parts.append(extra)

        try:
            self.prox_status.config(text=" | ".join(status_parts))
        except Exception:
            pass

        prox_table = getattr(self, "prox_table", None)
        if prox_table is not None:
            try:
                prox_table.delete(*prox_table.get_children())
            except Exception:
                pass
            table_rows: list[tuple[str, str, int, int, int, str, str, str]] = []
            sweet = data.get("sweet_bin")
            if isinstance(sweet, dict) and sweet.get("expectancy") is not None:
                completed_global = int(data.get("completed_trades") or 0)
                pending_global = int(data.get("pending_trades") or 0)
                total_global = completed_global + pending_global
                sr = sweet.get("success_rate")
                avg_rrr = sweet.get("avg_rrr")
                expectancy = sweet.get("expectancy")
                table_rows.append(
                    (
                        "All",
                        str(sweet.get("label") or ""),
                        completed_global,
                        total_global,
                        pending_global,
                        (
                            f"{float(sr) * 100:.1f}%"
                            if isinstance(sr, (int, float))
                            else "–"
                        ),
                        (
                            f"{float(avg_rrr):.2f}"
                            if isinstance(avg_rrr, (int, float))
                            else "–"
                        ),
                        (
                            f"{float(expectancy):+.2f}"
                            if isinstance(expectancy, (int, float))
                            else "–"
                        ),
                    )
                )

            cat_spots = data.get("category_sweet_spots") or []
            if isinstance(cat_spots, list):
                try:
                    cat_spots = sorted(
                        (spot for spot in cat_spots if isinstance(spot, dict)),
                        key=lambda s: float(s.get("expectancy") or 0.0),
                        reverse=True,
                    )
                except Exception:
                    pass
                for spot in cat_spots:
                    try:
                        category = spot.get("category", "")
                        label = spot.get("label", "")
                        completed = int(spot.get("completed") or 0)
                        total = int(spot.get("total") or completed)
                        pending = int(spot.get("pending") or max(0, total - completed))
                        sr = spot.get("success_rate")
                        avg_rrr = spot.get("avg_rrr")
                        expectancy = spot.get("expectancy")
                        table_rows.append(
                            (
                                str(category or ""),
                                str(label or ""),
                                completed,
                                total,
                                pending,
                                (
                                    f"{float(sr) * 100:.1f}%"
                                    if isinstance(sr, (int, float))
                                    else "–"
                                ),
                                (
                                    f"{float(avg_rrr):.2f}"
                                    if isinstance(avg_rrr, (int, float))
                                    else "–"
                                ),
                                (
                                    f"{float(expectancy):+.2f}"
                                    if isinstance(expectancy, (int, float))
                                    else "–"
                                ),
                            )
                        )
                    except Exception:
                        continue
            if not table_rows:
                table_rows.append(
                    ("–", "Not enough trades yet", 0, 0, 0, "–", "–", "–")
                )
            for row in table_rows:
                try:
                    prox_table.insert("", tk.END, values=row)
                except Exception:
                    continue

        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self._prox_ax_bins is None or self._prox_ax_symbols is None:
            self._init_prox_chart_widgets()
        ax_bins = self._prox_ax_bins
        ax_symbols = self._prox_ax_symbols
        if ax_bins is None or ax_symbols is None:
            return

        ax_bins.clear()
        ax_symbols.clear()
        ax_bins.set_ylabel("Expectancy (R)")
        ax_bins.grid(True, axis="y", linestyle="--", alpha=0.3)
        ax_bins.set_ylim(-1.5, 1.5)
        ax_symbols.set_xlabel("Average proximity to SL at entry")
        ax_symbols.set_ylabel("Expectancy (R multiples)")
        ax_symbols.set_ylim(-1.5, 2.5)
        ax_symbols.grid(True, linestyle="--", alpha=0.3)

        bin_stats = [b for b in (data.get("bin_stats") or []) if isinstance(b, dict)]
        plot_bins = [b for b in bin_stats if (b.get("completed") or 0) > 0]
        sweet_label = None
        sweet = data.get("sweet_bin")
        if isinstance(sweet, dict):
            sweet_label = sweet.get("label")

        global_expectancy = data.get("global_expectancy")

        if plot_bins:
            x_vals = list(range(len(plot_bins)))
            labels = [str(b.get("label")) for b in plot_bins]
            success_rates = [float(b.get("success_rate") or 0.0) for b in plot_bins]
            counts = [int(b.get("completed") or 0) for b in plot_bins]
            expectancies_raw = [b.get("expectancy") for b in plot_bins]
            avg_rrrs = [b.get("avg_rrr") for b in plot_bins]
            colors = [
                "#2ca02c" if b.get("label") == sweet_label else "#4c72b0"
                for b in plot_bins
            ]
            exp_values = []
            for raw in expectancies_raw:
                if isinstance(raw, (int, float)):
                    exp_values.append(float(raw))
                else:
                    exp_values.append(0.0)
            valid_exp = [
                float(raw) for raw in expectancies_raw if isinstance(raw, (int, float))
            ]
            if valid_exp:
                min_val = min(valid_exp + [0.0])
                max_val = max(valid_exp + [0.0])
                padding = max(0.1, (max_val - min_val) * 0.15)
                ax_bins.set_ylim(min_val - padding, max_val + padding)
            else:
                ax_bins.set_ylim(-1.5, 1.5)
            ax_bins.axhline(0, color="#cccccc", linewidth=0.8)
            bars = ax_bins.bar(x_vals, exp_values, color=colors, alpha=0.85)
            span = ax_bins.get_ylim()[1] - ax_bins.get_ylim()[0]
            offset = max(0.1, span * 0.05)
            for xi, bar, sr, count, raw_exp, avg_rrr in zip(
                x_vals, bars, success_rates, counts, expectancies_raw, avg_rrrs
            ):
                center_x = bar.get_x() + bar.get_width() / 2
                if isinstance(raw_exp, (int, float)):
                    exp_text = f"{float(raw_exp):+.2f}R"
                    exp_val = float(raw_exp)
                else:
                    exp_text = "n/a"
                    exp_val = bar.get_height()
                hit_text = None
                if isinstance(sr, (int, float)):
                    hit_text = f"{sr * 100:.0f}% ({count})"
                elif count > 0:
                    hit_text = f"({count})"
                va = "bottom" if exp_val >= 0 else "top"
                text_y = exp_val + (offset if exp_val >= 0 else -offset)
                label_lines = [exp_text]
                if hit_text:
                    label_lines.append(hit_text)
                if isinstance(avg_rrr, (int, float)):
                    label_lines.append(f"RRR {float(avg_rrr):.2f}")
                ax_bins.text(
                    center_x,
                    text_y,
                    "\n".join(label_lines),
                    ha="center",
                    va=va,
                    fontsize=8,
                    color="#2f4b7c",
                )
            ax_bins.set_xticks(x_vals)
            ax_bins.set_xticklabels(labels, rotation=45, ha="right")
        else:
            ax_bins.text(
                0.5,
                0.5,
                "No completed hits in range yet.",
                ha="center",
                va="center",
                transform=ax_bins.transAxes,
                fontsize=10,
            )

        if isinstance(global_expectancy, (int, float)):
            ax_bins.axhline(
                float(global_expectancy),
                color="#dd8452",
                linestyle="--",
                linewidth=1,
                label="Overall expectancy",
            )
            ax_bins.legend(loc="upper left")
            ax_symbols.axhline(
                float(global_expectancy),
                color="#dd8452",
                linestyle="--",
                linewidth=1,
                label="Overall expectancy",
            )

        symbol_stats = [
            s for s in (data.get("symbol_stats") or []) if isinstance(s, dict)
        ]
        if symbol_stats:
            cat_colors = {
                "Forex": "#1f77b4",
                "Crypto": "#ff7f0e",
                "Indices": "#2ca02c",
            }
            used_labels: set[str] = set()
            max_avg = 0.0
            min_exp = None
            max_exp = None
            for entry in symbol_stats:
                avg = float(
                    entry.get("avg_prox_completed") or entry.get("avg_prox") or 0.0
                )
                expectancy = entry.get("expectancy")
                if expectancy is None:
                    continue
                success = entry.get("success_rate")
                completed = int(entry.get("completed") or 0)
                cat = str(entry.get("category") or "Forex")
                color = cat_colors.get(cat, "#7f7f7f")
                label = cat if cat not in used_labels else None
                used_labels.add(cat)
                size = max(50, min(260, 50 + completed * 18))
                edge_color = "#2ca02c" if expectancy > 0 else "#d62728"
                ax_symbols.scatter(
                    avg,
                    expectancy,
                    s=size,
                    color=color,
                    alpha=0.78,
                    edgecolors=edge_color,
                    linewidths=1.0,
                    label=label,
                )
                if entry in (data.get("best_symbols") or []):
                    label_text = entry.get("symbol")
                    if success is not None:
                        label_text += f" {float(success) * 100:.0f}%"
                    ax_symbols.annotate(
                        label_text,
                        xy=(avg, expectancy),
                        xytext=(0, 6),
                        textcoords="offset points",
                        ha="center",
                        fontsize=9,
                    )
                max_avg = max(max_avg, avg)
                if min_exp is None or expectancy < min_exp:
                    min_exp = expectancy
                if max_exp is None or expectancy > max_exp:
                    max_exp = expectancy
            if used_labels:
                ax_symbols.legend(loc="lower right", title="Category")
            ax_symbols.set_xlim(0, max(1.05, max_avg * 1.15))
            if min_exp is not None and max_exp is not None:
                span = max_exp - min_exp
                pad = max(0.2, span * 0.15)
                ax_symbols.set_ylim(min_exp - pad, max_exp + pad)
        else:
            ax_symbols.text(
                0.5,
                0.5,
                f"Need ≥ {data.get('min_trades', 1)} completed trades per symbol",
                ha="center",
                va="center",
                transform=ax_symbols.transAxes,
                fontsize=10,
            )
            ax_symbols.set_xlim(0, 1.0)
            ax_symbols.set_ylim(-1.0, 1.0)
            if isinstance(global_expectancy, (int, float)):
                ax_symbols.legend(loc="lower right")

        sweet = data.get("sweet_bin")
        if isinstance(sweet, dict) and sweet.get("midpoint") is not None:
            try:
                ax_symbols.axvline(
                    float(sweet["midpoint"]),
                    color="#2ca02c",
                    linestyle=":",
                    linewidth=1,
                )
            except Exception:
                pass

        try:
            if self._prox_fig is not None:
                self._prox_fig.tight_layout()
            if self._prox_canvas is not None:
                self._prox_canvas.draw_idle()
        except Exception:
            pass

    def _make_top_tab(self, parent) -> None:
        """Create the Top Performers tab UI: controls + table + chart."""
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        row1 = ttk.Frame(top)
        row1.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(row1, text="Since(h):").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=1, to=24 * 365, textvariable=self.var_top_since_hours, width=6
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(row1, text="Min trades:").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=1, to=500, textvariable=self.var_top_min_trades, width=4
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Label(row1, text="View:").pack(side=tk.LEFT)
        ttk.Combobox(
            row1,
            textvariable=self.var_top_view,
            values=("Top performers", "Worst performers"),
            state="readonly",
            width=16,
        ).pack(side=tk.LEFT, padx=(4, 10))
        ttk.Button(row1, text="Refresh", command=self._top_refresh).pack(side=tk.LEFT)
        ttk.Checkbutton(
            row1, text="Auto", variable=self.var_top_auto, command=self._top_auto_toggle
        ).pack(side=tk.LEFT, padx=(10, 4))
        ttk.Label(row1, text="Every(s):").pack(side=tk.LEFT)
        ttk.Spinbox(
            row1, from_=15, to=3600, textvariable=self.var_top_interval, width=6
        ).pack(side=tk.LEFT, padx=(4, 10))

        chart_wrap = ttk.Frame(parent)
        chart_wrap.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
        self.top_status = ttk.Label(chart_wrap, text="Top performers pending refresh…")
        self.top_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(0, 4))

        table_frame = ttk.Frame(chart_wrap)
        table_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(0, 6))
        cols = (
            "rank",
            "symbol",
            "bins",
            "trades",
            "win_rate",
            "expectancy",
            "avg_rrr",
            "score",
        )
        self.top_table = ttk.Treeview(
            table_frame, columns=cols, show="headings", height=10
        )
        headings = {
            "rank": "Rank",
            "symbol": "Symbol",
            "bins": "Bin",
            "trades": "Trades",
            "win_rate": "Win %",
            "expectancy": "Edge (R)",
            "avg_rrr": "Avg RRR",
            "score": "Score",
        }
        for col in cols:
            self.top_table.heading(col, text=headings[col])
        self.top_table.column("rank", width=50, anchor=tk.CENTER)
        self.top_table.column("symbol", width=100, anchor=tk.W)
        self.top_table.column("bins", width=110, anchor=tk.W)
        self.top_table.column("trades", width=60, anchor=tk.E)
        self.top_table.column("win_rate", width=70, anchor=tk.E)
        self.top_table.column("expectancy", width=80, anchor=tk.E)
        self.top_table.column("avg_rrr", width=80, anchor=tk.E)
        self.top_table.column("score", width=60, anchor=tk.E)
        vs_table = ttk.Scrollbar(
            table_frame, orient=tk.VERTICAL, command=self.top_table.yview
        )
        self.top_table.configure(yscrollcommand=vs_table.set)
        self.top_table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs_table.pack(side=tk.RIGHT, fill=tk.Y)

        self.top_chart_frame = ttk.Frame(chart_wrap)
        self.top_chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        if FigureCanvasTkAgg is None or Figure is None:
            try:
                self.top_status.config(
                    text="Matplotlib not available; charts disabled."
                )
            except Exception:
                pass

    def _top_auto_toggle(self) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        if self.var_top_auto.get():
            self._top_schedule_next(soon=True)
        else:
            if self._top_auto_job is not None:
                try:
                    self.after_cancel(self._top_auto_job)
                except Exception:
                    pass
                self._top_auto_job = None

    def _top_schedule_next(self, soon: bool = False) -> None:
        if not self.var_top_auto.get():
            return
        delay = 1000 if soon else max(5, int(self.var_top_interval.get())) * 1000
        if self._top_auto_job is not None:
            try:
                self.after_cancel(self._top_auto_job)
            except Exception:
                pass
            self._top_auto_job = None
        self._top_auto_job = self.after(delay, self._top_refresh)

    def _schedule_top_refresh(self, delay_ms: int = 350) -> None:
        if self._top_refresh_job is not None:
            try:
                self.after_cancel(self._top_refresh_job)
            except Exception:
                pass
            self._top_refresh_job = None
        self._top_refresh_job = self.after(delay_ms, self._top_refresh)

    def _top_refresh(self) -> None:
        if self._top_loading:
            return
        if self._top_refresh_job is not None:
            try:
                self.after_cancel(self._top_refresh_job)
            except Exception:
                pass
            self._top_refresh_job = None
        if self.top_status is not None:
            try:
                self.top_status.config(text="Loading top performers…")
            except Exception:
                pass
        self._top_loading = True
        threading.Thread(target=self._top_fetch_thread, daemon=True).start()

    def _top_fetch_thread(self) -> None:
        dbname = self.var_db_name.get().strip()
        hours = max(1, int(self.var_top_since_hours.get()))
        min_trades = max(1, int(self.var_top_min_trades.get()))

        payload: dict[str, object] = {
            "error": None,
            "since_hours": hours,
            "min_trades": min_trades,
        }
        rows: list[dict[str, object]] = []

        try:
            try:
                import sqlite3  # type: ignore
            except Exception as exc:
                raise RuntimeError(f"sqlite3 not available: {exc}")
            db_path = db_path_str(dbname)
            conn = sqlite3.connect(db_path, timeout=12)
            try:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_setups_inserted_at ON timelapse_setups(inserted_at)"
                    )
                except Exception:
                    pass
                try:
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_hits_setup_id ON timelapse_hits(setup_id)"
                    )
                except Exception:
                    pass
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_setups'"
                )
                if cur.fetchone() is None:
                    payload["rows"] = []
                else:
                    thr = (
                        datetime.now(timezone.utc) - timedelta(hours=hours)
                    ).strftime("%Y-%m-%d %H:%M:%S")
                    sql = """
                        SELECT s.symbol, s.proximity_to_sl, s.rrr, s.inserted_at,
                               h.hit, h.hit_time, h.entry_price, h.hit_price, h.sl,
                               s.direction, s.proximity_bin
                        FROM timelapse_setups s
                        LEFT JOIN timelapse_hits h ON h.setup_id = s.id
                        WHERE s.inserted_at >= ?
                        ORDER BY s.inserted_at DESC
                        """
                    cur.execute(sql, (thr,))
                    raw_rows = cur.fetchmany(100000) or []
                    for (
                        symbol,
                        prox_raw,
                        rrr_raw,
                        inserted_at,
                        hit,
                        hit_time,
                        entry_price,
                        hit_price,
                        sl_val,
                        direction,
                        proximity_bin,
                    ) in raw_rows:
                        symbol_s = str(symbol) if symbol is not None else ""
                        if prox_raw is None:
                            continue
                        try:
                            prox_val = float(prox_raw)
                        except Exception:
                            continue
                        rrr_val = None
                        if rrr_raw is not None:
                            try:
                                rrr_val = float(rrr_raw)
                            except Exception:
                                rrr_val = None
                        hit_str = hit or ""
                        outcome = None
                        if isinstance(hit_str, str):
                            u = hit_str.upper()
                            if u == "TP":
                                outcome = "win"
                            elif u == "SL":
                                outcome = "loss"

                        # Calculate trade R multiple
                        trade_r = None
                        if (
                            outcome in ["win", "loss"]
                            and entry_price
                            and hit_price
                            and sl_val
                        ):
                            try:
                                ep = float(entry_price)
                                hp = float(hit_price)
                                slp = float(sl_val)
                                dir_s = (str(direction) or "").lower()
                                profit = (hp - ep) if dir_s == "buy" else (ep - hp)
                                risk = (ep - slp) if dir_s == "buy" else (slp - ep)
                                if risk > 0:
                                    trade_r = profit / risk
                            except Exception:
                                pass

                        rows.append(
                            {
                                "symbol": symbol_s,
                                "proximity": prox_val,
                                "rrr": rrr_val,
                                "outcome": outcome,
                                "trade_r": trade_r,
                                "inserted_at": inserted_at,
                                "hit_time": hit_time,
                                "proximity_bin": str(proximity_bin or "") or "",
                            }
                        )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as exc:
            payload["error"] = str(exc)

        # Build expectancy per (symbol, proximity_bin) within the lookback window.
        bin_totals: dict[tuple[str, str], dict[str, float]] = {}
        for row in rows:
            outcome = row.get("outcome")
            trade_r = row.get("trade_r")
            prox_bin = row.get("proximity_bin") or ""
            symbol = row.get("symbol")
            if outcome not in ["win", "loss"]:
                continue
            if not prox_bin or not isinstance(trade_r, (int, float)):
                continue
            key = (str(symbol or ""), str(prox_bin))
            agg = bin_totals.setdefault(key, {"sum": 0.0, "count": 0.0})
            agg["sum"] += float(trade_r)
            agg["count"] += 1.0

        expectancy_by_bin: dict[tuple[str, str], float] = {}
        eligible_bins: set[tuple[str, str]] = set()
        for key, agg in bin_totals.items():
            count = agg.get("count", 0.0) or 0.0
            if count <= 0.0:
                continue
            expectancy = agg.get("sum", 0.0) / count
            expectancy_by_bin[key] = expectancy
            eligible_bins.add(key)

        filtered_rows: list[dict[str, object]] = []
        for row in rows:
            symbol = str(row.get("symbol") or "")
            prox_bin = str(row.get("proximity_bin") or "")
            if not prox_bin:
                continue
            key = (symbol, prox_bin)
            # Only include rows from bins that meet both criteria
            if key in eligible_bins:
                row["bin_expectancy"] = expectancy_by_bin.get(key)
                filtered_rows.append(row)

        payload["rows"] = filtered_rows

        self.after(0, lambda: self._top_apply_result(payload))

    def _top_apply_result(self, payload: dict[str, object]) -> None:
        self._top_loading = False
        error = payload.get("error")
        if error:
            if self.top_status is not None:
                try:
                    self.top_status.config(text=f"Error: {error}")
                except Exception:
                    pass
            self._top_schedule_next()
            return

        rows = payload.get("rows")
        if not isinstance(rows, list):
            rows = []

        try:
            processed = self._top_compute_stats(rows, payload)
        except Exception as exc:
            if self.top_status is not None:
                try:
                    self.top_status.config(text=f"Error during compute: {exc}")
                except Exception:
                    pass
            print(f"[top_apply_result] compute error: {exc}", file=sys.stderr)
            self._top_schedule_next()
            return

        self._top_last_data = processed

        try:
            self._top_render(processed)
        except Exception as exc:
            if self.top_status is not None:
                try:
                    self.top_status.config(text=f"Error during render: {exc}")
                except Exception:
                    pass
            print(f"[top_apply_result] render error: {exc}", file=sys.stderr)
            self._top_schedule_next()
            return

        if self.top_status is not None:
            try:
                self.top_status.config(text="Top performers ready.")
            except Exception:
                pass
        self._top_schedule_next()

    def _top_compute_stats(
        self, rows: list[dict[str, object]], payload: dict[str, object]
    ) -> dict[str, object]:
        hours = payload.get("since_hours", 0)
        min_trades = payload.get("min_trades", 1)
        try:
            min_trades_int = max(1, int(min_trades))
        except Exception:
            min_trades_int = 1

        bin_stats: dict[tuple[str, str], dict[str, object]] = {}
        unique_symbols: set[str] = set()

        for row in rows:
            symbol = str(row.get("symbol") or "").strip()
            prox_bin = str(row.get("proximity_bin") or "").strip()
            if not symbol or not prox_bin:
                continue
            unique_symbols.add(symbol)

            outcome = row.get("outcome")
            rrr_val = row.get("rrr")
            trade_r = row.get("trade_r")
            inserted_at = row.get("inserted_at")
            hit_time = row.get("hit_time")

            event_time = hit_time if hit_time else inserted_at
            key = (symbol, prox_bin)

            stat = bin_stats.setdefault(
                key,
                {
                    "symbol": symbol,
                    "bin": prox_bin,
                    "trades": 0,
                    "completed": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_rrr_wins": 0.0,
                    "sum_trade_r": 0.0,
                    "recent_trades": [],
                    "bin_expectancy": None,
                },
            )

            stat["trades"] = int(stat.get("trades", 0)) + 1

            if event_time:
                stat["recent_trades"].append(
                    {"time": event_time, "outcome": outcome, "trade_r": trade_r}
                )

            if outcome in ["win", "loss"]:
                stat["completed"] = int(stat.get("completed", 0)) + 1
                if outcome == "win":
                    stat["wins"] = int(stat.get("wins", 0)) + 1
                else:
                    stat["losses"] = int(stat.get("losses", 0)) + 1

                if trade_r is not None:
                    try:
                        stat["sum_trade_r"] = float(
                            stat.get("sum_trade_r", 0.0)
                        ) + float(trade_r)
                    except Exception:
                        pass

                if outcome == "win" and rrr_val is not None:
                    try:
                        stat["sum_rrr_wins"] = float(
                            stat.get("sum_rrr_wins", 0.0)
                        ) + float(rrr_val)
                    except Exception:
                        pass

            if stat.get("bin_expectancy") is None:
                bin_exp = row.get("bin_expectancy")
                if isinstance(bin_exp, (int, float)):
                    stat["bin_expectancy"] = float(bin_exp)

        now = datetime.now(timezone.utc)
        results: list[dict[str, object]] = []
        eligible_bins = 0

        for (symbol, bin_name), stat in bin_stats.items():
            completed = int(stat.get("completed", 0))
            if completed < min_trades_int:
                continue
            eligible_bins += 1
            wins = int(stat.get("wins", 0))

            win_rate = wins / completed if completed > 0 else 0.0
            sum_trade_r = float(stat.get("sum_trade_r", 0.0) or 0.0)
            avg_trade_r = sum_trade_r / completed if completed > 0 else 0.0
            sum_rrr_wins = float(stat.get("sum_rrr_wins", 0.0) or 0.0)
            avg_rrr = sum_rrr_wins / wins if wins > 0 else 0.0

            frequency_factor = min(1.0, completed / 50.0)

            recency_factor = 0.0
            recent_trades = stat.get("recent_trades") or []
            if recent_trades:
                recent_weight = 0.0
                recent_score = 0.0
                for trade in recent_trades:
                    try:
                        trade_time = trade["time"]
                        if isinstance(trade_time, str):
                            trade_dt = datetime.fromisoformat(
                                trade_time.replace("Z", "+00:00")
                            )
                        else:
                            trade_dt = trade_time

                        days_ago = (now - trade_dt).days
                        if days_ago <= 30:
                            weight = 1.0 - (days_ago / 30.0)
                            recent_weight += weight
                            recent_score += weight if trade["outcome"] == "win" else 0.0
                    except Exception:
                        continue

                if recent_weight > 0:
                    recency_factor = recent_score / recent_weight

            confidence_boost = math.log1p(completed)
            score = avg_trade_r * (1.0 + confidence_boost) + 0.1 * recency_factor

            bin_expectancy = stat.get("bin_expectancy")
            if isinstance(bin_expectancy, (int, float)):
                expectancy_val = float(bin_expectancy)
            else:
                expectancy_val = avg_trade_r

            results.append(
                {
                    "symbol": symbol,
                    "bin": bin_name,
                    "bin_label": bin_name,
                    "trades": completed,
                    "win_rate": win_rate,
                    "expectancy": expectancy_val,
                    "avg_rrr": avg_rrr,
                    "score": score,
                    "frequency_factor": frequency_factor,
                    "recency_factor": recency_factor,
                    "bins": [bin_name],
                }
            )

        results_desc = sorted(results, key=lambda x: x["score"], reverse=True)
        top_results = [
            r
            for r in results_desc
            if r.get("score", 0.0) > TOP_SCORE_MIN
            and r.get("expectancy", 0.0) > TOP_EXPECTANCY_MIN_EDGE
        ]
        worst_candidates = [
            r
            for r in results
            if r.get("score", 0.0) < WORST_SCORE_MAX
            and r.get("expectancy", 0.0) < WORST_EXPECTANCY_MAX_EDGE
        ]
        worst_results = sorted(worst_candidates, key=lambda x: x["score"])[:25]
        if not worst_results:
            fallback = [r for r in results if r.get("score", 0.0) <= 0.0]
            if not fallback:
                fallback = [r for r in results if r.get("expectancy", 0.0) < 0.0]
            worst_results = sorted(fallback, key=lambda x: x["score"])[:25]

        unique_symbol_count = len(unique_symbols)

        return {
            "top_performers": top_results,
            "worst_performers": list(worst_results),
            "total_bins": eligible_bins,
            "total_symbols": unique_symbol_count,
            "unique_symbols": unique_symbol_count,
            "since_hours": hours,
            "min_trades": min_trades_int,
        }

    def _top_render(self, data: dict[str, object]) -> None:
        if self.top_status is None:
            return

        view_value = (self.var_top_view.get() or "Top performers").strip().lower()
        if view_value.startswith("worst"):
            performers = data.get("worst_performers", [])
            label = "Worst"
            descriptor = "lagging bins"
            view_kind = "worst"
        else:
            performers = data.get("top_performers", [])
            label = "Top"
            descriptor = "profitable bins"
            view_kind = "top"

        if not isinstance(performers, list):
            performers = list(performers) if performers else []
        total_bins = data.get("total_bins")
        if not isinstance(total_bins, int):
            total_bins = len(performers)
        unique_symbols = data.get("unique_symbols")
        if not isinstance(unique_symbols, int):
            unique_symbols = data.get("total_symbols", 0)
        since_hours = data.get("since_hours", 0)
        min_trades = data.get("min_trades", 1)

        # Update status
        try:
            status_text = (
                f"{label} {len(performers)} {descriptor} across {total_bins} bins "
                f"({unique_symbols} symbols, last {since_hours}h, "
                f"min {min_trades} trades)"
            )
            self.top_status.config(text=status_text)
        except Exception:
            pass

        # Update table
        top_table = getattr(self, "top_table", None)
        if top_table is not None:
            try:
                top_table.delete(*top_table.get_children())
            except Exception:
                pass

            for i, performer in enumerate(performers, 1):
                try:
                    symbol = performer.get("symbol", "")
                    bin_label = (
                        performer.get("bin")
                        or performer.get("bin_label")
                        or performer.get("bins")
                        or ""
                    )
                    if isinstance(bin_label, (list, tuple, set)):
                        bin_display = ", ".join(sorted(str(b) for b in bin_label if b))
                    else:
                        bin_display = str(bin_label)
                    trades = performer.get("trades", 0)
                    win_rate = performer.get("win_rate", 0.0)
                    expectancy = performer.get("expectancy", 0.0)
                    avg_rrr = performer.get("avg_rrr", 0.0)
                    score = performer.get("score", 0.0)

                    row = (
                        i,
                        symbol,
                        bin_display,
                        trades,
                        f"{win_rate * 100:.1f}%",
                        f"{expectancy:+.2f}",
                        f"{avg_rrr:.2f}",
                        f"{score:.3f}",
                    )
                    top_table.insert("", tk.END, values=row)
                except Exception:
                    continue

        # Render chart if matplotlib is available
        if FigureCanvasTkAgg is not None and Figure is not None:
            self._top_render_chart(performers, view_kind)

    def _top_render_chart(
        self, performers: list[dict[str, object]], view_kind: str
    ) -> None:
        if self.top_chart_frame is None:
            return

        # Initialize chart widgets if needed
        if self._top_ax is None or self._top_canvas is None:
            self._init_top_chart_widgets()

        if self._top_ax is None:
            return

        ax = self._top_ax
        ax.clear()

        if isinstance(performers, list):
            performers_list = performers[:10]
        else:
            performers_list = list(performers)[:10]
        if not performers_list:
            ax.text(
                0.5,
                0.5,
                "No data available",
                ha="center",
                va="center",
                transform=ax.transAxes,
                fontsize=12,
            )
            if self._top_canvas is not None:
                self._top_canvas.draw_idle()
            return

        # Prepare data for chart
        labels = []
        for p in performers_list:
            symbol = p.get("symbol", "")
            bin_label = p.get("bin") or p.get("bin_label") or ""
            if isinstance(bin_label, (list, tuple, set)):
                bin_label = ", ".join(str(b) for b in bin_label if b)
            label_text = f"{symbol} [{bin_label}]" if bin_label else str(symbol)
            labels.append(label_text)
        scores = [float(p.get("score", 0.0) or 0.0) for p in performers_list]
        win_rates = [float(p.get("win_rate", 0.0) or 0.0) for p in performers_list]

        # Create horizontal bar chart
        y_pos = range(len(labels))
        bars = ax.barh(y_pos, scores, color="#2ca02c", alpha=0.7)

        # Color bars based on win rate
        for i, (bar, win_rate) in enumerate(zip(bars, win_rates)):
            if view_kind == "worst":
                bar.set_color("#d62728")  # Red for underperformers
            else:
                if win_rate >= 0.7:
                    bar.set_color("#2ca02c")  # Green for high win rate
                elif win_rate >= 0.5:
                    bar.set_color("#ff7f0e")  # Orange for medium win rate
                else:
                    bar.set_color("#d62728")  # Red for low win rate

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels)
        ax.set_xlabel("Performance Score")
        if view_kind == "worst":
            ax.set_title("Worst 10 Performing Bins")
        else:
            ax.set_title("Top 10 Performing Bins")
        ax.grid(True, axis="x", linestyle="--", alpha=0.3)
        ax.axvline(0, color="#999999", linewidth=1, linestyle="--", alpha=0.6)

        # Add score labels on bars
        for i, (bar, score) in enumerate(zip(bars, scores)):
            width = bar.get_width()
            if width >= 0:
                x_text = width + max(abs(width) * 0.05, 0.01)
                ha = "left"
            else:
                x_text = width - max(abs(width) * 0.05, 0.01)
                ha = "right"
            ax.text(
                x_text,
                bar.get_y() + bar.get_height() / 2,
                f"{score:.3f}",
                ha=ha,
                va="center",
                fontsize=9,
            )

        if view_kind == "worst":
            min_score = min(scores) if scores else -1.0
            if min_score >= 0:
                min_score = -1.0
            ax.set_xlim(min_score * 1.15, 0)
        else:
            ax.set_xlim(0, max(scores) * 1.15 if scores else 1.0)

        try:
            if self._top_fig is not None:
                self._top_fig.tight_layout()
            if self._top_canvas is not None:
                self._top_canvas.draw_idle()
        except Exception:
            pass

    def _init_top_chart_widgets(self) -> None:
        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self.top_chart_frame is None:
            return

        # Clear existing widgets
        for child in list(self.top_chart_frame.winfo_children()):
            try:
                child.destroy()
            except Exception:
                pass

        fig = Figure(figsize=(8, 6), dpi=100)
        ax = fig.add_subplot(111)

        canvas = FigureCanvasTkAgg(fig, master=self.top_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        try:
            toolbar = NavigationToolbar2Tk(
                canvas, self.top_chart_frame, pack_toolbar=False
            )
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._top_toolbar = toolbar
        except Exception:
            self._top_toolbar = None

        self._top_fig = fig
        self._top_ax = ax
        self._top_canvas = canvas

    def _classify_symbol(self, sym: str) -> str:
        """Heuristically classify a symbol as 'forex', 'crypto', or 'indices'."""

        return classify_symbol(sym)

    def _enqueue_log(self, name: str, text: str) -> None:
        self.log_q.put((name, text))

    def _drain_log(self) -> None:
        try:
            while True:
                name, text = self.log_q.get_nowait()
                if name == "timelapse":
                    self._append_text(self.txt_tl, text)
                elif name == "hits":
                    self._append_text(self.txt_hits, text)
                else:
                    # Fallback: mirror to both
                    self._append_text(self.txt_tl, text)
                    self._append_text(self.txt_hits, text)
        except queue.Empty:
            pass
        self.after(50, self._drain_log)

    LOG_MAX_LINES = 4000  # cap per-text widget lines to avoid unbounded memory growth

    def _append_text(self, widget: tk.Text, s: str) -> None:
        widget.configure(state=tk.NORMAL)
        widget.insert(tk.END, s)
        try:
            end_index = widget.index("end-1c")
            line_count = int(end_index.split(".")[0]) if end_index else 0
        except Exception:
            line_count = 0
        if line_count > self.LOG_MAX_LINES:
            try:
                # Trim oldest lines while keeping at most LOG_MAX_LINES in the widget
                trim_line = line_count - self.LOG_MAX_LINES
                widget.delete("1.0", f"{trim_line + 1}.0")
            except Exception:
                pass
        widget.see(tk.END)
        widget.configure(state=tk.DISABLED)

    def _clear_log(self) -> None:
        # Clear both panes
        for w in (self.txt_tl, self.txt_hits):
            w.configure(state=tk.NORMAL)
            w.delete("1.0", tk.END)
            w.configure(state=tk.DISABLED)

    # --- DB refresh logic ---
    def _db_auto_toggle(self) -> None:
        if self.var_auto.get():
            self._db_schedule_next(soon=True)
        else:
            if self._db_auto_job is not None:
                try:
                    self.after_cancel(self._db_auto_job)
                except Exception:
                    pass
                self._db_auto_job = None

    def _db_schedule_next(self, soon: bool = False) -> None:
        if not self.var_auto.get():
            return
        delay = 1000 if soon else max(1, int(self.var_interval.get())) * 1000
        # ensure only one scheduled job
        if self._db_auto_job is not None:
            try:
                self.after_cancel(self._db_auto_job)
            except Exception:
                pass
            self._db_auto_job = None
        self._db_auto_job = self.after(delay, self._db_refresh)

    def _db_refresh(self) -> None:
        if self._db_loading:
            return
        self._db_loading = True
        self.db_status.config(text="Loading...")
        t = threading.Thread(target=self._db_fetch_thread, daemon=True)
        t.start()

    def _db_fetch_thread(self) -> None:
        dbname = self.var_db_name.get().strip()
        hours = max(1, int(self.var_since_hours.get()))

        # Get filter values
        symbol_category = self.var_symbol_category.get()
        hit_status = self.var_hit_status.get()
        symbol_filter = self.var_symbol_filter.get().strip()

        rows_display: list[tuple[str, str, str, str, str, str, str, str, str, str]] = []
        rows_meta: list[dict] = []
        error: str | None = None
        try:
            # Use SQLite for GUI DB results
            try:
                import sqlite3  # type: ignore
            except Exception as e:
                raise RuntimeError(f"sqlite3 not available: {e}")
            db_path = db_path_str(dbname)
            conn = sqlite3.connect(db_path, timeout=3)
            try:
                cur = conn.cursor()
                # If setups table does not exist, return empty
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_setups'"
                )
                if cur.fetchone() is None:
                    rows_display = []
                else:
                    # Ensure proximity_bin column exists for display
                    try:
                        cur.execute("PRAGMA table_info(timelapse_setups)")
                        cols = {str(r[1]) for r in (cur.fetchall() or [])}
                        if "proximity_bin" not in cols:
                            try:
                                cur.execute(
                                    "ALTER TABLE timelapse_setups ADD COLUMN proximity_bin TEXT"
                                )
                                conn.commit()
                            except Exception:
                                pass
                    except Exception:
                        pass

                    from datetime import timezone as _tz

                    thr = (datetime.now(_tz.utc) - timedelta(hours=hours)).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    sql = """
                        SELECT s.id, s.symbol, s.direction, s.inserted_at,
                               h.hit_time_utc3, h.hit_time, h.hit, h.hit_price,
                               s.tp, s.sl, COALESCE(h.entry_price, s.price) AS entry_price,
                               s.proximity_to_sl, s.proximity_bin
                        FROM timelapse_setups s
                        LEFT JOIN timelapse_hits h ON h.setup_id = s.id
                        WHERE s.inserted_at >= ?
                        ORDER BY s.inserted_at DESC, s.symbol
                        """
                    cur.execute(sql, (thr,))
                    all_rows = cur.fetchall() or []

                    # Apply filters in Python code instead of SQL
                    filtered_rows = []
                    for row in all_rows:
                        (
                            sid,
                            sym,
                            direction,
                            inserted_at,
                            hit_utc3,
                            hit_time,
                            hit,
                            hit_price,
                            tp,
                            sl,
                            entry_price,
                            proximity_to_sl,
                            proximity_bin,
                        ) = row

                        # Apply symbol category filter
                        if symbol_category != "All":
                            classified_category = self._classify_symbol(sym).title()
                            if classified_category != symbol_category:
                                continue

                        # Apply hit status filter
                        if hit_status != "All":
                            if hit_status == "Running":
                                if hit is not None:
                                    continue
                            elif hit_status == "Hits":
                                if hit is None:
                                    continue
                            else:  # TP or SL
                                if hit != hit_status:
                                    continue

                        # Apply symbol filter
                        if symbol_filter:
                            if symbol_filter.upper() not in sym.upper():
                                continue

                        filtered_rows.append(row)

                    # Process filtered rows
                    for (
                        sid,
                        sym,
                        direction,
                        inserted_at,
                        hit_utc3,
                        hit_time,
                        hit,
                        hit_price,
                        tp,
                        sl,
                        entry_price,
                        proximity_to_sl,
                        proximity_bin,
                    ) in filtered_rows:
                        sym_s = str(sym) if sym is not None else ""
                        dir_s = str(direction) if direction is not None else ""
                        try:
                            as_naive = (
                                datetime.fromisoformat(inserted_at)
                                if isinstance(inserted_at, str)
                                else inserted_at
                            )
                        except Exception:
                            as_naive = None
                        ent_s = ""
                        if as_naive is not None:
                            ent_s = (as_naive + timedelta(hours=3)).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        hit_s = ""
                        if hit_utc3 is not None:
                            hit_s = str(hit_utc3)
                        elif hit_time is not None:
                            try:
                                ht = (
                                    datetime.fromisoformat(hit_time)
                                    if isinstance(hit_time, str)
                                    else hit_time
                                )
                                hit_s = (ht + timedelta(hours=3)).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                            except Exception:
                                hit_s = ""
                        hit_str = str(hit) if hit is not None else ""

                        def fmt_price(v):
                            try:
                                if v is None:
                                    return ""
                                return f"{float(v):g}"
                            except Exception:
                                return str(v)

                        tp_s = fmt_price(tp)
                        sl_s = fmt_price(sl)
                        ep_s = fmt_price(entry_price)
                        prox_sl_s = fmt_price(proximity_to_sl)
                        prox_bin_s = (
                            str(proximity_bin)
                            if proximity_bin not in (None, "")
                            else ""
                        )
                        rows_display.append(
                            (
                                sym_s,
                                dir_s,
                                ent_s,
                                hit_s,
                                hit_str,
                                tp_s,
                                sl_s,
                                ep_s,
                                prox_sl_s,
                                prox_bin_s,
                            )
                        )
                        # Raw/meta for chart
                        rows_meta.append(
                            {
                                "iid": None,  # to fill on UI insert
                                "setup_id": sid,
                                "symbol": sym_s,
                                "direction": dir_s,
                                "entry_utc_str": (
                                    as_naive.strftime("%Y-%m-%d %H:%M:%S.%f")
                                    if as_naive
                                    else ""
                                ),
                                "entry_price": (
                                    float(entry_price)
                                    if entry_price is not None
                                    else None
                                ),
                                "tp": float(tp) if tp is not None else None,
                                "sl": float(sl) if sl is not None else None,
                                "hit_kind": hit_str if hit_str else None,
                                "hit_time_utc_str": (
                                    str(hit_time) if hit_time is not None else None
                                ),
                                "proximity_bin": prox_bin_s,
                                "hit_price": (
                                    float(hit_price) if hit_price is not None else None
                                ),
                            }
                        )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            error = str(e)

        # Hand off to UI thread
        self.after(0, self._db_update_ui, rows_display, rows_meta, error)

    def _db_update_ui(self, rows_display, rows_meta, error: str | None) -> None:
        self._db_loading = False

        # Save current selection before clearing
        current_selection = self.db_tree.selection()
        selected_item_data = None
        if current_selection:
            selected_iid = current_selection[0]
            if selected_iid in self._db_row_meta:
                selected_item_data = self._db_row_meta[selected_iid]

        self.db_tree.delete(*self.db_tree.get_children())
        self._db_row_meta.clear()

        if error:
            self.db_status.config(text=f"Error: {error}")
        else:
            new_selected_iid = None
            for idx, (
                sym,
                direction,
                ent_s,
                hit_s,
                hit,
                tp_s,
                sl_s,
                ep_s,
                prox_sl_s,
                prox_bin_s,
            ) in enumerate(rows_display):
                tags = ()
                if hit == "TP":
                    tags = ("tp",)
                elif hit == "SL":
                    tags = ("sl",)
                iid = self.db_tree.insert(
                    "",
                    tk.END,
                    values=(
                        sym,
                        direction,
                        ent_s,
                        hit_s,
                        hit,
                        tp_s,
                        sl_s,
                        ep_s,
                        prox_sl_s,
                        prox_bin_s,
                    ),
                    tags=tags,
                )
                if idx < len(rows_meta):
                    meta = rows_meta[idx]
                    meta["iid"] = iid
                    self._db_row_meta[iid] = meta

                    # Check if this item matches the previously selected item
                    if selected_item_data and not new_selected_iid:
                        if (
                            meta.get("symbol") == selected_item_data.get("symbol")
                            and meta.get("direction")
                            == selected_item_data.get("direction")
                            and meta.get("entry_utc_str")
                            == selected_item_data.get("entry_utc_str")
                        ):
                            new_selected_iid = iid

            # Restore selection if we found a matching item
            if new_selected_iid:
                self.db_tree.selection_set(new_selected_iid)
                self.db_tree.see(new_selected_iid)  # Ensure the item is visible
                self.db_tree.focus_set()  # Set keyboard focus to the treeview
                self.db_tree.focus(new_selected_iid)  # Set focus to the specific item

            self.db_status.config(
                text=f"Rows: {len(rows_display)} - Updated {datetime.now().strftime('%H:%M:%S')}"
            )

        # Schedule next auto refresh if enabled
        self._db_schedule_next()

    def _db_delete_selected(self) -> None:
        # Delete from DB both in timelapse_hits and timelapse_setups for a selected row
        sel = self.db_tree.selection()
        if not sel:
            self.db_status.config(text="Select a row first.")
            return
        iid = sel[0]
        meta = self._db_row_meta.get(iid)
        if not meta:
            self.db_status.config(text="No metadata for selection.")
            return
        setup_id = meta.get("setup_id")
        hit_kind = (meta.get("hit_kind") or "").upper()
        if not setup_id:
            self.db_status.config(text="Missing setup id; cannot delete.")
            return

        # Confirm
        try:
            from tkinter import messagebox

            sym = meta.get("symbol") or ""
            direction = meta.get("direction") or ""
            hit_info = f" and its {hit_kind} hit" if hit_kind in ("TP", "SL") else ""
            if not messagebox.askyesno(
                "Confirm Delete",
                f"Delete setup {setup_id} ({sym} {direction}){hit_info}? This cannot be undone.",
            ):
                return
        except Exception:
            pass

        # Run deletion in a thread then refresh
        def _do_delete():
            dbname = self.var_db_name.get().strip()
            db_path = db_path_str(dbname)
            err = None
            try:
                import sqlite3  # type: ignore

                conn = sqlite3.connect(db_path, timeout=5)
                try:
                    with conn:
                        cur = conn.cursor()
                        # Delete associated hit/state rows first, then setup
                        cur.execute(
                            "DELETE FROM timelapse_hits WHERE setup_id=?", (setup_id,)
                        )
                        cur.execute(
                            "DELETE FROM tp_sl_setup_state WHERE setup_id=?",
                            (setup_id,),
                        )
                        cur.execute(
                            "DELETE FROM timelapse_setups WHERE id=?", (setup_id,)
                        )
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
            except Exception as e:
                err = str(e)

            # UI thread update
            def _after():
                if err:
                    self.db_status.config(text=f"Delete error: {err}")
                else:
                    self.db_status.config(text=f"Deleted setup {setup_id}.")
                    self._db_refresh()

            self.after(0, _after)

        threading.Thread(target=_do_delete, daemon=True).start()

    # --- Chart helpers ---
    def _init_chart_widgets(self) -> None:
        # If Matplotlib not available, just leave status label
        if FigureCanvasTkAgg is None or Figure is None:
            return
        # Destroy previous if any
        for w in self.chart_frame.winfo_children():
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(5, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title("")
        ax.grid(True, which="both", linestyle="--", alpha=0.3)
        ax.set_xlabel("Time (UTC)")
        ax.set_ylabel("Price")
        canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._chart_toolbar = toolbar
        except Exception:
            self._chart_toolbar = None
        self._chart_fig = fig
        self._chart_ax = ax
        self._chart_canvas = canvas

    def _set_chart_message(self, msg: str) -> None:
        try:
            self.chart_status.config(text=msg)
        except Exception:
            pass

    def _chart_spinner_start(self, rid: int) -> None:
        spinner = getattr(self, "chart_spinner", None)
        if spinner is None:
            return
        self._chart_spinner_req_id = rid
        if not self._chart_spinner_visible:
            try:
                spinner.pack(side=tk.TOP, fill=tk.X, padx=4, pady=(2, 4))
                self._chart_spinner_visible = True
            except Exception:
                return
        try:
            spinner.start(12)
        except Exception:
            pass

    def _chart_spinner_stop(self, rid: int | None = None) -> None:
        spinner = getattr(self, "chart_spinner", None)
        if spinner is None:
            return
        if rid is not None and self._chart_spinner_req_id != rid:
            return
        try:
            spinner.stop()
        except Exception:
            pass
        if self._chart_spinner_visible:
            try:
                spinner.pack_forget()
            except Exception:
                pass
            self._chart_spinner_visible = False
        if rid is None or self._chart_spinner_req_id == rid:
            self._chart_spinner_req_id = None

    def _chart_clear(self) -> None:
        if self._chart_ax is None or self._chart_canvas is None:
            return
        try:
            self._chart_ax.clear()
            self._chart_canvas.draw_idle()
        except Exception:
            pass

    def _chart_pause_for_quiet(self) -> None:
        self._chart_quiet_paused = True
        self._chart_active_req_id = None
        self._ohlc_loading = False
        self._chart_spinner_stop()
        self._chart_clear()
        self._set_chart_message(QUIET_CHART_MESSAGE)

    def _chart_quiet_guard(self) -> None:
        try:
            last_symbol = getattr(self, "_chart_last_symbol", None)
            if is_quiet_time(datetime.now(UTC), symbol=last_symbol):
                if not self._chart_quiet_paused:
                    self._chart_pause_for_quiet()
            else:
                self._chart_quiet_paused = False
        finally:
            try:
                self.after(30000, self._chart_quiet_guard)
            except Exception:
                pass

    def _on_db_row_selected(self, event=None) -> None:
        # Debounce if already loading
        if self._ohlc_loading:
            return
        sel = self.db_tree.selection()
        if not sel:
            return
        iid = sel[0]
        meta = self._db_row_meta.get(iid)
        if not meta:
            return
        # Parse needed fields
        symbol = meta.get("symbol")
        direction = (meta.get("direction") or "").lower()
        entry_utc_str = meta.get("entry_utc_str")
        entry_price = meta.get("entry_price")
        tp = meta.get("tp")
        sl = meta.get("sl")
        hit_kind = meta.get("hit_kind")
        hit_time_utc_str = meta.get("hit_time_utc_str")
        hit_price = meta.get("hit_price")
        if not symbol or not entry_utc_str:
            self._set_chart_message(
                "Missing symbol or entry time; cannot render chart."
            )
            return
        self._chart_last_symbol = symbol
        try:
            entry_utc = datetime.fromisoformat(entry_utc_str).replace(tzinfo=UTC)
        except Exception:
            self._set_chart_message("Invalid entry time format.")
            return
        now_utc = datetime.now(UTC)
        if is_quiet_time(now_utc, symbol=symbol):
            self._chart_pause_for_quiet()
            return
        self._chart_quiet_paused = False
        start_utc = entry_utc - timedelta(minutes=20)
        end_utc = datetime.now(UTC)
        self._chart_req_id += 1
        rid = self._chart_req_id
        self._chart_active_req_id = rid
        self._set_chart_message(
            f"Loading 1m chart for {symbol} from {start_utc.strftime('%H:%M')} UTC (inserted time)…"
        )
        self._chart_spinner_start(rid)
        self._ohlc_loading = True
        # Watchdog to avoid indefinite waiting if MT5 blocks
        self.after(8000, self._chart_watchdog, rid, symbol)
        t = threading.Thread(
            target=self._fetch_and_render_chart_thread,
            args=(
                rid,
                symbol,
                direction,
                start_utc,
                end_utc,
                entry_utc,
                entry_price,
                sl,
                tp,
                hit_kind,
                hit_time_utc_str,
                hit_price,
            ),
            daemon=True,
        )
        t.start()

    def _ensure_mt5(self) -> tuple[bool, str | None]:
        if not _MT5_IMPORTED or mt5 is None:
            return (
                False,
                "MetaTrader5 module not available. Install with: pip install MetaTrader5",
            )
        try:
            # If not initialized, initialize now
            if not self._mt5_inited:
                timeout_env = os.environ.get(
                    "TIMELAPSE_MT5_TIMEOUT", os.environ.get("MT5_TIMEOUT", "30")
                )
                retries_env = os.environ.get(
                    "TIMELAPSE_MT5_RETRIES", os.environ.get("MT5_RETRIES", "1")
                )
                portable = str(os.environ.get("MT5_PORTABLE", "0")).strip().lower() in {
                    "1",
                    "true",
                    "yes",
                    "on",
                }
                try:
                    _INIT_MT5(
                        path=_MT5_PATH_OVERRIDE,
                        timeout=int(timeout_env),
                        retries=int(retries_env),
                        portable=portable,
                    )
                except RuntimeError as exc:
                    return False, f"MT5 init error: {exc}"
                self._mt5_inited = True
        except Exception as e:
            return False, f"MT5 init error: {e}"
        return True, None

    def _chart_watchdog(self, rid: int, symbol: str) -> None:
        # If the same request is still running, release lock and inform user
        if self._chart_active_req_id == rid and self._ohlc_loading:
            self._ohlc_loading = False
            self._chart_spinner_stop(rid)
            self._set_chart_message(
                f"Still loading {symbol}… MT5 may be busy. Try again or check terminal."
            )

    def _resolve_symbol(self, base: str) -> tuple[str | None, str | None]:
        # Prefer shared helper
        if _RESOLVE is not None:
            try:
                name = _RESOLVE(base)
                return name, None if name else f"Symbol '{base}' not found in MT5"
            except Exception as e:
                return None, f"resolve_symbol error: {e}"
        # Fallback: try selecting base and first wildcard
        try:
            if mt5.symbol_select(base, True):
                return base, None
            cands = mt5.symbols_get(f"{base}*") or []
            if cands:
                cand = getattr(cands[0], "name", None) or None
                if cand and mt5.symbol_select(cand, True):
                    return cand, None
        except Exception:
            pass
        return None, f"Symbol '{base}' not found in MT5"

    def _server_offset_hours(self, symbol_probe: str) -> int:
        if _GET_OFFS is not None:
            try:
                return int(_GET_OFFS(symbol_probe) or 0)
            except Exception:
                return 0
        # Fallback to 0 if helper not available
        return 0

    def _to_server_naive(self, dt_utc: datetime, offset_h: int) -> datetime:
        if _TO_SERVER is not None:
            try:
                return _TO_SERVER(dt_utc, offset_h)
            except Exception:
                pass
        # Fallback: naive from timestamp shifted by offset hours
        return datetime.fromtimestamp(dt_utc.timestamp() + offset_h * 3600.0)

    def _rate_field(self, rate: object, name: str) -> float | None:
        try:
            value = getattr(rate, name)
        except AttributeError:
            try:
                value = rate[name]  # type: ignore[index]
            except Exception:
                if isinstance(rate, dict):
                    value = rate.get(name)
                else:
                    value = None
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _rate_time(self, rate: object, offset_hours: int) -> datetime | None:
        ts = self._rate_field(rate, "time")
        if ts is None:
            return None
        try:
            dt_server = datetime.fromtimestamp(float(ts), tz=UTC)
        except Exception:
            return None
        return dt_server - timedelta(hours=offset_hours)

    def _rates_to_ohlc_lists(
        self,
        rates: Sequence[object] | None,
        offset_hours: int,
        timeframe_seconds: int,
    ) -> tuple[list[datetime], list[float], list[float], list[float], list[float]]:
        times: list[datetime] = []
        opens: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        closes: list[float] = []
        if not rates:
            return times, opens, highs, lows, closes
        for rate in rates:
            start = self._rate_time(rate, offset_hours)
            if start is None:
                continue
            open_px = self._rate_field(rate, "open")
            high_px = self._rate_field(rate, "high")
            low_px = self._rate_field(rate, "low")
            close_px = self._rate_field(rate, "close")
            if None in (open_px, high_px, low_px, close_px):
                continue
            times.append(start)
            opens.append(open_px)  # type: ignore[arg-type]
            highs.append(high_px)  # type: ignore[arg-type]
            lows.append(low_px)  # type: ignore[arg-type]
            closes.append(close_px)  # type: ignore[arg-type]
        if not times:
            return times, opens, highs, lows, closes
        # Ensure chronological order and clip duplicate entries to the bar span
        packed = sorted(zip(times, opens, highs, lows, closes), key=lambda x: x[0])
        times = []
        opens = []
        highs = []
        lows = []
        closes = []
        last_start: datetime | None = None
        for start, op, hi, lo, cl in packed:
            if last_start is not None and start < last_start:
                continue
            times.append(start)
            opens.append(op)
            highs.append(hi)
            lows.append(lo)
            closes.append(cl)
            last_start = start
        # Ensure each time represents the start of the interval; extend with span if needed downstream
        return times, opens, highs, lows, closes

    def _ticks_to_ohlc_lists(
        self,
        sym_name: str,
        offset_hours: int,
        active_ranges: Sequence[tuple[datetime, datetime]],
        direction: str,
    ) -> tuple[list[datetime], list[float], list[float], list[float], list[float]]:
        ticks_aggregate: list[object] = []
        for window_start, window_end in active_ranges:
            start_srv = self._to_server_naive(window_start, offset_hours)
            end_srv = self._to_server_naive(window_end, offset_hours)
            part = mt5.copy_ticks_range(
                sym_name, start_srv, end_srv, mt5.COPY_TICKS_ALL
            )
            if part is None or len(part) == 0:
                start_naive = window_start.replace(tzinfo=None)
                end_naive = window_end.replace(tzinfo=None)
                part = mt5.copy_ticks_range(
                    sym_name, start_naive, end_naive, mt5.COPY_TICKS_ALL
                )
            if part is None or len(part) == 0:
                continue
            try:
                for row in part:
                    ticks_aggregate.append(row)
            except Exception:
                ticks_aggregate.extend(list(part))
        if not ticks_aggregate:
            return [], [], [], [], []

        minute_data: dict[datetime, list[float]] = defaultdict(list)
        for tick_row in ticks_aggregate:
            try:
                bid = float(getattr(tick_row, "bid"))
            except Exception:
                try:
                    bid = float(tick_row["bid"])
                except Exception:
                    bid = None
            try:
                ask = float(getattr(tick_row, "ask"))
            except Exception:
                try:
                    ask = float(tick_row["ask"])
                except Exception:
                    ask = None
            if bid is None and ask is None:
                continue
            if (direction or "").lower() == "buy":
                price = bid
            elif ask is not None:
                price = ask
            else:
                price = bid
            if price is None:
                continue
            try:
                tms = getattr(tick_row, "time_msc")
            except Exception:
                try:
                    tms = tick_row["time_msc"]
                except Exception:
                    tms = None
            if tms:
                dt_raw = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
            else:
                try:
                    tse = getattr(tk, "time")
                except Exception:
                    try:
                        tse = tk["time"]
                    except Exception:
                        continue
                dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)
            dt_utc = dt_raw - timedelta(hours=offset_hours)
            minute = dt_utc.replace(second=0, microsecond=0)
            minute_data.setdefault(minute, []).append(price)

        if not minute_data:
            return [], [], [], [], []
        times = []
        opens = []
        highs = []
        lows = []
        closes = []
        for minute in sorted(minute_data):
            prices = minute_data[minute]
            if not prices:
                continue
            times.append(minute)
            opens.append(prices[0])
            highs.append(max(prices))
            lows.append(min(prices))
            closes.append(prices[-1])
        return times, opens, highs, lows, closes

    def _fetch_and_render_chart_thread(
        self,
        rid: int,
        symbol: str,
        direction: str,
        start_utc: datetime,
        end_utc: datetime,
        entry_utc: datetime,
        entry_price,
        sl,
        tp,
        hit_kind,
        hit_time_utc_str,
        hit_price,
    ) -> None:
        try:
            # Step 1: MT5 init
            ok, err = self._ensure_mt5()
            if not ok:
                msg = err or "MT5 initialize failed."
                self.after(0, self._chart_render_error, rid, msg)
                return
            self.after(
                0, self._set_chart_message, f"MT5 ready. Resolving symbol {symbol}…"
            )
            # Step 2: Resolve symbol
            sym_name, err2 = self._resolve_symbol(symbol)
            if sym_name is None:
                self.after(
                    0,
                    self._chart_render_error,
                    rid,
                    err2 or f"Symbol '{symbol}' not found.",
                )
                return
            # Step 3: Compute server window
            try:
                offset_h = self._server_offset_hours(sym_name)
                # If a hit exists, cap fetch end to 20 minutes after the hit
                fetch_end_utc = end_utc
                hit_dt = None
                if hit_time_utc_str and hit_kind in ("TP", "SL"):
                    try:
                        hit_dt = datetime.fromisoformat(str(hit_time_utc_str)).replace(
                            tzinfo=UTC
                        )
                        fetch_end_utc = min(
                            end_utc, hit_dt + timedelta(minutes=20, seconds=30)
                        )
                    except Exception:
                        hit_dt = None
                start_server = self._to_server_naive(start_utc, offset_h)
                end_server = self._to_server_naive(fetch_end_utc, offset_h)
            except Exception:
                offset_h = 0
                start_server = start_utc.replace(tzinfo=None)
                end_server = end_utc.replace(tzinfo=None)
            # Step 4: Fetch M1 bars first; fall back to ticks only if necessary
            quiet_segments = list(
                iter_quiet_utc_ranges(
                    start_utc,
                    fetch_end_utc,
                    symbol=symbol,
                )
            )
            active_ranges = list(
                iter_active_utc_ranges(
                    start_utc,
                    fetch_end_utc,
                    symbol=symbol,
                )
            )
            if not active_ranges:
                self.after(
                    0,
                    self._chart_render_error,
                    rid,
                    "Requested window falls entirely inside quiet trading hours (23:45-00:59 UTC+3 for this symbol).",
                )
                return

            timeframe = _TIMEFRAME_M1()
            timeframe_secs = _TIMEFRAME_SECONDS(timeframe)
            self.after(0, self._set_chart_message, f"Fetching M1 bars for {sym_name}…")
            rates = _RATES_RANGE(
                sym_name, timeframe, start_utc, fetch_end_utc, offset_h, trace=False
            )
            times, opens, highs, lows, closes = self._rates_to_ohlc_lists(
                rates, offset_h, timeframe_secs
            )

            if not times:
                self.after(
                    0,
                    self._set_chart_message,
                    f"No bars returned; falling back to raw ticks for {sym_name}…",
                )
                times, opens, highs, lows, closes = self._ticks_to_ohlc_lists(
                    sym_name, offset_h, active_ranges, direction
                )
                if not times:
                    self.after(
                        0,
                        self._chart_render_error,
                        rid,
                        "No price data available for requested range.",
                    )
                    return

            # Hard-trim arrays to include at most 20 minutes AFTER the hit time
            try:
                if (
                    "hit_dt" in locals()
                    and hit_dt is not None
                    and hit_kind in ("TP", "SL")
                    and times
                ):
                    cutoff = hit_dt + timedelta(minutes=20, seconds=30)
                    end_idx = 0
                    for i, t in enumerate(times):
                        if t <= cutoff:
                            end_idx = i
                        else:
                            break
                    times = times[: end_idx + 1]
                    opens = opens[: end_idx + 1]
                    highs = highs[: end_idx + 1]
                    lows = lows[: end_idx + 1]
                    closes = closes[: end_idx + 1]
            except Exception:
                pass

            # Hit info (reuse parsed hit_dt when available)
            if hit_time_utc_str and "hit_dt" not in locals():
                try:
                    hit_dt = datetime.fromisoformat(str(hit_time_utc_str)).replace(
                        tzinfo=UTC
                    )
                except Exception:
                    hit_dt = None

            # If this request is stale, ignore draw
            def _finish():
                if self._chart_active_req_id != rid:
                    return
                if is_quiet_time(datetime.now(UTC), symbol=symbol):
                    self._chart_render_quiet(rid)
                    return
                self._chart_render_draw(
                    rid,
                    symbol,
                    times,
                    opens,
                    highs,
                    lows,
                    closes,
                    entry_utc,
                    entry_price,
                    sl,
                    tp,
                    hit_kind,
                    hit_dt,
                    hit_price,
                    start_utc,
                    end_utc,
                    quiet_segments,
                )

            self.after(0, _finish)
        except Exception as e:
            self.after(0, self._chart_render_error, rid, f"Chart thread error: {e}")

    def _chart_render_error(self, rid: int, msg: str) -> None:
        if self._chart_active_req_id != rid:
            return
        self._ohlc_loading = False
        self._chart_spinner_stop(rid)
        self._set_chart_message(f"Chart error: {msg}")

    def _chart_render_quiet(self, rid: int) -> None:
        if self._chart_active_req_id != rid:
            return
        self._chart_pause_for_quiet()

    def _chart_render_draw(
        self,
        rid: int,
        symbol: str,
        times,
        opens,
        highs,
        lows,
        closes,
        entry_utc: datetime,
        entry_price,
        sl,
        tp,
        hit_kind,
        hit_dt,
        hit_price,
        start_utc: datetime,
        end_utc: datetime,
        quiet_segments: Sequence[tuple[datetime, datetime]] | None,
    ) -> None:
        if self._chart_active_req_id != rid:
            return
        self._ohlc_loading = False
        self._chart_spinner_stop(rid)
        if self._chart_ax is None or self._chart_canvas is None:
            self._init_chart_widgets()
        if self._chart_ax is None:
            self._set_chart_message("Matplotlib not available; cannot render chart.")
            return
        ax = self._chart_ax
        ax.clear()
        ax.grid(True, which="both", linestyle="--", alpha=0.3)
        if quiet_segments is None:
            quiet_segments = []
        if quiet_segments:
            keep_idx: list[int] = []
            for i, t in enumerate(times):
                in_quiet = any(qs <= t < qe for qs, qe in quiet_segments)
                if not in_quiet:
                    keep_idx.append(i)
            if not keep_idx:
                self._chart_render_quiet(rid)
                return
            times = [times[i] for i in keep_idx]
            opens = [opens[i] for i in keep_idx]
            highs = [highs[i] for i in keep_idx]
            lows = [lows[i] for i in keep_idx]
            closes = [closes[i] for i in keep_idx]
        # Convert all times to display timezone (UTC+3)
        try:
            times_disp = [t.astimezone(DISPLAY_TZ) for t in times]
        except Exception:
            times_disp = [t + timedelta(hours=3) for t in times]
        try:
            entry_disp = entry_utc.astimezone(DISPLAY_TZ)
        except Exception:
            entry_disp = entry_utc + timedelta(hours=3)
        hit_disp = None
        if hit_dt is not None:
            try:
                hit_disp = hit_dt.astimezone(DISPLAY_TZ)
            except Exception:
                hit_disp = hit_dt + timedelta(hours=3)
        try:
            start_disp = start_utc.astimezone(DISPLAY_TZ)
            end_disp = end_utc.astimezone(DISPLAY_TZ)
        except Exception:
            start_disp = start_utc + timedelta(hours=3)
            end_disp = end_utc + timedelta(hours=3)

        ax.set_title(
            f"{symbol} | 1m | {entry_disp.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC+3 inserted"
        )
        ax.set_xlabel("Time (UTC+3)")
        ax.set_ylabel("Price")

        # X-axis formatter
        try:
            locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
            # Ensure tick labels are rendered in UTC+3 (DISPLAY_TZ)
            formatter = mdates.ConciseDateFormatter(
                locator, tz=DISPLAY_TZ, show_offset=False
            )
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            pass

        # Draw simple candlesticks directly (robust, no extra deps)
        try:
            import matplotlib.dates as mdates_local
            from matplotlib.patches import Rectangle

            xs = [mdates_local.date2num(t) for t in times_disp]
            # body width ~= 60% of bar spacing
            if len(xs) >= 2:
                w = (xs[1] - xs[0]) * 0.6
            else:
                w = (1.0 / (24 * 60)) * 0.6  # fallback ~ 0.6 minute
            for x, o, h, l, c in zip(xs, opens, highs, lows, closes):
                col = "#2ca02c" if c >= o else "#d62728"  # green/red
                # wick
                ax.vlines(x, l, h, colors=col, linewidth=0.8, alpha=0.9)
                # body (ensure non-zero height is visible)
                bottom = min(o, c)
                height = max(abs(c - o), (max(highs) - min(lows)) * 0.0002)
                ax.add_patch(
                    Rectangle(
                        (x - w / 2, bottom),
                        w,
                        height,
                        facecolor=col,
                        edgecolor=col,
                        linewidth=0.8,
                        alpha=0.8,
                    )
                )
            ax.set_xlim(xs[0], xs[-1])
        except Exception:
            # Ultimate fallback: plot closes
            ax.plot(times_disp, closes, color="#1f77b4", linewidth=1.5, label="Close")

        # Overlays: Entry marker; SL/TP lines
        y_values = [v for v in closes]
        if isinstance(entry_price, (int, float)):
            y_values.append(float(entry_price))
            try:
                # Round entry time to the nearest minute (floor)
                rounded_entry_disp = entry_disp.replace(second=0, microsecond=0)
                # Determine next candle time to place the arrow body over that bar
                next_time = None
                for t in times_disp:
                    if t > rounded_entry_disp:
                        next_time = t
                        break
                if next_time is None:
                    next_time = rounded_entry_disp + timedelta(minutes=5)
                # Draw a left-pointing arrow so its tip is exactly at the rounded entry point
                ax.annotate(
                    "",
                    xy=(rounded_entry_disp, float(entry_price)),
                    xytext=(next_time, float(entry_price)),
                    arrowprops=dict(
                        arrowstyle="-|>", color="tab:blue", lw=1.4, shrinkA=0, shrinkB=0
                    ),
                    zorder=7,
                )
                # Legend proxy so "Entry" shows with a left arrow marker
                ax.plot(
                    [],
                    [],
                    color="tab:blue",
                    marker="<",
                    linestyle="None",
                    label="Entry",
                )
            except Exception:
                pass
        if isinstance(sl, (int, float)):
            y_values.append(float(sl))
            ax.axhline(
                float(sl), color="tab:red", linestyle="-", linewidth=1.0, label="SL"
            )
        if isinstance(tp, (int, float)):
            y_values.append(float(tp))
            ax.axhline(
                float(tp), color="tab:green", linestyle="-", linewidth=1.0, label="TP"
            )

        # Hit marker
        if hit_disp is not None and hit_kind in ("TP", "SL"):
            try:
                color = "skyblue" if hit_kind == "TP" else "orange"
                price = None
                if isinstance(hit_price, (int, float)):
                    price = float(hit_price)
                else:
                    # approximate by close at nearest time
                    try:
                        # find index of closest time
                        idx = min(
                            range(len(times_disp)),
                            key=lambda i: abs(
                                (times_disp[i] - hit_disp).total_seconds()
                            ),
                        )
                        price = closes[idx]
                    except Exception:
                        price = None
                ax.scatter(
                    [hit_disp],
                    [price] if price is not None else [],
                    color=color,
                    s=40,
                    marker="o",
                    zorder=5,
                    label=f"{hit_kind} hit",
                )
            except Exception:
                pass

        # X limits to requested window in display timezone; if hit exists, clamp to 20 min after hit
        left_xlim = None
        right_xlim = None
        try:
            # Round entry time to the nearest minute for consistent positioning
            rounded_entry_disp = entry_disp.replace(second=0, microsecond=0)
            left = (
                min(times_disp[0], rounded_entry_disp, hit_disp)
                if hit_disp
                else min(times_disp[0], rounded_entry_disp)
            )
            right = (
                max(times_disp[-1], rounded_entry_disp, hit_disp)
                if hit_disp
                else max(times_disp[-1], rounded_entry_disp)
            )
            left = min(left, start_disp)
            right = max(right, end_disp)
            # Clamp right edge if hit occurs: include only 20 minutes after the hit time
            if hit_disp is not None:
                # Directly clamp by time rather than index
                right = min(right, hit_disp + timedelta(minutes=20))
            pad_x = timedelta(minutes=2)
            left_xlim = left - pad_x
            right_xlim = right + pad_x
            ax.set_xlim(left_xlim, right_xlim)
            ax.margins(x=0)
        except Exception:
            pass

        # Y limits with padding, computed over visible x-range
        try:
            if left_xlim is not None and right_xlim is not None:
                idxs = [
                    i
                    for i, t in enumerate(times_disp)
                    if (t >= left_xlim and t <= right_xlim)
                ]
            else:
                idxs = list(range(len(times_disp)))
            vis_highs = [highs[i] for i in idxs] if idxs else highs
            vis_lows = [lows[i] for i in idxs] if idxs else lows
            ymin = min(
                [min(vis_lows)]
                + [v for v in (sl, tp, entry_price) if isinstance(v, (int, float))]
            )
            ymax = max(
                [max(vis_highs)]
                + [v for v in (sl, tp, entry_price) if isinstance(v, (int, float))]
            )
            pad = (ymax - ymin) * 0.05 if (ymax > ymin) else 1.0
            ax.set_ylim(ymin - pad, ymax + pad)
        except Exception:
            pass

        # Legend - REMOVED as per request

        # Tight layout
        try:
            if self._chart_fig is not None:
                self._chart_fig.tight_layout()
        except Exception:
            pass
        self._chart_canvas.draw_idle()
        quiet_note = ""
        try:
            if quiet_segments:
                quiet_note = " | quiet window skipped"
        except Exception:
            quiet_note = ""
        self._set_chart_message(
            f"Rendered {symbol} | 1m bars: {len(times)} (using inserted time){quiet_note}"
        )

    # Toggle button helpers
    def _update_buttons(self) -> None:
        try:
            self.btn_tl_toggle.configure(
                text=("Stop" if self.timelapse.is_running() else "Start")
            )
        except Exception:
            pass
        try:
            self.btn_hits_toggle.configure(
                text=("Stop" if self.hits.is_running() else "Start")
            )
        except Exception:
            pass

    def _toggle_timelapse(self) -> None:
        if self.timelapse.is_running():
            self._stop_timelapse()
        else:
            self._start_timelapse()
        self._update_buttons()

    def _toggle_hits(self) -> None:
        if self.hits.is_running():
            self._stop_hits()
        else:
            self._start_hits()
        self._update_buttons()

    def _update_buttons_loop(self) -> None:
        self._update_buttons()
        try:
            self.after(600, self._update_buttons_loop)
        except Exception:
            pass

    def _hits_quiet_guard(self) -> None:
        """Pause/resume the hits monitor when the quiet window is active."""

        now_utc = datetime.now(UTC)
        quiet_active = is_quiet_time(now_utc, asset_kind="crypto")
        try:
            transition = next_quiet_transition(now_utc, asset_kind="crypto")
            delta_ms = int(
                max(1.0, min(60.0, (transition - now_utc).total_seconds())) * 1000
            )
        except Exception:
            delta_ms = 30000

        if quiet_active:
            if self.hits.is_running():
                self.hits.stop()
            if self._hits_should_run and not self._hits_quiet_paused:
                self._enqueue_log(
                    "hits",
                    "Quiet trading window active (23:45-00:59 UTC+3); hits monitor paused.\n",
                )
            self._hits_quiet_paused = True
        else:
            was_paused = self._hits_quiet_paused
            self._hits_quiet_paused = False
            if was_paused and self._hits_should_run and not self.hits.is_running():
                self._enqueue_log(
                    "hits", "Quiet window ended; resuming hits monitor.\n"
                )
                self.hits.start()
        try:
            self._update_buttons()
        except Exception:
            pass
        try:
            self.after(max(5000, min(60000, delta_ms)), self._hits_quiet_guard)
        except Exception:
            pass

    # Button handlers
    def _start_timelapse(self) -> None:
        # Build command dynamically to include exclude list and prox sl if provided
        cmd = ["monitor-setup", "--watch"]
        if _MT5_PATH_OVERRIDE:
            cmd += ["--mt5-path", _MT5_PATH_OVERRIDE]
        try:
            ex = (self.var_exclude_symbols.get() or "").strip()
        except Exception:
            ex = ""
        if ex:
            cmd += ["--exclude", ex]
        self.timelapse.cmd = cmd
        self.timelapse.start()

    def _stop_timelapse(self) -> None:
        self.timelapse.stop()

    def _start_hits(self) -> None:
        self._hits_should_run = True
        now_utc = datetime.now(UTC)
        if is_quiet_time(now_utc, asset_kind="crypto"):
            if not self._hits_quiet_paused:
                self._enqueue_log(
                    "hits",
                    "Quiet trading window active (23:45-00:59 UTC+3); deferring hits monitor start.\n",
                )
            self._hits_quiet_paused = True
            try:
                self._update_buttons()
            except Exception:
                pass
            return
        self.hits.start()
        try:
            self._update_buttons()
        except Exception:
            pass

    def _stop_hits(self) -> None:
        self._hits_should_run = False
        self._hits_quiet_paused = False
        self.hits.stop()
        try:
            self._update_buttons()
        except Exception:
            pass

    def _restart_monitors(self) -> None:
        # Stop the current subprocesses
        self._stop_timelapse()
        self._stop_hits()

        # Save current logs to temporary files
        timelapse_log_path = None
        hits_log_path = None

        try:
            # Get current log content
            self.txt_tl.configure(state=tk.NORMAL)
            timelapse_content = self.txt_tl.get("1.0", tk.END)
            self.txt_tl.configure(state=tk.DISABLED)

            self.txt_hits.configure(state=tk.NORMAL)
            hits_content = self.txt_hits.get("1.0", tk.END)
            self.txt_hits.configure(state=tk.DISABLED)

            # Create temporary files
            if timelapse_content.strip():
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".log", delete=False, encoding="utf-8"
                ) as f:
                    f.write(timelapse_content)
                    timelapse_log_path = f.name

            if hits_content.strip():
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".log", delete=False, encoding="utf-8"
                ) as f:
                    f.write(hits_content)
                    hits_log_path = f.name
        except Exception:
            # If we can't save logs, continue with restart anyway
            timelapse_log_path = None
            hits_log_path = None

        # Start a new instance of the GUI with log restore arguments
        cmd = ["monitor-gui"]
        if timelapse_log_path:
            cmd.extend(["--restore-timelapse-log", timelapse_log_path])
        if hits_log_path:
            cmd.extend(["--restore-hits-log", hits_log_path])

        try:
            subprocess.Popen(cmd, cwd=HERE)
        except Exception as e:
            # If fails, just restart the processes
            print(f"[GUI] restart relaunch failed: {e}")
            self.after(1000, self._do_restart)
            return
        # Close this instance after a short delay
        self.after(2000, self.destroy)

    def _do_restart(self) -> None:
        self._start_timelapse()
        self._start_hits()
        self._update_buttons()

    def _on_close(self) -> None:
        # Stop child processes before exit
        try:
            self._save_settings()
        except Exception:
            pass
        try:
            self.timelapse.stop()
        except Exception:
            pass
        try:
            self.hits.stop()
        except Exception:
            pass
        try:
            if _MT5_IMPORTED and mt5 is not None:
                mt5.shutdown()
        except Exception:
            pass
        self.destroy()

    def _auto_start(self) -> None:
        try:
            self._start_timelapse()
        except Exception:
            pass
        try:
            self._start_hits()
        except Exception:
            pass
        # Initialize toggle labels and keep them updated
        try:
            self._update_buttons()
            self.after(600, self._update_buttons_loop)
        except Exception:
            pass

    # --- Settings persistence ---
    def _settings_path(self) -> str:
        return os.path.join(HERE, "monitor_gui_settings.json")

    def _set_initial_window_state(self) -> None:
        try:
            self.state("zoomed")
        except Exception:
            try:
                self.attributes("-zoomed", True)
            except Exception:
                pass

    def _load_settings(self) -> None:
        path = self._settings_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return
        ex = data.get("exclude_symbols")
        if isinstance(ex, str):
            try:
                self.var_exclude_symbols.set(ex)
            except Exception:
                pass
        since = data.get("since_hours")
        if isinstance(since, int):
            try:
                self.var_since_hours.set(since)
            except Exception:
                pass
        interval = data.get("interval")
        if isinstance(interval, int):
            try:
                self.var_interval.set(interval)
            except Exception:
                pass
        # Load filter settings
        symbol_category = data.get("symbol_category")
        if isinstance(symbol_category, str):
            try:
                self.var_symbol_category.set(symbol_category)
            except Exception:
                pass
        hit_status = data.get("hit_status")
        if isinstance(hit_status, str):
            try:
                self.var_hit_status.set(hit_status)
            except Exception:
                pass
        symbol_filter = data.get("symbol_filter")
        if isinstance(symbol_filter, str):
            try:
                self.var_symbol_filter.set(symbol_filter)
            except Exception:
                pass
        prox_since = data.get("prox_since_hours")
        if isinstance(prox_since, int):
            try:
                self.var_prox_since_hours.set(prox_since)
            except Exception:
                pass
        prox_min = data.get("prox_min_trades")
        if isinstance(prox_min, int):
            try:
                self.var_prox_min_trades.set(prox_min)
            except Exception:
                pass
        prox_symbol_filter = data.get("prox_symbol_filter")
        if isinstance(prox_symbol_filter, str):
            try:
                self.var_prox_symbol_filter.set(prox_symbol_filter)
            except Exception:
                pass
        prox_category = data.get("prox_category")
        if isinstance(prox_category, str):
            try:
                self.var_prox_category.set(prox_category)
            except Exception:
                pass
        prox_auto = data.get("prox_auto")
        if isinstance(prox_auto, bool):
            try:
                self.var_prox_auto.set(prox_auto)
            except Exception:
                pass
        prox_interval = data.get("prox_interval")
        if isinstance(prox_interval, int):
            try:
                self.var_prox_interval.set(prox_interval)
            except Exception:
                pass
        top_since_hours = data.get("top_since_hours")
        if isinstance(top_since_hours, int):
            try:
                self.var_top_since_hours.set(top_since_hours)
            except Exception:
                pass
        top_min_trades = data.get("top_min_trades")
        if isinstance(top_min_trades, int):
            try:
                self.var_top_min_trades.set(top_min_trades)
            except Exception:
                pass
        top_auto = data.get("top_auto")
        if isinstance(top_auto, bool):
            try:
                self.var_top_auto.set(top_auto)
            except Exception:
                pass
        top_interval = data.get("top_interval")
        if isinstance(top_interval, int):
            try:
                self.var_top_interval.set(top_interval)
            except Exception:
                pass
        top_view = data.get("top_view")
        if isinstance(top_view, str):
            try:
                self.var_top_view.set(top_view)
            except Exception:
                pass

    def _save_settings(self) -> None:
        data = {
            "exclude_symbols": (
                self.var_exclude_symbols.get()
                if self.var_exclude_symbols is not None
                else ""
            ),
            "since_hours": (
                self.var_since_hours.get() if self.var_since_hours is not None else 168
            ),
            "interval": (
                self.var_interval.get() if self.var_interval is not None else 60
            ),
            "symbol_category": (
                self.var_symbol_category.get()
                if self.var_symbol_category is not None
                else "All"
            ),
            "hit_status": (
                self.var_hit_status.get() if self.var_hit_status is not None else "All"
            ),
            "symbol_filter": (
                self.var_symbol_filter.get()
                if self.var_symbol_filter is not None
                else ""
            ),
            "prox_since_hours": (
                self.var_prox_since_hours.get()
                if self.var_prox_since_hours is not None
                else 336
            ),
            "prox_min_trades": (
                self.var_prox_min_trades.get()
                if self.var_prox_min_trades is not None
                else 5
            ),
            "prox_symbol_filter": (
                self.var_prox_symbol_filter.get()
                if self.var_prox_symbol_filter is not None
                else ""
            ),
            "prox_category": (
                self.var_prox_category.get()
                if self.var_prox_category is not None
                else "All"
            ),
            "prox_auto": (
                bool(self.var_prox_auto.get())
                if self.var_prox_auto is not None
                else False
            ),
            "prox_interval": (
                self.var_prox_interval.get()
                if self.var_prox_interval is not None
                else 300
            ),
            "top_since_hours": (
                self.var_top_since_hours.get()
                if self.var_top_since_hours is not None
                else 168
            ),
            "top_min_trades": (
                self.var_top_min_trades.get()
                if self.var_top_min_trades is not None
                else 10
            ),
            "top_auto": (
                bool(self.var_top_auto.get()) if self.var_top_auto is not None else True
            ),
            "top_interval": (
                self.var_top_interval.get()
                if self.var_top_interval is not None
                else 300
            ),
            "top_view": (
                self.var_top_view.get()
                if self.var_top_view is not None
                else "Top performers"
            ),
        }
        try:
            with open(self._settings_path(), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _on_exclude_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass

    def _on_prox_setting_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        self._schedule_prox_refresh()

    def _on_top_setting_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        self._schedule_top_refresh()

    def _on_top_view_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass
        if self._top_last_data is not None:
            try:
                self._top_render(self._top_last_data)
            except Exception:
                pass
        else:
            self._schedule_top_refresh()

    def _on_filter_changed(self, *args) -> None:
        """Trigger refresh when filter values change."""
        # Schedule a refresh with a small delay to avoid excessive refreshes
        if (
            hasattr(self, "_filter_refresh_job")
            and self._filter_refresh_job is not None
        ):
            try:
                self.after_cancel(self._filter_refresh_job)
            except Exception:
                pass
        self._filter_refresh_job = self.after(300, self._db_refresh)


def main() -> None:
    args = parse_args()
    app = App(
        restore_timelapse_log=args.restore_timelapse_log,
        restore_hits_log=args.restore_hits_log,
    )
    app.mainloop()


if __name__ == "__main__":
    main()
