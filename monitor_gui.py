#!/usr/bin/env python3
"""
Simple GUI launcher for:
 - timelapse_setups.py --watch
 - check_tp_sl_hits.py --watch

Provides Start/Stop buttons and a shared log output.

Usage:
  python monitor_gui.py

Notes:
  - Uses the same Python interpreter as this script (sys.executable).
  - Runs both child scripts with unbuffered output (-u) so logs stream live.
  - Stops processes via terminate() when pressing Stop or closing the window.
"""

from __future__ import annotations

import os
import sys
import subprocess
import threading
import queue
import signal
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import tkinter as tk
from tkinter import ttk
import json
import argparse
import tempfile
import shutil
from typing import List, Sequence
from monitor.config import db_path_str, default_db_path
from monitor.mt5_client import (
    resolve_symbol as _RESOLVE,
    get_server_offset_hours as _GET_OFFS,
    to_server_naive as _TO_SERVER,
    rates_range_utc as _RATES_RANGE,
    timeframe_m1 as _TIMEFRAME_M1,
    timeframe_seconds as _TIMEFRAME_SECONDS,
)
from monitor.quiet_hours import (
    iter_active_utc_ranges,
    iter_quiet_utc_ranges,
    is_quiet_time,
    next_quiet_transition,
)

# Plotting
try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
    from matplotlib.figure import Figure
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt  # noqa: F401
except Exception:
    FigureCanvasTkAgg = None  # type: ignore
    NavigationToolbar2Tk = None  # type: ignore
    Figure = None  # type: ignore
    mdates = None  # type: ignore


HERE = os.path.dirname(os.path.abspath(__file__))

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the monitor GUI.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description="EASY Insight Monitor GUI")
    parser.add_argument("--restore-timelapse-log",
                       help="Path to timelapse log file to restore on startup")
    parser.add_argument("--restore-hits-log",
                       help="Path to hits log file to restore on startup")
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
            self.proc = subprocess.Popen(
                self.cmd,
                cwd=HERE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                bufsize=1,
                universal_newlines=True,
                creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0),
            )
        except Exception as e:
            self.log_put(self.name, f"Failed to start: {e}\n")
            self.proc = None
            return

        self.log_put(self.name, f"Started: {' '.join(self.cmd)}\n")
        self._reader_thread = threading.Thread(target=self._reader_loop, name=f"{self.name}-reader", daemon=True)
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
                if os.name == 'nt':
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
    def __init__(self, restore_timelapse_log: str | None = None,
                 restore_hits_log: str | None = None) -> None:
        super().__init__()
        self.title("EASY Insight - Timelapse Monitors")
        self.geometry("1000x600")
        self.minsize(800, 400)

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

        self.tab_pnl = ttk.Frame(self.nb)
        self.nb.add(self.tab_pnl, text="PnL")

        # Set DB Results tab as default active tab
        self.nb.select(self.tab_db)

        # User-configurable exclude list for timelapse setups (comma-separated symbols)
        self.var_exclude_symbols = tk.StringVar(value="")
        # Min prox SL for timelapse setups
        self.var_min_prox_sl = tk.StringVar(value="0.25")
        # Max prox SL for timelapse setups
        self.var_max_prox_sl = tk.StringVar(value="0.5")
        # DB tab variables
        self.var_db_name = tk.StringVar(value=str(default_db_path()))
        self.var_since_hours = tk.IntVar(value=168)
        self.var_auto = tk.BooleanVar(value=True)
        self.var_interval = tk.IntVar(value=60)
        # Load persisted settings (if any) before building controls
        try:
            self._load_settings()
        except Exception:
            pass
        # Persist on any change
        try:
            self.var_exclude_symbols.trace_add("write", self._on_exclude_changed)
            self.var_min_prox_sl.trace_add("write", self._on_min_prox_changed)
            self.var_max_prox_sl.trace_add("write", self._on_max_prox_changed)
        except Exception:
            pass

        # UI elements in Monitors tab
        self._make_controls(self.tab_mon)
        self._make_logs(self.tab_mon)

        # Restore logs if provided
        self._restore_logs()

        # UI elements in DB tab
        self._make_db_tab(self.tab_db)
        # UI elements in PnL tab
        self._make_pnl_tab(self.tab_pnl)
        # Ensure DB results refresh once at startup and auto-refresh is active
        try:
            self.var_auto.set(True)
        except Exception:
            pass
        self._db_refresh()
        # Also refresh PnL once at startup
        try:
            self._pnl_refresh()
        except Exception:
            pass

        # Log queue for thread-safe updates
        self.log_q: queue.Queue[tuple[str, str]] = queue.Queue()
        self.after(50, self._drain_log)

        py = sys.executable or "python"
        self.timelapse = ProcController(
            name="timelapse",
            cmd=[py, "-u", "timelapse_setups.py", "--watch"],
            log_put=self._enqueue_log,
        )
        self.hits = ProcController(
            name="hits",
            cmd=[py, "-u", "check_tp_sl_hits.py", "--watch", "--interva", "1"],
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

        # Timelapse controls
        tl = ttk.LabelFrame(frm, text="Timelapse Setups --watch")
        tl.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.Y, expand=True)
        self.btn_tl_toggle = ttk.Button(tl, text="Start", command=self._toggle_timelapse)
        self.btn_tl_toggle.pack(side=tk.TOP, padx=4, pady=6)
        # Exclude symbols input (comma-separated)
        ex_frame = ttk.Frame(tl)
        ex_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=2)
        ttk.Label(ex_frame, text="Exclude (comma):").pack(side=tk.LEFT)
        ent_ex = ttk.Entry(ex_frame, textvariable=self.var_exclude_symbols)
        ent_ex.pack(side=tk.LEFT, padx=(4, 0), fill=tk.X, expand=True)
        # Min Prox SL input
        mps_frame = ttk.Frame(tl)
        mps_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=2)
        ttk.Label(mps_frame, text="Min Prox SL:").pack(side=tk.LEFT)
        ent_mps = ttk.Entry(mps_frame, textvariable=self.var_min_prox_sl)
        ent_mps.pack(side=tk.LEFT, padx=(4, 0), fill=tk.X, expand=True)
        # Max Prox SL input
        mxps_frame = ttk.Frame(tl)
        mxps_frame.pack(side=tk.TOP, fill=tk.X, padx=4, pady=2)
        ttk.Label(mxps_frame, text="Max Prox SL:").pack(side=tk.LEFT)
        ent_mxps = ttk.Entry(mxps_frame, textvariable=self.var_max_prox_sl)
        ent_mxps.pack(side=tk.LEFT, padx=(4, 0), fill=tk.X, expand=True)

        # TP/SL Hits controls
        ht = ttk.LabelFrame(frm, text="TP/SL Hits --watch")
        ht.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.X, expand=True)
        self.btn_hits_toggle = ttk.Button(ht, text="Start", command=self._toggle_hits)
        self.btn_hits_toggle.pack(side=tk.LEFT, padx=4, pady=6)

        # Misc
        misc = ttk.Frame(frm)
        misc.pack(side=tk.RIGHT)
        ttk.Button(misc, text="Clear Log", command=self._clear_log).pack(side=tk.TOP, padx=4, pady=4)

    def _make_logs(self, parent) -> None:
        # Two side-by-side log panes
        paned = ttk.Panedwindow(parent, orient=tk.HORIZONTAL)
        paned.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Timelapse pane
        lf_tl = ttk.LabelFrame(paned, text="Timelapse Log")
        frm_tl = ttk.Frame(lf_tl)
        frm_tl.pack(fill=tk.BOTH, expand=True)
        self.txt_tl = tk.Text(frm_tl, wrap=tk.NONE, state=tk.DISABLED, font=("Consolas", 10))
        self.txt_tl.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs_tl = ttk.Scrollbar(frm_tl, orient=tk.VERTICAL, command=self.txt_tl.yview)
        vs_tl.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt_tl.configure(yscrollcommand=vs_tl.set)

        # Hits pane
        lf_hits = ttk.LabelFrame(paned, text="TP/SL Hits Log")
        frm_hits = ttk.Frame(lf_hits)
        frm_hits.pack(fill=tk.BOTH, expand=True)
        self.txt_hits = tk.Text(frm_hits, wrap=tk.NONE, state=tk.DISABLED, font=("Consolas", 10))
        self.txt_hits.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs_hits = ttk.Scrollbar(frm_hits, orient=tk.VERTICAL, command=self.txt_hits.yview)
        vs_hits.pack(side=tk.RIGHT, fill=tk.Y)
        self.txt_hits.configure(yscrollcommand=vs_hits.set)

        paned.add(lf_tl, weight=1)
        paned.add(lf_hits, weight=1)

    def _restore_logs(self) -> None:
        """Restore logs from files if provided."""
        # Restore timelapse log
        if self.restore_timelapse_log and os.path.exists(self.restore_timelapse_log):
            try:
                with open(self.restore_timelapse_log, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content:
                        self._append_text(self.txt_tl, content)
            except Exception:
                pass  # Ignore errors during restore

        # Restore hits log
        if self.restore_hits_log and os.path.exists(self.restore_hits_log):
            try:
                with open(self.restore_hits_log, 'r', encoding='utf-8') as f:
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
        add_labeled(row1, "Since(h):", ttk.Spinbox(row1, from_=1, to=24*365, textvariable=self.var_since_hours, width=6)).pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row1, text="Refresh", command=self._db_refresh).pack(side=tk.LEFT)
        ttk.Checkbutton(row1, text="Auto", variable=self.var_auto, command=self._db_auto_toggle).pack(side=tk.LEFT, padx=(10, 4))
        add_labeled(row1, "Every(s):", ttk.Spinbox(row1, from_=5, to=3600, textvariable=self.var_interval, width=6)).pack(side=tk.LEFT)
        ttk.Button(row1, text="Restart", command=self._restart_monitors).pack(side=tk.RIGHT, padx=(10, 0))

        # Add filter row
        row2 = ttk.Frame(top)
        row2.pack(side=tk.TOP, fill=tk.X, pady=(4, 0))
        add_labeled(row2, "Category:", ttk.Combobox(row2, textvariable=self.var_symbol_category,
                                                  values=["All", "Forex", "Crypto", "Indices"],
                                                  state="readonly", width=10)).pack(side=tk.LEFT, padx=(0, 10))
        add_labeled(row2, "Status:", ttk.Combobox(row2, textvariable=self.var_hit_status,
                                                 values=["All", "TP", "SL", "Running", "Hits"],
                                                 state="readonly", width=10)).pack(side=tk.LEFT)

        # Tree (table)
        # Splitter: top table, bottom chart
        splitter = ttk.Panedwindow(parent, orient=tk.VERTICAL)
        splitter.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Top: table container
        mid = ttk.Frame(splitter)
        cols = ("symbol", "direction", "entry_utc3", "hit_time_utc3", "hit", "tp", "sl", "entry_price", "proximity_to_sl")
        self.db_tree = ttk.Treeview(mid, columns=cols, show='headings', height=12)
        self.db_tree.heading("symbol", text="Symbol")
        self.db_tree.heading("direction", text="Direction")
        self.db_tree.heading("entry_utc3", text="Inserted UTC+3")
        self.db_tree.heading("hit_time_utc3", text="Hit Time UTC+3")
        self.db_tree.heading("hit", text="Hit")
        self.db_tree.heading("tp", text="TP")
        self.db_tree.heading("sl", text="SL")
        self.db_tree.heading("entry_price", text="Entry Price")
        self.db_tree.heading("proximity_to_sl", text="Prox to SL")
        self.db_tree.column("symbol", width=120, anchor=tk.W)
        self.db_tree.column("direction", width=80, anchor=tk.W)
        self.db_tree.column("entry_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit_time_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit", width=80, anchor=tk.W)
        self.db_tree.column("tp", width=100, anchor=tk.E)
        self.db_tree.column("sl", width=100, anchor=tk.E)
        self.db_tree.column("entry_price", width=120, anchor=tk.E)
        self.db_tree.column("proximity_to_sl", width=100, anchor=tk.E)

        vs = ttk.Scrollbar(mid, orient=tk.VERTICAL, command=self.db_tree.yview)
        self.db_tree.configure(yscrollcommand=vs.set)
        self.db_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vs.pack(side=tk.RIGHT, fill=tk.Y)

        splitter.add(mid, weight=3)

        # Bottom: chart container
        chart_wrap = ttk.Frame(splitter)
        self.chart_status = ttk.Label(chart_wrap, text="Select a row to render 1m chart (Inserted±) with SL/TP.")
        self.chart_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(4, 0))
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
            self.db_tree.bind('<<TreeviewSelect>>', self._on_db_row_selected)
        except Exception:
            pass

        # Row tags for coloring
        self.db_tree.tag_configure('tp', background='#d8f3dc')  # greenish
        self.db_tree.tag_configure('sl', background='#f8d7da')  # reddish

        # Status bar
        bot = ttk.Frame(parent)
        bot.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0, 8))
        self.db_status = ttk.Label(bot, text="Ready.")
        self.db_status.pack(side=tk.LEFT)
        # Delete button for selected row (works for all entries)
        ttk.Button(bot, text="Delete Selected", command=self._db_delete_selected).pack(side=tk.RIGHT)

        self._db_loading = False
        self._db_auto_job: str | None = None
        self._ohlc_loading = False
        self._chart_req_id = 0
        self._chart_active_req_id: int | None = None
        self._mt5_inited = False
        # PnL chart state
        self._pnl_fig = None
        self._pnl_ax = None
        self._pnl_canvas = None
        self._pnl_toolbar = None
        self._pnl_loading = False
        self.pnl_status = None
        self.pnl_chart_frame = None
        # Second PnL (10k notional) chart state
        self._pnl2_fig = None
        self._pnl2_ax = None
        self._pnl2_canvas = None
        self._pnl2_toolbar = None
        self.pnl2_status = None
        self.pnl2_chart_frame = None
        # Category PnL (10k notional) chart states
        self._fx_fig = None
        self._fx_ax = None
        self._fx_canvas = None
        self._fx_toolbar = None
        self._crypto_fig = None
        self._crypto_ax = None
        self._crypto_canvas = None
        self._crypto_toolbar = None
        self._indices_fig = None
        self._indices_ax = None
        self._indices_canvas = None
        self._indices_toolbar = None
        self.pnl_fx_status = None
        self.pnl_fx_chart_frame = None
        self.pnl_crypto_status = None
        self.pnl_crypto_chart_frame = None
        self.pnl_indices_status = None
        self.pnl_indices_chart_frame = None
        # Filter refresh job
        self._filter_refresh_job = None

        # PnL helper methods moved to class level (avoids nested defs in __init__)

        def _pnl_render_draw(self, times, returns, cum, avg) -> None:
            """Draw the PnL chart on the PnL axes."""
            if FigureCanvasTkAgg is None or Figure is None:
                try:
                    if self.pnl_status is not None:
                        self.pnl_status.config(text="Matplotlib not available; cannot render PnL.")
                except Exception:
                    pass
                return
            if self._pnl_ax is None or self._pnl_canvas is None:
                self._init_pnl_chart_widgets()
            ax = self._pnl_ax
            ax.clear()
            ax.grid(True, which='both', linestyle='--', alpha=0.3)

            try:
                times_disp = [t.astimezone(DISPLAY_TZ) for t in times]
            except Exception:
                times_disp = [t + timedelta(hours=3) for t in times]

            # Plot cumulative and avg
            try:
                ax.plot(times_disp, cum, color='#1f77b4', linewidth=2, label='Cumulative PnL (sum of +RRR/-1)')
                ax.plot(times_disp, avg, color='#ff7f0e', linewidth=1.5, linestyle='--', label='Avg PnL per trade')
            except Exception:
                pass

            # scatter markers for wins/losses
            try:
                wins_x = [times_disp[i] for i, v in enumerate(returns) if v > 0]
                wins_y = [cum[i] for i, v in enumerate(returns) if v > 0]
                losses_x = [times_disp[i] for i, v in enumerate(returns) if v < 0]
                losses_y = [cum[i] for i, v in enumerate(returns) if v < 0]
                if wins_x:
                    ax.scatter(wins_x, wins_y, color='green', marker='^', s=40, label='TP')
                if losses_x:
                    ax.scatter(losses_x, losses_y, color='red', marker='v', s=40, label='SL')
            except Exception:
                pass

            # Formatter
            try:
                locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
                formatter = mdates.ConciseDateFormatter(locator, tz=DISPLAY_TZ, show_offset=False)
                ax.xaxis.set_major_locator(locator)
                ax.xaxis.set_major_formatter(formatter)
            except Exception:
                pass

            try:
                ax.legend(loc='upper left')
            except Exception:
                pass
            try:
                if self._pnl_fig is not None:
                    self._pnl_fig.tight_layout()
                self._pnl_canvas.draw_idle()
            except Exception:
                pass
            try:
                if self.pnl_status is not None:
                    self.pnl_status.config(text=f"Rendered PnL: {len(times)} trades, cumulative {cum[-1]:.2f}, avg {avg[-1]:.3f}")
            except Exception:
                pass

    def _make_pnl_tab(self, parent) -> None:
        """Create the PnL tab UI: simple controls + Matplotlib chart."""
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        row1 = ttk.Frame(top)
        row1.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(row1, text="Since(h):").pack(side=tk.LEFT)
        ttk.Spinbox(row1, from_=1, to=24*365, textvariable=self.var_since_hours, width=6).pack(side=tk.LEFT, padx=6)
        ttk.Button(row1, text="Refresh", command=self._pnl_refresh).pack(side=tk.LEFT)

        chart_wrap = ttk.Frame(parent)

        # 10k-notional PnL split into three charts
        self.pnl_fx_status = ttk.Label(chart_wrap, text="Press 'Refresh' to load PnL (10k - Forex).")
        self.pnl_fx_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(4, 0))
        self.pnl_fx_chart_frame = ttk.Frame(chart_wrap)
        self.pnl_fx_chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.pnl_crypto_status = ttk.Label(chart_wrap, text="Press 'Refresh' to load PnL (10k - Crypto).")
        self.pnl_crypto_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(8, 0))
        self.pnl_crypto_chart_frame = ttk.Frame(chart_wrap)
        self.pnl_crypto_chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.pnl_indices_status = ttk.Label(chart_wrap, text="Press 'Refresh' to load PnL (10k - Indices).")
        self.pnl_indices_status.pack(side=tk.TOP, anchor=tk.W, padx=4, pady=(8, 0))
        self.pnl_indices_chart_frame = ttk.Frame(chart_wrap)
        self.pnl_indices_chart_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        chart_wrap.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        # Initialize Matplotlib canvases
        self._init_fx_chart_widgets()
        self._init_crypto_chart_widgets()
        self._init_indices_chart_widgets()

    def _init_pnl_chart_widgets(self) -> None:
        """Initialize Matplotlib widgets for the PnL chart."""
        if FigureCanvasTkAgg is None or Figure is None:
            return
        # Destroy previous widgets if present
        if self.pnl_chart_frame is None:
            return
        for w in (self.pnl_chart_frame.winfo_children() if self.pnl_chart_frame is not None else []):
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title('PnL (normalized wins/losses)')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Normalized PnL')
        canvas = FigureCanvasTkAgg(fig, master=self.pnl_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.pnl_chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._pnl_toolbar = toolbar
        except Exception:
            self._pnl_toolbar = None
        self._pnl_fig = fig
        self._pnl_ax = ax
        self._pnl_canvas = canvas

    def _init_pnl2_chart_widgets(self) -> None:
        """Initialize Matplotlib widgets for the 10k-notional PnL chart."""
        if FigureCanvasTkAgg is None or Figure is None:
            return
    def _init_fx_chart_widgets(self) -> None:
        """Initialize Matplotlib widgets for the 10k-notional Forex PnL chart."""
        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self.pnl_fx_chart_frame is None:
            return
        for w in (self.pnl_fx_chart_frame.winfo_children() if self.pnl_fx_chart_frame is not None else []):
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title('PnL (10k - Forex)')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Profit (quote currency)')
        canvas = FigureCanvasTkAgg(fig, master=self.pnl_fx_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.pnl_fx_chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._fx_toolbar = toolbar
        except Exception:
            self._fx_toolbar = None
        self._fx_fig = fig
        self._fx_ax = ax
        self._fx_canvas = canvas

    def _init_crypto_chart_widgets(self) -> None:
        """Initialize Matplotlib widgets for the 10k-notional Crypto PnL chart."""
        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self.pnl_crypto_chart_frame is None:
            return
        for w in (self.pnl_crypto_chart_frame.winfo_children() if self.pnl_crypto_chart_frame is not None else []):
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title('PnL (10k - Crypto)')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Profit (quote currency)')
        canvas = FigureCanvasTkAgg(fig, master=self.pnl_crypto_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.pnl_crypto_chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._crypto_toolbar = toolbar
        except Exception:
            self._crypto_toolbar = None
        self._crypto_fig = fig
        self._crypto_ax = ax
        self._crypto_canvas = canvas

    def _init_indices_chart_widgets(self) -> None:
        """Initialize Matplotlib widgets for the 10k-notional Indices PnL chart."""
        if FigureCanvasTkAgg is None or Figure is None:
            return
        if self.pnl_indices_chart_frame is None:
            return
        for w in (self.pnl_indices_chart_frame.winfo_children() if self.pnl_indices_chart_frame is not None else []):
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title('PnL (10k - Indices)')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Profit (quote currency)')
        canvas = FigureCanvasTkAgg(fig, master=self.pnl_indices_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.pnl_indices_chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._indices_toolbar = toolbar
        except Exception:
            self._indices_toolbar = None
        self._indices_fig = fig
        self._indices_ax = ax
        self._indices_canvas = canvas
        if self.pnl2_chart_frame is None:
            return
        for w in (self.pnl2_chart_frame.winfo_children() if self.pnl2_chart_frame is not None else []):
            try:
                w.destroy()
            except Exception:
                pass
        fig = Figure(figsize=(6, 3), dpi=100)
        ax = fig.add_subplot(111)
        ax.set_title('PnL (10k notional)')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Profit (quote currency)')
        canvas = FigureCanvasTkAgg(fig, master=self.pnl2_chart_frame)
        canvas_widget = canvas.get_tk_widget()
        canvas_widget.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        try:
            toolbar = NavigationToolbar2Tk(canvas, self.pnl2_chart_frame, pack_toolbar=False)
            toolbar.update()
            toolbar.pack(side=tk.BOTTOM, fill=tk.X)
            self._pnl2_toolbar = toolbar
        except Exception:
            self._pnl2_toolbar = None
        self._pnl2_fig = fig
        self._pnl2_ax = ax
        self._pnl2_canvas = canvas

    def _pnl_refresh(self) -> None:
        """Trigger background fetch of PnL data and redraw chart."""
        if self._pnl_loading:
            return
        self._pnl_loading = True
        try:
            if self.pnl_fx_status is not None:
                self.pnl_fx_status.config(text="Loading PnL (10k - Forex)...")
            if self.pnl_crypto_status is not None:
                self.pnl_crypto_status.config(text="Loading PnL (10k - Crypto)...")
            if self.pnl_indices_status is not None:
                self.pnl_indices_status.config(text="Loading PnL (10k - Indices)...")
        except Exception:
            pass
        t = threading.Thread(target=self._pnl_fetch_thread, daemon=True)
        t.start()

    def _pnl_fetch_thread(self) -> None:
        """Fetch PnL-relevant rows from the SQLite DB in a background thread and compute ATR-normalized P/L."""
        dbname = self.var_db_name.get().strip()
        hours = max(1, int(self.var_since_hours.get()))
        rows: list[tuple] = []
        error: str | None = None
        try:
            import sqlite3  # type: ignore
            db_path = db_path_str(dbname)
            conn = sqlite3.connect(db_path, timeout=3)
            try:
                cur = conn.cursor()
                from datetime import timezone as _tz
                thr = (datetime.now(_tz.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
                sql = (
                    """
                    SELECT COALESCE(h.hit_time, s.inserted_at) as event_time,
                           h.hit,
                           s.symbol,
                           COALESCE(h.entry_price, s.price) AS entry_price,
                           h.hit_price,
                           s.sl,
                           s.direction
                    FROM timelapse_setups s
                    JOIN timelapse_hits h ON h.setup_id = s.id
                    WHERE COALESCE(h.hit_time, s.inserted_at) >= ?
                    ORDER BY COALESCE(h.hit_time, s.inserted_at) ASC
                    """
                )
                cur.execute(sql, (thr,))
                for (event_time, hit, symbol, entry_price, hit_price, sl, direction) in cur.fetchall() or []:
                    rows.append((event_time, hit, symbol, entry_price, hit_price, sl, direction))
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception as e:
            error = str(e)

        # Compute ATR per symbol (D1, period 14-ish) and normalize P/L as profit / ATR.
        times: list[datetime] = []
        norm_returns: list[float] = []
        symbols: list[str] = []  # trade symbol per hit
        notional_returns: list[float] = []
        try:
            atr_map: dict[str, float | None] = {}
            if _MT5_IMPORTED and mt5 is not None:
                try:
                    init_ok, init_err = self._ensure_mt5()
                    if init_ok:
                        atr_syms = sorted({r[2] for r in rows if r and r[2]})
                        for sym in atr_syms:
                            atr_map[sym] = None
                            try:
                                try:
                                    mt5.symbol_select(sym, True)
                                except Exception:
                                    pass
                                tf = getattr(mt5, "TIMEFRAME_D1", 0)
                                rates = mt5.copy_rates_from_pos(sym, tf, 0, 15)
                                if rates is None or len(rates) < 2:
                                    atr_map[sym] = None
                                    continue
                                vals = []
                                for b in rates[-15:]:
                                    try:
                                        high = float(b['high'])
                                        low = float(b['low'])
                                        close = float(b['close'])
                                    except Exception:
                                        try:
                                            high = float(getattr(b, 'high', 0.0))
                                            low = float(getattr(b, 'low', 0.0))
                                            close = float(getattr(b, 'close', 0.0))
                                        except Exception:
                                            high = low = close = 0.0
                                    vals.append((high, low, close))
                                if len(vals) >= 2:
                                    trs = []
                                    prev_close = vals[0][2]
                                    for h, l, c in vals[1:]:
                                        tr1 = h - l
                                        tr2 = abs(h - prev_close)
                                        tr3 = abs(prev_close - l)
                                        trs.append(max(tr1, tr2, tr3))
                                        prev_close = c
                                    atr_map[sym] = (sum(trs) / len(trs)) if trs else None
                                else:
                                    atr_map[sym] = None
                            except Exception:
                                atr_map[sym] = None
                except Exception:
                    atr_map = {}

            # Compute normalized returns using ATR
            for event_time, hit, symbol, entry_price, hit_price, sl_val, direction in rows:
                if not hit:
                    continue
                dt = None
                if isinstance(event_time, str):
                    try:
                        dt = datetime.fromisoformat(event_time)
                    except Exception:
                        try:
                            dt = datetime.strptime(event_time.split('.')[0], '%Y-%m-%d %H:%M:%S')
                        except Exception:
                            dt = None
                elif isinstance(event_time, datetime):
                    dt = event_time
                if dt is None:
                    continue
                try:
                    dt = dt.replace(tzinfo=UTC)
                except Exception:
                    pass

                try:
                    ep = float(entry_price) if entry_price is not None else None
                except Exception:
                    ep = None
                try:
                    hp = float(hit_price) if hit_price is not None else None
                except Exception:
                    hp = None
                try:
                    slp = float(sl_val) if sl_val is not None else None
                except Exception:
                    slp = None
                if ep is None or hp is None or slp is None:
                    continue
                dir_s = (str(direction) or '').lower()
                profit = (hp - ep) if dir_s == 'buy' else (ep - hp)
                atr = atr_map.get(symbol)
                if atr is None or atr == 0:
                    # skip if ATR not available
                    continue
                norm = profit / atr
                # 10k-notional absolute PnL (units = 10000 / entry_price)
                try:
                    units = 10000.0 / ep if ep not in (None, 0.0) else 0.0
                except Exception:
                    units = 0.0
                notional_profit = units * profit
                times.append(dt)
                norm_returns.append(norm)
                symbols.append(str(symbol))
                notional_returns.append(notional_profit)
        except Exception as e:
            if error is None:
                error = str(e)

        # Compute cumulative and average per trade (ATR-normalized)
        cum: list[float] = []
        ssum = 0.0
        for v in norm_returns:
            ssum += v
            cum.append(ssum)
        avg = [c / (i + 1) for i, c in enumerate(cum)] if cum else []

        # Compute cumulative and average for 10k-notional series
        not_cum: list[float] = []
        nsum = 0.0
        for v in notional_returns:
            nsum += v
            not_cum.append(nsum)
        not_avg = [c / (i + 1) for i, c in enumerate(not_cum)] if not_cum else []

        # Hand off to UI thread
        self.after(0, self._pnl_update_ui, times, norm_returns, cum, avg, symbols, notional_returns, not_cum, not_avg, error)

    def _pnl_update_ui(self, times, norm_returns, cum, avg, symbols, notional_returns, not_cum, not_avg, error: str | None) -> None:
        """UI-thread handler for ATR-normalized and 10k-notional PnL series.

        Expects:
          - times: list[datetime] (UTC-aware or naive)
          - norm_returns: list[float] (per-trade profit divided by ATR)
          - cum: list[float] (cumulative sums of norm_returns)
          - avg: list[float] (average per-trade for normalized series)
          - symbols: list[str] (trade symbol at each point)
          - notional_returns: list[float] (per-trade PnL for 10k notional)
          - not_cum: list[float] (cumulative notional PnL)
          - not_avg: list[float] (average notional PnL per trade)
          - error: optional error message
        """
        self._pnl_loading = False

        if error:
            try:
                if self.pnl_status is not None:
                    self.pnl_status.config(text=f"Error: {error}")
            except Exception:
                pass
            # Clear any previous chart
            if self._pnl_ax is not None:
                try:
                    self._pnl_ax.clear()
                    if self._pnl_fig is not None:
                        self._pnl_fig.tight_layout()
                    if self._pnl_canvas is not None:
                        self._pnl_canvas.draw_idle()
                except Exception:
                    pass
            return

        if not times or not notional_returns:
            try:
                if self.pnl_fx_status is not None:
                    self.pnl_fx_status.config(text="No 10k-notional hits in the requested time range.")
                if self.pnl_crypto_status is not None:
                    self.pnl_crypto_status.config(text="No 10k-notional hits in the requested time range.")
                if self.pnl_indices_status is not None:
                    self.pnl_indices_status.config(text="No 10k-notional hits in the requested time range.")
            except Exception:
                pass
            # Clear charts if available
            for ax, fig, canvas in ((getattr(self, '_fx_ax', None), getattr(self, '_fx_fig', None), getattr(self, '_fx_canvas', None)),
                                    (getattr(self, '_crypto_ax', None), getattr(self, '_crypto_fig', None), getattr(self, '_crypto_canvas', None)),
                                    (getattr(self, '_indices_ax', None), getattr(self, '_indices_fig', None), getattr(self, '_indices_canvas', None))):
                if ax is not None:
                    try:
                        ax.clear()
                        if fig is not None:
                            fig.tight_layout()
                        if canvas is not None:
                            canvas.draw_idle()
                    except Exception:
                        pass
            return

        # Ensure lists align
        n = min(len(times), len(notional_returns), len(symbols))
        times = times[:n]
        symbols = symbols[:n]
        notional_returns = notional_returns[:n]

        # Split by instrument class
        def _sel(idxs, seq):
            return [seq[i] for i in idxs]

        idx_fx = [i for i, s in enumerate(symbols) if self._classify_symbol(s) == 'forex']
        idx_crypto = [i for i, s in enumerate(symbols) if self._classify_symbol(s) == 'crypto']
        idx_indices = [i for i, s in enumerate(symbols) if self._classify_symbol(s) == 'indices']

        # Build series for each category
        series = []
        for idxs in (idx_fx, idx_crypto, idx_indices):
            ts = _sel(idxs, times)
            rets = _sel(idxs, notional_returns)
            syms = _sel(idxs, symbols)
            cum_ = []
            ssum = 0.0
            for v in rets:
                ssum += v
                cum_.append(ssum)
            avg_ = [c / (i + 1) for i, c in enumerate(cum_)] if cum_ else []
            series.append((ts, rets, cum_, avg_, syms))

        # Render using the prepared series (Forex, Crypto, Indices)
        try:
            self._pnl_fx_render_draw(*series[0])
            self._pnl_crypto_render_draw(*series[1])
            self._pnl_indices_render_draw(*series[2])
        except Exception as e:
            try:
                if self.pnl_fx_status is not None:
                    self.pnl_fx_status.config(text=f"Render error: {e}")
            except Exception:
                pass

    def _pnl_render_draw(self, times, returns, cum, avg, symbols) -> None:
        """Draw the PnL chart with step lines, baseline, and per-trade annotations."""
        if FigureCanvasTkAgg is None or Figure is None:
            try:
                if self.pnl_status is not None:
                    self.pnl_status.config(text="Matplotlib not available; cannot render PnL.")
            except Exception:
                pass
            return
        if self._pnl_ax is None or self._pnl_canvas is None:
            self._init_pnl_chart_widgets()
        ax = self._pnl_ax
        ax.clear()
        ax.grid(True, which='both', linestyle='--', alpha=0.3)

        # Convert times to display timezone
        try:
            times_disp = [t.astimezone(DISPLAY_TZ) for t in times]
        except Exception:
            times_disp = [t + timedelta(hours=3) for t in times]

        # Add baseline so N trades -> N visible segments
        try:
            base_time = times_disp[0] - timedelta(seconds=1)
        except Exception:
            base_time = None
        if base_time is not None:
            times_plot = [base_time] + list(times_disp)
            cum_plot = [0.0] + list(cum)
            avg_plot = [0.0] + (list(avg) if avg else [0.0] * len(cum))
        else:
            times_plot = list(times_disp)
            cum_plot = list(cum)
            avg_plot = list(avg) if avg else list(cum)

        # Use smooth curves to show trends; add breakeven line
        try:
            ax.plot(times_plot, cum_plot, color='#1f77b4', linewidth=2,
                    label='Cumulative PnL (sum of +RRR/-1)', marker='o', markersize=3)
            ax.plot(times_plot, avg_plot, color='#ff7f0e', linewidth=1.2, linestyle='--',
                    label='Avg PnL per trade', marker='s', markersize=2)
            ax.axhline(0.0, color='#888888', linewidth=2.0, linestyle='-', alpha=0.9)
        except Exception:
            pass

        # Mark wins/losses at the end of each trade (TP green ^, SL red v)
        try:
            wins_x = [times_disp[i] for i, v in enumerate(returns) if v > 0]
            wins_y = [cum[i] for i, v in enumerate(returns) if v > 0]
            losses_x = [times_disp[i] for i, v in enumerate(returns) if v < 0]
            losses_y = [cum[i] for i, v in enumerate(returns) if v < 0]
            if wins_x:
                ax.scatter(wins_x, wins_y, color='green', marker='^', s=40, label='TP')
            if losses_x:
                ax.scatter(losses_x, losses_y, color='red', marker='v', s=40, label='SL')
        except Exception:
            pass

        # Annotate per-trade change values; always annotate last, annotate all when small series
        try:
            if returns:
                last_idx = len(returns) - 1
                ax.annotate(f"{returns[last_idx]:+.2f}",
                            xy=(times_disp[last_idx], cum[last_idx]),
                            xytext=(0, -16), textcoords="offset points",
                            ha="right", va="top", fontsize=9,
                            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="gray", alpha=0.9))
                if len(returns) <= 12:
                    for i, r in enumerate(returns):
                        color = 'green' if r > 0 else ('red' if r < 0 else '#333333')
                        ax.annotate(f"{r:+.2f}",
                                    xy=(times_disp[i], cum[i]),
                                    xytext=(0, 8), textcoords="offset points",
                                    ha="center", va="bottom", fontsize=8, color=color, alpha=0.9)
        except Exception:
            pass

        # Annotate symbol at each trade point (to avoid clutter, show last 20 if many)
        try:
            if returns:
                max_labels = 20
                n = len(returns)
                start = 0 if n <= max_labels else n - max_labels
                for i in range(start, n):
                    sym = (symbols[i] if symbols and i < len(symbols) else '')
                    if not sym:
                        continue
                    ax.annotate(str(sym),
                                xy=(times_disp[i], cum[i]),
                                xytext=(0, 18), textcoords="offset points",
                                ha="center", va="bottom", fontsize=8, color='#1f1f1f', alpha=0.9)
        except Exception:
            pass

        # X-axis formatter
        try:
            locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
            formatter = mdates.ConciseDateFormatter(locator, tz=DISPLAY_TZ, show_offset=False)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            pass

        try:
            ax.legend(loc='upper left')
        except Exception:
            pass
        try:
            if self._pnl_fig is not None:
                self._pnl_fig.tight_layout()
            self._pnl_canvas.draw_idle()
        except Exception:
            pass
        try:
            if self.pnl_status is not None:
                last_change = returns[-1] if returns else 0.0
                last_sym = (symbols[-1] if symbols else '')
                self.pnl_status.config(text=f"Rendered PnL: {len(times)} trades, last {last_sym} {last_change:+.2f}, cumulative {cum[-1]:.2f}, avg {avg[-1]:.3f}")
        except Exception:
            pass

    def _classify_symbol(self, sym: str) -> str:
        """Heuristically classify a symbol as 'forex', 'crypto', or 'indices'."""
        s = (sym or '').upper()

        # Crypto keywords/tickers
        crypto_tickers = [
            # Common crypto tickers (include broker aliases)
            'BTC', 'ETH', 'XRP', 'ADA', 'SOL', 'DOGE', 'BNB', 'DOT', 'AVAX', 'LINK',
            'LNK',  # alias used by some brokers for Chainlink (LINK)
            'LTC', 'BCH', 'XLM', 'TRX', 'ETC', 'UNI', 'ATOM', 'APT', 'SHIB', 'PEPE',
            'AVX',  # Avalanche alias
            'DOG',  # Dogecoin alias
            'XTZ'   # Tezos
        ]
        if any(t in s for t in crypto_tickers):
            return 'crypto'

        # Indices keywords
        index_keys = [
            'US30', 'US100', 'US500', 'SP500', 'SPX', 'NDX', 'NAS100', 'USTEC',
            'DAX', 'DE30', 'DE40', 'GER30', 'GER40',
            'FTSE', 'UK100', 'CAC', 'FCHI', 'FR40',
            'JP225', 'NIKKEI', 'N225',
            'AUS200', 'ASX200',
            'HK50', 'HSI',
            'ES35', 'IBEX', 'IT40', 'EU50', 'STOXX'
        ]
        if any(k in s for k in index_keys):
            return 'indices'

        # Forex detection by ISO currency pairs
        iso_ccy = {
            'USD','EUR','JPY','GBP','AUD','NZD','CAD','CHF','NOK','SEK','DKK',
            'ZAR','TRY','MXN','PLN','CZK','HUF','CNH','CNY','HKD','SGD'
        }
        metals = {'XAU','XAG','XPT','XPD'}  # treat metals as forex-style pairs
        def is_pair(x: str) -> bool:
            if len(x) >= 6:
                a = x[:3]; b = x[3:6]
                if (a in iso_ccy or a in metals) and (b in iso_ccy):
                    return True
            return False
        if is_pair(s):
            return 'forex'

        # Fallbacks
        if s.endswith('USD') and any(t in s for t in crypto_tickers):
            return 'crypto'
        if any(ch.isdigit() for ch in s):
            return 'indices'
        return 'forex'

    def _pnl_category_render(self, title: str, ax, fig, canvas, status_label, times, returns_abs, cum_abs, avg_abs, symbols) -> None:
        """Common renderer for 10k-notional category charts."""
        if FigureCanvasTkAgg is None or Figure is None:
            try:
                if status_label is not None:
                    status_label.config(text="Matplotlib not available; cannot render.")
            except Exception:
                pass
            return

        # Ensure axis exists (caller should have initialized)
        if ax is None or canvas is None:
            try:
                if title.endswith("Forex"):
                    self._init_fx_chart_widgets()
                    ax, fig, canvas = self._fx_ax, self._fx_fig, self._fx_canvas
                elif title.endswith("Crypto"):
                    self._init_crypto_chart_widgets()
                    ax, fig, canvas = self._crypto_ax, self._crypto_fig, self._crypto_canvas
                else:
                    self._init_indices_chart_widgets()
                    ax, fig, canvas = self._indices_ax, self._indices_fig, self._indices_canvas
            except Exception:
                return
        if ax is None:
            return

        ax.clear()
        ax.grid(True, which='both', linestyle='--', alpha=0.3)

        # Convert times to display timezone
        try:
            times_disp = [t.astimezone(DISPLAY_TZ) for t in times]
        except Exception:
            times_disp = [t + timedelta(hours=3) for t in times]

        # Baseline so N trades -> N segments
        try:
            base_time = times_disp[0] - timedelta(seconds=1) if times_disp else None
        except Exception:
            base_time = None
        if base_time is not None:
            times_plot = [base_time] + list(times_disp)
            cum_plot = [0.0] + list(cum_abs)
            avg_plot = [0.0] + (list(avg_abs) if avg_abs else [0.0] * len(cum_abs))
        else:
            times_plot = list(times_disp)
            cum_plot = list(cum_abs)
            avg_plot = list(avg_abs) if avg_abs else list(cum_abs)

        # Plot cumulative and avg as smooth curves, add breakeven line
        try:
            ax.set_title(title)
            ax.plot(times_plot, cum_plot, color='#2c7fb8', linewidth=2, label='Cumulative (10k)', marker='o', markersize=3)
            # ax.plot(times_plot, avg_plot, color='#f28e2b', linewidth=1.2, linestyle='--', label='Avg per trade (10k)', marker='s', markersize=2)
            ax.axhline(0.0, color='#888888', linewidth=2.0, linestyle='-', alpha=0.9)
        except Exception:
            pass

        # Markers for wins/losses
        try:
            wins_x = [times_disp[i] for i, v in enumerate(returns_abs) if v > 0]
            wins_y = [cum_abs[i] for i, v in enumerate(returns_abs) if v > 0]
            losses_x = [times_disp[i] for i, v in enumerate(returns_abs) if v < 0]
            losses_y = [cum_abs[i] for i, v in enumerate(returns_abs) if v < 0]
            if wins_x:
                ax.scatter(wins_x, wins_y, color='green', marker='^', s=40, label='Win')
            if losses_x:
                ax.scatter(losses_x, losses_y, color='red', marker='v', s=40, label='Loss')
        except Exception:
            pass

        # Annotations: last value and recent symbols
        try:
            if returns_abs:
                last_idx = len(returns_abs) - 1
                ax.annotate(f"{returns_abs[last_idx]:+.2f}",
                            xy=(times_disp[last_idx], cum_abs[last_idx]),
                            xytext=(0, -16), textcoords="offset points",
                            ha="right", va="top", fontsize=9,
                            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="gray", alpha=0.9))
                max_labels = 20
                n = len(returns_abs)
                start = 0 if n <= max_labels else n - max_labels
                for i in range(start, n):
                    sym = symbols[i] if symbols and i < len(symbols) else ''
                    if not sym:
                        continue
                    ax.annotate(str(sym),
                                xy=(times_disp[i], cum_abs[i]),
                                xytext=(0, 18), textcoords="offset points",
                                ha="center", va="bottom", fontsize=8, color='#1f1f1f', alpha=0.9)
        except Exception:
            pass

        # Axis formatter
        try:
            locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
            formatter = mdates.ConciseDateFormatter(locator, tz=DISPLAY_TZ, show_offset=False)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            pass

        try:
            ax.legend(loc='upper left')
        except Exception:
            pass
        try:
            if fig is not None:
                fig.tight_layout()
            if canvas is not None:
                canvas.draw_idle()
        except Exception:
            pass
        try:
            if status_label is not None:
                if cum_abs:
                    last_change = returns_abs[-1] if returns_abs else 0.0
                    last_sym = symbols[-1] if symbols else ''
                    status_label.config(text=f"{title}: {len(times)} trades, last {last_sym} {last_change:+.2f}, cumulative {cum_abs[-1]:.2f}")
                else:
                    status_label.config(text=f"{title}: no trades")
        except Exception:
            pass

    def _pnl_fx_render_draw(self, times, returns_abs, cum_abs, avg_abs, symbols) -> None:
        if self._fx_ax is None or self._fx_canvas is None:
            self._init_fx_chart_widgets()
        self._pnl_category_render("PnL (10k - Forex)", self._fx_ax, self._fx_fig, self._fx_canvas, self.pnl_fx_status,
                                  times, returns_abs, cum_abs, avg_abs, symbols)

    def _pnl_crypto_render_draw(self, times, returns_abs, cum_abs, avg_abs, symbols) -> None:
        if self._crypto_ax is None or self._crypto_canvas is None:
            self._init_crypto_chart_widgets()
        self._pnl_category_render("PnL (10k - Crypto)", self._crypto_ax, self._crypto_fig, self._crypto_canvas, self.pnl_crypto_status,
                                  times, returns_abs, cum_abs, avg_abs, symbols)

    def _pnl_indices_render_draw(self, times, returns_abs, cum_abs, avg_abs, symbols) -> None:
        if self._indices_ax is None or self._indices_canvas is None:
            self._init_indices_chart_widgets()
        self._pnl_category_render("PnL (10k - Indices)", self._indices_ax, self._indices_fig, self._indices_canvas, self.pnl_indices_status,
                                  times, returns_abs, cum_abs, avg_abs, symbols)
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

    def _append_text(self, widget: tk.Text, s: str) -> None:
        widget.configure(state=tk.NORMAL)
        widget.insert(tk.END, s)
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

        rows_display: list[tuple[str, str, str, str, str, str, str, str, str]] = []
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
                cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='timelapse_setups'")
                if cur.fetchone() is None:
                    rows_display = []
                else:
                    from datetime import timezone as _tz
                    thr = (datetime.now(_tz.utc) - timedelta(hours=hours)).strftime('%Y-%m-%d %H:%M:%S')
                    sql = (
                        """
                        SELECT s.id, s.symbol, s.direction, s.inserted_at,
                               h.hit_time_utc3, h.hit_time, h.hit, h.hit_price,
                               s.tp, s.sl, COALESCE(h.entry_price, s.price) AS entry_price,
                               s.proximity_to_sl
                        FROM timelapse_setups s
                        LEFT JOIN timelapse_hits h ON h.setup_id = s.id
                        WHERE s.inserted_at >= ?
                        ORDER BY s.inserted_at DESC, s.symbol
                        """
                    )
                    cur.execute(sql, (thr,))
                    all_rows = cur.fetchall() or []

                    # Apply filters in Python code instead of SQL
                    filtered_rows = []
                    for row in all_rows:
                        (sid, sym, direction, inserted_at, hit_utc3, hit_time, hit, hit_price, tp, sl, entry_price, proximity_to_sl) = row

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

                        filtered_rows.append(row)

                    # Process filtered rows
                    for (sid, sym, direction, inserted_at, hit_utc3, hit_time, hit, hit_price, tp, sl, entry_price, proximity_to_sl) in filtered_rows:
                        sym_s = str(sym) if sym is not None else ''
                        dir_s = str(direction) if direction is not None else ''
                        try:
                            as_naive = datetime.fromisoformat(inserted_at) if isinstance(inserted_at, str) else inserted_at
                        except Exception:
                            as_naive = None
                        ent_s = ''
                        if as_naive is not None:
                            ent_s = (as_naive + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                        hit_s = ''
                        if hit_utc3 is not None:
                            hit_s = str(hit_utc3)
                        elif hit_time is not None:
                            try:
                                ht = datetime.fromisoformat(hit_time) if isinstance(hit_time, str) else hit_time
                                hit_s = (ht + timedelta(hours=3)).strftime('%Y-%m-%d %H:%M:%S')
                            except Exception:
                                hit_s = ''
                        hit_str = str(hit) if hit is not None else ''
                        def fmt_price(v):
                            try:
                                if v is None:
                                    return ''
                                return f"{float(v):g}"
                            except Exception:
                                return str(v)
                        tp_s = fmt_price(tp)
                        sl_s = fmt_price(sl)
                        ep_s = fmt_price(entry_price)
                        prox_sl_s = fmt_price(proximity_to_sl)
                        rows_display.append((sym_s, dir_s, ent_s, hit_s, hit_str, tp_s, sl_s, ep_s, prox_sl_s))
                        # Raw/meta for chart
                        rows_meta.append({
                            'iid': None,  # to fill on UI insert
                            'setup_id': sid,
                            'symbol': sym_s,
                            'direction': dir_s,
                            'entry_utc_str': (as_naive.strftime('%Y-%m-%d %H:%M:%S.%f') if as_naive else ''),
                            'entry_price': float(entry_price) if entry_price is not None else None,
                            'tp': float(tp) if tp is not None else None,
                            'sl': float(sl) if sl is not None else None,
                            'hit_kind': hit_str if hit_str else None,
                            'hit_time_utc_str': (str(hit_time) if hit_time is not None else None),
                            'hit_price': (float(hit_price) if hit_price is not None else None),
                        })
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
            for idx, (sym, direction, ent_s, hit_s, hit, tp_s, sl_s, ep_s, prox_sl_s) in enumerate(rows_display):
                tags = ()
                if hit == 'TP':
                    tags = ('tp',)
                elif hit == 'SL':
                    tags = ('sl',)
                iid = self.db_tree.insert('', tk.END, values=(sym, direction, ent_s, hit_s, hit, tp_s, sl_s, ep_s, prox_sl_s), tags=tags)
                if idx < len(rows_meta):
                    meta = rows_meta[idx]
                    meta['iid'] = iid
                    self._db_row_meta[iid] = meta

                    # Check if this item matches the previously selected item
                    if selected_item_data and not new_selected_iid:
                        if (meta.get('symbol') == selected_item_data.get('symbol') and
                            meta.get('direction') == selected_item_data.get('direction') and
                            meta.get('entry_utc_str') == selected_item_data.get('entry_utc_str')):
                            new_selected_iid = iid

            # Restore selection if we found a matching item
            if new_selected_iid:
                self.db_tree.selection_set(new_selected_iid)
                self.db_tree.see(new_selected_iid)  # Ensure the item is visible
                self.db_tree.focus_set()  # Set keyboard focus to the treeview
                self.db_tree.focus(new_selected_iid)  # Set focus to the specific item

            self.db_status.config(text=f"Rows: {len(rows_display)} - Updated {datetime.now().strftime('%H:%M:%S')}")

        # Schedule next auto refresh if enabled
        self._db_schedule_next()
        # Also refresh PnL so changes are reflected immediately
        try:
            self._pnl_refresh()
        except Exception:
            pass

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
        setup_id = meta.get('setup_id')
        hit_kind = (meta.get('hit_kind') or '').upper()
        if not setup_id:
            self.db_status.config(text="Missing setup id; cannot delete.")
            return

        # Confirm
        try:
            from tkinter import messagebox
            sym = meta.get('symbol') or ''
            direction = meta.get('direction') or ''
            hit_info = f" and its {hit_kind} hit" if hit_kind in ('TP', 'SL') else ""
            if not messagebox.askyesno("Confirm Delete", f"Delete setup {setup_id} ({sym} {direction}){hit_info}? This cannot be undone."):
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
                        # Delete hit first (if exists), then setup
                        if hit_kind in ('TP', 'SL'):
                            cur.execute("DELETE FROM timelapse_hits WHERE setup_id=?", (setup_id,))
                        cur.execute("DELETE FROM timelapse_setups WHERE id=?", (setup_id,))
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
        ax.set_title('')
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        ax.set_xlabel('Time (UTC)')
        ax.set_ylabel('Price')
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
        symbol = meta.get('symbol')
        direction = (meta.get('direction') or '').lower()
        entry_utc_str = meta.get('entry_utc_str')
        entry_price = meta.get('entry_price')
        tp = meta.get('tp')
        sl = meta.get('sl')
        hit_kind = meta.get('hit_kind')
        hit_time_utc_str = meta.get('hit_time_utc_str')
        hit_price = meta.get('hit_price')
        if not symbol or not entry_utc_str:
            self._set_chart_message('Missing symbol or entry time; cannot render chart.')
            return
        try:
            entry_utc = datetime.fromisoformat(entry_utc_str).replace(tzinfo=UTC)
        except Exception:
            self._set_chart_message('Invalid entry time format.')
            return
        start_utc = entry_utc - timedelta(minutes=20)
        end_utc = datetime.now(UTC)
        self._chart_req_id += 1
        rid = self._chart_req_id
        self._chart_active_req_id = rid
        self._set_chart_message(f"Loading 1m chart for {symbol} from {start_utc.strftime('%H:%M')} UTC (inserted time)…")
        self._ohlc_loading = True
        # Watchdog to avoid indefinite waiting if MT5 blocks
        self.after(8000, self._chart_watchdog, rid, symbol)
        t = threading.Thread(target=self._fetch_and_render_chart_thread,
                              args=(rid, symbol, direction, start_utc, end_utc, entry_utc, entry_price, sl, tp, hit_kind, hit_time_utc_str, hit_price),
                              daemon=True)
        t.start()

    def _ensure_mt5(self) -> tuple[bool, str | None]:
        if not _MT5_IMPORTED or mt5 is None:
            return False, 'MetaTrader5 module not available. Install with: pip install MetaTrader5'
        try:
            # If not initialized, initialize now
            if not self._mt5_inited:
                if not mt5.initialize():
                    return False, f"mt5.initialize failed: {mt5.last_error()}"
                self._mt5_inited = True
        except Exception as e:
            return False, f"MT5 init error: {e}"
        return True, None

    def _chart_watchdog(self, rid: int, symbol: str) -> None:
        # If the same request is still running, release lock and inform user
        if self._chart_active_req_id == rid and self._ohlc_loading:
            self._ohlc_loading = False
            self._set_chart_message(f"Still loading {symbol}… MT5 may be busy. Try again or check terminal.")

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
                cand = getattr(cands[0], 'name', None) or None
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
        span = timedelta(seconds=max(1, timeframe_seconds))
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
            part = mt5.copy_ticks_range(sym_name, start_srv, end_srv, mt5.COPY_TICKS_ALL)
            if part is None or len(part) == 0:
                start_naive = window_start.replace(tzinfo=None)
                end_naive = window_end.replace(tzinfo=None)
                part = mt5.copy_ticks_range(sym_name, start_naive, end_naive, mt5.COPY_TICKS_ALL)
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
        for tk in ticks_aggregate:
            try:
                bid = float(getattr(tk, 'bid'))
            except Exception:
                try:
                    bid = float(tk['bid'])
                except Exception:
                    bid = None
            try:
                ask = float(getattr(tk, 'ask'))
            except Exception:
                try:
                    ask = float(tk['ask'])
                except Exception:
                    ask = None
            if bid is None and ask is None:
                continue
            price = bid if (direction or '').lower() == 'buy' else ask if ask is not None else bid
            if price is None:
                continue
            try:
                tms = getattr(tk, 'time_msc')
            except Exception:
                try:
                    tms = tk['time_msc']
                except Exception:
                    tms = None
            if tms:
                dt_raw = datetime.fromtimestamp(float(tms) / 1000.0, tz=UTC)
            else:
                try:
                    tse = getattr(tk, 'time')
                except Exception:
                    try:
                        tse = tk['time']
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

    def _fetch_and_render_chart_thread(self, rid: int, symbol: str, direction: str, start_utc: datetime, end_utc: datetime,
                                        entry_utc: datetime, entry_price, sl, tp,
                                        hit_kind, hit_time_utc_str, hit_price) -> None:
        try:
            # Step 1: MT5 init
            ok, err = self._ensure_mt5()
            if not ok:
                self.after(0, self._chart_render_error, err)
                return
            self.after(0, self._set_chart_message, f"MT5 ready. Resolving symbol {symbol}…")
            # Step 2: Resolve symbol
            sym_name, err2 = self._resolve_symbol(symbol)
            if sym_name is None:
                self.after(0, self._chart_render_error, err2 or f"Symbol '{symbol}' not found.")
                return
            # Step 3: Compute server window
            try:
                offset_h = self._server_offset_hours(sym_name)
                # If a hit exists, cap fetch end to 20 minutes after the hit
                fetch_end_utc = end_utc
                hit_dt = None
                if hit_time_utc_str and hit_kind in ('TP', 'SL'):
                    try:
                        hit_dt = datetime.fromisoformat(str(hit_time_utc_str)).replace(tzinfo=UTC)
                        fetch_end_utc = min(end_utc, hit_dt + timedelta(minutes=20, seconds=30))
                    except Exception:
                        hit_dt = None
                start_server = self._to_server_naive(start_utc, offset_h)
                end_server = self._to_server_naive(fetch_end_utc, offset_h)
            except Exception:
                offset_h = 0
                start_server = start_utc.replace(tzinfo=None)
                end_server = end_utc.replace(tzinfo=None)
            # Step 4: Fetch M1 bars first; fall back to ticks only if necessary
            quiet_segments = list(iter_quiet_utc_ranges(start_utc, fetch_end_utc))
            active_ranges = list(iter_active_utc_ranges(start_utc, fetch_end_utc))
            if not active_ranges:
                self.after(
                    0,
                    self._chart_render_error,
                    "Requested window falls entirely inside quiet trading hours (23:30–01:00 UTC+3).",
                )
                return

            timeframe = _TIMEFRAME_M1()
            timeframe_secs = _TIMEFRAME_SECONDS(timeframe)
            self.after(0, self._set_chart_message, f"Fetching M1 bars for {sym_name}…")
            rates = _RATES_RANGE(sym_name, timeframe, start_utc, fetch_end_utc, offset_h, trace=False)
            times, opens, highs, lows, closes = self._rates_to_ohlc_lists(rates, offset_h, timeframe_secs)

            if not times:
                self.after(0, self._set_chart_message, f"No bars returned; falling back to raw ticks for {sym_name}…")
                times, opens, highs, lows, closes = self._ticks_to_ohlc_lists(sym_name, offset_h, active_ranges, direction)
                if not times:
                    self.after(0, self._chart_render_error, "No price data available for requested range.")
                    return

            # Hard-trim arrays to include at most 20 minutes AFTER the hit time
            try:
                if 'hit_dt' in locals() and hit_dt is not None and hit_kind in ('TP', 'SL') and times:
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
            if hit_time_utc_str and 'hit_dt' not in locals():
                try:
                    hit_dt = datetime.fromisoformat(str(hit_time_utc_str)).replace(tzinfo=UTC)
                except Exception:
                    hit_dt = None

            # If this request is stale, ignore draw
            def _finish():
                if self._chart_active_req_id != rid:
                    return
                self._chart_render_draw(symbol, times, opens, highs, lows, closes,
                                        entry_utc, entry_price, sl, tp, hit_kind, hit_dt, hit_price,
                                        start_utc, end_utc, quiet_segments)
            self.after(0, _finish)
        except Exception as e:
            self.after(0, self._chart_render_error, f"Chart thread error: {e}")

    def _chart_render_error(self, msg: str) -> None:
        self._ohlc_loading = False
        self._set_chart_message(f"Chart error: {msg}")

    def _chart_render_draw(
        self,
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
        self._ohlc_loading = False
        if self._chart_ax is None or self._chart_canvas is None:
            self._init_chart_widgets()
        if self._chart_ax is None:
            self._set_chart_message('Matplotlib not available; cannot render chart.')
            return
        ax = self._chart_ax
        ax.clear()
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
        if quiet_segments is None:
            quiet_segments = []
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

        ax.set_title(f"{symbol} | 1m | {entry_disp.strftime('%Y-%m-%d %H:%M:%S.%f')} UTC+3 inserted")
        ax.set_xlabel('Time (UTC+3)')
        ax.set_ylabel('Price')

        # X-axis formatter
        try:
            locator = mdates.AutoDateLocator(minticks=5, maxticks=12)
            # Ensure tick labels are rendered in UTC+3 (DISPLAY_TZ)
            formatter = mdates.ConciseDateFormatter(locator, tz=DISPLAY_TZ, show_offset=False)
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(formatter)
        except Exception:
            pass

        # Draw simple candlesticks directly (robust, no extra deps)
        try:
            from matplotlib.patches import Rectangle
            import matplotlib.dates as mdates_local
            xs = [mdates_local.date2num(t) for t in times_disp]
            # body width ~= 60% of bar spacing
            if len(xs) >= 2:
                w = (xs[1] - xs[0]) * 0.6
            else:
                w = (1.0 / (24*60)) * 0.6  # fallback ~ 0.6 minute
            for x, o, h, l, c in zip(xs, opens, highs, lows, closes):
                col = '#2ca02c' if c >= o else '#d62728'  # green/red
                # wick
                ax.vlines(x, l, h, colors=col, linewidth=0.8, alpha=0.9)
                # body (ensure non-zero height is visible)
                bottom = min(o, c)
                height = max(abs(c - o), (max(highs) - min(lows)) * 0.0002)
                ax.add_patch(Rectangle((x - w/2, bottom), w, height, facecolor=col, edgecolor=col, linewidth=0.8, alpha=0.8))
            ax.set_xlim(xs[0], xs[-1])
        except Exception:
            # Ultimate fallback: plot closes
            ax.plot(times_disp, closes, color='#1f77b4', linewidth=1.5, label='Close')

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
                ax.annotate('',
                            xy=(rounded_entry_disp, float(entry_price)),
                            xytext=(next_time, float(entry_price)),
                            arrowprops=dict(arrowstyle='-|>', color='tab:blue', lw=1.4, shrinkA=0, shrinkB=0),
                            zorder=7)
                # Legend proxy so "Entry" shows with a left arrow marker
                ax.plot([], [], color='tab:blue', marker='<', linestyle='None', label='Entry')
            except Exception:
                pass
        if isinstance(sl, (int, float)):
            y_values.append(float(sl))
            ax.axhline(float(sl), color='tab:red', linestyle='-', linewidth=1.0, label='SL')
        if isinstance(tp, (int, float)):
            y_values.append(float(tp))
            ax.axhline(float(tp), color='tab:green', linestyle='-', linewidth=1.0, label='TP')

        # Hit marker
        if hit_disp is not None and hit_kind in ('TP', 'SL'):
            try:
                color = 'skyblue' if hit_kind == 'TP' else 'orange'
                price = None
                if isinstance(hit_price, (int, float)):
                    price = float(hit_price)
                else:
                    # approximate by close at nearest time
                    try:
                        # find index of closest time
                        idx = min(range(len(times_disp)), key=lambda i: abs((times_disp[i] - hit_disp).total_seconds()))
                        price = closes[idx]
                    except Exception:
                        price = None
                ax.scatter([hit_disp], [price] if price is not None else [], color=color, s=40, marker='o', zorder=5, label=f'{hit_kind} hit')
            except Exception:
                pass

        # X limits to requested window in display timezone; if hit exists, clamp to 20 min after hit
        left_xlim = None
        right_xlim = None
        try:
            # Round entry time to the nearest minute for consistent positioning
            rounded_entry_disp = entry_disp.replace(second=0, microsecond=0)
            left = min(times_disp[0], rounded_entry_disp, hit_disp) if hit_disp else min(times_disp[0], rounded_entry_disp)
            right = max(times_disp[-1], rounded_entry_disp, hit_disp) if hit_disp else max(times_disp[-1], rounded_entry_disp)
            left = min(left, start_disp)
            right = max(right, end_disp)
            # Clamp right edge if hit occurs: include only 20 minutes after the hit time
            if hit_disp is not None:
                # Find index of bar that starts at or before hit time
                hit_idx = 0
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
                idxs = [i for i, t in enumerate(times_disp) if (t >= left_xlim and t <= right_xlim)]
            else:
                idxs = list(range(len(times_disp)))
            vis_highs = [highs[i] for i in idxs] if idxs else highs
            vis_lows = [lows[i] for i in idxs] if idxs else lows
            ymin = min([min(vis_lows)] + [v for v in (sl, tp, entry_price) if isinstance(v, (int, float))])
            ymax = max([max(vis_highs)] + [v for v in (sl, tp, entry_price) if isinstance(v, (int, float))])
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
            self.btn_tl_toggle.configure(text=("Stop" if self.timelapse.is_running() else "Start"))
        except Exception:
            pass
        try:
            self.btn_hits_toggle.configure(text=("Stop" if self.hits.is_running() else "Start"))
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
        quiet_active = is_quiet_time(now_utc)
        try:
            transition = next_quiet_transition(now_utc)
            delta_ms = int(max(1.0, min(60.0, (transition - now_utc).total_seconds())) * 1000)
        except Exception:
            delta_ms = 30000

        if quiet_active:
            if self.hits.is_running():
                self.hits.stop()
            if self._hits_should_run and not self._hits_quiet_paused:
                self._enqueue_log(
                    "hits",
                    "Quiet trading window active (23:30–01:00 UTC+3); hits monitor paused.\n",
                )
            self._hits_quiet_paused = True
        else:
            was_paused = self._hits_quiet_paused
            self._hits_quiet_paused = False
            if was_paused and self._hits_should_run and not self.hits.is_running():
                self._enqueue_log("hits", "Quiet window ended; resuming hits monitor.\n")
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
        py = sys.executable or "python"
        cmd = [py, "-u", "timelapse_setups.py", "--watch"]
        try:
            ex = (self.var_exclude_symbols.get() or "").strip()
        except Exception:
            ex = ""
        if ex:
            cmd += ["--exclude", ex]
        try:
            mps = (self.var_min_prox_sl.get() or "").strip()
        except Exception:
            mps = ""
        if mps:
            cmd += ["--min-prox-sl", mps]
        try:
            mxps = (self.var_max_prox_sl.get() or "").strip()
        except Exception:
            mxps = ""
        if mxps:
            cmd += ["--max-prox-sl", mxps]
        self.timelapse.cmd = cmd
        self.timelapse.start()

    def _stop_timelapse(self) -> None:
        self.timelapse.stop()

    def _start_hits(self) -> None:
        self._hits_should_run = True
        now_utc = datetime.now(UTC)
        if is_quiet_time(now_utc):
            if not self._hits_quiet_paused:
                self._enqueue_log(
                    "hits",
                    "Quiet trading window active (23:30–01:00 UTC+3); deferring hits monitor start.\n",
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
                with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False,
                                               encoding='utf-8') as f:
                    f.write(timelapse_content)
                    timelapse_log_path = f.name

            if hits_content.strip():
                with tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False,
                                               encoding='utf-8') as f:
                    f.write(hits_content)
                    hits_log_path = f.name
        except Exception:
            # If we can't save logs, continue with restart anyway
            timelapse_log_path = None
            hits_log_path = None

        # Start a new instance of the GUI with log restore arguments
        cmd = [sys.executable, 'run_monitor_gui.pyw']
        if timelapse_log_path:
            cmd.extend(['--restore-timelapse-log', timelapse_log_path])
        if hits_log_path:
            cmd.extend(['--restore-hits-log', hits_log_path])

        try:
            subprocess.Popen(cmd, cwd=HERE)
        except Exception as e:
            # If fails, just restart the processes
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
        mps = data.get("min_prox_sl")
        if isinstance(mps, str):
            try:
                self.var_min_prox_sl.set(mps)
            except Exception:
                pass
        mxps = data.get("max_prox_sl")
        if isinstance(mxps, str):
            try:
                self.var_max_prox_sl.set(mxps)
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

    def _save_settings(self) -> None:
        data = {
            "exclude_symbols": self.var_exclude_symbols.get() if self.var_exclude_symbols is not None else "",
            "min_prox_sl": self.var_min_prox_sl.get() if self.var_min_prox_sl is not None else "0.25",
            "max_prox_sl": self.var_max_prox_sl.get() if self.var_max_prox_sl is not None else "0.5",
            "since_hours": self.var_since_hours.get() if self.var_since_hours is not None else 168,
            "interval": self.var_interval.get() if self.var_interval is not None else 60,
            "symbol_category": self.var_symbol_category.get() if self.var_symbol_category is not None else "All",
            "hit_status": self.var_hit_status.get() if self.var_hit_status is not None else "All",
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

    def _on_min_prox_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass

    def _on_max_prox_changed(self, *args) -> None:
        try:
            self._save_settings()
        except Exception:
            pass


    def _on_filter_changed(self, *args) -> None:
        """Trigger refresh when filter values change."""
        # Schedule a refresh with a small delay to avoid excessive refreshes
        if hasattr(self, '_filter_refresh_job') and self._filter_refresh_job is not None:
            try:
                self.after_cancel(self._filter_refresh_job)
            except Exception:
                pass
        self._filter_refresh_job = self.after(300, self._db_refresh)



def main() -> None:
    args = parse_args()
    app = App(restore_timelapse_log=args.restore_timelapse_log,
              restore_hits_log=args.restore_hits_log)
    app.mainloop()


if __name__ == "__main__":
    main()
