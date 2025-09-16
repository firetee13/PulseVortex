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

# MT5 imports (optional at module import; initialized lazily when needed)
_MT5_IMPORTED = False
try:
    import MetaTrader5 as mt5  # type: ignore
    _MT5_IMPORTED = True
except Exception:
    mt5 = None  # type: ignore
    _MT5_IMPORTED = False

# Reuse helpers from check_tp_sl_hits when available
_RESOLVE = None
_GET_OFFS = None
_TO_SERVER = None
try:
    from check_tp_sl_hits import resolve_symbol as _RESOLVE, get_server_offset_hours as _GET_OFFS, to_server_naive as _TO_SERVER
except Exception:
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
    def __init__(self) -> None:
        super().__init__()
        self.title("EASY Insight - Timelapse Monitors")
        self.geometry("1000x600")
        self.minsize(800, 400)

        # Notebook with tabs
        self.nb = ttk.Notebook(self)
        self.nb.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.tab_mon = ttk.Frame(self.nb)
        self.nb.add(self.tab_mon, text="Monitors")

        self.tab_db = ttk.Frame(self.nb)
        self.nb.add(self.tab_db, text="DB Results")

        # Set DB Results tab as default active tab
        self.nb.select(self.tab_db)

        # User-configurable exclude list for timelapse setups (comma-separated symbols)
        self.var_exclude_symbols = tk.StringVar(value="")
        # DB tab variables
        self.var_db_name = tk.StringVar(value="timelapse.db")
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
        except Exception:
            pass

        # UI elements in Monitors tab
        self._make_controls(self.tab_mon)
        self._make_logs(self.tab_mon)

        # UI elements in DB tab
        self._make_db_tab(self.tab_db)
        # Ensure DB results refresh once at startup and auto-refresh is active
        try:
            self.var_auto.set(True)
        except Exception:
            pass
        self._db_refresh()

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
            cmd=[py, "-u", "check_tp_sl_hits.py", "--watch"],
            log_put=self._enqueue_log,
        )

        self.protocol("WM_DELETE_WINDOW", self._on_close)

        # Autostart both services shortly after UI loads
        self.after(300, self._auto_start)

    def _make_controls(self, parent) -> None:
        frm = ttk.Frame(parent)
        frm.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        # Timelapse controls
        tl = ttk.LabelFrame(frm, text="Timelapse Setups --watch")
        tl.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.X, expand=True)
        self.btn_tl_toggle = ttk.Button(tl, text="Start", command=self._toggle_timelapse)
        self.btn_tl_toggle.pack(side=tk.LEFT, padx=4, pady=6)
        # Exclude symbols input (comma-separated)
        ttk.Label(tl, text="Exclude (comma):").pack(side=tk.LEFT, padx=(10, 4))
        ent_ex = ttk.Entry(tl, textvariable=self.var_exclude_symbols)
        ent_ex.pack(side=tk.LEFT, padx=(0, 4), fill=tk.X, expand=True)

        # TP/SL Hits controls
        ht = ttk.LabelFrame(frm, text="TP/SL Hits --watch")
        ht.pack(side=tk.LEFT, padx=6, pady=4, fill=tk.X, expand=True)
        self.btn_hits_toggle = ttk.Button(ht, text="Start", command=self._toggle_hits)
        self.btn_hits_toggle.pack(side=tk.LEFT, padx=4, pady=6)

        # Misc
        misc = ttk.Frame(frm)
        misc.pack(side=tk.RIGHT)
        ttk.Button(misc, text="Clear Log", command=self._clear_log).pack(side=tk.RIGHT, padx=4)

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

    # --- DB TAB ---
    def _make_db_tab(self, parent) -> None:
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)

        # DB config (variables already created in __init__)

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

        # Tree (table)
        # Splitter: top table, bottom chart
        splitter = ttk.Panedwindow(parent, orient=tk.VERTICAL)
        splitter.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # Top: table container
        mid = ttk.Frame(splitter)
        cols = ("symbol", "direction", "entry_utc3", "hit_time_utc3", "hit", "tp", "sl", "entry_price")
        self.db_tree = ttk.Treeview(mid, columns=cols, show='headings', height=12)
        self.db_tree.heading("symbol", text="Symbol")
        self.db_tree.heading("direction", text="Direction")
        self.db_tree.heading("entry_utc3", text="Inserted UTC+3")
        self.db_tree.heading("hit_time_utc3", text="Hit Time UTC+3")
        self.db_tree.heading("hit", text="Hit")
        self.db_tree.heading("tp", text="TP")
        self.db_tree.heading("sl", text="SL")
        self.db_tree.heading("entry_price", text="Entry Price")
        self.db_tree.column("symbol", width=120, anchor=tk.W)
        self.db_tree.column("direction", width=80, anchor=tk.W)
        self.db_tree.column("entry_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit_time_utc3", width=180, anchor=tk.W)
        self.db_tree.column("hit", width=80, anchor=tk.W)
        self.db_tree.column("tp", width=100, anchor=tk.E)
        self.db_tree.column("sl", width=100, anchor=tk.E)
        self.db_tree.column("entry_price", width=120, anchor=tk.E)

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
        # Delete button for selected row (only deletes rows with TP/SL hits)
        ttk.Button(bot, text="Delete Selected (TP/SL)", command=self._db_delete_selected).pack(side=tk.RIGHT)

        self._db_loading = False
        self._db_auto_job: str | None = None
        self._ohlc_loading = False
        self._chart_req_id = 0
        self._chart_active_req_id: int | None = None
        self._mt5_inited = False

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

        rows_display: list[tuple[str, str, str, str, str, str, str, str]] = []
        rows_meta: list[dict] = []
        error: str | None = None
        try:
            # Use SQLite for GUI DB results
            try:
                import sqlite3  # type: ignore
            except Exception as e:
                raise RuntimeError(f"sqlite3 not available: {e}")
            db_path = dbname if dbname.lower().endswith('.db') else os.path.join(HERE, 'timelapse.db')
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
                               s.tp, s.sl, COALESCE(h.entry_price, s.price) AS entry_price
                        FROM timelapse_setups s
                        LEFT JOIN timelapse_hits h ON h.setup_id = s.id
                        WHERE s.inserted_at >= ?
                        ORDER BY s.inserted_at DESC, s.symbol
                        """
                    )
                    cur.execute(sql, (thr,))
                    for (sid, sym, direction, inserted_at, hit_utc3, hit_time, hit, hit_price, tp, sl, entry_price) in cur.fetchall() or []:
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
                        rows_display.append((sym_s, dir_s, ent_s, hit_s, hit_str, tp_s, sl_s, ep_s))
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
        self.db_tree.delete(*self.db_tree.get_children())
        self._db_row_meta.clear()
        if error:
            self.db_status.config(text=f"Error: {error}")
        else:
            for idx, (sym, direction, ent_s, hit_s, hit, tp_s, sl_s, ep_s) in enumerate(rows_display):
                tags = ()
                if hit == 'TP':
                    tags = ('tp',)
                elif hit == 'SL':
                    tags = ('sl',)
                iid = self.db_tree.insert('', tk.END, values=(sym, direction, ent_s, hit_s, hit, tp_s, sl_s, ep_s), tags=tags)
                if idx < len(rows_meta):
                    meta = rows_meta[idx]
                    meta['iid'] = iid
                    self._db_row_meta[iid] = meta
            self.db_status.config(text=f"Rows: {len(rows_display)} - Updated {datetime.now().strftime('%H:%M:%S')}")
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
        setup_id = meta.get('setup_id')
        hit_kind = (meta.get('hit_kind') or '').upper()
        if not setup_id:
            self.db_status.config(text="Missing setup id; cannot delete.")
            return
        # Only allow deletion if there is a recorded TP/SL
        if hit_kind not in ('TP', 'SL'):
            self.db_status.config(text="Delete allowed only for rows with TP/SL.")
            return

        # Confirm
        try:
            from tkinter import messagebox
            sym = meta.get('symbol') or ''
            direction = meta.get('direction') or ''
            if not messagebox.askyesno("Confirm Delete", f"Delete setup {setup_id} ({sym} {direction}) and its hit? This cannot be undone."):
                return
        except Exception:
            pass

        # Run deletion in a thread then refresh
        def _do_delete():
            dbname = self.var_db_name.get().strip()
            db_path = dbname if dbname.lower().endswith('.db') else os.path.join(HERE, 'timelapse.db')
            err = None
            try:
                import sqlite3  # type: ignore
                conn = sqlite3.connect(db_path, timeout=5)
                try:
                    with conn:
                        cur = conn.cursor()
                        # Delete hit first (if exists), then setup
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
        # Prefer helper from check_tp_sl_hits
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
            # Step 4: Fetch ticks
            self.after(0, self._set_chart_message, f"Fetching ticks for {sym_name}…")
            ticks = mt5.copy_ticks_range(sym_name, start_server, end_server, mt5.COPY_TICKS_ALL)
            # Fallback: try UTC-naive window if nothing returned
            if ticks is None or len(ticks) == 0:
                start_naive = start_utc.replace(tzinfo=None)
                end_naive = end_utc.replace(tzinfo=None)
                ticks = mt5.copy_ticks_range(sym_name, start_naive, end_naive, mt5.COPY_TICKS_ALL)
            if ticks is None or len(ticks) == 0:
                self.after(0, self._chart_render_error, "No ticks for requested range.")
                return
            # Aggregate ticks into 1m OHLC
            minute_data = defaultdict(list)
            for tk in ticks:
                try:
                    bid = float(getattr(tk, 'bid'))
                except Exception:
                    try:
                        bid = float(tk['bid'])
                    except Exception:
                        continue
                try:
                    ask = float(getattr(tk, 'ask'))
                except Exception:
                    try:
                        ask = float(tk['ask'])
                    except Exception:
                        continue
                price = bid if direction.lower() == 'buy' else ask
                # Time
                try:
                    tms = getattr(tk, 'time_msc')
                except Exception:
                    try:
                        tms = tk['time_msc']
                    except Exception:
                        tms = None
                if tms:
                    dt_raw = datetime.fromtimestamp(float(tms)/1000.0, tz=UTC)
                else:
                    try:
                        tse = getattr(tk, 'time')
                    except Exception:
                        try:
                            tse = tk['time']
                        except Exception:
                            continue
                    dt_raw = datetime.fromtimestamp(float(tse), tz=UTC)
                dt_utc = dt_raw - timedelta(hours=offset_h)
                # Floor to minute
                minute = dt_utc.replace(second=0, microsecond=0)
                minute_data[minute].append(price)
            # Prepare OHLC data
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
            if not times:
                self.after(0, self._chart_render_error, "No data after processing ticks.")
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
                                        start_utc, end_utc)
            self.after(0, _finish)
        except Exception as e:
            self.after(0, self._chart_render_error, f"Chart thread error: {e}")

    def _chart_render_error(self, msg: str) -> None:
        self._ohlc_loading = False
        self._set_chart_message(f"Chart error: {msg}")

    def _chart_render_draw(self, symbol: str, times, opens, highs, lows, closes,
                            entry_utc: datetime, entry_price, sl, tp,
                            hit_kind, hit_dt, hit_price,
                            start_utc: datetime, end_utc: datetime) -> None:
        self._ohlc_loading = False
        if self._chart_ax is None or self._chart_canvas is None:
            self._init_chart_widgets()
        if self._chart_ax is None:
            self._set_chart_message('Matplotlib not available; cannot render chart.')
            return
        ax = self._chart_ax
        ax.clear()
        ax.grid(True, which='both', linestyle='--', alpha=0.3)
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

        # Legend
        try:
            ax.legend(loc='lower right')
        except Exception:
            pass

        # Tight layout
        try:
            if self._chart_fig is not None:
                self._chart_fig.tight_layout()
        except Exception:
            pass
        self._chart_canvas.draw_idle()
        self._set_chart_message(f"Rendered {symbol} | 1m bars: {len(times)} (using inserted time)")

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

    # Button handlers
    def _start_timelapse(self) -> None:
        # Build command dynamically to include exclude list if provided
        py = sys.executable or "python"
        cmd = [py, "-u", "timelapse_setups.py", "--watch"]
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
        self.hits.start()

    def _stop_hits(self) -> None:
        self.hits.stop()

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

    def _save_settings(self) -> None:
        data = {
            "exclude_symbols": self.var_exclude_symbols.get() if self.var_exclude_symbols is not None else "",
            "since_hours": self.var_since_hours.get() if self.var_since_hours is not None else 168,
            "interval": self.var_interval.get() if self.var_interval is not None else 60,
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



def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
