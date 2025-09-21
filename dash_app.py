#!/usr/bin/env python3
"""Dash web UI for - improved UI wiring and polish.

Improvements implemented in this version:
- Table sorting and native filtering enabled for the DB Results table
- CSV export of visible rows or selected rows (uses dcc.Download / send_data_frame)
- Better error/status messages using dbc.Alert
- Minor UX tweaks: fixed header, scrollable table, adjustable interval and since-hours
- Tail logs (limited) shown in the Monitors tab

Notes:
- Requires dash, dash-bootstrap-components, plotly and pandas (pandas optional fallback to manual CSV)
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
import json
from typing import Any, List, Dict

from dash import Dash, dcc, html, dash_table, no_update, callback_context
from dash import Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objs as go

# Optional pandas import for CSV export convenience
try:
    import pandas as pd
except Exception:
    pd = None  # fallback to manual CSV assembly

# Local helpers (best-effort imports)
try:
    from monitor.proc import create_controller, get_controller
    from monitor import logs, web_db, chart
except Exception:
    create_controller = None
    get_controller = None
    logs = None
    web_db = None
    chart = None

HERE = os.path.dirname(os.path.abspath(__file__))

# Ensure controllers exist and attach named buffers
if create_controller is not None and logs is not None:
    try:
        tl_cmd = [sys.executable or "python", "-u", "timelapse_setups.py", "--watch"]
        create_controller("timelapse", tl_cmd, cwd=HERE, log_put=logs.attach_named("timelapse"))
    except Exception:
        pass
    try:
        hits_cmd = [sys.executable or "python", "-u", "check_tp_sl_hits.py", "--watch"]
        create_controller("hits", hits_cmd, cwd=HERE, log_put=logs.attach_named("hits"))
    except Exception:
        pass

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP], suppress_callback_exceptions=True)
server = app.server


# --- Layout pieces ---
def monitors_layout():
    return dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(dbc.Button("Start Timelapse", id="btn-tl-toggle", color="primary", className="me-2"), width="auto"),
                            dbc.Col(html.Span(id="status-tl", children="Unavailable", style={"marginLeft": "8px"}), width="auto"),
                        ],
                        align="center",
                    ),
                    html.Br(),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Button("Start Hits", id="btn-hits-toggle", color="secondary"), width="auto"),
                            dbc.Col(html.Span(id="status-hits", children="Unavailable", style={"marginLeft": "8px"}), width="auto"),
                        ],
                        align="center",
                    ),
                    html.Hr(),
                    dbc.Label("Exclude (comma):"),
                    dbc.Input(id="input-exclude", placeholder="GLMUSD,BCHUSD", value="", type="text"),
                    html.Br(),
                    dbc.Label("Min Prox SL (fraction 0.0-0.49):"),
                    dbc.Input(id="input-min-prox-sl", placeholder="0.25", value="0.25", type="number", step="0.01"),
                    html.Br(),
                    dbc.Label("Auto-refresh interval (s):"),
                    dbc.Input(id="input-interval-sec", placeholder="5", value=5, type="number", step="1", min=1),
                    html.Br(),
                    dbc.Label("Since (hours) for DB / PnL:"),
                    dbc.Input(id="input-since-hours", placeholder="168", value=168, type="number", step="1", min=1),
                    html.Br(),
                    dbc.Button("Clear Logs", id="btn-clear-logs", color="danger"),
                ],
                md=4,
            ),
            dbc.Col(
                [
                    html.H5("Timelapse Log"),
                    html.Pre(id="log-tl", style={"height": "220px", "overflow": "auto", "backgroundColor": "#f8f9fa", "whiteSpace": "pre-wrap"}),
                    html.H5("Hits Log"),
                    html.Pre(id="log-hits", style={"height": "220px", "overflow": "auto", "backgroundColor": "#f8f9fa", "whiteSpace": "pre-wrap"}),
                ],
                md=8,
            ),
        ]
    )


def db_layout():
    columns = [
        {"name": "Symbol", "id": "symbol"},
        {"name": "Direction", "id": "direction"},
        {"name": "Inserted UTC+3", "id": "entry_utc3"},
        {"name": "Hit Time UTC+3", "id": "hit_time_utc3"},
        {"name": "Hit", "id": "hit"},
        {"name": "TP", "id": "tp"},
        {"name": "SL", "id": "sl"},
        {"name": "Entry Price", "id": "entry_price"},
    ]
    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(dbc.Button("Export Visible CSV", id="btn-export-csv", color="primary", size="sm"), width="auto"),
                    dbc.Col(dbc.Button("Export Selected CSV", id="btn-export-selected-csv", color="secondary", size="sm"), width="auto"),
                    dbc.Col(dcc.Download(id="download-db-csv"), width="auto"),
                ],
                align="center",
            ),
            html.Br(),
            dash_table.DataTable(
                id="db-table",
                columns=columns,
                data=[],
                page_size=20,
                cell_selectable=True,
                sort_action="native",
                filter_action="native",
                filter_options={"placeholder_text": "Filter..."},
                style_table={"overflowX": "auto", "maxHeight": "500px"},
                fixed_rows={"headers": True},
                style_cell={"textAlign": "left", "minWidth": "80px", "width": "120px", "maxWidth": "240px", "whiteSpace": "normal"},
                css=[
                    {"selector": "td.dash-cell--selected, td.dash-cell--selected *", "rule": "background-color: inherit !important;"},
                    {"selector": "td.focused, td.focused *", "rule": "background-color: inherit !important; outline: none !important;"},
                    {"selector": "td.dash-cell.dash-cell--selected.dash-cell--focused", "rule": "background-color: inherit !important; outline: none !important;"},
                ],
                style_data_conditional=[
                    {"if": {"state": "active"}, "backgroundColor": "inherit", "outline": "none"},
                    {"if": {"state": "selected"}, "backgroundColor": "inherit", "outline": "none"},
                    {
                        "if": {"filter_query": '{hit} = "SL"'},
                        "backgroundColor": "#f8d7da",
                    },
                    {
                        "if": {"filter_query": '{hit} = "TP"'},
                        "backgroundColor": "#d4edda",
                    },
                ],
            ),
            html.Br(),
            dcc.Graph(id="chart-ohlc"),
        ]
    )


def pnl_layout():
    return html.Div(
        [
            dcc.Graph(id="pnl-forex"),
            dcc.Graph(id="pnl-crypto"),
            dcc.Graph(id="pnl-indices"),
        ]
    )


# --- App layout ---
app.layout = dbc.Container(
    [
        html.H2("Timelapse Monitors (Dash)"),
        html.Div(id="status-msg"),
        dcc.Store(id="since-hours-store", data=168),
        dbc.Tabs(
            [
                dbc.Tab(label="Monitors", tab_id="tab-monitors"),
                dbc.Tab(label="DB Results", tab_id="tab-db"),
                dbc.Tab(label="PnL", tab_id="tab-pnl"),
            ],
            id="tabs",
            active_tab="tab-db",
        ),
        html.Div(id="tab-content", style={"marginTop": "1rem"}),
        dcc.Interval(id="interval-refresh", interval=5 * 1000, n_intervals=0),
    ],
    fluid=True,
)


# --- Tab renderer ---

# Validation layout ensures Dash knows about all tab content components even when hidden.
app.validation_layout = html.Div([
    app.layout,
    monitors_layout(),
    db_layout(),
    pnl_layout(),
])

@app.callback(Output("tab-content", "children"), [Input("tabs", "active_tab")])
def render_tab(tab):
    if tab == "tab-monitors":
        return monitors_layout()
    if tab == "tab-db":
        return db_layout()
    if tab == "tab-pnl":
        return pnl_layout()
    return html.Div()


# --- Interval control from input ---
@app.callback(Output("interval-refresh", "interval"), [Input("input-interval-sec", "value")])
def set_interval_sec(val):
    try:
        v = float(val) if val is not None else 5.0
        v = max(1.0, v)
        return int(v * 1000)
    except Exception:
        return 5000


@app.callback(Output("since-hours-store", "data"), [Input("input-since-hours", "value")], prevent_initial_call=True)
def sync_since_hours_store(val):
    try:
        hours = int(val) if val is not None else 168
        return max(1, hours)
    except Exception:
        return 168


# --- Status badges updated periodically ---
@app.callback(
    [Output("status-tl", "children"), Output("status-hits", "children")],
    [Input("interval-refresh", "n_intervals")],
    [State("tabs", "active_tab")],
)
def update_statuses(n, active_tab):
    if active_tab != "tab-monitors":
        return no_update, no_update
    tl_status = "Unavailable"
    hits_status = "Unavailable"
    try:
        if get_controller is not None:
            ctrl_tl = get_controller("timelapse")
            ctrl_hits = get_controller("hits")
            tl_status = "Running" if (ctrl_tl is not None and ctrl_tl.is_running()) else "Stopped"
            hits_status = "Running" if (ctrl_hits is not None and ctrl_hits.is_running()) else "Stopped"
    except Exception:
        tl_status = "Error"
        hits_status = "Error"
    return tl_status, hits_status


# --- Toggle timelapse (uses exclude / min-prox values) ---
@app.callback(
    [
        Output("btn-tl-toggle", "children"),
        Output("status-msg", "children", allow_duplicate=True),
        Output("status-tl", "children", allow_duplicate=True),
    ],
    [Input("btn-tl-toggle", "n_clicks")],
    [State("input-exclude", "value"), State("input-min-prox-sl", "value")],
    prevent_initial_call=True,
)
def toggle_timelapse(n_clicks, exclude, min_prox_sl):
    if not n_clicks:
        return "Start Timelapse", "", no_update
    try:
        if get_controller is None:
            return "Start Timelapse", dbc.Alert("Process controller unavailable", color="warning"), no_update
        ctrl = get_controller("timelapse")
        if ctrl is None:
            return "Start Timelapse", dbc.Alert("Timelapse controller not configured", color="warning"), no_update
        py = sys.executable or "python"
        cmd = [py, "-u", "timelapse_setups.py", "--watch"]
        try:
            ex = (exclude or "").strip()
        except Exception:
            ex = ""
        if ex:
            cmd += ["--exclude", ex]
        try:
            mps = (min_prox_sl or "")
            if isinstance(mps, (int, float)):
                mps = str(mps)
            mps = str(mps).strip()
        except Exception:
            mps = ""
        if mps:
            cmd += ["--min-prox-sl", mps]
        ctrl.cmd = cmd
        if not ctrl.is_running():
            # Immediate feedback
            ctrl.start()
            return "Stop Timelapse", "", "Starting..."
        else:
            ctrl.stop()
            return "Start Timelapse", "", "Stopping..."
    except Exception as e:
        return "Start Timelapse", dbc.Alert(f"Error toggling timelapse: {e}", color="danger"), no_update


# --- Initialize button text based on process status ---
@app.callback(
    [Output("btn-tl-toggle", "children", allow_duplicate=True),
     Output("btn-hits-toggle", "children", allow_duplicate=True)],
    [Input("interval-refresh", "n_intervals")],
    prevent_initial_call=True,
)
def sync_button_text(n):
    """Sync button text with actual process status"""
    try:
        if get_controller is None:
            return "Start Timelapse", "Start Hits"

        ctrl_tl = get_controller("timelapse")
        ctrl_hits = get_controller("hits")

        tl_text = "Stop Timelapse" if (ctrl_tl is not None and ctrl_tl.is_running()) else "Start Timelapse"
        hits_text = "Stop Hits" if (ctrl_hits is not None and ctrl_hits.is_running()) else "Start Hits"

        return tl_text, hits_text
    except Exception:
        return "Start Timelapse", "Start Hits"


# --- Auto-start monitors once on app load ---
@app.callback(
    Output("status-msg", "children", allow_duplicate=True),
    [Input("interval-refresh", "n_intervals")],
    prevent_initial_call='initial_duplicate',
)
def autostart_monitors(n):
    # Run only on initial load (n == 0); ignore subsequent intervals
    if n != 0:
        return no_update
    try:
        if get_controller is None:
            return ""
        # Timelapse: start with safe defaults; user can stop/restart with UI to apply custom values
        ctrl_tl = get_controller("timelapse")
        if ctrl_tl is not None and not ctrl_tl.is_running():
            py = sys.executable or "python"
            cmd = [py, "-u", "timelapse_setups.py", "--watch", "--min-prox-sl", "0.25"]
            # No default exclude; leave empty
            ctrl_tl.cmd = cmd
            ctrl_tl.start()
        # Hits: start if not running (command already configured in create_controller)
        ctrl_hits = get_controller("hits")
        if ctrl_hits is not None and not ctrl_hits.is_running():
            ctrl_hits.start()
        return ""
    except Exception as e:
        return dbc.Alert(f"Auto-start error: {e}", color="danger")
# --- Toggle hits ---
@app.callback(
    [
        Output("btn-hits-toggle", "children"),
        Output("status-msg", "children", allow_duplicate=True),
        Output("status-hits", "children", allow_duplicate=True),
    ],
    [Input("btn-hits-toggle", "n_clicks")],
    prevent_initial_call=True,
)
def toggle_hits(n_clicks):
    if not n_clicks:
        return "Start Hits", "", no_update
    try:
        if get_controller is None:
            return "Start Hits", dbc.Alert("Process controller unavailable", color="warning"), no_update
        ctrl = get_controller("hits")
        if ctrl is None:
            return "Start Hits", dbc.Alert("Hits controller not configured", color="warning"), no_update
        if not ctrl.is_running():
            ctrl.start()
            return "Stop Hits", "", "Starting..."
        else:
            ctrl.stop()
            return "Start Hits", "", "Stopping..."
    except Exception as e:
        return "Start Hits", dbc.Alert(f"Error toggling hits: {e}", color="danger"), no_update


# --- DB refresh callback (uses since-hours) ---
@app.callback(
    Output("db-table", "data"),
    [Input("interval-refresh", "n_intervals")],
    [State("since-hours-store", "data"), State("tabs", "active_tab")],
)
def refresh_db(n, since_hours, active_tab):
    if active_tab not in (None, "tab-db"):
        return no_update
    try:
        if web_db is not None:
            hours = int(since_hours) if since_hours is not None else 168
            rows = web_db.get_db_rows(hours)
            sanitized = []
            for row in rows or []:
                if not isinstance(row, dict):
                    sanitized.append(row)
                    continue
                copy = dict(row)
                meta = copy.get("_meta")
                try:
                    copy["_meta"] = json.dumps(meta) if meta is not None else ""
                except Exception:
                    copy["_meta"] = ""
                sanitized.append(copy)
            return sanitized
    except Exception as e:
        # Surface a compact status alert (do not crash UI)
        return []  # status-msg will display details from other callbacks as needed
    return []


# --- Logs refresh and Clear Logs ---
@app.callback(
    [Output("log-tl", "children"), Output("log-hits", "children")],
    [Input("interval-refresh", "n_intervals"), Input("btn-clear-logs", "n_clicks")],
    [State("tabs", "active_tab")],
)
def refresh_logs(n_intervals, clear_clicks, active_tab):
    if active_tab != "tab-monitors":
        return no_update, no_update
    triggered = callback_context.triggered
    triggered_id = triggered[0]["prop_id"].split(".")[0] if triggered else ""
    try:
        if logs is not None and triggered_id == "btn-clear-logs":
            try:
                logs.clear("timelapse")
            except Exception:
                pass
            try:
                logs.clear("hits")
            except Exception:
                pass
    except Exception:
        pass
    try:
        if logs is not None:
            tl_lines = logs.tail("timelapse", 800)
            ht_lines = logs.tail("hits", 800)
            tl_text = "".join(tl_lines)
            ht_text = "".join(ht_lines)
            return tl_text, ht_text
    except Exception:
        pass
    return "", ""


# --- OHLC chart for selected DB row ---
@app.callback(Output("chart-ohlc", "figure"), [Input("db-table", "active_cell")], [State("db-table", "data")])
def on_row_select(active_cell, data):
    if not active_cell or not data:
        return go.Figure()
    try:
        idx = int(active_cell.get("row")) if isinstance(active_cell, dict) else None
    except Exception:
        idx = None
    if idx is None:
        return go.Figure()
    try:
        row = data[idx]
    except Exception:
        return go.Figure()
    raw_meta = row.get("_meta") if isinstance(row, dict) else None
    meta = None
    if isinstance(raw_meta, dict):
        meta = raw_meta
    elif isinstance(raw_meta, str) and raw_meta:
        try:
            meta = json.loads(raw_meta)
        except Exception:
            meta = None
    if meta is None:
        fig = go.Figure()
        fig.update_layout(title="No metadata for selected row")
        return fig
    try:
        if chart is None:
            fig = go.Figure()
            fig.update_layout(title=f"{meta.get('symbol', '')} — chart helper unavailable")
            return fig
        ohlc = chart.get_ohlc_for_setup(meta)
        if ohlc is None:
            fig = go.Figure()
            fig.update_layout(title=f"{meta.get('symbol','')} — no tick data")
            return fig
        fig = chart.candlestick_figure_from_ohlc(ohlc)
        return fig
    except Exception as e:
        fig = go.Figure()
        fig.update_layout(title=f"Chart error: {e}")
        return fig


# --- PnL charts refresh (uses since-hours) ---
@app.callback(
    [Output("pnl-forex", "figure"), Output("pnl-crypto", "figure"), Output("pnl-indices", "figure")],
    [Input("interval-refresh", "n_intervals")],
    [State("since-hours-store", "data"), State("tabs", "active_tab")],
)
def refresh_pnl(n, since_hours, active_tab):
    if active_tab != "tab-pnl":
        return no_update, no_update, no_update
    try:
        if web_db is not None and chart is not None:
            hours = int(since_hours) if since_hours is not None else 168
            series = web_db.compute_pnl_series(hours)
            figs = chart.pnl_figures_from_series(series)
            return figs.get("forex", go.Figure()), figs.get("crypto", go.Figure()), figs.get("indices", go.Figure())
    except Exception:
        pass
    return go.Figure(), go.Figure(), go.Figure()


# --- CSV export helpers ---
def _strip_internal_keys(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in rows:
        nr = {k: v for k, v in r.items() if not k.startswith("_")}
        out.append(nr)
    return out


@app.callback(
    Output("download-db-csv", "data"),
    [Input("btn-export-csv", "n_clicks"), Input("btn-export-selected-csv", "n_clicks")],
    [State("db-table", "data"), State("db-table", "selected_rows"), State("db-table", "active_cell")],
    prevent_initial_call=True,
)
def export_csv(n_vis, n_sel, data, selected_rows, active_cell):
    """Unified CSV export handler for visible or selected rows."""
    try:
        triggered = callback_context.triggered
        triggered_id = triggered[0]["prop_id"].split(".")[0] if triggered else ""
    except Exception:
        triggered_id = ""
    if not data:
        return no_update
    # Choose rows to export based on which button was clicked
    if triggered_id == "btn-export-selected-csv":
        indices = selected_rows or []
        if not indices:
            if active_cell and isinstance(active_cell, dict) and "row" in active_cell:
                try:
                    indices = [int(active_cell["row"])]
                except Exception:
                    indices = []
        if not indices:
            return no_update
        selected = []
        try:
            for idx in indices:
                selected.append(data[int(idx)])
        except Exception:
            return no_update
        rows = _strip_internal_keys(selected)
        filename = "db_selected.csv"
    else:
        # Default: export visible/current table data
        rows = _strip_internal_keys(list(data))
        filename = "db_visible.csv"
    # Use pandas when available for robust CSV creation
    if pd is not None:
        df = pd.DataFrame(rows)
        if hasattr(dcc, "send_data_frame"):
            return dcc.send_data_frame(df.to_csv, filename, index=False)
        csv_text = df.to_csv(index=False)
        if hasattr(dcc, "send_string"):
            return dcc.send_string(csv_text, filename)
        return no_update
    # Fallback manual CSV assembly
    if not rows:
        return no_update
    cols = list(rows[0].keys())
    lines = [",".join(cols)]
    for r in rows:
        vals = [str(r.get(c, "")) for c in cols]
        lines.append(",".join(vals))
    csv_text = "\n".join(lines)
    if hasattr(dcc, "send_string"):
        return dcc.send_string(csv_text, filename)
    return no_update


# Duplicate export_selected_csv removed — handled by unified export_csv callback above.
# This avoids "Duplicate callback outputs" errors by ensuring a single callback writes to
# the dcc.Download id="download-db-csv" output.


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8050"))
    app.run(host="127.0.0.1", port=port, debug=True)
