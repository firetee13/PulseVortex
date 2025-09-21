# Monitor - MT5 Trade Setup Analyzer & Monitor

An MT5-powered toolchain for discovering timelapse-based trade setups, monitoring TP/SL hits, and inspecting results in a Dash web UI.

Components:
- CLI Setup Analyzer: [timelapse_setups.py](timelapse_setups.py)
- TP/SL Hit Checker: [check_tp_sl_hits.py](check_tp_sl_hits.py)
- Dash Web UI: [dash_app.py](dash_app.py)
- Lightweight process controller: [monitor/proc.py](monitor/proc.py)


## Requirements

- Python 3.8+
- MetaTrader 5 Terminal installed and running, with desired symbols in MarketWatch
- SQLite3 (bundled with Python)
- Python packages (install below)


## Installation

1) Clone or download this repository.
2) Install Python dependencies:

```bash
pip install -r requirements.txt
```

Notes:
- Dash UI uses Plotly; no Tkinter is required.
- watchdog is optional but recommended.
- matplotlib is not required for the Dash UI.


## Launching the Web UI

Windows (recommended):

```batch
Run_Monitors.bat
```

The server starts on http://127.0.0.1:8050 by default.


## Web UI behavior at a glance

- Auto-start on load: when the app loads, both monitors are started automatically.
  - Timelapse starts as: `python -u timelapse_setups.py --watch --min-prox-sl 0.25`
  - Hits starts as: `python -u check_tp_sl_hits.py --watch`
- Immediate status feedback: status badges show "Starting..." or "Stopping..." as you toggle, then settle to "Running"/"Stopped".
- Button text reflects current state and is kept in sync by a periodic refresh.
- Logs: recent tail of both processes is visible in the Monitors tab and can be cleared.


## Monitors tab

Controls:
- Start/Stop Timelapse button
- Start/Stop Hits button
- Exclude (comma-separated symbols, e.g. GLMUSD,BCHUSD)
- Min Prox SL (0.0–0.49). Example default: 0.25
- Auto-refresh interval (seconds)
- Since hours for DB/PnL tabs
- Clear Logs

Applying settings:
- The app auto-starts with safe defaults (Timelapse min-prox-sl 0.25, no exclude).
- To apply custom Exclude/Min Prox SL values, set them and then click Stop Timelapse, then Start Timelapse.

Status:
- status-tl and status-hits show Starting.../Stopping... immediately on toggle, and switch to Running/Stopped shortly after, based on the actual process state.

Logging:
- Logs are saved to the `logs` directory with timestamps
- Previous logs are restored when the application restarts
- Log files: `logs/timelapse.log` and `logs/hits.log`
- UI events (button presses, auto-starts, log clears) are also logged with timestamps


## DB Results tab

- Shows detected setups from the SQLite database with sortable columns and native filtering.
- Selecting a row renders an M1 candlestick chart with SL/TP overlays when data is available.
- Export buttons allow CSV export of visible or selected rows (uses pandas when available).


## PnL tab

- Displays aggregated PnL series for Forex, Crypto, and Indices categories.
- Uses the Since hours control (from the Monitors tab) to bound the analysis window.


## CLI tools

Setup Analyzer: [timelapse_setups.py](timelapse_setups.py)

```bash
# Analyze all visible MarketWatch symbols once
python timelapse_setups.py

# Analyze specific symbols
python timelapse_setups.py --symbols "EURUSD,GBPUSD,XAUUSD"

# Watch mode (polling)
python timelapse_setups.py --watch --interval 2.0

# Common options
python timelapse_setups.py --min-rrr 1.0 --min-prox-sl 0.25 --min-sl-pct 0.0 --exclude "GLMUSD,BCHUSD" --debug
```

Hit Checker: [check_tp_sl_hits.py](check_tp_sl_hits.py)

```bash
# Check recent setups
python check_tp_sl_hits.py --since-hours 24

# Watch mode
python check_tp_sl_hits.py --watch --interval 60

# Filtered checks
python check_tp_sl_hits.py --ids 1,2,3
python check_tp_sl_hits.py --symbols EURUSD,GBPUSD
```


## Database

- Location: timelapse.db (created on first write)
- Tables:
  - timelapse_setups: detected setups including price, SL/TP, RRR, score, explanation, timestamps
  - timelapse_hits: recorded hits (TP/SL) with price and timestamps
- Behavior:
  - New setups are not inserted if there is already an unsettled setup for the same symbol.
  - Hit checker uses MT5 ticks and records at most one row per setup.


## Troubleshooting

Web UI
- Server not starting: run `python dash_app.py` at the project root and check the terminal for errors.
- Missing dependencies: run `pip install -r requirements.txt`.
- Browser cache: hard-reload (Ctrl+F5) or open a private window.
- Buttons do nothing: verify the child processes can be spawned by Python on your OS.

MT5
- Ensure terminal is running and symbols are visible in MarketWatch.
- Increase MT5 initialization timeout via env MT5_TIMEOUT or CLI for [check_tp_sl_hits.py](check_tp_sl_hits.py).

Database
- If you see write errors, ensure the working directory is writable.

Performance
- Reduce symbol universe via --symbols.
- Increase polling intervals in watch modes.


## Development

- Web UI server: [dash_app.py](dash_app.py)
- Subprocess management: [monitor/proc.py](monitor/proc.py)
- DB access helpers: [monitor/db.py](monitor/db.py)
- Chart helpers: [monitor/chart.py](monitor/chart.py)
- Configuration: [monitor/config.py](monitor/config.py)

Run tests:

```bash
# Run all tests
python run_tests.py

# Run specific test modules
python -m unittest tests.test_timelapse_setups -v
python -m unittest tests.test_mt5_client -v
python -m unittest tests.test_db -v
python -m unittest tests.test_config -v
python -m unittest tests.test_domain -v
```

Code style:
- Follow PEP 8 and add type hints where practical.
- Keep analysis logic in the CLI modules; keep UI wiring in [dash_app.py](dash_app.py).


## Notes

- This tool is for educational/research use. Always verify setups manually. Past performance does not guarantee future results.