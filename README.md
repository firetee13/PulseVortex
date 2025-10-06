# PulseVortex: Trade Setup Analyzer & Monitor

A comprehensive Python application suite for automated trade setup detection and monitoring via MetaTrader 5 (MT5) integration. Designed for forex, crypto, and indices traders, it provides CLI tools for analysis and hit detection, plus a GUI for visualization, SL proximity optimization, and PnL analytics.

Key Components:
- **CLI Setup Analyzer** (`timelapse_setups.py`): Analyzes MT5 symbols to identify high-confidence trade setups based on multi-timeframe strength, ATR volatility, pivot S/R levels, and spread/volume filters
- **PulseVortex GUI Monitor** (`monitor_gui.py`): Visual interface for real-time monitoring, database results viewing, SL proximity stats, and PnL analytics with interactive charts
- **TP/SL Hit Checker** (`check_tp_sl_hits.py`): Monitors take-profit and stop-loss hits using MT5 ticks, with bar prefiltering and quiet-hour awareness

Supports automated setup detection, real-time TP/SL hit monitoring, database persistence, and advanced analytics including SL proximity optimization and ATR-normalized PnL visualization.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [GUI Interface](#gui-interface)
- [CLI Tools](#cli-tools)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Testing](#testing)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features

### Core Analysis Engine

- **Real-time MT5 Integration**: Connects to MetaTrader 5 for live tick data, rates, and symbol info with caching for efficiency
- **Multi-Timeframe Strength Consensus**: Computes directional strength from 1H/4H/1D close changes; requires majority agreement (≥2/3 timeframes aligned) with 4H as tiebreaker
- **ATR(14) Volatility Normalization**: D1 ATR for risk sizing and bonus scoring when ATR% in [60,150]; used for PnL normalization in GUI
- **Pivot Point S/R Levels**: Previous D1 high/low/close for S1/R1 pivots; fallbacks to D1 extremes if pivots unavailable
- **Live Bid/Ask Precision**: Entry at Ask (Buy)/Bid (Sell); SL/TP distances validated against 10x spread minimum for execution realism

### Advanced Filtering System

- **Spread Classification & Filter**: Rejects "Avoid" spreads (>0.3%); classifies as Excellent(<0.1%), Good(0.1-0.2%), Acceptable(0.2-0.3%)
- **Tick Freshness Check**: Requires recent tick (≤60s old) to confirm market activity; skips closed symbols
- **SL/TP Distance Guards**: SL ≥10x spread from trigger side (Bid for Buy SL, Ask for Sell SL); same for TP from entry side
- **Proximity-to-SL Binning**: Tracks entry position within SL-TP range (0.0=at SL, 1.0=at TP); GUI analyzes sweet spots (e.g., 0.3-0.4 often optimal)
- **Quiet Hours Exclusion**: Skips analysis during 23:45-00:59 UTC+3 (forex/indices) or weekends (crypto); configurable via `monitor/quiet_hours.py`

### Risk Management

- **Risk-Reward Ratio (RRR)**: No minimum filter (post-analysis sorting); computed as reward/risk where reward=TP-entry, risk=entry-SL
- **Stop-Loss Logic**:
  - Buy: SL = min(S1, D1 Low) ensuring SL < entry
  - Sell: SL = max(R1, D1 High) ensuring SL > entry
- **Take-Profit Logic**:
  - Buy: TP = max(R1, D1 High) ensuring TP > entry
  - Sell: TP = min(S1, D1 Low) ensuring TP < entry
- **Entry Validation**: Price must be between SL/TP; rejects invalid orientations (e.g., Buy with entry > TP)

### Momentum Context

- **Timelapse Momentum**: Compares current vs. prior snapshot (D1 close delta, 4H strength change); +1.0 score for aligned D1 trend, +0.8 for 4H momentum
- **Composite Scoring**: Base = consensus count * 1.5; +0.5 for ATR% in range; spread class bonuses (-2 to +1); -0.4 penalty for late entries (prox >0.65)
- **Timeframe Consensus Flags**: Post-analysis DB computation: 1H( score≥3.0, RRR≥1.2, prox≤0.6), 4H(≥4.5/1.4/0.45), 1D(≥6.0/1.6/0.35)

### Operational Modes

- **Single Run**: One-time analysis of visible MT5 symbols or `--symbols "EURUSD,GBPUSD"`
- **Watch Mode**: Continuous polling (`--watch --interval 2.0`); defaults to 1s, honors quiet hours
- **Symbol Targeting**: Defaults to MarketWatch visibles; `--symbols` for list, `--exclude` for filtering (e.g., "GLMUSD,BCHUSD")
- **Deduplication**: Skips symbols with open setups in same proximity bin (0.1 buckets) to avoid over-trading

### GUI Interface Features

- **Monitors Tab**: Start/stop CLI tools (`timelapse_setups.py --watch`, `check_tp_sl_hits.py --watch`); live dual-pane logs; exclude symbols input
- **DB Results Tab**: Table of setups/hits with filters (time, category, status, symbol); delete selected; auto-refresh; 1m candlestick charts with SL/TP overlays (pauses in quiet hours)
- **SL Proximity Tab**: Analyzes entry position within SL-TP range; sweet-spot bins (e.g., 0.3-0.4 often >0.2R expectancy); per-symbol/category stats; auto-refresh
- **PnL Tab**: Cumulative/average charts by category (Forex/Crypto/Indices) at 10k notional; win/loss markers; time-range filter
- **PnL (Normalized) Tab**: ATR-normalized returns (risk units, log equity, vol-target, notional); category/bin filters; interactive metric switching
- **Persistence**: Settings saved to `monitor_gui_settings.json` (excludes, filters, intervals); quiet-hour aware (pauses charts/monitors 23:45-00:59 UTC+3)

### TP/SL Hit Monitoring

- **Tick-Based Detection**: Scans ticks since last check; Buy SL on Bid≤SL, TP on Bid≥TP; Sell SL on Ask≥SL, TP on Ask≤TP
- **Bar Prefiltering**: M1 bars to identify candidate windows (bars crossing SL/TP ± spread guard); fetches ticks only for those (~200ms poll)
- **Quiet-Hour Awareness**: Pauses monitoring 23:45-00:59 UTC+3 (forex/indices) or weekends (crypto); resumes automatically
- **Deduplication & State**: Tracks last-checked per setup; ignores hits ≤ entry time or in quiet windows; persists state in DB

### Data Persistence

- **SQLite Schema**: `timelapse_setups` (id, symbol, direction, price, sl, tp, rrr, score, as_of, detected_at, proximity_to_sl, proximity_bin, inserted_at); `timelapse_hits` (setup_id, hit, hit_price/time, entry_price/time)
- **Deduplication**: ON CONFLICT DO NOTHING on (symbol, direction, as_of); open-setup gating by proximity bin
- **Consensus Extension**: Optional `consensus` table flags 1H/4H/1D agreement based on score/RRR/prox thresholds
- **GUI Analytics**: Queries for PnL (hits only), proximity stats (bins 0.1-wide), normalized returns (ATR/vol)

### Performance Optimizations

- **MT5 Rate Caching**: TTL per timeframe (W1:120s, D1/H4:45s, H1:12s, M15:6s); reduces IPC calls
- **Tick Fetching**: Chunked by minute (default 1440min/page); prefiltered via M1 bars crossing SL/TP
- **DB Indexing**: On symbol/setup_id/hit_time; lazy connections with managed close

## Requirements

### Software Dependencies

- **Python 3.8+** (tested 3.10-3.12)
- **MetaTrader 5 Terminal**: Installed and running; symbols in MarketWatch
- **SQLite3**: Built-in with Python stdlib

### Python Packages

- `MetaTrader5>=5.0.45` (MT5 Python API)
- `matplotlib>=3.5.0` (GUI charts, candlesticks, PnL analytics)
- `numpy>=1.21.0` (ATR calculations, array ops)

### System Requirements

- **Windows 10/11** (primary; MT5 Python API compatibility)
- **4GB+ RAM** (for 100+ symbols, charts, MT5 terminal)
- **Stable internet** (MT5 data streaming; GUI local only)
- **Tkinter** (included in most Python installs; `pip install tk` if missing)

## Installation

1. **Clone/Download** repository to `c:/monitor_prod` (or your workspace)
2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   (MetaTrader5, matplotlib, numpy; no extras needed)
3. **Install/Run MT5 Terminal**: Ensure `terminal64.exe` is accessible (set `MT5_TERMINAL_PATH` env if non-standard)
4. **Configure MT5**: Add symbols (e.g., EURUSD, BTCUSD) to MarketWatch; enable tick history
5. **GUI Setup**: Tkinter auto-included; test with `python -c "import tkinter"`

## Quick Start

### Basic Analysis (CLI)

```bash
# Single analysis of visible MarketWatch symbols
python timelapse_setups.py

# Target specific symbols (comma-separated)
python timelapse_setups.py --symbols "EURUSD,GBPUSD,BTCUSD"

# Continuous watch (1s default; honors quiet hours)
python timelapse_setups.py --watch --interval 2.0 --exclude "GLMUSD,BCHUSD"

# Debug filtering/spreads
python timelapse_setups.py --debug --brief
```

### Example Output

```
EURUSD | Buy @ 1.0850 (Ask) | SL 1.0820 (S1) | TP 1.0920 (R1) | RRR 2.33 | score 3.2
  -> Consensus: 1H/4H/1D (2/3); Strength 1H/4H/1D: +2.1/+1.8/+0.9; ATR: 15.2 pips (1.40%, bonus); Prox: 0.32 (near support); Timelapse: D1 up 0.0032 (+1.0), 4H momentum +0.8; Spread: 0.08% (Excellent +1.0)
[DB] Inserted 1 new setup(s): EURUSD | tick time 14:23:45 UTC+3 | bid 1.0849 | ask 1.0850
```

### TP/SL Monitoring (CLI)

```bash
# Single check of recent setups (last 24h)
python check_tp_sl_hits.py --since-hours 24

# Target specific setups/ symbols
python check_tp_sl_hits.py --ids 1,2,3 --verbose
python check_tp_sl_hits.py --symbols "EURUSD,BTCUSD" --max-mins 60

# Continuous watch (1s default; auto-pauses quiet hours)
python check_tp_sl_hits.py --watch --interval 1 --bar-timeframe M1 --tick-padding 1.0
```

### Example Output

```
[NO HIT] #123 EURUSD Buy | window 45.2 mins | ticks 1,234 | pages 2 | fetch=45ms scan=12ms thr=102k avg_pg=617
[HIT TIMING] #124 BTCUSD Sell | windows 3 | ticks 567 | pages 1 | fetch=23ms scan=4ms | TP hit at 2025-10-06 14:23:45 UTC+3
[DB] Recorded TP hit for setup #124 BTCUSD Sell | entry 65,200 | hit_price 64,800 | rrr 2.1
Checked 15 setup(s); hits recorded: 2. BTCUSD EURUSD
```

## GUI Interface

### Launching the GUI

**Windows (Recommended)**:
```batch
cscript Run_Monitors.vbs
```
(Launches minimized; right-click tray icon to restore)

**Direct Launch**:
```bash
python run_monitor_gui.pyw
# Or with log restore (if restarting):
python run_monitor_gui.pyw --restore-timelapse-log /path/to/timelapse.log --restore-hits-log /path/to/hits.log
```

**Debug Mode**:
```bash
python monitor_gui.py
```
(Visible console for errors; use for troubleshooting)

### GUI Workflow

1. Launch GUI (minimized via VBS or direct)
2. **Monitors Tab**: Set exclude symbols (e.g., "GLMUSD,BCHUSD"); click Start for both tools (auto-restarts on crash)
3. **DB Results**: Filter by time/category/status/symbol; select row for 1m chart with SL/TP (pauses in quiet hours)
4. **SL Proximity**: Auto-computes sweet spots (e.g., 0.3-0.4 bin often >50% TP, +0.2R expectancy); per-category leaders
5. **PnL Tabs**: 10k-notional cumulative by category; normalized (ATR/risk/vol/notional) with bin filters
6. **Restart**: "Restart" button relaunches GUI preserving logs; settings persist across sessions

### GUI Settings

- **Persistent**: Excludes, filters, intervals saved to `monitor_gui_settings.json`
- **Quiet Hours**: Charts pause 23:45-00:59 UTC+3 (forex/indices) or weekends (crypto); hits monitor auto-pauses/resumes
- **Charts**: Matplotlib candlesticks (M1 bars or ticks); SL/TP lines; entry arrow; hit markers; timezone UTC+3 display

## CLI Tools

### Setup Analyzer (`timelapse_setups.py`)

```bash
python timelapse_setups.py [OPTIONS]
```

#### Usage Options
- `--symbols SYMBOLS`: Target list (default: MarketWatch visibles)
- `--exclude SYMBOLS`: Skip list (e.g., `--exclude "GLMUSD,BCHUSD"`)
- `--watch`: Continuous mode (default interval 1s; `--interval 2.0`)
- `--debug`: Diagnostics (filter reasons, spread calcs, tick ages)
- `--brief`: Compact output (no JSON details)

#### Output Control
- `--top N`: Limit to top N setups by score/RRR (post-filter)

### TP/SL Hit Checker (`check_tp_sl_hits.py`)

```bash
python check_tp_sl_hits.py [OPTIONS]
```

#### Usage Options
- `--since-hours HOURS`: Last N hours of setups (default: all)
- `--ids IDS`: Specific setup IDs (e.g., `--ids 1,2,3`)
- `--symbols SYMBOLS`: Filter by symbols (e.g., `--symbols "BTCUSD,SOLUSD"`)
- `--watch`: Continuous (default 1s; `--interval 5`)
- `--max-mins MINS`: Tick chunk size (default 1440min; smaller for perf)
- `--bar-timeframe TF`: Prefilter bars (default M1; e.g., M5 for less noise)
- `--bar-backtrack MINS`: Bar history buffer (default 2min before as_of)
- `--tick-padding SECS`: Extra seconds around candidate windows (default 1.0)
- `--dry-run`: Test without DB writes
- `--verbose`: Timings/pages/ticks per setup

#### Detection Logic
- **Prefilter**: M1 bars crossing SL/TP ±1.5x spread; fetches ticks only for candidates
- **Trigger Prices**: Buy (Bid≤SL, Bid≥TP); Sell (Ask≥SL, Ask≤TP)
- **Quiet Awareness**: Ignores hits in 23:45-00:59 UTC+3 (forex/indices) or weekends (crypto)
- **State Tracking**: Per-setup last-checked timestamp; resumes from there on restart
- **Server Offset**: Auto-detects broker timezone (±hours) for accurate tick alignment

## Configuration

### Database (`timelapse.db`)
- **Path**: Resolved via `monitor/config.py` (default `./timelapse.db`; env `TIMELAPSE_DB_PATH`)
- **Auto-Migration**: Adds columns (e.g., `proximity_bin`, `detected_at`) on startup
- **Backup**: Manual via SQLite tools; GUI shows recent setups/hits only
- **Schema**: See [Database Schema](#database-schema) below

### MT5 Terminal
- **Setup**: Run MT5; add symbols to MarketWatch (View > Market Watch > right-click > Show All)
- **Permissions**: Allow DLL imports (Tools > Options > Expert Advisors); enable Algo Trading if needed
- **Path Override**: Set `MT5_TERMINAL_PATH` env for non-standard installs (e.g., portable)
- **Timeout/Retries**: Defaults 90s/2 retries; increase via `--mt5-timeout 120 --mt5-retries 3` in CLI

### Environment Variables
- `TIMELAPSE_DB_PATH`: Custom DB location (default `./timelapse.db`)
- `TIMELAPSE_SPREAD_MULT`: SL/TP distance multiplier (default 10x spread)
- `MT5_TERMINAL_PATH`: Path to `terminal64.exe` (e.g., `C:/Users/You/AppData/Roaming/MetaQuotes/...`)
- `MT5_TIMEOUT`: MT5 init timeout (default 90s)
- `MT5_RETRIES`: Init retries (default 2)
- `MT5_PORTABLE`: Set to 1 for portable MT5 mode

## Database Schema

### `timelapse_setups` Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY AUTOINCREMENT | Unique setup ID |
| symbol | TEXT NOT NULL | e.g., "EURUSD", "BTCUSD" |
| direction | TEXT NOT NULL | "Buy" or "Sell" |
| price | REAL | Entry price (Ask for Buy, Bid for Sell) |
| sl | REAL | Stop-loss level (S1/D1 Low for Buy; R1/D1 High for Sell) |
| tp | REAL | Take-profit level (R1/D1 High for Buy; S1/D1 Low for Sell) |
| rrr | REAL | Reward/risk ratio (TP-entry)/(entry-SL) |
| score | REAL | Composite score (consensus + ATR bonus + spread + momentum - late penalty) |
| as_of | TEXT NOT NULL | Analysis timestamp (UTC naive ISO) |
| detected_at | TEXT | Detection time (UTC+3 ISO; optional) |
| proximity_to_sl | REAL | Entry position in SL-TP range (0.0=at SL, 1.0=at TP) |
| proximity_bin | TEXT | Binned prox (e.g., "0.3-0.4") for dedup/gating |
| inserted_at | TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP | DB insertion time |

### `timelapse_hits` Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY AUTOINCREMENT | Hit record ID |
| setup_id | INTEGER UNIQUE | References timelapse_setups.id (CASCADE delete) |
| symbol | TEXT NOT NULL | Copied from setup |
| direction | TEXT NOT NULL | "Buy" or "Sell" |
| sl | REAL | Copied from setup |
| tp | REAL | Copied from setup |
| hit | TEXT NOT NULL CHECK (TP,SL) | "TP" or "SL" |
| hit_price | REAL | Actual trigger price (Bid/Ask based on side) |
| hit_time | TEXT NOT NULL | Hit timestamp (UTC naive ISO) |
| hit_time_utc3 | TEXT | Display time (UTC+3 ISO) |
| entry_time_utc3 | TEXT | Setup insertion (UTC+3 ISO) |
| entry_price | REAL | Copied from setup/hit |
| checked_at | TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP | Scan completion time |

### Indexes & Optimizations
- `timelapse_hits`: INDEX on (symbol), (setup_id)
- `timelapse_setups`: UNIQUE(symbol, direction, as_of); gating via LEFT JOIN hits
- **Consensus Table** (optional): Flags per-setup (is_1h_consensus, is_4h_consensus, is_1d_consensus) based on thresholds

## Testing

### Unit Tests
Run via `unittest` (≥90% coverage target; MT5 fakes for isolation):
```bash
python -m unittest discover -s tests -p "test_*.py" -v
```
- `tests/test_timelapse_setups.py`: Strength, pivots, ATR, filtering, scoring
- `tests/test_check_tp_sl_hits.py`: Tick scanning, bar prefilter, quiet hours
- `tests/test_mt5_client.py`: Caching, offsets, symbol resolution
- `tests/test_quiet_hours.py`: Timezone logic, ranges
- `tests/test_tp_sl_state.py`: DB state persistence

Add tests for new features; use fakes for MT5/SQLite (no live terminal needed).

## Architecture

### Core Modules (`monitor/`)

- **`mt5_client.py`**: MT5 wrapper; init/shutdown, symbol resolve, rates/ticks with caching (TTL per TF), server offset detection
- **`config.py`**: DB path resolution (`TIMELAPSE_DB_PATH`), env defaults
- **`db.py`**: SQLite helpers; table creation, inserts with gating/dedup, consensus rebuild, hit recording
- **`domain.py`**: Dataclasses (Setup, Hit, TickFetchStats); no business logic
- **`quiet_hours.py`**: Timezone-aware ranges (UTC+3 quiet 23:45-00:59; crypto weekends); iter_active/quiet
- **`symbols.py`**: Heuristic classification (forex/currency pairs, crypto/*USD, indices like NAS100)

### Data Flow

1. **Symbol Discovery**: MT5 MarketWatch visibles (or `--symbols`)
2. **Data Fetch**: Cached rates (D1/H4/W1/H1/M15); live Bid/Ask/tick time; pivots from prev D1
3. **Analysis**: Strength deltas, consensus (2/3 TFs), S/R SL/TP, RRR/prox, filters (spread/tick/quiet/distance)
4. **Scoring & Filter**: Composite score; reject invalid/no-consensus/low-spread; dedup open setups by bin
5. **DB Insert**: Gated by open status/prox bin; proximity_bin for stats; consensus flags post-insert
6. **Monitoring**: `check_tp_sl_hits.py` polls active setups; bar-prefilter → tick scan → hit record
7. **GUI**: Tabs query DB for results/prox/PnL; MT5 for charts; auto-pause quiet hours

## Troubleshooting

### Common Issues

#### MT5 Connection
```
[MT5] initialize() failed; cannot read symbols.
```
- Ensure MT5 running (not updating); only one Python/MT5 connection
- Check `MT5_TERMINAL_PATH` env; try `--mt5-timeout 120 --mt5-retries 3`
- Firewall/antivirus blocking; run as admin if needed
- Portable MT5: set `MT5_PORTABLE=1`

#### No Setups Detected
- Verify symbols in MarketWatch (right-click > Show All)
- Check tick history (Tools > History Center > download if empty)
- Relax filters: `--debug` for reasons (e.g., spread_avoid, no_recent_ticks)
- Quiet hours active? Wait or test non-quiet symbol (e.g., BTCUSD anytime)

#### Database Issues
- Write permissions? Run as admin or move DB outside protected folders
- Corruption: Delete `timelapse.db` (recreates); backup via `sqlite3 timelapse.db .dump > backup.sql`
- Schema errors: GUI auto-migrates (adds columns); check with `sqlite3 timelapse.db .schema`

#### GUI/Charts Not Rendering
- **Matplotlib missing**: `pip install matplotlib numpy`
- **Tkinter absent**: Reinstall Python with Tkinter (or `pip install tk` on some systems)
- **Quiet pause**: Charts auto-pause 23:45-00:59 UTC+3; resume after
- **MT5 busy**: Close other Python/MT5 scripts; increase timeout

#### Performance Slow
- Limit symbols: `--symbols "EURUSD,GBPUSD"` or fewer in MarketWatch
- Increase intervals: `--interval 5` (setups), `--max-mins 60` (hits)
- Cache TTLs: Higher timeframes reuse data longer (W1:120s)
- Close MT5 charts/tools using same terminal

#### Hit Checker No Hits
- Verify setups in DB: `sqlite3 timelapse.db "SELECT * FROM timelapse_setups LEFT JOIN timelapse_hits ON setup_id=id WHERE hit_time IS NULL"`
- Tick history: Download in MT5 History Center (F2 > Symbol > Bars)
- Quiet ignore: Hits in quiet windows skipped; check `--verbose` for "IGNORED HIT"
- Server offset: `--verbose` shows offset; mismatches cause missed ticks

### Debug Mode

Enable verbose logging:
```bash
# Setups: full filter diagnostics
python timelapse_setups.py --debug

# Hits: per-setup timings/pages/ticks
python check_tp_sl_hits.py --verbose --trace-pages

# GUI: Run `python monitor_gui.py` (console output)
```
- Setups: Symbol evals, rejection counts (e.g., "spread_avoid: 5"), SL/TP details
- Hits: Chunk timings, tick counts, "NO HIT" vs "HIT TIMING", ignored reasons

## Contributing

### Development Setup

1. Fork/clone repo; create feature branch (`feat(gui): add bin filter`)
2. Install deps: `pip install -r requirements.txt`
3. Add tests in `tests/` (mirror existing; ≥90% coverage)
4. Run suite: `python -m unittest discover -s tests -v`
5. Validate: Manual MT5 session (symbols visible), GUI screenshots, DB queries
6. Commit/PR: Conventional messages; link issues; flag DB schema changes (backup timelapse.db)

### Code Style

- **PEP 8**: 4-space indent; snake_case functions/vars, PascalCase classes
- **Type Hints**: On public APIs (e.g., `def analyze(series: Dict[str, List[Snapshot]]) -> Tuple[...]` )
- **Docstrings**: Google/Numpy style for modules/functions; explain params/returns
- **Modularity**: Shared logic in `monitor/` (e.g., MT5 client, DB ops); no CLI/GUI duplication
- **Error Handling**: Try/except for MT5/DB; log non-fatal (e.g., `[DB] Skipped insert: {e}`)

### Security Notes

- **No Credentials**: MT5 uses terminal session; never hardcode logins
- **DB Local**: SQLite file-based; protect `timelapse.db` (contains prices, no sensitive data)
- **Env Vars**: Use for paths/timeouts; avoid committing personal MT5 paths

## License

MIT License (see LICENSE file or add one). For educational/research use; verify setups manually. Trading involves risk; past performance ≠ future results.
