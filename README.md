# EASY Insight Monitor - MT5 Trade Setup Analyzer

A sophisticated Python application that analyzes MetaTrader 5 (MT5) symbols to identify high-confidence trade setups based on technical indicators, price momentum, and support/resistance levels. This tool is designed for forex and crypto traders seeking automated setup detection with advanced filtering and risk management features.

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage](#usage)
- [Command Line Options](#command-line-options)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Testing](#testing)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features

### Core Analysis Engine

- **Real-time MT5 Integration**: Directly connects to MetaTrader 5 terminal for live symbol data
- **Multi-Timeframe Strength Analysis**: Evaluates symbol strength across 4H, 1D, and 1W timeframes
- **ATR-Based Volatility Assessment**: Incorporates Average True Range (ATR) and ATR percentage for setup validation
- **Pivot Point Support/Resistance**: Uses previous day's pivot points (S1/R1) for stop-loss and take-profit levels
- **Bid/Ask Price Handling**: Precise entry pricing using current Bid/Ask spreads from MT5

### Advanced Filtering System

- **Spread Filtering**: Only accepts symbols with spreads below 0.3% (Excellent/Good/Acceptable classes)
- **Volume Validation**: Filters out symbols with low tick volume in the last 5 M1 bars (minimum 10 ticks per bar)
- **SL Distance Protection**: Ensures stop-loss is at least 10x the current spread away from entry price
- **Proximity Guards**: Optional filters for entries too close to support/resistance levels
- **Time Window Filtering**: Excludes low-volume trading hours (23:00-01:00 UTC+3)

### Risk Management

- **Risk-Reward Ratio (RRR)**: Configurable minimum RRR threshold (default 1.0)
- **Stop-Loss Logic**:
  - Buy setups: SL = S1 (support) or D1 Low
  - Sell setups: SL = R1 (resistance) or D1 High
- **Take-Profit Logic**:
  - Buy setups: TP = R1 (resistance) or D1 High
  - Sell setups: TP = S1 (support) or D1 Low
- **Price Validation**: Ensures entry price lies between SL and TP for valid setups

### Momentum Context

- **Timelapse Simulation**: Compares current strength values against previous periods for momentum confirmation
- **Trend Alignment Scoring**: Bonus points for setups aligned with D1 close trends and 4H strength changes
- **Consensus Requirements**: Requires majority agreement across timeframes for direction confirmation

### Operational Modes

- **Single Run Mode**: Analyze symbols once and exit
- **Watch Mode**: Continuous monitoring with configurable polling intervals
- **Symbol Selection**: Analyze all visible MarketWatch symbols or specify custom symbol lists
- **Exclusion Lists**: Filter out unwanted symbols by name

### Data Persistence

- **SQLite Database**: Automatic storage of detected setups with deduplication
- **Setup Tracking**: Prevents duplicate setups for the same symbol/direction/time
- **Hit Tracking**: Integration with timelapse_hits table for settled trade management
- **Metadata Logging**: Stores tick timestamps, bid/ask data, and backfill status

### Performance Optimizations

- **Rate Caching**: Intelligent caching of MT5 rate data with TTL-based expiration
- **Tick History Optimization**: Minimal IPC overhead with cached tick data
- **Batch Processing**: Efficient symbol processing with connection reuse

## Requirements

### Software Dependencies

- **Python 3.8+**
- **MetaTrader 5 Terminal**: Running and configured with symbols
- **SQLite3**: Included with Python 3.8+

### Python Packages

- `MetaTrader5` (via pip install MetaTrader5)
- `watchdog` (optional, for file watching - pip install watchdog)

### System Requirements

- Windows (MT5 compatibility)
- Sufficient RAM for symbol processing (4GB+ recommended)
- Network connection for MT5 data access

## Installation

1. **Clone or Download** the repository to your local machine
2. **Install Python Dependencies**:
   ```bash
   pip install MetaTrader5
   pip install watchdog  # Optional for advanced watching
   ```
3. **Ensure MT5 Terminal** is installed and running
4. **Configure MT5** with desired symbols in MarketWatch

## Quick Start

### Basic Analysis

```bash
# Analyze all visible MT5 symbols once
python timelapse_setups.py

# Analyze specific symbols
python timelapse_setups.py --symbols "EURUSD,GBPUSD,XAUUSD"

# Watch mode with 2-second intervals
python timelapse_setups.py --watch --interval 2.0
```

### Example Output

```
EURUSD | Buy @ 1.0850 | SL 1.0820 | TP 1.0920 | RRR 2.33 | score 3.2
  -> Strength 4H/1D/1W: 2.1/1.8/0.9; ATR: 15.2 pips (1.40%); S/R: S1=1.0820, R1=1.0920 near support; Timelapse: D1 Close up 0.0032; Spread: 0.08% (Excellent)
```

## Usage

### Command Line Interface

```bash
python timelapse_setups.py [OPTIONS]
```

### Command Line Options

#### Symbol Selection
- `--symbols SYMBOLS`: Comma-separated list of symbols (default: all visible in MarketWatch)
- `--exclude SYMBOLS`: Comma-separated symbols to exclude (e.g., "GLMUSD,BCHUSD")

#### Risk Parameters
- `--min-rrr FLOAT`: Minimum risk-reward ratio (default: 1.0)
- `--min-prox-sl FLOAT`: Minimum distance from SL as fraction of SL-TP range (0.0-0.49, default: 0.0)
- `--min-sl-pct FLOAT`: Minimum SL distance as percentage of price (default: 0.0)

#### Operational Modes
- `--watch`: Run continuously and poll MT5 for updates
- `--interval FLOAT`: Polling interval in seconds when watching (default: 1.0)
- `--top N`: Limit output to top N setups by score/RRR

#### Output Control
- `--brief`: Brief output without detailed explanations
- `--debug`: Enable debug output with filtering diagnostics

## Configuration

### MT5 Connection

The application automatically connects to MT5 on startup. Ensure:

- MT5 terminal is running
- Symbols are added to MarketWatch
- Sufficient tick history is available

### Database Configuration

- **Location**: `timelapse.db` in the script directory
- **Auto-creation**: Tables created automatically on first run
- **Backup**: Regular backups recommended for production use

## Database Schema

### timelapse_setups Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Auto-incrementing setup ID |
| symbol | TEXT NOT NULL | Symbol name (e.g., EURUSD) |
| direction | TEXT NOT NULL | Buy or Sell |
| price | REAL | Entry price (Bid/Ask based on direction) |
| sl | REAL | Stop-loss level |
| tp | REAL | Take-profit level |
| rrr | REAL | Risk-reward ratio |
| score | REAL | Composite setup score |
| explain | TEXT | Detailed setup explanation |
| as_of | TEXT NOT NULL | Analysis timestamp (UTC naive) |
| detected_at | TEXT | Detection timestamp |
| inserted_at | TEXT NOT NULL | Database insertion time |

### timelapse_hits Table

| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER PRIMARY KEY | Hit record ID |
| setup_id | INTEGER UNIQUE | Reference to setup |
| symbol | TEXT NOT NULL | Symbol name |
| direction | TEXT NOT NULL | Buy or Sell |
| sl | REAL | Stop-loss level |
| tp | REAL | Take-profit level |
| hit | TEXT NOT NULL | TP or SL |
| hit_price | REAL | Actual hit price |
| hit_time | TEXT NOT NULL | Hit timestamp |
| entry_time_utc3 | TEXT | Entry time (UTC+3) |
| entry_price | REAL | Entry price |
| checked_at | TEXT NOT NULL | Check timestamp |

## Testing

Run the comprehensive test suite:

```bash
python -m unittest tests.test_timelapse_setups -v
```

### Test Coverage

- **MT5 Integration**: Connection, rate caching, tick backfill
- **Analysis Logic**: Strength consensus, SL/TP calculation, filtering
- **Database Operations**: Setup insertion, deduplication
- **Volume Filtering**: Tick volume validation
- **SL Distance Checks**: Spread-based distance validation

## Architecture

### Core Components

1. **MT5 Integration Layer**
   - Connection management and symbol selection
   - Rate data fetching with caching
   - Tick history backfill for Bid/Ask

2. **Analysis Engine**
   - Strength calculation across timeframes
   - Pivot point computation
   - Setup scoring and filtering

3. **Data Persistence Layer**
   - SQLite database operations
   - Setup deduplication
   - Result serialization

4. **Operational Modes**
   - Single-run processing
   - Continuous watching with polling
   - Symbol filtering and exclusion

### Data Flow

1. **Symbol Discovery**: Get visible symbols from MT5 MarketWatch
2. **Data Collection**: Fetch rates and ticks for each symbol
3. **Analysis**: Compute strength, pivots, and validate setups
4. **Filtering**: Apply all configured filters and guards
5. **Storage**: Save valid setups to database
6. **Reporting**: Display results with detailed explanations

## Troubleshooting

### Common Issues

#### MT5 Connection Problems
```
[MT5] initialize() failed; cannot read symbols.
```
- Ensure MT5 terminal is running and accessible
- Check firewall/antivirus settings
- Verify Python can access MT5 API

#### No Setups Found
- Check symbol visibility in MarketWatch
- Verify sufficient tick history
- Adjust filtering parameters (min-rrr, min-sl-pct)
- Use --debug flag for detailed diagnostics

#### Database Errors
- Ensure write permissions to script directory
- Check for database corruption (backup and recreate)
- Verify SQLite3 availability

#### Performance Issues
- Reduce symbol count with --symbols
- Increase polling interval in watch mode
- Check MT5 terminal resource usage

### Debug Mode

Enable debug output for detailed information:

```bash
python timelapse_setups.py --debug
```

This provides:
- Symbol evaluation counts
- Filtering reasons and counts
- SL/TP calculation details
- Volume check results

## Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make changes with comprehensive tests
4. Ensure all tests pass
5. Submit pull request

### Code Style

- Follow PEP 8 conventions
- Add type hints for new functions
- Include docstrings for public APIs
- Maintain test coverage above 90%

### Testing Guidelines

- Add unit tests for new features
- Include integration tests for MT5 interactions
- Test edge cases and error conditions
- Verify database operations

---

**Note**: This tool is for educational and research purposes. Always verify setups manually before entering trades. Past performance does not guarantee future results. Use at your own risk.