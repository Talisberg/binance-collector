# Binance Collector

Fast, schema-validated data ingestion for Binance cryptocurrency markets.

## Features

- **🚀 Incremental updates**: Only fetch new data since last collection
- **📊 Two core data sources**: Trades + Order Book (OHLCV derived from trades)
- **✅ Schema validation**: Type-safe with Pydantic models
- **💾 Efficient storage**: Parquet with automatic deduplication
- **🔑 Zero config**: No API keys needed for public data
- **⚡ Production ready**: Retry logic, rate limiting, parallel collection

## Installation

```bash
# Clone or download
git clone https://github.com/yourusername/binance-collector.git
cd binance-collector

# Install dependencies
pip install -r requirements.txt

# Or install as package
pip install -e .
```

## Quick Start

### 1. Collect Trades (Incremental)

```python
from binance_collector import TradesCollector

collector = TradesCollector(symbols=['BTCUSDT', 'ETHUSDT'])
stats = collector.update()  # Fetches only new trades

print(stats)
# {'BTCUSDT': {'rows': 1004, 'start_time': ..., 'end_time': ...}}
```

### 2. Collect Order Book Snapshots

```python
from binance_collector import OrderBookCollector

collector = OrderBookCollector(
    symbols=['BTCUSDT'],
    tick_sizes={'BTCUSDT': [10, 50, 100]},  # Multi-tick aggregation
    num_levels=15
)

stats = collector.update()
```

### 3. Derive OHLCV from Trades

```python
from binance_collector import StorageEngine, trades_to_ohlcv

storage = StorageEngine()
trades_df = storage.read('trades', 'BTCUSDT')

# Create hourly candles
ohlcv = trades_to_ohlcv(trades_df, timeframe='1h')
```

## Data Types

### 1. Aggregate Trades
- Incremental collection using `fromId`
- Columns: `agg_trade_id`, `timestamp`, `symbol`, `price`, `quantity`, `is_buyer_maker`, etc.
- Auto-deduplication on `agg_trade_id`
- ~1ms latency from exchange

### 2. Order Book Snapshots
- Multi-tick aggregation (e.g., $10, $50, $100 buckets)
- 15 levels per side (configurable)
- Cumulative quantities and USD values
- Imbalance and depth ratio metrics

### 3. OHLCV (Derived)
- Generated from trade data
- Any timeframe: 1m, 5m, 1h, 4h, 1d, etc.
- More accurate than exchange candles

## Storage

**Parquet format** (10x smaller than CSV, columnar compression):
```
data/
├── trades/
│   ├── BTCUSDT.parquet
│   └── ETHUSDT.parquet
└── orderbook/
    ├── BTCUSDT.parquet
    └── ETHUSDT.parquet
```

## Performance

**Benchmark** (100k trades):
- Parquet (snappy): 36ms write, 21ms read, 5.7 MB
- Parquet (gzip): 173ms write, 7ms read, 5.2 MB
- Feather: 27ms write, 3ms read, 5.0 MB

**Recommendation**: Parquet with snappy (default) - best balance

## Configuration (Optional)

```python
from binance_collector import Config

# Load from file
config = Config.from_file('config.yaml')

# Or from environment variables
config = Config.from_env()

# Or create programmatically
config = Config(
    storage=StorageConfig(
        base_path='data',
        compression='snappy'
    )
)
```

## Examples

See `examples/` directory:

1. **01_basic_trades.py** - Basic incremental trades collection
2. **02_orderbook_snapshot.py** - Order book snapshots
3. **03_derive_ohlcv.py** - Create OHLCV from trades
4. **04_continuous_collection.py** - Daemon mode (parallel collection)

## Run Examples

```bash
cd examples

# Collect trades
python 01_basic_trades.py

# Collect orderbook
python 02_orderbook_snapshot.py

# Derive OHLCV
python 03_derive_ohlcv.py

# Run continuous (Ctrl+C to stop)
python 04_continuous_collection.py
```

## Architecture

```
binance_collector/
├── collectors/          # Data collectors
│   ├── trades.py       # Incremental trades
│   └── orderbook.py    # Order book snapshots
├── schema/             # Pydantic models + validation
│   └── models.py
├── storage/            # Parquet storage engine
│   └── engine.py
└── config.py           # Configuration management
```

## API Reference

### TradesCollector

```python
collector = TradesCollector(
    symbols=['BTCUSDT'],
    storage=StorageEngine()
)

# Update (incremental)
stats = collector.update(save=True)

# Backfill from start time (max ~7 days via REST API)
df = collector.backfill('BTCUSDT', start_time=datetime(2024, 1, 1))
```

### OrderBookCollector

```python
collector = OrderBookCollector(
    symbols=['BTCUSDT'],
    tick_sizes={'BTCUSDT': [10, 50, 100]},
    num_levels=15
)

# Collect single snapshot
df = collector.collect_snapshot('BTCUSDT')

# Continuous mode
collector.run_continuous(interval_seconds=30, duration_hours=24)
```

### StorageEngine

```python
storage = StorageEngine(base_path='data', compression='snappy')

# Write with deduplication
storage.write(
    df,
    data_type='trades',
    symbol='BTCUSDT',
    dedup_columns=['agg_trade_id']
)

# Read with time filter
df = storage.read(
    'trades',
    'BTCUSDT',
    start_time=pd.Timestamp('2024-01-01'),
    end_time=pd.Timestamp('2024-02-01')
)

# Get file info
info = storage.get_file_info('trades', 'BTCUSDT')
```

## Data Inspection

### Quick Stats

```bash
# Check collected data
python << 'EOF'
import pandas as pd
from pathlib import Path
from binance_collector import StorageEngine

storage = StorageEngine(base_path='data')

print("TRADES:")
for symbol in ['BTCUSDT', 'ETHUSDT']:
    df = storage.read('trades', symbol)
    if not df.empty:
        size_mb = Path(f'data/trades/{symbol}.parquet').stat().st_size / 1024 / 1024
        print(f"  {symbol}: {len(df):,} trades, {size_mb:.2f} MB")
        print(f"    Range: {df['timestamp'].min()} → {df['timestamp'].max()}")

print("\nORDERBOOK:")
for symbol in ['BTCUSDT', 'ETHUSDT']:
    df = storage.read('orderbook', symbol)
    if not df.empty:
        print(f"  {symbol}: {len(df)} snapshots, {df['tick_size'].nunique()} tick sizes")
EOF
```

### View Sample Data

```python
from binance_collector import StorageEngine

storage = StorageEngine(base_path='data')

# Read trades
trades = storage.read('trades', 'BTCUSDT')
print(trades.tail(10))  # Last 10 trades

# Read orderbook
orderbook = storage.read('orderbook', 'BTCUSDT')
print(orderbook[orderbook['tick_size'] == 50].tail(5))  # Last 5 snapshots for $50 tick
```

## No API Keys Required

All data is public and accessible without authentication:
- ✅ Aggregate trades
- ✅ Order book depth
- ❌ Account trades (requires API keys)

## Rate Limits

Binance public API: **1200 requests/minute**

This package:
- Default: 10 parallel workers max
- Auto-retry with exponential backoff
- Sleep between requests (configurable)

## License

MIT License - Free for commercial and personal use

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests
4. Submit pull request

## Roadmap

- [ ] WebSocket support for real-time streaming
- [ ] More exchanges (Coinbase, Kraken, etc.)
- [ ] Data quality metrics
- [ ] Compression benchmarks
- [ ] Docker image

## Support

- 📖 Documentation: See `examples/`
- 🐛 Issues: GitHub Issues
- 💬 Discussions: GitHub Discussions
