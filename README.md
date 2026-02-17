# binance-collector

Fast, schema-validated market data collection for Binance. Stores trades and order book snapshots in Parquet. Exposes data via SDK or REST API.

## Features

- **Incremental trades** - Uses `fromId`, never re-fetches
- **Multi-tick order book** - Multiple price bucket sizes per snapshot (e.g. $10, $50, $100)
- **Hot snapshots** - Last N rows always ready at <1ms for dashboards
- **REST API** - Query data over HTTP with optional auth
- **SDK client** - 3 access modes: local, hot, remote
- **No API keys** - Public Binance endpoints only
- **Parquet storage** - ~20 bytes/trade, snappy compressed

---

## Installation

```bash
pip install git+https://github.com/Talisberg/binance-collector.git
```

---

## 1. Collect Data

### Trades (incremental)

```python
from binance_collector import TradesCollector

collector = TradesCollector(symbols=['BTCUSDT', 'ETHUSDT'])
stats = collector.update()  # fetches only new trades since last run
```

### Order Book Snapshots

```python
from binance_collector import OrderBookCollector

collector = OrderBookCollector(
    symbols=['BTCUSDT'],
    tick_sizes={'BTCUSDT': [10, 50, 100]},  # price bucket sizes
    num_levels=15
)
stats = collector.update()
```

### Continuous Collection (daemon)

```python
# See examples/04_continuous_collection.py
# Runs trades + orderbook collectors in parallel threads
```

---

## 2. Deploy API Server

The API server exposes collected data over HTTP, so dashboards and other services can query without downloading full files.

### Config (`config/remote.yaml`, git-ignored)

```yaml
storage:
  base_path: /root/crypto_data
  compression: snappy

api:
  host: 0.0.0.0
  port: 8000
  api_key: null  # set to enable auth
```

### Start

```bash
python app.py --config config/remote.yaml
```

### As a systemd service

```bash
cp config/binance-api.service /etc/systemd/system/
systemctl enable binance-api
systemctl start binance-api
```

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/trades/{symbol}` | Recent trades |
| GET | `/orderbook/{symbol}` | Order book snapshots |
| GET | `/ohlcv/{symbol}` | OHLCV candles (derived from trades) |
| GET | `/stats` | Row counts, file sizes, time ranges |
| GET | `/symbols` | Available symbols |
| GET | `/health` | Health check |

**Query params:**
```
/trades/BTCUSDT?limit=1000&start_time=2026-02-15T00:00:00
/orderbook/BTCUSDT?tick_size=10&limit=100
/ohlcv/BTCUSDT?timeframe=1h&limit=24
```

**Authentication (if `api_key` set in config):**
```bash
curl -H "X-API-Key: your-key" http://your-host:8000/trades/BTCUSDT
```

---

## 3. Consume Data (SDK)

Three access modes depending on use case:

### Local (reads from local filesystem)

```python
from binance_collector import get_local_client

client = get_local_client(data_path='data')

trades  = client.get_trades('BTCUSDT', limit=1000)
orderbook = client.get_orderbook('BTCUSDT', tick_size=10, limit=100)
ohlcv   = client.get_ohlcv('BTCUSDT', timeframe='1h')
```

### Hot (last 1024 rows, ~1ms read - for dashboards)

```python
from binance_collector import get_hot_client

client = get_hot_client(data_path='data')

recent = client.get_trades('BTCUSDT')      # last 1024 rows
recent = client.get_orderbook('BTCUSDT', tick_size=10)  # last ~200 snapshots
```

### Remote (sync from collector machine)

```python
from binance_collector import BinanceCollectorClient, StorageEngine

remote = BinanceCollectorClient(
    mode='remote',
    remote_host='your.host',
    remote_user='root',
    ssh_key_path='~/.ssh/id_rsa',
    remote_data_path='/root/crypto_data'
)
local = StorageEngine(base_path='artifacts/live')

# Dashboard: sync hot snapshot only (~200 KB, <1s)
remote.sync_hot_snapshot('orderbook', 'BTCUSDT', local)
df = local.read_hot('orderbook', 'BTCUSDT')  # instant

# Training: incremental sync (only new rows since last sync)
n_new = remote.sync_incremental('trades', 'BTCUSDT', local)
print(f"Synced {n_new:,} new trades")
```

### API Client (query via HTTP)

```python
from binance_collector import BinanceCollectorAPIClient

client = BinanceCollectorAPIClient(
    base_url='http://your-host:8000',
    api_key='your-key'  # optional
)

trades    = client.get_trades('BTCUSDT', limit=100)
orderbook = client.get_orderbook('ETHUSDT', tick_size=10)
ohlcv     = client.get_ohlcv('BTCUSDT', timeframe='1h')
stats     = client.get_stats()
```

---

## Data Schema

### Trades

| Column | Type | Description |
|--------|------|-------------|
| `agg_trade_id` | int64 | Unique trade ID (dedup key) |
| `timestamp` | datetime64 | Execution time (UTC) |
| `symbol` | string | Trading pair |
| `price` | float64 | Execution price |
| `quantity` | float64 | Trade size (base asset) |
| `is_buyer_maker` | bool | True if buyer is maker |

~20 bytes/row compressed. See `SCHEMA.md` for full schema.

### Order Book

- **9 core columns**: timestamp, symbol, tick_size, best_bid, best_ask, spread, spread_pct, imbalance, depth_ratio
- **120 level columns**: bid/ask price, qty, cumulative qty, cumulative USD for 15 levels per side
- **5 tick sizes**: $10, $50, $100, $500, $1000 (configurable)

### OHLCV (derived from trades)

```python
from binance_collector import trades_to_ohlcv

ohlcv = trades_to_ohlcv(trades_df, timeframe='15min')
# columns: timestamp, open, high, low, close, volume
```

---

## Storage Layout

```
data/
├── trades/
│   ├── BTCUSDT.parquet        # full history
│   ├── BTCUSDT_hot.parquet    # last 1024 rows (fast reads)
│   └── ETHUSDT.parquet
└── orderbook/
    ├── BTCUSDT.parquet
    ├── BTCUSDT_hot.parquet
    └── ETHUSDT.parquet
```

Hot snapshots are maintained by the collector after each write:

```python
storage.maintain_hot_snapshot('trades', 'BTCUSDT', window_size=1024)
df = storage.read_hot('trades', 'BTCUSDT')  # ~1ms
```

---

## Performance

| Format | Write | Read | Size (100k rows) |
|--------|-------|------|------------------|
| Parquet (snappy) | 37ms | 21ms | 5.7 MB |
| Parquet (gzip) | 173ms | 7ms | 5.2 MB |
| Feather | 27ms | 3ms | 5.0 MB |

**Default:** Parquet + snappy (best balance)

**Hot snapshot read:** ~1ms (fixed 1024 rows)

**Growth rate:** ~3 MB/hour → ~2.2 GB/month per symbol set

---

## Project Layout

```
binance_collector/
├── collectors/
│   ├── trades.py        # incremental trades
│   └── orderbook.py     # multi-tick snapshots
├── schema/
│   └── models.py        # Pydantic models, trades_to_ohlcv()
├── storage/
│   └── engine.py        # Parquet engine, hot snapshots
├── client.py            # SDK client (local / hot / remote modes)
├── api_client.py        # HTTP client for API server
├── api_server.py        # FastAPI server
└── config.py            # YAML/env configuration

app.py                   # API server entry point (reads config/*)
examples/                # Usage examples
SCHEMA.md                # Full data schema reference
SDK_USAGE.md             # SDK usage guide
```

---

## Examples

```bash
python examples/01_basic_trades.py          # collect trades
python examples/02_orderbook_snapshot.py    # collect orderbook
python examples/03_derive_ohlcv.py          # derive OHLCV
python examples/04_continuous_collection.py # daemon mode
```

---

## License

MIT - free for commercial and personal use.
