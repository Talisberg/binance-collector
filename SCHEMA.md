# Data Schema Reference

Complete schema documentation for binance-collector data files.

---

## Trades Data

**File:** `trades/{SYMBOL}.parquet`

**Description:** Aggregate trades from Binance. Each row represents a single trade aggregated at millisecond level.

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `agg_trade_id` | int64 | Unique aggregate trade ID | 2847392847 |
| `timestamp` | datetime64[ns] | Trade execution time (UTC) | 2026-02-15 23:19:45.437 |
| `symbol` | string | Trading pair | BTCUSDT |
| `price` | float64 | Execution price | 68513.59 |
| `quantity` | float64 | Trade quantity (in base asset) | 0.00292 |
| `first_trade_id` | int64 | First trade ID in aggregate | 4029384756 |
| `last_trade_id` | int64 | Last trade ID in aggregate | 4029384756 |
| `is_buyer_maker` | bool | True if buyer is maker | False |

**Storage:**
- Format: Parquet with snappy compression
- Compression ratio: ~20 bytes/row
- Deduplication: By `agg_trade_id`
- Sorting: By `timestamp` ascending

**Sample Query:**
```python
from binance_collector import StorageEngine

storage = StorageEngine()
df = storage.read('trades', 'BTCUSDT')

# Filter by time
df_filtered = df[df['timestamp'] >= '2026-02-15']

# Get OHLCV
from binance_collector import trades_to_ohlcv
ohlcv = trades_to_ohlcv(df, timeframe='1h')
```

**Performance (850k rows):**
- File size: 16.34 MB
- Load time: ~200ms
- Memory: ~50 MB in DataFrame

---

## Order Book Data

**File:** `orderbook/{SYMBOL}.parquet`

**Description:** Order book snapshots with multi-tick aggregation. Each row represents one tick size snapshot at a specific timestamp.

### Core Columns

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `timestamp` | datetime64[ns] | Snapshot time (UTC) | 2026-02-15 23:49:45.665 |
| `symbol` | string | Trading pair | BTCUSDT |
| `tick_size` | float64 | Price aggregation bucket ($) | 10.0, 50.0, 100.0, 500.0, 1000.0 |
| `best_bid` | float64 | Best bid price | 68512.95 |
| `best_ask` | float64 | Best ask price | 68513.58 |
| `spread` | float64 | Bid-ask spread | 0.63 |
| `spread_pct` | float64 | Spread as % of mid | 0.00092 |
| `imbalance` | float64 | (bid_vol - ask_vol) / total | -0.045 |
| `depth_ratio` | float64 | bid_depth / ask_depth | 0.91 |

### Bid Levels (1-15)

**Pattern:** `bid_price_{N}`, `bid_qty_{N}`, `bid_cum_qty_{N}`, `bid_cum_usd_{N}`

| Column | Type | Description |
|--------|------|-------------|
| `bid_price_1` | float64 | Price at level 1 (best bid) |
| `bid_qty_1` | float64 | Quantity at level 1 |
| `bid_cum_qty_1` | float64 | Cumulative quantity (levels 1-N) |
| `bid_cum_usd_1` | float64 | Cumulative USD value (levels 1-N) |
| ... | ... | Repeats for levels 2-15 |

### Ask Levels (1-15)

**Pattern:** `ask_price_{N}`, `ask_qty_{N}`, `ask_cum_qty_{N}`, `ask_cum_usd_{N}`

| Column | Type | Description |
|--------|------|-------------|
| `ask_price_1` | float64 | Price at level 1 (best ask) |
| `ask_qty_1` | float64 | Quantity at level 1 |
| `ask_cum_qty_1` | float64 | Cumulative quantity (levels 1-N) |
| `ask_cum_usd_1` | float64 | Cumulative USD value (levels 1-N) |
| ... | ... | Repeats for levels 2-15 |

**Total Columns:** 129

**Storage:**
- Format: Parquet with snappy compression
- Compression ratio: ~333 bytes/row
- Snapshots per tick: ~1,891 (30s intervals over 15h)
- Sorting: By `timestamp`, `tick_size` ascending

**Sample Query:**
```python
storage = StorageEngine()
df = storage.read('orderbook', 'BTCUSDT')

# Filter by tick size
df_10 = df[df['tick_size'] == 10.0]

# Get level 1-5 depth
depth_5 = df_10[['timestamp', 'bid_cum_usd_5', 'ask_cum_usd_5']]

# Calculate total liquidity within $100
df_100 = df[df['tick_size'] == 100.0]
total_liq = df_100['bid_cum_usd_15'] + df_100['ask_cum_usd_15']
```

**Performance (9,455 rows):**
- File size: 3.0 MB
- Load time: ~100ms
- Memory: ~120 MB in DataFrame (wide schema)

---

## OHLCV (Derived)

**Generated from trades using `trades_to_ohlcv()`**

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime64[ns] | Candle open time |
| `open` | float64 | First trade price in period |
| `high` | float64 | Highest trade price |
| `low` | float64 | Lowest trade price |
| `close` | float64 | Last trade price in period |
| `volume` | float64 | Total quantity traded |

**Usage:**
```python
from binance_collector import trades_to_ohlcv

trades = storage.read('trades', 'BTCUSDT')

# Any timeframe
ohlcv_1h = trades_to_ohlcv(trades, timeframe='1h')
ohlcv_5m = trades_to_ohlcv(trades, timeframe='5min')
ohlcv_1d = trades_to_ohlcv(trades, timeframe='1D')
```

---

## Data Access Patterns

### 1. Remote Client (SCP)

```python
from remote_client import RemoteCollectorClient

client = RemoteCollectorClient()

# Downloads full file, returns DataFrame
trades = client.get_trades('BTCUSDT')
orderbook = client.get_orderbook('ETHUSDT', tick_size=50)
ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h')

# Get stats without downloading
stats = client.get_stats()
```

### 2. Local Storage

```python
from binance_collector import StorageEngine

storage = StorageEngine(base_path='data')

# Read with time filter
df = storage.read(
    'trades',
    'BTCUSDT',
    start_time=pd.Timestamp('2026-02-15'),
    end_time=pd.Timestamp('2026-02-16')
)

# Get file info
info = storage.get_file_info('trades', 'BTCUSDT')
# {'path': Path('...'), 'size_mb': 16.34, 'rows': 850425}
```

---

## File Naming Convention

```
{base_path}/
├── trades/
│   ├── BTCUSDT.parquet
│   ├── ETHUSDT.parquet
│   ├── BNBUSDT.parquet
│   ├── SOLUSDT.parquet
│   ├── ADAUSDT.parquet
│   └── XRPUSDT.parquet
└── orderbook/
    ├── BTCUSDT.parquet
    ├── ETHUSDT.parquet
    ├── BNBUSDT.parquet
    ├── SOLUSDT.parquet
    ├── ADAUSDT.parquet
    └── XRPUSDT.parquet
```

---

## Data Quality Notes

### Trades
- **Incremental collection:** No gaps in `agg_trade_id` sequence
- **Deduplication:** Automatic on merge
- **Latency:** ~1ms from exchange
- **Completeness:** 100% (no missed trades)

### Order Book
- **Snapshot frequency:** 30 seconds
- **Tick sizes:** Configurable (default: $10, $50, $100, $500, $1000)
- **Levels:** 15 per side (30 total)
- **NaN values:** Possible in higher levels if insufficient liquidity

### Known Limitations
- REST API limit: ~7 days backfill for trades
- Order book is snapshots, not tick-by-tick
- No account/private trades (public data only)

---

## Schema Validation

All data is validated via Pydantic models before storage:

```python
from binance_collector.schema import Trade, OrderBookSnapshot

# Automatic validation on collection
collector.update()  # Raises ValidationError if schema mismatch
```

**Validation Rules:**
- `agg_trade_id`: Positive integer
- `price`, `quantity`: Positive floats
- `timestamp`: Valid datetime
- `symbol`: Non-empty string
- `tick_size`: In configured list

---

## Typical Data Sizes

**15 hours of collection (6 symbols):**

| Data Type | Total Size | Avg per Symbol | Rows per Symbol |
|-----------|------------|----------------|-----------------|
| Trades | 50 MB | 8.3 MB | ~400k |
| Orderbook | 18 MB | 3.0 MB | ~9.5k |
| **Total** | **68 MB** | **11.3 MB** | **~410k** |

**Growth rate:** ~3.3 MB/hour → ~2.4 GB/month → ~29 GB/year

---

## Performance Tips

1. **Time filtering:** Use `storage.read()` with `start_time`/`end_time` params
2. **Tick filtering:** Filter orderbook by `tick_size` before analysis
3. **Partial reads:** Use pandas `read_parquet(columns=[...])` for column subset
4. **Memory:** Orderbook is wide (129 cols) - filter early
5. **Compression:** Keep as parquet, avoid CSV conversion

---

## Example Queries

### Get last hour of trades
```python
df = storage.read('trades', 'BTCUSDT')
last_hour = df[df['timestamp'] > pd.Timestamp.now() - pd.Timedelta(hours=1)]
```

### Calculate VWAP
```python
trades['value'] = trades['price'] * trades['quantity']
vwap = trades['value'].sum() / trades['quantity'].sum()
```

### Order book imbalance timeseries
```python
ob = storage.read('orderbook', 'BTCUSDT')
ob_10 = ob[ob['tick_size'] == 10.0]
imbalance_ts = ob_10[['timestamp', 'imbalance']].set_index('timestamp')
```

### Bid-ask spread analysis
```python
ob['mid'] = (ob['best_bid'] + ob['best_ask']) / 2
ob['spread_bps'] = (ob['spread'] / ob['mid']) * 10000  # basis points
print(ob.groupby('tick_size')['spread_bps'].describe())
```
