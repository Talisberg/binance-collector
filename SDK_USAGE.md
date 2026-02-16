# SDK Usage Guide

Complete guide for consuming binance-collector data as an SDK.

---

## Installation

```bash
pip install git+https://github.com/Talisberg/binance-collector.git
```

Or for local development:
```bash
git clone https://github.com/Talisberg/binance-collector.git
cd binance-collector
pip install -e .
```

---

## Quick Start

### 1. Local Access (Fastest)

Read data from local filesystem:

```python
from binance_collector import get_local_client

# Create client
client = get_local_client(data_path='data')

# Get trades
trades = client.get_trades('BTCUSDT', limit=1000)
print(f"Got {len(trades)} trades")

# Get orderbook (specific tick size)
orderbook = client.get_orderbook('ETHUSDT', tick_size=50, limit=100)

# Get OHLCV (derived from trades)
ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h', limit=24)

# Get stats
stats = client.get_stats()
print(f"Available symbols: {client.get_available_symbols()}")
```

### 2. Hot Access (Dashboard Mode)

Read only recent data (last 1024 rows) for real-time dashboards:

```python
from binance_collector import get_hot_client

# Create hot client
client = get_hot_client(data_path='data')

# Get recent trades (last 1024 rows, ~1ms read)
recent_trades = client.get_trades('BTCUSDT')

# Get recent orderbook snapshots
recent_ob = client.get_orderbook('ETHUSDT', tick_size=10)

# Perfect for streaming dashboards
print(f"Latest price: ${recent_trades['price'].iloc[-1]:.2f}")
```

**Performance:**
- Read time: **~1ms** (trades), **~2ms** (orderbook)
- Memory: **~200 KB** per symbol
- Use case: Real-time dashboards, live monitoring

### 3. Remote Access (Full Data)

Download full files from remote collector:

```python
from binance_collector import get_remote_client

# Create remote client
client = get_remote_client(
    remote_host='your.remote.host',
    remote_user='root',
    ssh_key_path='~/.ssh/id_rsa',
    remote_data_path='/root/crypto_data'
)

# Downloads full file via SCP
trades = client.get_trades('BTCUSDT')
print(f"Downloaded {len(trades):,} trades")

# Get stats without downloading files
stats = client.get_stats()
```

---

## Access Patterns Comparison

| Mode | Speed | Data Coverage | Use Case |
|------|-------|---------------|----------|
| **Local** | Fast (~20ms) | Full history | Local analysis |
| **Hot** | Fastest (~1ms) | Last 1024 rows | Dashboards, real-time |
| **Remote** | Slow (file download) | Full history | Remote analysis |

---

## Usage Examples

### Example 1: Real-time Dashboard

```python
from binance_collector import get_hot_client
import time

client = get_hot_client()

while True:
    # Get latest data (1ms read)
    trades = client.get_trades('BTCUSDT')

    if not trades.empty:
        latest = trades.iloc[-1]
        print(f"BTC Price: ${latest['price']:.2f} | "
              f"Volume: {latest['quantity']:.4f} | "
              f"Time: {latest['timestamp']}")

    time.sleep(1)
```

### Example 2: Historical Analysis

```python
from binance_collector import get_local_client

client = get_local_client()

# Get all trades from last 24 hours
trades = client.get_trades(
    'BTCUSDT',
    start_time='2026-02-15T00:00:00',
    end_time='2026-02-16T00:00:00'
)

# Calculate VWAP
trades['value'] = trades['price'] * trades['quantity']
vwap = trades['value'].sum() / trades['quantity'].sum()
print(f"24h VWAP: ${vwap:.2f}")
```

### Example 3: Multi-Symbol OHLCV

```python
from binance_collector import get_local_client
import pandas as pd

client = get_local_client()

# Get hourly candles for multiple symbols
symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
ohlcv_data = {}

for symbol in symbols:
    ohlcv_data[symbol] = client.get_ohlcv(
        symbol,
        timeframe='1h',
        limit=24
    )

# Combine into single DataFrame
combined = pd.concat(ohlcv_data, names=['symbol', 'index'])
print(combined)
```

### Example 4: Order Book Depth Analysis

```python
from binance_collector import get_local_client

client = get_local_client()

# Get recent orderbook snapshots (last 100)
ob = client.get_orderbook('BTCUSDT', tick_size=100, limit=100)

# Analyze bid-ask imbalance
print(f"Average imbalance: {ob['imbalance'].mean():.3f}")
print(f"Average spread: {ob['spread'].mean():.2f}")

# Get liquidity depth at level 5
liquidity = ob['bid_cum_usd_5'] + ob['ask_cum_usd_5']
print(f"Average depth (±5 levels): ${liquidity.mean():,.0f}")
```

### Example 5: Live Price Monitoring

```python
from binance_collector import get_hot_client
import pandas as pd

client = get_hot_client()

def get_current_prices():
    """Get current prices for all symbols"""
    symbols = client.get_available_symbols()['trades']
    prices = {}

    for symbol in symbols:
        trades = client.get_trades(symbol, limit=1)
        if not trades.empty:
            prices[symbol] = trades['price'].iloc[-1]

    return prices

# Monitor prices
while True:
    prices = get_current_prices()
    for symbol, price in prices.items():
        print(f"{symbol}: ${price:.2f}")
    time.sleep(5)
```

---

## API Reference

### BinanceCollectorClient

```python
from binance_collector import BinanceCollectorClient

client = BinanceCollectorClient(
    mode='local',           # 'local', 'remote', or 'hot'
    data_path='data',       # Local data directory
    remote_host=None,       # For remote mode
    remote_user=None,       # For remote mode
    ssh_key_path=None,      # For remote mode
    remote_data_path=None   # For remote mode
)
```

#### Methods

**get_trades(symbol, limit=None, start_time=None, end_time=None)**
- Returns: DataFrame with trades
- Columns: `agg_trade_id`, `timestamp`, `price`, `quantity`, `is_buyer_maker`, etc.

**get_orderbook(symbol, tick_size=None, limit=None, start_time=None, end_time=None)**
- Returns: DataFrame with orderbook snapshots
- Columns: 129 columns (see SCHEMA.md)

**get_ohlcv(symbol, timeframe='1h', limit=None, start_time=None, end_time=None)**
- Returns: DataFrame with OHLCV candles
- Columns: `timestamp`, `open`, `high`, `low`, `close`, `volume`

**get_available_symbols()**
- Returns: Dict with `{'trades': [...], 'orderbook': [...]}`

**get_stats()**
- Returns: Dict with stats per symbol (rows, size, time range)

---

## Convenience Functions

```python
from binance_collector import get_local_client, get_hot_client, get_remote_client

# Local access
client = get_local_client(data_path='data')

# Hot snapshot access (fast, recent data)
client = get_hot_client(data_path='data')

# Remote access
client = get_remote_client(
    remote_host='your.remote.host',
    remote_user='root',
    ssh_key_path='~/.ssh/id_rsa',
    remote_data_path='/root/crypto_data'
)
```

---

## Performance Tips

### 1. Use Hot Mode for Dashboards
```python
# Instead of:
client = get_local_client()
trades = client.get_trades('BTCUSDT', limit=1000)  # 20ms

# Use:
client = get_hot_client()
trades = client.get_trades('BTCUSDT')  # 1ms
```

### 2. Filter Early
```python
# Bad: Load all, then filter
trades = client.get_trades('BTCUSDT')
recent = trades.tail(1000)

# Good: Filter during read
trades = client.get_trades('BTCUSDT', limit=1000)
```

### 3. Cache Frequently Accessed Data
```python
from functools import lru_cache

@lru_cache(maxsize=10)
def get_ohlcv_cached(symbol, timeframe):
    client = get_local_client()
    return client.get_ohlcv(symbol, timeframe=timeframe)
```

---

## Integration Examples

### Streamlit Dashboard

```python
import streamlit as st
from binance_collector import get_hot_client

client = get_hot_client()

# Sidebar symbol selector
symbol = st.sidebar.selectbox('Symbol', ['BTCUSDT', 'ETHUSDT'])

# Get recent trades
trades = client.get_trades(symbol, limit=100)

# Display metrics
st.metric('Latest Price', f"${trades['price'].iloc[-1]:.2f}")
st.metric('Volume (last 100)', f"{trades['quantity'].sum():.4f}")

# Chart
st.line_chart(trades.set_index('timestamp')['price'])
```

### Flask API

```python
from flask import Flask, jsonify
from binance_collector import get_local_client

app = Flask(__name__)
client = get_local_client()

@app.route('/trades/<symbol>')
def get_trades(symbol):
    trades = client.get_trades(symbol, limit=100)
    return jsonify(trades.to_dict(orient='records'))

@app.route('/ohlcv/<symbol>/<timeframe>')
def get_ohlcv(symbol, timeframe):
    ohlcv = client.get_ohlcv(symbol, timeframe=timeframe, limit=24)
    return jsonify(ohlcv.to_dict(orient='records'))
```

### Jupyter Notebook

```python
from binance_collector import get_local_client
import matplotlib.pyplot as plt

client = get_local_client()

# Get hourly candles
ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h', limit=168)

# Plot
fig, ax = plt.subplots(figsize=(12, 6))
ax.plot(ohlcv['timestamp'], ohlcv['close'])
ax.set_title('BTC Price (7 days)')
ax.set_xlabel('Time')
ax.set_ylabel('Price ($)')
plt.show()
```

---

## Troubleshooting

### Error: "No data found"
```python
# Check available symbols first
symbols = client.get_available_symbols()
print(symbols)

# Check if collector is running
stats = client.get_stats()
```

### Error: "SSH connection failed" (remote mode)
```bash
# Test SSH manually
ssh -i ~/.ssh/id_rsa root@your.remote.host

# Check SSH key permissions
chmod 600 ~/.ssh/id_rsa
```

### Hot files not updating
```python
# Hot files are maintained by collector
# Make sure collector calls maintain_hot_snapshot()

from binance_collector import StorageEngine

storage = StorageEngine()
storage.maintain_hot_snapshot('trades', 'BTCUSDT', window_size=1024)
```

---

## Best Practices

1. **Use hot mode for dashboards** - 20x faster than local mode
2. **Filter data early** - Use `limit`, `start_time`, `end_time` params
3. **Cache OHLCV** - Expensive to recompute frequently
4. **Monitor file sizes** - Hot files are tiny, full files grow over time
5. **Handle missing data** - Check `df.empty` before processing

---

## Next Steps

- See `SCHEMA.md` for complete data schema
- See `examples/` for collector usage
- See `README.md` for package overview
- Check GitHub for latest updates
