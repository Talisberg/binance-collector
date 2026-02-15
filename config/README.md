# Configuration Examples

This directory contains example configuration files for deploying binance-collector.

## Files

### `deployment_example.yaml`
Example YAML configuration for remote deployment. Copy and customize:

```bash
cp config/deployment_example.yaml my_config.yaml
# Edit my_config.yaml with your settings
```

### `remote_client_example.py`
Example client for accessing data from a remote collector. Copy and customize:

```bash
cp config/remote_client_example.py remote_client.py
# Edit remote_client.py with your server details
```

## Quick Start

### 1. Deploy Collector to Remote Server

```bash
# On remote server
git clone https://github.com/Talisberg/binance-collector.git
cd binance-collector
python3 -m venv venv
source venv/bin/activate
pip install -e .

# Create your config
cp config/deployment_example.yaml config.yaml
# Edit config.yaml with your symbols and paths

# Run collector (see examples/04_continuous_collection.py)
python examples/04_continuous_collection.py
```

### 2. Access Data from Client

```bash
# On local machine
cp config/remote_client_example.py remote_client.py

# Edit remote_client.py:
# - Set remote_host to your server IP
# - Set ssh_key_path to your SSH key
# - Set remote_data_path to match your deployment

# Use the client
python remote_client.py
```

## Configuration Options

### Collection Intervals

Adjust based on your needs and rate limits:

```yaml
intervals:
  trades: 60      # Conservative (18 req/min per symbol)
  orderbook: 30   # Conservative (36 req/min per symbol)
```

**Faster alternatives** (for fewer symbols):
```yaml
intervals:
  trades: 6       # Fast (180 req/min per symbol)
  orderbook: 3    # Fast (360 req/min per symbol)
```

### Tick Sizes

Customize orderbook aggregation buckets per symbol:

```yaml
orderbook:
  tick_sizes:
    BTCUSDT: [10, 50, 100]    # $10, $50, $100 buckets
    ETHUSDT: [5, 25, 50]      # Smaller ticks for lower price
```

### Storage

```yaml
storage:
  base_path: /path/to/data
  compression: snappy  # Options: snappy, gzip, none
```

## Security Notes

**Never commit actual deployment configs to public repos!**

✅ Safe to commit: Example configs with placeholders
❌ Never commit: Actual IPs, SSH keys, server paths

Keep your actual configs in `.gitignore`:
```
config.yaml
remote_client.py
my_deployment.yaml
```
