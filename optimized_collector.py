#!/usr/bin/env python3
"""
Optimized Binance collector with rotating files and memory management.
Based on performance testing results showing memory leaks and file corruption issues.
"""

import yaml
import logging
import logging.handlers
import time
import gc
import psutil
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional

from binance_collector import TradesCollector, OrderBookCollector


class OptimizedStorageEngine:
    """Storage engine with daily rotation and batch writing"""

    def __init__(self, base_path: str, rotate_daily: bool = True):
        self.base_path = Path(base_path)
        self.rotate_daily = rotate_daily
        self.current_date = datetime.now().date()
        self.write_buffer = {}
        self.buffer_size = 5000  # Flush every 5000 rows

    def get_filepath(self, data_type: str, symbol: str) -> Path:
        """Generate filepath with date suffix"""
        dir_path = self.base_path / data_type
        dir_path.mkdir(parents=True, exist_ok=True)

        if self.rotate_daily:
            date_str = datetime.now().strftime("%Y%m%d")
            return dir_path / f"{symbol}_{date_str}.parquet"
        else:
            return dir_path / f"{symbol}.parquet"

    def add_to_buffer(self, data_type: str, symbol: str, data: List[Dict]):
        """Add data to write buffer"""
        key = f"{data_type}:{symbol}"

        if key not in self.write_buffer:
            self.write_buffer[key] = []

        self.write_buffer[key].extend(data)

        # Auto-flush if buffer is full
        if len(self.write_buffer[key]) >= self.buffer_size:
            self.flush_buffer(data_type, symbol)

    def flush_buffer(self, data_type: str, symbol: str):
        """Flush buffer to disk"""
        key = f"{data_type}:{symbol}"

        if key not in self.write_buffer or not self.write_buffer[key]:
            return

        try:
            df = pd.DataFrame(self.write_buffer[key])
            filepath = self.get_filepath(data_type, symbol)

            # Write new file or append to existing
            if filepath.exists():
                # Read existing data
                existing_df = pd.read_parquet(filepath)
                # Combine and deduplicate based on timestamp
                df = pd.concat([existing_df, df], ignore_index=True)

                # Remove duplicates for trades (based on agg_trade_id)
                if data_type == 'trades' and 'agg_trade_id' in df.columns:
                    df = df.drop_duplicates(subset=['agg_trade_id'], keep='last')
                # For orderbook, keep latest snapshot per timestamp
                elif data_type == 'orderbook' and 'timestamp' in df.columns:
                    df = df.drop_duplicates(subset=['timestamp', 'tick_size'], keep='last')

            # Write to parquet with compression
            df.to_parquet(filepath, compression='snappy', index=False)
            logging.info(f"Flushed {len(self.write_buffer[key])} rows to {filepath}")

            # Clear buffer and force garbage collection
            self.write_buffer[key] = []
            del df
            gc.collect()

        except Exception as e:
            logging.error(f"Error flushing buffer for {symbol}: {e}")

    def flush_all(self):
        """Flush all buffers"""
        for key in list(self.write_buffer.keys()):
            data_type, symbol = key.split(':', 1)
            self.flush_buffer(data_type, symbol)

    def rotate_if_needed(self):
        """Check if date changed and rotate files"""
        current = datetime.now().date()
        if current != self.current_date:
            logging.info(f"Date changed from {self.current_date} to {current}, rotating files")
            self.flush_all()
            self.current_date = current
            # Clean old files (keep last 30 days)
            self.cleanup_old_files(days_to_keep=30)

    def cleanup_old_files(self, days_to_keep: int = 30):
        """Remove files older than specified days"""
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)

        for data_type in ['trades', 'orderbook']:
            dir_path = self.base_path / data_type
            if not dir_path.exists():
                continue

            for file in dir_path.glob("*.parquet"):
                # Extract date from filename (format: SYMBOL_YYYYMMDD.parquet)
                try:
                    date_str = file.stem.split('_')[-1]
                    file_date = datetime.strptime(date_str, "%Y%m%d")

                    if file_date < cutoff_date:
                        file.unlink()
                        logging.info(f"Deleted old file: {file}")
                except Exception as e:
                    logging.warning(f"Could not process file {file}: {e}")


class OptimizedCollector:
    """Main collector with memory management"""

    def __init__(self, config_path: str):
        # Load config
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Setup logging
        self.setup_logging()

        # Initialize storage
        self.storage = OptimizedStorageEngine(
            self.config['storage']['base_path'],
            rotate_daily=True
        )

        # Initialize collectors
        self.trades_collector = TradesCollector(self.config['symbols'])
        self.orderbook_collector = OrderBookCollector(
            self.config['symbols'],
            num_levels=self.config['orderbook']['num_levels'],
            tick_sizes=self.config['orderbook']['tick_sizes']
        )

        # Memory monitoring
        self.process = psutil.Process(os.getpid())
        self.max_memory_percent = 50  # Kill if using >50% memory

        # Collection state
        self.last_trades_collection = {}
        self.last_orderbook_collection = time.time()

    def setup_logging(self):
        """Configure rotating log files"""
        log_file = self.config['logging']['file'].replace('.log', '_optimized.log')

        # Create rotating file handler
        handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config['logging']['max_bytes'],
            backupCount=self.config['logging']['backup_count']
        )
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))

        # Configure root logger
        logger = logging.getLogger()
        logger.setLevel(self.config['logging']['level'])
        logger.handlers = []  # Clear existing handlers
        logger.addHandler(handler)

        # Also log to console
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logger.addHandler(console)

    def check_memory(self):
        """Check memory usage and cleanup if needed"""
        mem_percent = self.process.memory_percent()
        mem_mb = self.process.memory_info().rss / 1024 / 1024

        logging.info(f"Memory: {mem_mb:.1f}MB ({mem_percent:.1f}%)")

        if mem_percent > self.max_memory_percent:
            logging.warning(f"High memory usage: {mem_percent:.1f}%, forcing cleanup")
            self.storage.flush_all()
            gc.collect()

            # Check again
            new_percent = self.process.memory_percent()
            if new_percent > self.max_memory_percent:
                logging.error(f"Memory still high after cleanup: {new_percent:.1f}%, exiting")
                raise MemoryError("Memory usage too high, exiting to prevent OOM")

    def collect_trades(self):
        """Collect trades for all symbols"""
        for symbol in self.config['symbols']:
            try:
                # Get last trade ID to resume from
                last_id = self.last_trades_collection.get(symbol)

                # Collect new trades
                trades = self.trades_collector.collect(
                    symbol=symbol,
                    from_id=last_id,
                    limit=1000
                )

                if trades:
                    # Add to buffer
                    self.storage.add_to_buffer('trades', symbol, trades)

                    # Update last ID
                    self.last_trades_collection[symbol] = trades[-1].get('agg_trade_id')

                    logging.info(f"Collected {len(trades)} trades for {symbol}")

            except Exception as e:
                logging.error(f"Error collecting trades for {symbol}: {e}")

    def collect_orderbook(self):
        """Collect orderbook snapshots"""
        for symbol in self.config['symbols']:
            try:
                # Get orderbook snapshots at different tick sizes
                snapshots = self.orderbook_collector.collect(symbol)

                if snapshots:
                    self.storage.add_to_buffer('orderbook', symbol, snapshots)
                    logging.info(f"Collected {len(snapshots)} orderbook snapshots for {symbol}")

            except Exception as e:
                logging.error(f"Error collecting orderbook for {symbol}: {e}")

    def run(self):
        """Main collection loop"""
        logging.info("Starting optimized collector...")
        logging.info(f"Symbols: {self.config['symbols']}")
        logging.info(f"Trades interval: {self.config['intervals']['trades']}s")
        logging.info(f"Orderbook interval: {self.config['intervals']['orderbook']}s")

        last_memory_check = time.time()

        try:
            while True:
                current_time = time.time()

                # Check for date rotation
                self.storage.rotate_if_needed()

                # Collect trades (every symbol independently)
                if current_time - self.last_orderbook_collection >= self.config['intervals']['trades']:
                    self.collect_trades()

                # Collect orderbook
                if current_time - self.last_orderbook_collection >= self.config['intervals']['orderbook']:
                    self.collect_orderbook()
                    self.last_orderbook_collection = current_time

                # Memory check every minute
                if current_time - last_memory_check >= 60:
                    self.check_memory()
                    last_memory_check = current_time

                # Flush buffers periodically
                if int(current_time) % 300 == 0:  # Every 5 minutes
                    self.storage.flush_all()
                    gc.collect()

                time.sleep(1)

        except KeyboardInterrupt:
            logging.info("Shutting down collector...")
        except Exception as e:
            logging.error(f"Fatal error: {e}")
        finally:
            # Final flush
            self.storage.flush_all()
            logging.info("Collector stopped")


# New configuration with optimized settings
OPTIMIZED_CONFIG = """
# Optimized Binance Collector Configuration
# Addresses memory leaks and file corruption issues

symbols:
  - BTCUSDT
  - ETHUSDT
  - BNBUSDT
  - SOLUSDT
  - ADAUSDT
  - XRPUSDT

intervals:
  trades: 30        # Reduced from 60s for more frequent updates
  orderbook: 10     # Reduced from 30s for higher frequency snapshots

storage:
  base_path: /root/crypto_data_v2
  compression: snappy

orderbook:
  num_levels: 10    # Reduced from 15 to save memory
  tick_sizes:
    BTCUSDT: [100, 500, 1000]  # Reduced granularity
    ETHUSDT: [10, 50, 100]
    BNBUSDT: [1, 5, 10]
    SOLUSDT: [1, 5, 10]
    ADAUSDT: [0.01, 0.1]
    XRPUSDT: [0.01, 0.1]

logging:
  level: INFO
  file: /root/logs/binance_collector.log
  max_bytes: 10485760
  backup_count: 5

rate_limit:
  max_workers: 4      # Reduced from 8 to save memory
  requests_per_minute: 600  # Reduced to be conservative

# Memory management
memory:
  max_percent: 50     # Restart if >50% memory used
  buffer_size: 5000   # Flush every 5000 rows
  rotation_hours: 24  # New file every day
  days_to_keep: 30   # Keep 30 days of history
"""


if __name__ == "__main__":
    import sys

    # Save optimized config if requested
    if len(sys.argv) > 1 and sys.argv[1] == "--save-config":
        config_path = sys.argv[2] if len(sys.argv) > 2 else "/root/optimized_config.yaml"
        with open(config_path, 'w') as f:
            f.write(OPTIMIZED_CONFIG)
        print(f"Saved optimized config to {config_path}")
        sys.exit(0)

    # Run collector
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/root/optimized_config.yaml"
    collector = OptimizedCollector(config_path)
    collector.run()