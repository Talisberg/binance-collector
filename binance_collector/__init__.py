"""
Binance Collector - Fast, schema-validated data ingestion for Binance.

Core components:
- TradesCollector: Incremental aggregate trades collection
- OrderBookCollector: Multi-tick orderbook snapshots
- StorageEngine: Parquet-based storage with deduplication
- Config: Configuration management

Example:
    >>> from binance_collector import TradesCollector
    >>> collector = TradesCollector(symbols=['BTCUSDT'])
    >>> stats = collector.update()
"""

__version__ = '0.1.0'

from .collectors import TradesCollector, OrderBookCollector
from .storage import StorageEngine
from .config import Config, create_example_config
from .schema import trades_to_ohlcv, orderbook_level_columns
from .client import BinanceCollectorClient, get_local_client, get_hot_client, get_remote_client
from .api_client import BinanceCollectorAPIClient

__all__ = [
    'TradesCollector',
    'OrderBookCollector',
    'StorageEngine',
    'Config',
    'create_example_config',
    'trades_to_ohlcv',
    'orderbook_level_columns',
    'BinanceCollectorClient',
    'get_local_client',
    'get_hot_client',
    'get_remote_client',
    'BinanceCollectorAPIClient'
]
