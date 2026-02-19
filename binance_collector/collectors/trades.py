"""
Incremental trades collector from Binance API.
Fetches aggregate trades with automatic deduplication.
"""
import pandas as pd
import requests
import json
import time
import logging
from typing import Optional, List
from datetime import datetime

from ..storage import StorageEngine
from ..schema import TRADE_SCHEMA, validate_dataframe

logger = logging.getLogger(__name__)


class TradesCollector:
    """
    Collect aggregate trades from Binance incrementally.
    Uses fromId parameter to avoid duplicates.
    """

    def __init__(
        self,
        symbols: List[str],
        storage: Optional[StorageEngine] = None,
        base_url: str = 'https://api.binance.com'
    ):
        """
        Args:
            symbols: List of trading pairs (e.g., ['BTCUSDT', 'ETHUSDT'])
            storage: Storage engine (default: creates new one)
            base_url: Binance API base URL
        """
        self.symbols = symbols
        self.storage = storage or StorageEngine()
        self.base_url = base_url
        self.last_trade_ids = {}  # symbol -> last seen trade ID

    def _fetch_trades(
        self,
        symbol: str,
        from_id: Optional[int] = None,
        limit: int = 1000
    ) -> List[dict]:
        """
        Fetch aggregate trades from Binance API.

        Args:
            symbol: Trading pair
            from_id: Start from this trade ID (exclusive)
            limit: Max trades per request (1-1000)

        Returns:
            List of trade dicts
        """
        url = f"{self.base_url}/api/v3/aggTrades"
        params = {
            'symbol': symbol,
            'limit': min(limit, 1000)
        }

        if from_id is not None:
            params['fromId'] = from_id + 1  # fromId is inclusive, we want exclusive

        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise RuntimeError(f"Binance API error: {response.status_code} - {response.text}")

        return json.loads(response.text)

    def _parse_trades(self, raw_trades: List[dict], symbol: str) -> pd.DataFrame:
        """
        Parse raw Binance trades into DataFrame.

        Args:
            raw_trades: List of trade dicts from API
            symbol: Trading symbol

        Returns:
            DataFrame with validated schema
        """
        if not raw_trades:
            return pd.DataFrame()

        _RENAME = {
            'a': 'agg_trade_id',
            'T': 'timestamp',
            'p': 'price',
            'q': 'quantity',
            'f': 'first_trade_id',
            'l': 'last_trade_id',
            'm': 'is_buyer_maker',
            'M': 'is_best_match'
        }
        df = pd.DataFrame(raw_trades).rename(columns=_RENAME)
        df['symbol'] = symbol
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df['price'] = df['price'].astype('float64')
        df['quantity'] = df['quantity'].astype('float64')

        return validate_dataframe(df, TRADE_SCHEMA)

    def collect_symbol(
        self,
        symbol: str,
        max_requests: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Collect all new trades for a single symbol.

        Args:
            symbol: Trading pair
            max_requests: Max API requests (default: unlimited)

        Returns:
            DataFrame with new trades
        """
        logger.info(f"Collecting trades for {symbol}...")

        # Get last known trade ID from storage
        latest_ts = self.storage.get_latest_timestamp('trades', symbol)
        from_id = self.last_trade_ids.get(symbol)

        if from_id is None and latest_ts:
            # Bootstrap: read last trade ID from storage
            existing = self.storage.read('trades', symbol)
            if not existing.empty:
                from_id = existing['agg_trade_id'].max()
                logger.info(f"  Resuming from trade ID {from_id}")

        all_trades = []
        request_count = 0

        while True:
            if max_requests and request_count >= max_requests:
                break

            raw_trades = self._fetch_trades(symbol, from_id=from_id, limit=1000)

            if not raw_trades:
                logger.info(f"  No new trades for {symbol}")
                break

            df = self._parse_trades(raw_trades, symbol)
            all_trades.append(df)

            # Update last seen ID
            last_id = df['agg_trade_id'].max()
            self.last_trade_ids[symbol] = last_id
            from_id = last_id

            request_count += 1

            # Log progress
            if request_count % 10 == 0:
                total_rows = sum(len(d) for d in all_trades)
                logger.info(f"  {symbol}: {total_rows:,} trades ({request_count} requests)")

            # If we got less than 1000, we've caught up
            if len(raw_trades) < 1000:
                break

            # Rate limiting
            time.sleep(0.1)

        if not all_trades:
            return pd.DataFrame()

        combined = pd.concat(all_trades, ignore_index=True)
        logger.info(f"  ✓ {symbol}: {len(combined):,} new trades")

        return combined

    def update(self, save: bool = True, maintain_hot: bool = True) -> dict:
        """
        Update all symbols incrementally.

        Args:
            save: Save to storage (default: True)
            maintain_hot: Update hot snapshot after write (default: True)

        Returns:
            Dict with collection stats per symbol
        """
        stats = {}

        for symbol in self.symbols:
            try:
                df = self.collect_symbol(symbol)

                if save and not df.empty:
                    self.storage.write(
                        df,
                        data_type='trades',
                        symbol=symbol,
                        schema=TRADE_SCHEMA,
                        dedup_columns=['agg_trade_id'],
                        sort_columns=['timestamp', 'agg_trade_id']
                    )
                    if maintain_hot:
                        self.storage.maintain_hot_snapshot('trades', symbol)

                stats[symbol] = {
                    'rows': len(df),
                    'start_time': df['timestamp'].min() if not df.empty else None,
                    'end_time': df['timestamp'].max() if not df.empty else None
                }

            except Exception as e:
                logger.error(f"Failed to collect {symbol}: {e}", exc_info=True)
                stats[symbol] = {'error': str(e)}

        return stats

    def backfill(
        self,
        symbol: str,
        start_time: datetime,
        max_trades: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Backfill historical trades from a start time.
        Note: Binance only provides ~7 days of trade history via REST API.

        Args:
            symbol: Trading pair
            start_time: Start datetime
            max_trades: Max trades to fetch (default: unlimited)

        Returns:
            DataFrame with historical trades
        """
        logger.info(f"Backfilling {symbol} from {start_time}...")

        url = f"{self.base_url}/api/v3/aggTrades"
        start_time_ms = int(start_time.timestamp() * 1000)

        all_trades = []
        from_id = None

        while True:
            params = {
                'symbol': symbol,
                'startTime': start_time_ms,
                'limit': 1000
            }

            if from_id:
                params['fromId'] = from_id + 1

            response = requests.get(url, params=params)

            if response.status_code != 200:
                raise RuntimeError(f"API error: {response.status_code}")

            raw_trades = json.loads(response.text)

            if not raw_trades:
                break

            df = self._parse_trades(raw_trades, symbol)
            all_trades.append(df)

            from_id = df['agg_trade_id'].max()

            total_rows = sum(len(d) for d in all_trades)
            logger.info(f"  Fetched {total_rows:,} trades...")

            if max_trades and total_rows >= max_trades:
                break

            if len(raw_trades) < 1000:
                break

            time.sleep(0.1)

        if not all_trades:
            return pd.DataFrame()

        combined = pd.concat(all_trades, ignore_index=True)
        logger.info(f"  ✓ Backfilled {len(combined):,} trades")

        return combined
