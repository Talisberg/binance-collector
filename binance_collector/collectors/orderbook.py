"""
Order book snapshot collector with multi-tick aggregation.
Collects snapshots at configurable intervals.
"""
import pandas as pd
import requests
import json
import time
import logging
from typing import Optional, List, Dict
from datetime import datetime

from ..storage import StorageEngine
from ..schema import ORDERBOOK_BASE_SCHEMA, validate_dataframe

logger = logging.getLogger(__name__)


class OrderBookCollector:
    """
    Collect order book snapshots with multi-tick aggregation.
    """

    # Default tick sizes per symbol (in quote currency)
    DEFAULT_TICK_SIZES = {
        'BTCUSDT': [10, 50, 100, 1000],
        'ETHUSDT': [1, 10],
        'BNBUSDT': [0.1, 1],
        'SOLUSDT': [0.01, 0.1, 1],
        'ADAUSDT': [0.0001, 0.001],
        'XRPUSDT': [0.001, 0.01],
    }

    def __init__(
        self,
        symbols: List[str],
        tick_sizes: Optional[Dict[str, List[float]]] = None,
        num_levels: int = 15,
        storage: Optional[StorageEngine] = None,
        base_url: str = 'https://api.binance.com'
    ):
        """
        Args:
            symbols: List of trading pairs
            tick_sizes: Custom tick sizes per symbol (default: DEFAULT_TICK_SIZES)
            num_levels: Number of levels per side
            storage: Storage engine
            base_url: Binance API base URL
        """
        self.symbols = symbols
        self.tick_sizes = tick_sizes or self.DEFAULT_TICK_SIZES
        self.num_levels = num_levels
        self.storage = storage or StorageEngine()
        self.base_url = base_url

    def _fetch_orderbook(self, symbol: str, limit: int = 1000) -> dict:
        """
        Fetch raw order book from Binance.

        Args:
            symbol: Trading pair
            limit: Depth limit (5, 10, 20, 50, 100, 500, 1000, 5000)

        Returns:
            Dict with 'bids', 'asks', 'lastUpdateId'
        """
        url = f"{self.base_url}/api/v3/depth"
        params = {'symbol': symbol, 'limit': limit}

        response = requests.get(url, params=params)

        if response.status_code != 200:
            raise RuntimeError(f"Binance API error: {response.status_code} - {response.text}")

        return json.loads(response.text)

    def _aggregate_levels(
        self,
        raw_levels: List[List[str]],
        tick_size: float,
        is_bid: bool,
        num_levels: int
    ) -> List[Dict]:
        """
        Aggregate order book levels by tick size.

        Args:
            raw_levels: List of [price, qty] from API
            tick_size: Price bucket size
            is_bid: True for bids, False for asks
            num_levels: Number of levels to return

        Returns:
            List of aggregated levels with cumulative metrics
        """
        # Aggregate by tick
        aggregated = {}

        for price_str, qty_str in raw_levels:
            price = float(price_str)
            qty = float(qty_str)

            # Floor price to tick
            level = (price // tick_size) * tick_size
            aggregated[level] = aggregated.get(level, 0) + qty

        # Sort and take top N
        if is_bid:
            sorted_levels = sorted(aggregated.items(), reverse=True)[:num_levels]
        else:
            sorted_levels = sorted(aggregated.items())[:num_levels]

        # Compute cumulative
        result = []
        cum_qty = 0
        cum_usd = 0

        for level, qty in sorted_levels:
            cum_qty += qty
            cum_usd += level * qty

            result.append({
                'price': level,
                'qty': qty,
                'cum_qty': cum_qty,
                'cum_usd': cum_usd
            })

        return result

    def collect_snapshot(self, symbol: str, timestamp: Optional[float] = None) -> pd.DataFrame:
        """
        Collect single snapshot for symbol with all configured tick sizes.

        Args:
            symbol: Trading pair
            timestamp: Custom timestamp (default: now)

        Returns:
            DataFrame with one row per tick size
        """
        if timestamp is None:
            timestamp = time.time()

        ts = pd.to_datetime(timestamp, unit='s')

        # Get tick sizes for this symbol
        tick_sizes = self.tick_sizes.get(symbol, [1.0])

        if not tick_sizes:
            logger.warning(f"No tick sizes configured for {symbol}, using [1.0]")
            tick_sizes = [1.0]

        # Fetch raw orderbook
        raw_ob = self._fetch_orderbook(symbol, limit=1000)

        # Get best bid/ask for metrics
        best_bid = float(raw_ob['bids'][0][0]) if raw_ob['bids'] else 0
        best_ask = float(raw_ob['asks'][0][0]) if raw_ob['asks'] else 0

        if best_bid == 0 or best_ask == 0:
            logger.warning(f"Invalid orderbook for {symbol}")
            return pd.DataFrame()

        mid_price = (best_bid + best_ask) / 2
        spread = best_ask - best_bid
        spread_pct = (spread / mid_price) * 100 if mid_price > 0 else 0

        rows = []

        for tick_size in tick_sizes:
            # Aggregate bids and asks
            bids = self._aggregate_levels(
                raw_ob['bids'], tick_size, is_bid=True, num_levels=self.num_levels
            )
            asks = self._aggregate_levels(
                raw_ob['asks'], tick_size, is_bid=False, num_levels=self.num_levels
            )

            # Build row
            row = {
                'timestamp': ts,
                'symbol': symbol,
                'tick_size': tick_size,
                'best_bid': best_bid,
                'best_ask': best_ask,
                'spread': spread,
                'spread_pct': spread_pct
            }

            # Add bid levels
            for i, bid in enumerate(bids, start=1):
                row[f'bid_price_{i}'] = bid['price']
                row[f'bid_qty_{i}'] = bid['qty']
                row[f'bid_cum_qty_{i}'] = bid['cum_qty']
                row[f'bid_cum_usd_{i}'] = bid['cum_usd']

            # Add ask levels
            for i, ask in enumerate(asks, start=1):
                row[f'ask_price_{i}'] = ask['price']
                row[f'ask_qty_{i}'] = ask['qty']
                row[f'ask_cum_qty_{i}'] = ask['cum_qty']
                row[f'ask_cum_usd_{i}'] = ask['cum_usd']

            # Derived metrics
            total_bid_qty = bids[-1]['cum_qty'] if bids else 0
            total_ask_qty = asks[-1]['cum_qty'] if asks else 0

            row['imbalance'] = (total_bid_qty - total_ask_qty) / (total_bid_qty + total_ask_qty + 1e-9)
            row['depth_ratio'] = total_bid_qty / (total_ask_qty + 1e-9)

            rows.append(row)

        return pd.DataFrame(rows)

    def collect_all(self, timestamp: Optional[float] = None) -> pd.DataFrame:
        """
        Collect snapshots for all symbols.

        Args:
            timestamp: Custom timestamp (default: now)

        Returns:
            Combined DataFrame for all symbols
        """
        snapshots = []

        for symbol in self.symbols:
            try:
                df = self.collect_snapshot(symbol, timestamp)
                if not df.empty:
                    snapshots.append(df)
                    logger.info(f"  ✓ {symbol}: {len(df)} tick sizes")
            except Exception as e:
                logger.error(f"Failed to collect {symbol}: {e}", exc_info=True)

        if not snapshots:
            return pd.DataFrame()

        return pd.concat(snapshots, ignore_index=True)

    def update(self, save: bool = True) -> dict:
        """
        Collect and save snapshots for all symbols.

        Args:
            save: Save to storage (default: True)

        Returns:
            Collection stats per symbol
        """
        logger.info("Collecting orderbook snapshots...")

        df = self.collect_all()

        if df.empty:
            logger.warning("No snapshots collected")
            return {}

        stats = {}

        if save:
            # Save per symbol
            for symbol in df['symbol'].unique():
                symbol_df = df[df['symbol'] == symbol]

                self.storage.write(
                    symbol_df,
                    data_type='orderbook',
                    symbol=symbol,
                    schema=None,  # Dynamic columns based on num_levels
                    dedup_columns=['timestamp', 'tick_size'],
                    sort_columns=['timestamp', 'tick_size']
                )

                stats[symbol] = {
                    'rows': len(symbol_df),
                    'tick_sizes': symbol_df['tick_size'].nunique(),
                    'timestamp': symbol_df['timestamp'].iloc[0]
                }

        return stats

    def run_continuous(
        self,
        interval_seconds: int = 30,
        duration_hours: Optional[float] = None
    ):
        """
        Run continuous collection at fixed interval.

        Args:
            interval_seconds: Seconds between snapshots
            duration_hours: Run duration in hours (default: forever)
        """
        logger.info(f"Starting continuous collection (interval: {interval_seconds}s)")

        start_time = time.time()
        iteration = 0

        while True:
            iteration += 1
            iter_start = time.time()

            logger.info(f"\n[Iteration {iteration}] {datetime.now().strftime('%H:%M:%S')}")

            try:
                stats = self.update(save=True)

                for symbol, info in stats.items():
                    logger.info(f"  {symbol}: {info['rows']} rows, {info['tick_sizes']} ticks")

            except Exception as e:
                logger.error(f"Collection failed: {e}", exc_info=True)

            # Check duration
            if duration_hours:
                elapsed_hours = (time.time() - start_time) / 3600
                if elapsed_hours >= duration_hours:
                    logger.info(f"\nCompleted {duration_hours}h collection")
                    break

            # Sleep
            elapsed = time.time() - iter_start
            sleep_time = max(0, interval_seconds - elapsed)

            if sleep_time > 0:
                time.sleep(sleep_time)
