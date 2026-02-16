"""
API Client for reading data from remote binance-collector API server.

This client communicates with the API server over HTTP, avoiding file downloads.
Perfect for dashboards and real-time applications.
"""
import requests
import pandas as pd
from typing import Optional


class BinanceCollectorAPIClient:
    """
    Client for querying binance-collector API server.

    Example:
        # Connect to API server
        client = BinanceCollectorAPIClient(
            base_url='http://your-host:8000',
            api_key='your-secret-key'  # Optional, if auth enabled
        )

        # Get recent trades (fast, no file download)
        trades = client.get_trades('BTCUSDT', limit=1000)

        # Get orderbook snapshots
        orderbook = client.get_orderbook('ETHUSDT', tick_size=50, limit=100)

        # Get OHLCV
        ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h', limit=24)
    """

    def __init__(
        self,
        base_url: str = 'http://localhost:8000',
        api_key: Optional[str] = None,
        timeout: int = 30
    ):
        """
        Initialize API client.

        Args:
            base_url: API server URL (e.g., 'http://your-host:8000')
            api_key: API key for authentication (if enabled on server)
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout

        # Setup headers
        self.headers = {}
        if api_key:
            self.headers['X-API-Key'] = api_key

    def get_trades(
        self,
        symbol: str,
        limit: int = 1000,
        offset: int = 0,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        tail: bool = True
    ) -> pd.DataFrame:
        """
        Get trades for a symbol.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            limit: Max rows to return
            offset: Skip N rows
            start_time: ISO timestamp filter (e.g., '2026-02-15T00:00:00')
            end_time: ISO timestamp filter
            tail: Return last N rows (most recent)

        Returns:
            DataFrame with trades

        Example:
            trades = client.get_trades('BTCUSDT', limit=100)
            print(f"Latest price: ${trades['price'].iloc[-1]:.2f}")
        """
        params = {
            'limit': limit,
            'offset': offset,
            'tail': tail
        }
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time

        response = requests.get(
            f'{self.base_url}/trades/{symbol}',
            params=params,
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data['data'])

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_orderbook(
        self,
        symbol: str,
        tick_size: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        tail: bool = True
    ) -> pd.DataFrame:
        """
        Get orderbook snapshots for a symbol.

        Args:
            symbol: Trading pair
            tick_size: Filter by tick size (e.g., 10, 50, 100)
            limit: Max snapshots to return
            offset: Skip N snapshots
            start_time: ISO timestamp filter
            end_time: ISO timestamp filter
            tail: Return last N snapshots (most recent)

        Returns:
            DataFrame with orderbook snapshots

        Example:
            ob = client.get_orderbook('BTCUSDT', tick_size=10, limit=50)
            print(f"Spread: {ob['spread'].mean():.2f}")
        """
        params = {
            'limit': limit,
            'offset': offset,
            'tail': tail
        }
        if tick_size is not None:
            params['tick_size'] = tick_size
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time

        response = requests.get(
            f'{self.base_url}/orderbook/{symbol}',
            params=params,
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data['data'])

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1h',
        limit: int = 100,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get OHLCV candles derived from trades.

        Args:
            symbol: Trading pair
            timeframe: Resample rule (e.g., '1h', '5min', '1D')
            limit: Max candles to return
            start_time: ISO timestamp filter
            end_time: ISO timestamp filter

        Returns:
            DataFrame with OHLCV candles

        Example:
            ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h', limit=24)
            print(ohlcv[['timestamp', 'close']].tail())
        """
        params = {
            'timeframe': timeframe,
            'limit': limit
        }
        if start_time:
            params['start_time'] = start_time
        if end_time:
            params['end_time'] = end_time

        response = requests.get(
            f'{self.base_url}/ohlcv/{symbol}',
            params=params,
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()

        data = response.json()
        df = pd.DataFrame(data['data'])

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    def get_stats(self) -> dict:
        """
        Get statistics for all collected data.

        Returns:
            Dict with stats per symbol (rows, size, time range)

        Example:
            stats = client.get_stats()
            for symbol, info in stats['trades'].items():
                print(f"{symbol}: {info['rows']:,} rows")
        """
        response = requests.get(
            f'{self.base_url}/stats',
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def get_available_symbols(self) -> dict:
        """
        Get list of available symbols.

        Returns:
            Dict with 'trades' and 'orderbook' symbol lists

        Example:
            symbols = client.get_available_symbols()
            print(f"Available: {symbols['trades']}")
        """
        response = requests.get(
            f'{self.base_url}/symbols',
            headers=self.headers,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def health_check(self) -> dict:
        """
        Check API server health.

        Returns:
            Dict with health status

        Example:
            health = client.health_check()
            print(f"Status: {health['status']}")
        """
        response = requests.get(
            f'{self.base_url}/health',
            timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()


# Convenience function
def get_api_client(
    base_url: str = 'http://localhost:8000',
    api_key: Optional[str] = None
) -> BinanceCollectorAPIClient:
    """
    Create an API client.

    Example:
        client = get_api_client(
            base_url='http://your-host:8000',
            api_key='your-secret-key'
        )
    """
    return BinanceCollectorAPIClient(base_url=base_url, api_key=api_key)
