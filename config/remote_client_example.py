"""
Remote data client for binance-collector.
Fetch data from remote machine to local DataFrame.

SETUP:
1. Copy this file to your project
2. Update the RemoteCollectorClient() parameters with your server details
3. Ensure SSH key is configured for passwordless access
"""
import pandas as pd
import subprocess
import tempfile
from pathlib import Path
from typing import Optional, List
from datetime import datetime


class RemoteCollectorClient:
    """
    Client to fetch data from remote binance-collector deployment.
    """

    def __init__(
        self,
        remote_host: str,
        remote_user: str = 'root',
        ssh_key_path: str = '~/.ssh/id_rsa',
        remote_data_path: str = '/root/crypto_data'
    ):
        """
        Args:
            remote_host: Remote server IP/hostname
            remote_user: SSH username
            ssh_key_path: Path to SSH private key
            remote_data_path: Path to crypto_data on remote
        """
        self.remote_host = remote_host
        self.remote_user = remote_user
        self.ssh_key_path = Path(ssh_key_path).expanduser()
        self.remote_data_path = remote_data_path

    def _scp_file(self, remote_path: str, local_path: str):
        """Download file via SCP"""
        subprocess.run([
            'scp',
            '-i', str(self.ssh_key_path),
            f'{self.remote_user}@{self.remote_host}:{remote_path}',
            local_path
        ], check=True)

    def get_trades(
        self,
        symbol: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Get trades for a symbol.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            start_time: Filter start (optional)
            end_time: Filter end (optional)

        Returns:
            DataFrame with trades
        """
        remote_path = f'{self.remote_data_path}/trades/{symbol}.parquet'

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download file
            self._scp_file(remote_path, tmp_path)

            # Read parquet
            df = pd.read_parquet(tmp_path)

            # Apply time filters
            if start_time or end_time:
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                if start_time:
                    df = df[df['timestamp'] >= start_time]

                if end_time:
                    df = df[df['timestamp'] <= end_time]

            return df

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def get_orderbook(
        self,
        symbol: str,
        tick_size: Optional[float] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Get orderbook snapshots for a symbol.

        Args:
            symbol: Trading pair
            tick_size: Filter by specific tick size (optional)
            start_time: Filter start (optional)
            end_time: Filter end (optional)

        Returns:
            DataFrame with orderbook snapshots
        """
        remote_path = f'{self.remote_data_path}/orderbook/{symbol}.parquet'

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download file
            self._scp_file(remote_path, tmp_path)

            # Read parquet
            df = pd.read_parquet(tmp_path)

            # Apply filters
            if tick_size:
                df = df[df['tick_size'] == tick_size]

            if start_time or end_time:
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                if start_time:
                    df = df[df['timestamp'] >= start_time]

                if end_time:
                    df = df[df['timestamp'] <= end_time]

            return df

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1h',
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Derive OHLCV from trades.

        Args:
            symbol: Trading pair
            timeframe: Resample timeframe (e.g., '1h', '5min', '1D')
            start_time: Filter start (optional)
            end_time: Filter end (optional)

        Returns:
            DataFrame with OHLCV candles
        """
        from binance_collector import trades_to_ohlcv

        # Get trades
        trades_df = self.get_trades(symbol, start_time, end_time)

        # Derive OHLCV
        return trades_to_ohlcv(trades_df, timeframe=timeframe)

    def get_available_symbols(self, data_type: str = 'trades') -> List[str]:
        """
        Get list of available symbols on remote.

        Args:
            data_type: 'trades' or 'orderbook'

        Returns:
            List of symbol names
        """
        result = subprocess.run([
            'ssh',
            '-i', str(self.ssh_key_path),
            f'{self.remote_user}@{self.remote_host}',
            f'ls {self.remote_data_path}/{data_type}/*.parquet 2>/dev/null | xargs -n1 basename | sed "s/.parquet$//"'
        ], capture_output=True, text=True)

        return result.stdout.strip().split('\n') if result.stdout.strip() else []

    def get_stats(self) -> dict:
        """
        Get collection statistics from remote.

        Returns:
            Dict with stats per symbol
        """
        cmd = f"""
source /root/binance-collector-env/bin/activate && python3 << 'EOF'
from pathlib import Path
import pandas as pd

base = Path("{self.remote_data_path}")
stats = {{'trades': {{}}, 'orderbook': {{}}}}

for data_type in ['trades', 'orderbook']:
    data_dir = base / data_type
    if data_dir.exists():
        for file in data_dir.glob('*.parquet'):
            df = pd.read_parquet(file)
            stats[data_type][file.stem] = {{
                'rows': len(df),
                'size_mb': round(file.stat().st_size / 1024 / 1024, 2),
                'start': str(df['timestamp'].min()),
                'end': str(df['timestamp'].max())
            }}

import json
print(json.dumps(stats))
EOF
"""

        result = subprocess.run([
            'ssh',
            '-i', str(self.ssh_key_path),
            f'{self.remote_user}@{self.remote_host}',
            cmd
        ], capture_output=True, text=True, check=True)

        import json
        return json.loads(result.stdout.strip())


# Example usage
if __name__ == '__main__':
    # CONFIGURE YOUR SERVER HERE
    client = RemoteCollectorClient(
        remote_host='YOUR_SERVER_IP',          # e.g., '123.456.78.90'
        remote_user='root',
        ssh_key_path='~/.ssh/id_rsa',          # Path to your SSH key
        remote_data_path='/root/crypto_data'   # Remote data directory
    )

    print("Available symbols:")
    print("  Trades:", client.get_available_symbols('trades'))
    print("  Orderbook:", client.get_available_symbols('orderbook'))

    print("\nFetching BTCUSDT trades...")
    trades = client.get_trades('BTCUSDT')
    print(f"  Rows: {len(trades):,}")
    print(f"  Columns: {list(trades.columns)}")
    print("\nLast 5 trades:")
    print(trades.tail())

    print("\nDeriving 1h OHLCV...")
    ohlcv = client.get_ohlcv('BTCUSDT', timeframe='1h')
    print(f"  Candles: {len(ohlcv)}")
    print(ohlcv.tail())

    print("\nCollection stats:")
    stats = client.get_stats()
    for data_type, symbols in stats.items():
        print(f"\n{data_type.upper()}:")
        for symbol, info in symbols.items():
            print(f"  {symbol}: {info['rows']:,} rows, {info['size_mb']} MB")
