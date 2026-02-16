"""
Client SDK for consuming binance-collector data.

Provides three access patterns:
1. Local: Direct file access via StorageEngine
2. Remote (SCP): Full file download via SSH
3. Remote (Hot): Fast access to recent data via hot snapshots
"""
import pandas as pd
from typing import Optional, Literal
from pathlib import Path

from .storage import StorageEngine
from .schema import trades_to_ohlcv


class BinanceCollectorClient:
    """
    Client for consuming binance-collector data.

    Supports multiple access patterns:
    - Local: Read from local filesystem
    - Remote: SCP full files from remote machine
    - Hot: Read only recent data (fast, for dashboards)

    Examples:
        # Local access
        client = BinanceCollectorClient(mode='local', data_path='data')
        trades = client.get_trades('BTCUSDT', limit=1000)

        # Remote access (full files)
        client = BinanceCollectorClient(
            mode='remote',
            remote_host='your.remote.host',
            remote_user='root',
            ssh_key_path='~/.ssh/id_rsa',
            remote_data_path='/root/crypto_data'
        )
        trades = client.get_trades('BTCUSDT')

        # Hot access (recent data only, fast)
        client = BinanceCollectorClient(mode='hot', data_path='data')
        recent_trades = client.get_trades('BTCUSDT')  # Last 1024 rows
    """

    def __init__(
        self,
        mode: Literal['local', 'remote', 'hot'] = 'local',
        data_path: str = 'data',
        remote_host: Optional[str] = None,
        remote_user: Optional[str] = None,
        ssh_key_path: Optional[str] = None,
        remote_data_path: Optional[str] = None
    ):
        """
        Initialize client.

        Args:
            mode: Access mode ('local', 'remote', 'hot')
            data_path: Local data path (for local/hot modes)
            remote_host: Remote server IP/hostname (for remote mode)
            remote_user: SSH username (for remote mode)
            ssh_key_path: Path to SSH key (for remote mode)
            remote_data_path: Remote data directory (for remote mode)
        """
        self.mode = mode
        self.data_path = data_path

        if mode == 'local' or mode == 'hot':
            self.storage = StorageEngine(base_path=data_path)
        elif mode == 'remote':
            if not all([remote_host, remote_user, ssh_key_path, remote_data_path]):
                raise ValueError("Remote mode requires: remote_host, remote_user, ssh_key_path, remote_data_path")

            self.remote_host = remote_host
            self.remote_user = remote_user
            self.ssh_key_path = Path(ssh_key_path).expanduser()
            self.remote_data_path = remote_data_path
            self.storage = StorageEngine(base_path=data_path)  # For temp storage
        else:
            raise ValueError(f"Invalid mode: {mode}. Must be 'local', 'remote', or 'hot'")

    def get_trades(
        self,
        symbol: str,
        limit: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get trades for a symbol.

        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            limit: Max rows to return (if None, returns all)
            start_time: ISO timestamp filter (e.g., '2026-02-15T00:00:00')
            end_time: ISO timestamp filter

        Returns:
            DataFrame with trades

        Behavior by mode:
            - local: Read from local files
            - remote: Download full file via SCP
            - hot: Read from hot snapshot (last 1024 rows only)
        """
        if self.mode == 'hot':
            df = self.storage.read_hot('trades', symbol)
        elif self.mode == 'remote':
            df = self._scp_and_read('trades', symbol)
        else:
            df = self.storage.read('trades', symbol)

        if df.empty:
            return df

        # Apply time filters
        if start_time or end_time:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if start_time:
                df = df[df['timestamp'] >= pd.Timestamp(start_time)]
            if end_time:
                df = df[df['timestamp'] <= pd.Timestamp(end_time)]

        # Apply limit
        if limit:
            df = df.tail(limit)

        return df

    def get_orderbook(
        self,
        symbol: str,
        tick_size: Optional[float] = None,
        limit: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get orderbook snapshots for a symbol.

        Args:
            symbol: Trading pair
            tick_size: Filter by tick size (e.g., 10, 50, 100)
            limit: Max snapshots to return
            start_time: ISO timestamp filter
            end_time: ISO timestamp filter

        Returns:
            DataFrame with orderbook snapshots
        """
        if self.mode == 'hot':
            df = self.storage.read_hot('orderbook', symbol)
        elif self.mode == 'remote':
            df = self._scp_and_read('orderbook', symbol)
        else:
            df = self.storage.read('orderbook', symbol)

        if df.empty:
            return df

        # Filter by tick size
        if tick_size is not None:
            df = df[df['tick_size'] == tick_size]

        # Apply time filters
        if start_time or end_time:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if start_time:
                df = df[df['timestamp'] >= pd.Timestamp(start_time)]
            if end_time:
                df = df[df['timestamp'] <= pd.Timestamp(end_time)]

        # Apply limit
        if limit:
            df = df.tail(limit)

        return df

    def get_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1h',
        limit: Optional[int] = None,
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
        """
        # Get trades
        trades_df = self.get_trades(symbol, start_time=start_time, end_time=end_time)

        if trades_df.empty:
            return pd.DataFrame()

        # Derive OHLCV
        ohlcv = trades_to_ohlcv(trades_df, timeframe=timeframe)

        # Apply limit
        if limit:
            ohlcv = ohlcv.tail(limit)

        return ohlcv

    def get_available_symbols(self) -> dict:
        """
        Get list of available symbols.

        Returns:
            Dict with 'trades' and 'orderbook' symbol lists
        """
        if self.mode == 'remote':
            return self._get_remote_symbols()

        base = Path(self.data_path)
        symbols = {
            'trades': [],
            'orderbook': []
        }

        for data_type in ['trades', 'orderbook']:
            data_dir = base / data_type
            if data_dir.exists():
                # Include both main and hot files
                files = list(data_dir.glob('*.parquet'))
                symbols[data_type] = sorted(set(
                    f.stem.replace('_hot', '') for f in files
                ))

        return symbols

    def get_stats(self) -> dict:
        """
        Get statistics for all collected data.

        Returns:
            Dict with stats per symbol (rows, size, time range)
        """
        if self.mode == 'remote':
            return self._get_remote_stats()

        base = Path(self.data_path)
        stats = {'trades': {}, 'orderbook': {}}

        for data_type in ['trades', 'orderbook']:
            data_dir = base / data_type
            if data_dir.exists():
                for file in data_dir.glob('*.parquet'):
                    # Skip hot files
                    if file.stem.endswith('_hot'):
                        continue

                    try:
                        df = pd.read_parquet(file)
                        stats[data_type][file.stem] = {
                            'rows': len(df),
                            'size_mb': round(file.stat().st_size / 1024 / 1024, 2),
                            'start': str(df['timestamp'].min()),
                            'end': str(df['timestamp'].max()),
                            'columns': len(df.columns)
                        }
                    except Exception as e:
                        stats[data_type][file.stem] = {'error': str(e)}

        return stats

    def _scp_and_read(self, data_type: str, symbol: str) -> pd.DataFrame:
        """Download file via SCP and read"""
        import subprocess
        import tempfile

        remote_path = f'{self.remote_data_path}/{data_type}/{symbol}.parquet'

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # SCP download
            subprocess.run([
                'scp',
                '-i', str(self.ssh_key_path),
                f'{self.remote_user}@{self.remote_host}:{remote_path}',
                tmp_path
            ], check=True, capture_output=True)

            # Read
            df = pd.read_parquet(tmp_path)
            return df

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def _get_remote_symbols(self) -> dict:
        """Get available symbols on remote"""
        import subprocess

        result = subprocess.run([
            'ssh',
            '-i', str(self.ssh_key_path),
            f'{self.remote_user}@{self.remote_host}',
            f'ls {self.remote_data_path}/trades/*.parquet 2>/dev/null | xargs -n1 basename | sed "s/.parquet$//" && '
            f'ls {self.remote_data_path}/orderbook/*.parquet 2>/dev/null | xargs -n1 basename | sed "s/.parquet$//"'
        ], capture_output=True, text=True)

        lines = result.stdout.strip().split('\n')
        # First half is trades, second half is orderbook
        mid = len(lines) // 2
        return {
            'trades': [l.replace('_hot', '') for l in lines[:mid] if l],
            'orderbook': [l.replace('_hot', '') for l in lines[mid:] if l]
        }

    def sync_hot_snapshot(
        self,
        data_type: str,
        symbol: str,
        target_storage: 'StorageEngine'
    ) -> int:
        """
        Sync hot snapshot from remote to local storage.

        Fast sync for dashboards - downloads only the hot snapshot file
        (last 1024 rows, ~200 KB) instead of full file.

        Args:
            data_type: 'trades' or 'orderbook'
            symbol: Trading pair
            target_storage: Local StorageEngine to sync to

        Returns:
            Number of rows synced

        Example:
            # Dashboard usage - fast sync
            remote = BinanceCollectorClient(mode='remote', ...)
            local = StorageEngine(base_path='artifacts/live')

            remote.sync_hot_snapshot('orderbook', 'BTCUSDT', local)

            # Now read instantly from local hot file
            df = local.read_hot('orderbook', 'BTCUSDT')  # <1ms
        """
        if self.mode != 'remote':
            raise ValueError("sync_hot_snapshot only works in 'remote' mode")

        import subprocess
        import tempfile

        remote_path = f'{self.remote_data_path}/{data_type}/{symbol}_hot.parquet'

        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download hot snapshot via SCP
            subprocess.run([
                'scp',
                '-i', str(self.ssh_key_path),
                f'{self.remote_user}@{self.remote_host}:{remote_path}',
                tmp_path
            ], check=True, capture_output=True)

            # Read hot snapshot
            df = pd.read_parquet(tmp_path)

            # Write to local hot file
            local_hot_path = target_storage.base_path / data_type / f"{symbol}_hot.parquet"
            local_hot_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(local_hot_path, compression='snappy', index=False)

            return len(df)

        finally:
            Path(tmp_path).unlink(missing_ok=True)

    def sync_incremental(
        self,
        data_type: str,
        symbol: str,
        target_storage: 'StorageEngine'
    ) -> int:
        """
        Incremental sync: download only new data since last sync.

        For training pipelines - efficiently syncs only new rows by comparing
        timestamps between remote and local files.

        Args:
            data_type: 'trades' or 'orderbook'
            symbol: Trading pair
            target_storage: Local StorageEngine to sync to

        Returns:
            Number of new rows synced

        Example:
            # Training pipeline - incremental sync
            remote = BinanceCollectorClient(mode='remote', ...)
            local = StorageEngine(base_path='artifacts/historical')

            for symbol in ['BTCUSDT', 'ETHUSDT']:
                n_new = remote.sync_incremental('trades', symbol, local)
                print(f"Synced {n_new} new trades for {symbol}")

            # Now train on local data
            trades = local.read('trades', 'BTCUSDT')
        """
        if self.mode != 'remote':
            raise ValueError("sync_incremental only works in 'remote' mode")

        # Get local latest timestamp
        local_latest = target_storage.get_latest_timestamp(data_type, symbol)

        # Download remote file
        remote_df = self._scp_and_read(data_type, symbol)

        if remote_df.empty:
            return 0

        # Filter only new data
        if local_latest:
            remote_df['timestamp'] = pd.to_datetime(remote_df['timestamp'])
            new_data = remote_df[remote_df['timestamp'] > local_latest]
        else:
            new_data = remote_df

        if new_data.empty:
            return 0

        # Write to local storage (will merge with existing)
        dedup_col = 'agg_trade_id' if data_type == 'trades' else None
        target_storage.write(
            new_data,
            data_type,
            symbol,
            dedup_columns=[dedup_col] if dedup_col else None,
            sort_columns=['timestamp']
        )

        return len(new_data)

    def _get_remote_stats(self) -> dict:
        """Get stats from remote"""
        import subprocess
        import json

        cmd = f"""
source {self.remote_data_path}/../binance-collector-env/bin/activate && python3 << 'EOF'
from pathlib import Path
import pandas as pd
import json

base = Path("{self.remote_data_path}")
stats = {{'trades': {{}}, 'orderbook': {{}}}}

for data_type in ['trades', 'orderbook']:
    data_dir = base / data_type
    if data_dir.exists():
        for file in data_dir.glob('*.parquet'):
            if file.stem.endswith('_hot'):
                continue
            df = pd.read_parquet(file)
            stats[data_type][file.stem] = {{
                'rows': len(df),
                'size_mb': round(file.stat().st_size / 1024 / 1024, 2),
                'start': str(df['timestamp'].min()),
                'end': str(df['timestamp'].max()),
                'columns': len(df.columns)
            }}

print(json.dumps(stats))
EOF
"""

        result = subprocess.run([
            'ssh',
            '-i', str(self.ssh_key_path),
            f'{self.remote_user}@{self.remote_host}',
            cmd
        ], capture_output=True, text=True, check=True)

        return json.loads(result.stdout.strip())


# Convenience functions for quick access

def get_local_client(data_path: str = 'data') -> BinanceCollectorClient:
    """Create a local client"""
    return BinanceCollectorClient(mode='local', data_path=data_path)


def get_hot_client(data_path: str = 'data') -> BinanceCollectorClient:
    """Create a hot snapshot client (fast, recent data only)"""
    return BinanceCollectorClient(mode='hot', data_path=data_path)


def get_remote_client(
    remote_host: str,
    remote_user: str = 'root',
    ssh_key_path: str = '~/.ssh/id_rsa',
    remote_data_path: str = '/root/crypto_data'
) -> BinanceCollectorClient:
    """Create a remote client"""
    return BinanceCollectorClient(
        mode='remote',
        remote_host=remote_host,
        remote_user=remote_user,
        ssh_key_path=ssh_key_path,
        remote_data_path=remote_data_path
    )
