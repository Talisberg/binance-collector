"""
Storage engine with Parquet backend.
Handles incremental updates, deduplication, and schema validation.
"""
import pandas as pd
from pathlib import Path
from typing import Optional, List
import logging

from ..schema import validate_dataframe

logger = logging.getLogger(__name__)


class StorageEngine:
    """
    Parquet-based storage with incremental updates and deduplication.
    """

    def __init__(
        self,
        base_path: str = 'data',
        compression: str = 'snappy'
    ):
        """
        Args:
            base_path: Base directory for data storage
            compression: Compression algorithm ('snappy', 'gzip', 'zstd')
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.compression = compression

    def write(
        self,
        df: pd.DataFrame,
        data_type: str,
        symbol: str,
        schema: Optional[dict] = None,
        dedup_columns: Optional[List[str]] = None,
        sort_columns: Optional[List[str]] = None
    ) -> Path:
        """
        Write DataFrame to parquet with optional merge and deduplication.

        Args:
            df: Data to write
            data_type: Data type ('trades', 'orderbook')
            symbol: Trading symbol
            schema: Schema to validate against
            dedup_columns: Columns for deduplication
            sort_columns: Columns to sort by

        Returns:
            Path to output file
        """
        if df.empty:
            logger.warning(f"Attempted to write empty DataFrame for {symbol} {data_type}")
            return None

        # Validate schema if provided
        if schema:
            df = validate_dataframe(df, schema)

        # Output path
        filepath = self.base_path / data_type / f"{symbol}.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        # Load existing data if present
        if filepath.exists():
            existing = pd.read_parquet(filepath)

            # Merge
            combined = pd.concat([existing, df], ignore_index=True)

            # Deduplicate if specified
            if dedup_columns:
                before_count = len(combined)
                combined = combined.drop_duplicates(subset=dedup_columns, keep='last')
                dropped = before_count - len(combined)
                if dropped > 0:
                    logger.info(f"Dropped {dropped} duplicate rows for {symbol}")

            # Sort if specified
            if sort_columns:
                combined = combined.sort_values(sort_columns)

            df_to_write = combined
        else:
            # First write
            if sort_columns and not df.empty:
                df = df.sort_values(sort_columns)
            df_to_write = df

        # Write to parquet
        df_to_write.to_parquet(
            filepath,
            compression=self.compression,
            index=False
        )

        logger.info(f"Wrote {len(df_to_write):,} rows to {filepath}")
        return filepath

    def read(
        self,
        data_type: str,
        symbol: str,
        start_time: Optional[pd.Timestamp] = None,
        end_time: Optional[pd.Timestamp] = None
    ) -> pd.DataFrame:
        """
        Read data from parquet with optional time filtering.

        Args:
            data_type: Data type ('trades', 'orderbook')
            symbol: Trading symbol
            start_time: Filter start (inclusive)
            end_time: Filter end (inclusive)

        Returns:
            DataFrame with requested data
        """
        filepath = self.base_path / data_type / f"{symbol}.parquet"

        if not filepath.exists():
            logger.warning(f"File not found: {filepath}")
            return pd.DataFrame()

        df = pd.read_parquet(filepath)

        # Apply time filters if specified
        if 'timestamp' in df.columns and (start_time or end_time):
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            if start_time:
                df = df[df['timestamp'] >= start_time]

            if end_time:
                df = df[df['timestamp'] <= end_time]

        return df

    def get_latest_timestamp(
        self,
        data_type: str,
        symbol: str
    ) -> Optional[pd.Timestamp]:
        """
        Get the latest timestamp for incremental updates.

        Args:
            data_type: Data type ('trades', 'orderbook')
            symbol: Trading symbol

        Returns:
            Latest timestamp or None if no data
        """
        filepath = self.base_path / data_type / f"{symbol}.parquet"

        if not filepath.exists():
            return None

        df = pd.read_parquet(filepath, columns=['timestamp'])

        if df.empty:
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df['timestamp'].max()

    def get_file_info(self, data_type: str, symbol: str) -> dict:
        """
        Get metadata about stored file.

        Args:
            data_type: Data type
            symbol: Trading symbol

        Returns:
            Dict with file info
        """
        filepath = self.base_path / data_type / f"{symbol}.parquet"

        if not filepath.exists():
            return {'exists': False}

        file_size = filepath.stat().st_size / 1024 / 1024  # MB
        df = pd.read_parquet(filepath)

        return {
            'exists': True,
            'path': str(filepath),
            'size_mb': file_size,
            'rows': len(df),
            'columns': list(df.columns),
            'start_time': df['timestamp'].min() if 'timestamp' in df.columns else None,
            'end_time': df['timestamp'].max() if 'timestamp' in df.columns else None
        }
