"""
Configuration management for Binance Collector.
Supports config files, environment variables, and defaults.
"""
import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class BinanceConfig:
    """Binance API configuration"""
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    base_url: str = 'https://api.binance.com'


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    requests_per_minute: int = 1200
    max_workers: int = 10
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class StorageConfig:
    """Storage configuration"""
    base_path: str = 'data'
    format: str = 'parquet'  # parquet or feather
    compression: str = 'snappy'  # snappy, gzip, zstd


@dataclass
class Config:
    """Main configuration object"""
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)

    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """
        Load configuration from YAML file.

        Args:
            config_path: Path to config.yaml

        Returns:
            Config object
        """
        config_file = Path(config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_file, 'r') as f:
            data = yaml.safe_load(f)

        return cls.from_dict(data or {})

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Config':
        """
        Create Config from dictionary.

        Args:
            data: Configuration dict

        Returns:
            Config object
        """
        binance_data = data.get('binance', {})
        rate_limit_data = data.get('rate_limit', {})
        storage_data = data.get('storage', {})

        return cls(
            binance=BinanceConfig(**binance_data),
            rate_limit=RateLimitConfig(**rate_limit_data),
            storage=StorageConfig(**storage_data)
        )

    @classmethod
    def from_env(cls) -> 'Config':
        """
        Load configuration from environment variables.

        Environment variables:
            BINANCE_API_KEY
            BINANCE_API_SECRET
            BINANCE_BASE_URL
            STORAGE_BASE_PATH
            STORAGE_FORMAT
            STORAGE_COMPRESSION

        Returns:
            Config object
        """
        return cls(
            binance=BinanceConfig(
                api_key=os.getenv('BINANCE_API_KEY'),
                api_secret=os.getenv('BINANCE_API_SECRET'),
                base_url=os.getenv('BINANCE_BASE_URL', 'https://api.binance.com')
            ),
            storage=StorageConfig(
                base_path=os.getenv('STORAGE_BASE_PATH', 'data'),
                format=os.getenv('STORAGE_FORMAT', 'parquet'),
                compression=os.getenv('STORAGE_COMPRESSION', 'snappy')
            )
        )

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> 'Config':
        """
        Load configuration with priority: file > env > defaults.

        Args:
            config_path: Optional path to config.yaml

        Returns:
            Config object
        """
        # Try config file
        if config_path and Path(config_path).exists():
            return cls.from_file(config_path)

        # Try default config.yaml in current directory
        default_path = Path('config.yaml')
        if default_path.exists():
            return cls.from_file(str(default_path))

        # Fall back to environment variables
        return cls.from_env()


def create_example_config(output_path: str = 'config.yaml'):
    """
    Create example configuration file.

    Args:
        output_path: Where to save example config
    """
    example = {
        'binance': {
            'api_key': 'YOUR_API_KEY_HERE',
            'api_secret': 'YOUR_API_SECRET_HERE',
            'base_url': 'https://api.binance.com'
        },
        'rate_limit': {
            'requests_per_minute': 1200,
            'max_workers': 10,
            'retry_attempts': 3,
            'retry_delay_seconds': 1.0
        },
        'storage': {
            'base_path': 'data',
            'format': 'parquet',
            'compression': 'snappy'
        }
    }

    with open(output_path, 'w') as f:
        yaml.dump(example, f, default_flow_style=False, sort_keys=False)

    print(f"Example config created: {output_path}")
    print("\nNote: API keys are optional for public data (trades, orderbook)")
