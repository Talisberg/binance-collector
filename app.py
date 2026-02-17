"""
API server entry point.

Usage:
    python app.py                        # uses config/remote.yaml
    python app.py --config my.yaml       # custom config
    python app.py --host 0.0.0.0 --port 8000
"""
import argparse
import yaml
import uvicorn
import os
from pathlib import Path


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(description='Binance Collector API Server')
    parser.add_argument('--config', default='config/remote.yaml', help='Config file path')
    parser.add_argument('--host', default=None, help='Override host')
    parser.add_argument('--port', type=int, default=None, help='Override port')
    args = parser.parse_args()

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    config = load_config(config_path)
    api_cfg = config.get('api', {})

    # Settings (args override config)
    host = args.host or api_cfg.get('host', '0.0.0.0')
    port = args.port or api_cfg.get('port', 8000)
    data_path = config.get('storage', {}).get('base_path', '/root/crypto_data')
    api_key = api_cfg.get('api_key', None)

    # Set env vars for api_server.py to pick up
    os.environ['DATA_PATH'] = data_path
    if api_key:
        os.environ['API_KEY'] = api_key

    print(f"Starting API server")
    print(f"  Data path: {data_path}")
    print(f"  Host: {host}:{port}")
    print(f"  Auth: {'enabled' if api_key else 'disabled'}")

    uvicorn.run(
        'binance_collector.api_server:app',
        host=host,
        port=port,
        log_level='info'
    )


if __name__ == '__main__':
    main()
