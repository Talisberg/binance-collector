"""
Example 1: Basic trades collection
Incrementally collect aggregate trades for Bitcoin and Ethereum.
"""
import logging
import sys
sys.path.insert(0, '..')

from binance_collector import TradesCollector, StorageEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 60)
    print("Example 1: Basic Trades Collection")
    print("=" * 60)

    # Create storage engine
    storage = StorageEngine(base_path='data', compression='snappy')

    # Create trades collector
    collector = TradesCollector(
        symbols=['BTCUSDT', 'ETHUSDT'],
        storage=storage
    )

    # Collect new trades
    print("\nCollecting new trades...")
    stats = collector.update(save=True)

    # Print stats
    print("\n" + "=" * 60)
    print("Collection Summary:")
    print("=" * 60)

    for symbol, info in stats.items():
        if 'error' in info:
            print(f"\n{symbol}: ERROR - {info['error']}")
        else:
            print(f"\n{symbol}:")
            print(f"  Rows: {info['rows']:,}")
            if info['rows'] > 0:
                print(f"  Start: {info['start_time']}")
                print(f"  End:   {info['end_time']}")

    # Show file info
    print("\n" + "=" * 60)
    print("Storage Info:")
    print("=" * 60)

    for symbol in ['BTCUSDT', 'ETHUSDT']:
        file_info = storage.get_file_info('trades', symbol)
        if file_info['exists']:
            print(f"\n{symbol}:")
            print(f"  Path: {file_info['path']}")
            print(f"  Size: {file_info['size_mb']:.2f} MB")
            print(f"  Rows: {file_info['rows']:,}")
            print(f"  Range: {file_info['start_time']} → {file_info['end_time']}")

if __name__ == '__main__':
    main()
