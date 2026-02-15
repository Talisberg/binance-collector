"""
Example 2: Order book snapshot collection
Collect multi-tick orderbook snapshots.
"""
import logging
import sys
sys.path.insert(0, '..')

from binance_collector import OrderBookCollector, StorageEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    print("=" * 60)
    print("Example 2: Order Book Snapshots")
    print("=" * 60)

    # Create storage engine
    storage = StorageEngine(base_path='data')

    # Configure custom tick sizes
    tick_sizes = {
        'BTCUSDT': [10, 50, 100, 500, 1000],
        'ETHUSDT': [1, 5, 10, 50]
    }

    # Create collector
    collector = OrderBookCollector(
        symbols=['BTCUSDT', 'ETHUSDT'],
        tick_sizes=tick_sizes,
        num_levels=15,
        storage=storage
    )

    # Collect single snapshot
    print("\nCollecting orderbook snapshots...")
    stats = collector.update(save=True)

    # Print stats
    print("\n" + "=" * 60)
    print("Collection Summary:")
    print("=" * 60)

    for symbol, info in stats.items():
        print(f"\n{symbol}:")
        print(f"  Rows: {info['rows']}")
        print(f"  Tick sizes: {info['tick_sizes']}")
        print(f"  Timestamp: {info['timestamp']}")

    # Read and display sample
    print("\n" + "=" * 60)
    print("Sample Data (BTCUSDT, first tick size):")
    print("=" * 60)

    df = storage.read('orderbook', 'BTCUSDT')
    if not df.empty:
        # Show latest snapshot for smallest tick size
        latest = df.iloc[-1]
        print(f"\nTimestamp: {latest['timestamp']}")
        print(f"Tick size: ${latest['tick_size']:.0f}")
        print(f"Mid price: ${latest['best_bid']:.2f}")
        print(f"Spread: ${latest['spread']:.2f} ({latest['spread_pct']:.3f}%)")
        print(f"Imbalance: {latest['imbalance']:.3f}")
        print(f"Depth ratio: {latest['depth_ratio']:.2f}")

        # Show first 3 bid levels
        print("\nTop 3 Bid Levels:")
        for i in range(1, 4):
            price = latest.get(f'bid_price_{i}', 0)
            qty = latest.get(f'bid_qty_{i}', 0)
            cum_usd = latest.get(f'bid_cum_usd_{i}', 0)
            print(f"  Level {i}: ${price:.2f} | {qty:.4f} BTC | ${cum_usd:,.0f} cumulative")

if __name__ == '__main__':
    main()
