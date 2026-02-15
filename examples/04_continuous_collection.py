"""
Example 4: Continuous data collection
Run trades and orderbook collectors in parallel (daemon mode).
"""
import logging
import sys
import threading
import time
sys.path.insert(0, '..')

from binance_collector import TradesCollector, OrderBookCollector, StorageEngine

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def run_trades_collector(symbols, storage, interval_seconds=60):
    """Run trades collector in loop"""
    collector = TradesCollector(symbols=symbols, storage=storage)

    iteration = 0
    while True:
        iteration += 1
        logger.info(f"Trades collection #{iteration}")

        try:
            stats = collector.update(save=True)

            for symbol, info in stats.items():
                if 'error' not in info and info['rows'] > 0:
                    logger.info(f"  {symbol}: {info['rows']:,} new trades")

        except Exception as e:
            logger.error(f"Trades collection failed: {e}")

        time.sleep(interval_seconds)


def run_orderbook_collector(symbols, storage, interval_seconds=30):
    """Run orderbook collector in loop"""
    collector = OrderBookCollector(symbols=symbols, storage=storage)

    iteration = 0
    while True:
        iteration += 1
        logger.info(f"Orderbook collection #{iteration}")

        try:
            stats = collector.update(save=True)

            for symbol, info in stats.items():
                logger.info(f"  {symbol}: {info['rows']} rows, {info['tick_sizes']} ticks")

        except Exception as e:
            logger.error(f"Orderbook collection failed: {e}")

        time.sleep(interval_seconds)


def main():
    print("=" * 60)
    print("Example 4: Continuous Collection (Daemon Mode)")
    print("=" * 60)
    print("\nPress Ctrl+C to stop\n")

    # Configuration
    symbols = ['BTCUSDT', 'ETHUSDT']
    trades_interval = 60  # 1 minute
    orderbook_interval = 30  # 30 seconds

    # Shared storage
    storage = StorageEngine(base_path='data')

    # Create threads
    trades_thread = threading.Thread(
        target=run_trades_collector,
        args=(symbols, storage, trades_interval),
        name='TradesThread',
        daemon=True
    )

    orderbook_thread = threading.Thread(
        target=run_orderbook_collector,
        args=(symbols, storage, orderbook_interval),
        name='OrderbookThread',
        daemon=True
    )

    # Start threads
    logger.info("Starting continuous collection...")
    logger.info(f"  Trades interval: {trades_interval}s")
    logger.info(f"  Orderbook interval: {orderbook_interval}s")
    logger.info(f"  Symbols: {symbols}")

    trades_thread.start()
    orderbook_thread.start()

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nShutting down...")
        print("\n✓ Collection stopped")


if __name__ == '__main__':
    main()
