"""
Example 3: Derive OHLCV from trades
Show how to create candlesticks from raw trade data.
"""
import sys
sys.path.insert(0, '..')

from binance_collector import StorageEngine, trades_to_ohlcv

def main():
    print("=" * 60)
    print("Example 3: Derive OHLCV from Trades")
    print("=" * 60)

    # Load storage
    storage = StorageEngine(base_path='data')

    # Read trades for BTCUSDT
    print("\nReading trades...")
    trades_df = storage.read('trades', 'BTCUSDT')

    if trades_df.empty:
        print("No trades found. Run example 01_basic_trades.py first.")
        return

    print(f"Loaded {len(trades_df):,} trades")

    # Derive different timeframes
    timeframes = ['1min', '5min', '15min', '1h', '4h', '1D']

    for tf in timeframes:
        print(f"\n{tf} candles:")

        ohlcv = trades_to_ohlcv(trades_df, timeframe=tf)

        if not ohlcv.empty:
            print(f"  Generated {len(ohlcv):,} candles")
            print(f"  Time range: {ohlcv['timestamp'].min()} → {ohlcv['timestamp'].max()}")

            # Show last 3 candles
            print("\n  Last 3 candles:")
            print(ohlcv[['timestamp', 'open', 'high', 'low', 'close', 'volume']].tail(3).to_string(index=False))

    # Save 1h OHLCV
    print("\n" + "=" * 60)
    print("Saving 1h OHLCV to data/ohlcv/BTCUSDT_1h.parquet...")
    print("=" * 60)

    ohlcv_1h = trades_to_ohlcv(trades_df, timeframe='1h')

    if not ohlcv_1h.empty:
        output_path = storage.base_path / 'ohlcv'
        output_path.mkdir(exist_ok=True)

        filepath = output_path / 'BTCUSDT_1h.parquet'
        ohlcv_1h.to_parquet(filepath, compression='snappy', index=False)

        print(f"✓ Saved {len(ohlcv_1h):,} candles to {filepath}")

if __name__ == '__main__':
    main()
