"""
Quick comparison: Parquet vs Feather for time-series data
"""
import pandas as pd
import numpy as np
import time
from pathlib import Path

def generate_sample_ohlcv(n_rows=100000):
    """Generate sample OHLCV data"""
    return pd.DataFrame({
        'timestamp': pd.date_range('2020-01-01', periods=n_rows, freq='1min'),
        'symbol': 'BTCUSDT',
        'open': np.random.uniform(20000, 60000, n_rows),
        'high': np.random.uniform(20000, 60000, n_rows),
        'low': np.random.uniform(20000, 60000, n_rows),
        'close': np.random.uniform(20000, 60000, n_rows),
        'volume': np.random.uniform(0, 1000, n_rows),
    })

def benchmark_format(df, format_name, write_func, read_func, path):
    """Benchmark write/read performance"""
    # Write
    t0 = time.time()
    write_func(df, path)
    write_time = time.time() - t0
    file_size = Path(path).stat().st_size / 1024 / 1024  # MB

    # Read
    t0 = time.time()
    df_read = read_func(path)
    read_time = time.time() - t0

    # Cleanup
    Path(path).unlink()

    return {
        'format': format_name,
        'write_time': write_time,
        'read_time': read_time,
        'file_size_mb': file_size,
        'rows': len(df)
    }

if __name__ == '__main__':
    print("=" * 60)
    print("Storage Format Benchmark: Parquet vs Feather")
    print("=" * 60)

    df = generate_sample_ohlcv(100000)
    print(f"\nDataset: {len(df):,} rows, {len(df.columns)} columns")
    print(f"Memory: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

    results = []

    # Parquet (snappy compression)
    print("\n[1/3] Testing Parquet (snappy)...")
    results.append(benchmark_format(
        df, 'parquet_snappy',
        lambda d, p: d.to_parquet(p, compression='snappy', index=False),
        pd.read_parquet,
        'test.parquet'
    ))

    # Parquet (gzip compression)
    print("[2/3] Testing Parquet (gzip)...")
    results.append(benchmark_format(
        df, 'parquet_gzip',
        lambda d, p: d.to_parquet(p, compression='gzip', index=False),
        pd.read_parquet,
        'test.parquet'
    ))

    # Feather
    print("[3/3] Testing Feather...")
    results.append(benchmark_format(
        df, 'feather',
        lambda d, p: d.to_feather(p),
        pd.read_feather,
        'test.feather'
    ))

    # Results
    results_df = pd.DataFrame(results)
    print("\n" + "=" * 60)
    print("RESULTS:")
    print("=" * 60)
    print(results_df.to_string(index=False))

    print("\n" + "=" * 60)
    print("RECOMMENDATION:")
    print("=" * 60)

    # Scoring (lower is better)
    results_df['write_score'] = results_df['write_time'] / results_df['write_time'].min()
    results_df['read_score'] = results_df['read_time'] / results_df['read_time'].min()
    results_df['size_score'] = results_df['file_size_mb'] / results_df['file_size_mb'].min()
    results_df['total_score'] = results_df['write_score'] + results_df['read_score'] + results_df['size_score']

    winner = results_df.loc[results_df['total_score'].idxmin()]

    print(f"\n✓ Best format: {winner['format']}")
    print(f"  - Write: {winner['write_time']:.3f}s")
    print(f"  - Read: {winner['read_time']:.3f}s")
    print(f"  - Size: {winner['file_size_mb']:.2f} MB")
    print(f"  - Total score: {winner['total_score']:.2f}")

    # Specific recommendation
    if 'parquet' in winner['format']:
        compression = winner['format'].split('_')[1]
        print(f"\n→ Use Parquet with {compression} compression")
        print(f"  Reasons: Better compression, columnar format, schema enforcement")
    else:
        print(f"\n→ Use Feather")
        print(f"  Reasons: Faster I/O, simpler format")
