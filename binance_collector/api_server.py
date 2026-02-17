"""
FastAPI server for querying binance-collector data.
Designed for dashboard/streaming use cases - returns only requested data.
"""
from fastapi import FastAPI, Query, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import pandas as pd
import os
from pathlib import Path

from .storage import StorageEngine
from .schema import trades_to_ohlcv

app = FastAPI(
    title="Binance Collector API",
    description="Query trades and orderbook data efficiently",
    version="0.1.0"
)

# CORS for dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize storage (configurable via env)
DATA_PATH = os.getenv('DATA_PATH', '/root/crypto_data')
storage = StorageEngine(base_path=DATA_PATH)

# API Key authentication
API_KEY = os.getenv('API_KEY', None)
api_key_header = APIKeyHeader(name='X-API-Key', auto_error=False)

async def verify_api_key(api_key: str = Security(api_key_header)):
    """
    Verify API key if configured.

    Set API_KEY environment variable to enable authentication:
        export API_KEY="your-secret-key"
        uvicorn binance_collector.api_server:app --host 0.0.0.0 --port 8000

    If API_KEY is not set, authentication is disabled (open access).
    """
    if API_KEY is None:
        # No auth configured - allow all
        return True

    if api_key is None or api_key != API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid or missing API key. Include 'X-API-Key' header."
        )

    return True


@app.get("/")
def root():
    """API info"""
    return {
        "service": "binance-collector-api",
        "version": "0.1.0",
        "data_path": DATA_PATH,
        "endpoints": [
            "/trades/{symbol}",
            "/orderbook/{symbol}",
            "/ohlcv/{symbol}",
            "/stats",
            "/symbols"
        ]
    }


@app.get("/symbols")
def get_symbols():
    """Get available symbols"""
    trades_dir = Path(DATA_PATH) / 'trades'
    orderbook_dir = Path(DATA_PATH) / 'orderbook'

    symbols = {
        'trades': [f.stem for f in trades_dir.glob('*.parquet')] if trades_dir.exists() else [],
        'orderbook': [f.stem for f in orderbook_dir.glob('*.parquet')] if orderbook_dir.exists() else []
    }
    return symbols


@app.get("/trades/{symbol}")
def get_trades(
    symbol: str,
    limit: int = Query(1000, ge=1, le=100000, description="Max rows to return"),
    authenticated: bool = Depends(verify_api_key),
    offset: int = Query(0, ge=0, description="Skip N rows"),
    start_time: Optional[str] = Query(None, description="ISO timestamp (e.g., 2026-02-15T00:00:00)"),
    end_time: Optional[str] = Query(None, description="ISO timestamp"),
    tail: bool = Query(True, description="Return last N rows (most recent)")
):
    """
    Get trades for a symbol.

    Examples:
    - Last 100 trades: /trades/BTCUSDT?limit=100
    - Time range: /trades/BTCUSDT?start_time=2026-02-15T00:00:00&end_time=2026-02-15T01:00:00
    - Pagination: /trades/BTCUSDT?limit=1000&offset=5000
    """
    try:
        df = storage.read('trades', symbol)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"No trades found for {symbol}")

        # Time filtering
        if start_time:
            df = df[df['timestamp'] >= pd.Timestamp(start_time)]
        if end_time:
            df = df[df['timestamp'] <= pd.Timestamp(end_time)]

        # Pagination
        if tail:
            # Get last N rows (most recent)
            df = df.tail(limit + offset).iloc[offset:offset + limit]
        else:
            # Get first N rows from offset
            df = df.iloc[offset:offset + limit]

        # Convert timestamp to ISO string for JSON
        df['timestamp'] = df['timestamp'].astype(str)
        df = df.fillna(0)

        return {
            'symbol': symbol,
            'count': len(df),
            'data': df.to_dict(orient='records')
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orderbook/{symbol}")
def get_orderbook(
    symbol: str,
    authenticated: bool = Depends(verify_api_key),
    tick_size: Optional[float] = Query(None, description="Filter by tick size (e.g., 10, 50, 100)"),
    limit: int = Query(100, ge=1, le=10000, description="Max snapshots to return"),
    offset: int = Query(0, ge=0, description="Skip N snapshots"),
    start_time: Optional[str] = Query(None, description="ISO timestamp"),
    end_time: Optional[str] = Query(None, description="ISO timestamp"),
    tail: bool = Query(True, description="Return last N snapshots (most recent)")
):
    """
    Get orderbook snapshots for a symbol.

    Examples:
    - Last 100 snapshots: /orderbook/BTCUSDT?limit=100
    - Specific tick: /orderbook/BTCUSDT?tick_size=10&limit=50
    - Time range: /orderbook/BTCUSDT?start_time=2026-02-15T00:00:00
    """
    try:
        df = storage.read('orderbook', symbol)

        if df.empty:
            raise HTTPException(status_code=404, detail=f"No orderbook data for {symbol}")

        # Tick size filter
        if tick_size is not None:
            df = df[df['tick_size'] == tick_size]

        # Time filtering
        if start_time:
            df = df[df['timestamp'] >= pd.Timestamp(start_time)]
        if end_time:
            df = df[df['timestamp'] <= pd.Timestamp(end_time)]

        # Pagination
        if tail:
            df = df.tail(limit + offset).iloc[offset:offset + limit]
        else:
            df = df.iloc[offset:offset + limit]

        # Convert timestamp to string
        df['timestamp'] = df['timestamp'].astype(str)
        df = df.fillna(0)

        return {
            'symbol': symbol,
            'tick_size': tick_size,
            'count': len(df),
            'data': df.to_dict(orient='records')
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ohlcv/{symbol}")
def get_ohlcv(
    symbol: str,
    authenticated: bool = Depends(verify_api_key),
    timeframe: str = Query('1h', description="Resample rule (e.g., 1h, 5min, 1D)"),
    limit: int = Query(100, ge=1, le=10000, description="Max candles to return"),
    start_time: Optional[str] = Query(None, description="ISO timestamp"),
    end_time: Optional[str] = Query(None, description="ISO timestamp")
):
    """
    Get OHLCV candles derived from trades.

    Examples:
    - Last 100 hourly candles: /ohlcv/BTCUSDT?timeframe=1h&limit=100
    - 5-minute candles: /ohlcv/BTCUSDT?timeframe=5min&limit=200
    - Daily candles: /ohlcv/BTCUSDT?timeframe=1D
    """
    try:
        # Read trades
        trades_df = storage.read('trades', symbol)

        if trades_df.empty:
            raise HTTPException(status_code=404, detail=f"No trades found for {symbol}")

        # Time filtering before OHLCV
        if start_time:
            trades_df = trades_df[trades_df['timestamp'] >= pd.Timestamp(start_time)]
        if end_time:
            trades_df = trades_df[trades_df['timestamp'] <= pd.Timestamp(end_time)]

        # Derive OHLCV
        ohlcv = trades_to_ohlcv(trades_df, timeframe=timeframe)

        # Get last N candles
        ohlcv = ohlcv.tail(limit)

        # Convert timestamp to string
        ohlcv['timestamp'] = ohlcv['timestamp'].astype(str)

        return {
            'symbol': symbol,
            'timeframe': timeframe,
            'count': len(ohlcv),
            'data': ohlcv.to_dict(orient='records')
        }

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
def get_stats(authenticated: bool = Depends(verify_api_key)):
    """
    Get statistics for all collected data.

    Returns row counts, file sizes, time ranges per symbol.
    """
    try:
        base = Path(DATA_PATH)
        stats = {'trades': {}, 'orderbook': {}}

        for data_type in ['trades', 'orderbook']:
            data_dir = base / data_type
            if data_dir.exists():
                for file in data_dir.glob('*.parquet'):
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

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    """Health check endpoint"""
    base = Path(DATA_PATH)
    return {
        'status': 'healthy',
        'data_path': DATA_PATH,
        'data_path_exists': base.exists(),
        'trades_files': len(list((base / 'trades').glob('*.parquet'))) if (base / 'trades').exists() else 0,
        'orderbook_files': len(list((base / 'orderbook').glob('*.parquet'))) if (base / 'orderbook').exists() else 0
    }


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000)
