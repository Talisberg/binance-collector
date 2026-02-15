"""
Data schemas for Binance market data.
Core data: Trades + Order Book (OHLCV derived from trades)
"""
from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
import pandas as pd


class TradeRecord(BaseModel):
    """Single aggregate trade from Binance"""
    agg_trade_id: int = Field(description="Aggregate trade ID")
    timestamp: datetime
    symbol: str
    price: float = Field(gt=0)
    quantity: float = Field(gt=0)
    first_trade_id: int
    last_trade_id: int
    is_buyer_maker: bool  # True = seller was aggressor
    is_best_match: bool

    @property
    def side(self) -> str:
        """Trade side from taker perspective"""
        return 'sell' if self.is_buyer_maker else 'buy'

    @property
    def notional(self) -> float:
        """Trade value in quote currency"""
        return self.price * self.quantity


class OrderBookLevel(BaseModel):
    """Single order book level"""
    price: float = Field(gt=0)
    quantity: float = Field(gt=0)


class OrderBookSnapshot(BaseModel):
    """
    Order book snapshot with multi-tick aggregation.
    Stores N levels on each side with cumulative metrics.
    """
    timestamp: datetime
    symbol: str
    tick_size: float = Field(gt=0, description="Price aggregation step (e.g., $50)")

    # Best bid/ask (raw, not aggregated)
    best_bid: float = Field(gt=0)
    best_ask: float = Field(gt=0)

    # Spread metrics
    spread: float = Field(ge=0)
    spread_pct: float = Field(ge=0)

    # Number of levels stored
    num_levels: int = Field(gt=0, default=15)


# DataFrame schemas (for validation and type enforcement)

TRADE_SCHEMA = {
    'agg_trade_id': 'int64',
    'timestamp': 'datetime64[ns]',
    'symbol': 'string',
    'price': 'float64',
    'quantity': 'float64',
    'first_trade_id': 'int64',
    'last_trade_id': 'int64',
    'is_buyer_maker': 'bool',
    'is_best_match': 'bool'
}

# Order book stored in wide format:
# timestamp, symbol, tick_size, best_bid, best_ask, spread, spread_pct,
# bid_price_1, bid_qty_1, bid_cum_qty_1, bid_cum_usd_1, ... (N levels)
# ask_price_1, ask_qty_1, ask_cum_qty_1, ask_cum_usd_1, ... (N levels)
# imbalance, depth_ratio

ORDERBOOK_BASE_SCHEMA = {
    'timestamp': 'datetime64[ns]',
    'symbol': 'string',
    'tick_size': 'float64',
    'best_bid': 'float64',
    'best_ask': 'float64',
    'spread': 'float64',
    'spread_pct': 'float64',
    'imbalance': 'float64',
    'depth_ratio': 'float64'
}


def validate_dataframe(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    """
    Validate and enforce DataFrame schema.

    Args:
        df: DataFrame to validate
        schema: Expected schema dict (column -> dtype)

    Returns:
        Validated DataFrame with enforced types

    Raises:
        ValueError: If validation fails
    """
    # Check required columns exist
    missing = set(schema.keys()) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Enforce types
    for col, dtype in schema.items():
        if col in df.columns:
            try:
                if dtype == 'datetime64[ns]':
                    df[col] = pd.to_datetime(df[col])
                else:
                    df[col] = df[col].astype(dtype)
            except Exception as e:
                raise ValueError(f"Failed to convert '{col}' to {dtype}: {e}")

    return df


def trades_to_ohlcv(trades_df: pd.DataFrame, timeframe: str = '1h') -> pd.DataFrame:
    """
    Derive OHLCV candles from trade data.

    Args:
        trades_df: DataFrame with trade records (must have: timestamp, price, quantity)
        timeframe: Pandas resample rule (e.g., '1h', '5min', '1D')

    Returns:
        DataFrame with OHLCV columns
    """
    if trades_df.empty:
        return pd.DataFrame()

    # Ensure timestamp index
    if 'timestamp' not in trades_df.index.names:
        trades_df = trades_df.set_index('timestamp')

    # Group by symbol if present
    if 'symbol' in trades_df.columns:
        grouped = trades_df.groupby('symbol')
        ohlcv_list = []

        for symbol, group in grouped:
            ohlcv = group.resample(timeframe).agg({
                'price': ['first', 'max', 'min', 'last'],
                'quantity': 'sum'
            })
            ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
            ohlcv['symbol'] = symbol
            ohlcv_list.append(ohlcv.reset_index())

        return pd.concat(ohlcv_list, ignore_index=True)
    else:
        # Single symbol
        ohlcv = trades_df.resample(timeframe).agg({
            'price': ['first', 'max', 'min', 'last'],
            'quantity': 'sum'
        })
        ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
        return ohlcv.reset_index()
