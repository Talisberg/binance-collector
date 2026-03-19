#!/usr/bin/env python3
"""
Backfill script for missing historical data
Fetches Feb 25 - Mar 15, 2026 data that was missed when collector was down
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import requests
import logging
from typing import List, Dict

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class BackfillCollector:
    def __init__(self, base_path: str = '/root/crypto_data_v2'):
        self.base_path = Path(base_path)
        self.base_url = 'https://api.binance.com/api/v3'
        self.symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT']

    def get_last_trade_id(self, symbol: str, date: datetime) -> int:
        """Get the last trade ID for a given date"""
        # Get trades around the target time
        timestamp = int(date.timestamp() * 1000)
        url = f"{self.base_url}/aggTrades"
        params = {
            'symbol': symbol,
            'startTime': timestamp - 60000,  # 1 minute before
            'endTime': timestamp + 60000,    # 1 minute after
            'limit': 1
        }

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data:
                    return data[0]['a']  # aggregate trade ID
        except Exception as e:
            logging.error(f"Error getting last trade ID: {e}")

        return None

    def fetch_trades_batch(self, symbol: str, from_id: int = None, limit: int = 1000) -> List[Dict]:
        """Fetch a batch of trades"""
        url = f"{self.base_url}/aggTrades"
        params = {
            'symbol': symbol,
            'limit': limit
        }

        if from_id:
            params['fromId'] = from_id

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                trades = []
                for trade in data:
                    trades.append({
                        'agg_trade_id': trade['a'],
                        'timestamp': pd.to_datetime(trade['T'], unit='ms'),
                        'symbol': symbol,
                        'price': float(trade['p']),
                        'quantity': float(trade['q']),
                        'first_trade_id': trade['f'],
                        'last_trade_id': trade['l'],
                        'is_buyer_maker': trade['m'],
                        'is_best_match': True
                    })
                return trades
        except Exception as e:
            logging.error(f"Error fetching trades: {e}")

        return []

    def backfill_symbol(self, symbol: str, start_date: datetime, end_date: datetime):
        """Backfill trades for a single symbol"""
        logging.info(f"Starting backfill for {symbol} from {start_date} to {end_date}")

        # Get starting trade ID
        start_id = self.get_last_trade_id(symbol, start_date)
        if not start_id:
            logging.warning(f"Could not find starting trade ID for {symbol}")
            return

        # Get ending trade ID
        end_id = self.get_last_trade_id(symbol, end_date)
        if not end_id:
            logging.warning(f"Could not find ending trade ID for {symbol}")
            return

        logging.info(f"{symbol}: Fetching trades from ID {start_id} to {end_id}")

        all_trades = []
        current_id = start_id
        request_count = 0

        while current_id and current_id < end_id:
            # Fetch batch
            trades = self.fetch_trades_batch(symbol, current_id, 1000)

            if not trades:
                break

            all_trades.extend(trades)
            current_id = trades[-1]['agg_trade_id'] + 1
            request_count += 1

            # Progress update every 100 requests
            if request_count % 100 == 0:
                last_time = trades[-1]['timestamp']
                progress = ((current_id - start_id) / (end_id - start_id)) * 100
                logging.info(f"{symbol}: {len(all_trades):,} trades fetched, "
                           f"last time: {last_time}, progress: {progress:.1f}%")

            # Rate limit: max 1200 requests/minute = 20/second
            time.sleep(0.05)  # 20 requests/second

            # Save batch every 500k trades to manage memory
            if len(all_trades) >= 500000:
                self.save_trades(symbol, all_trades)
                all_trades = []

        # Save remaining trades
        if all_trades:
            self.save_trades(symbol, all_trades)

        logging.info(f"Completed {symbol}: {len(all_trades):,} trades in {request_count} requests")

    def save_trades(self, symbol: str, trades: List[Dict]):
        """Save trades to parquet file"""
        if not trades:
            return

        df = pd.DataFrame(trades)

        # Group by date for daily files
        df['date'] = df['timestamp'].dt.date

        for date, group in df.groupby('date'):
            date_str = date.strftime('%Y%m%d')
            filepath = self.base_path / 'trades' / f'{symbol}_{date_str}.parquet'
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Append to existing file if it exists
            if filepath.exists():
                existing = pd.read_parquet(filepath)
                group = pd.concat([existing, group.drop('date', axis=1)], ignore_index=True)
                # Remove duplicates
                group = group.drop_duplicates(subset=['agg_trade_id'], keep='last')
            else:
                group = group.drop('date', axis=1)

            group.to_parquet(filepath, compression='snappy', index=False)
            logging.info(f"Saved {len(group)} trades to {filepath}")

    def run_backfill(self):
        """Run the full backfill process"""
        start_date = datetime(2026, 2, 25, 11, 30)  # After last collected data
        end_date = datetime(2026, 3, 15, 0, 0)      # Today

        logging.info("=" * 60)
        logging.info("STARTING BACKFILL PROCESS")
        logging.info(f"Period: {start_date} to {end_date}")
        logging.info(f"Symbols: {', '.join(self.symbols)}")
        logging.info("=" * 60)

        for symbol in self.symbols:
            try:
                self.backfill_symbol(symbol, start_date, end_date)
                time.sleep(2)  # Brief pause between symbols
            except Exception as e:
                logging.error(f"Error backfilling {symbol}: {e}")
                continue

        logging.info("=" * 60)
        logging.info("BACKFILL COMPLETE!")
        logging.info("=" * 60)


if __name__ == "__main__":
    backfiller = BackfillCollector()
    backfiller.run_backfill()