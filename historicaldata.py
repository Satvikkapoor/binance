import requests
import pandas as pd
from datetime import datetime

class Historical_Trades:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        
    def get_historical_trades(self, symbol, limit=1000, fromId=None):
        
        params = {
            'symbol': symbol.upper(),
            'limit': limit
        }
        if fromId:
            params['fromId'] = fromId
            
        try:
            response = requests.get(f"{self.base_url}/historicalTrades", params=params, headers={'X-MBX-APIKEY': ''})
            response.raise_for_status()
            trades = response.json()
            
            df = pd.DataFrame(trades)
            df['time'] = pd.to_datetime(df['time'], unit='ms')
            df['price'] = df['price'].astype(float)
            df['qty'] = df['qty'].astype(float)
            
            return df
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching trades: {e}")
            return None

    def get_all_trades(self, symbol, start_id=None, end_id=None):
        all_trades = []
        current_id = start_id
        
        while True:
            df = self.get_historical_trades(symbol, fromId=current_id)
            if df is None or df.empty:
                break
                
            all_trades.append(df)
            
            if end_id and df['id'].max() >= end_id:
                break
                
            current_id = df['id'].max() + 1
            
        return pd.concat(all_trades) if all_trades else None


fetcher = Historical_Trades()

#trades = fetcher.get_historical_trades('BTCUSDT')

# trades = fetcher.get_all_trades('BTCUSDT', start_id=1000, end_id=2000)