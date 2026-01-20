import json
import os
import pandas as pd
from .storage import DataManager
from .quality import DataGuard
# 新增导入
from utils.network import retry_request

db_manager = DataManager()
guard = DataGuard()

class StrategyConfig:
    def __init__(self, config_name='config.json'):
        self.config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', config_name)
    def load(self):
        try: return json.load(open(self.config_path))
        except: return {}

def fetch_data_ohlcv(symbols, timeframe, limit=1500, exchange=None, only_close=False):
    data_store = {}
    
    for sym in symbols:
        try:
            last_ts = db_manager.get_latest_timestamp(sym, timeframe)
            if exchange:
                new_ohlcv = None
                
                # === 使用 retry_request 包裹 API 请求 ===
                if last_ts:
                    since = last_ts + 1
                    # 这里的 func 是 exchange.fetch_ohlcv
                    new_ohlcv = retry_request(exchange.fetch_ohlcv, sym, timeframe, since=since)
                else:
                    new_ohlcv = retry_request(exchange.fetch_ohlcv, sym, timeframe, limit=limit)
                # ========================================

                if new_ohlcv:
                    db_manager.save_data(sym, timeframe, new_ohlcv)
            
            df = db_manager.load_dataframe(sym, timeframe, limit)
            
            if not df.empty:
                df = guard.check_and_fix(df, asset_name=sym)
                
            if not df.empty:
                if only_close: data_store[sym] = df['c']
                else: data_store[sym] = df
                    
        except Exception as e:
            print(f"⚠️ Fetch Error ({sym}): {e}")
    
    if only_close: return pd.DataFrame(data_store).ffill()
    else: return data_store