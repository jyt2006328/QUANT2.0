import ccxt
import pandas as pd
import time
import sys
import os
from datetime import datetime

# 动态添加父目录到 sys.path 以便导入 storage
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data.storage import DataManager 

# 配置
API_KEY = os.getenv('BITGET_API_KEY', '')
SECRET = os.getenv('BITGET_SECRET', '')
PASSWORD = os.getenv('BITGET_PASSWORD', '')

if not API_KEY:
    print("⚠️ 警告: 未找到 API Key，请检查环境变量设置！")

# 移除 ZEC/PUMP，只留主力
SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
    'AVAX/USDT:USDT', 'DOGE/USDT:USDT'
]

exchange = ccxt.bitget({
    'apiKey': API_KEY, 'secret': SECRET, 'password': PASSWORD,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'},
    # 既然是下载海量数据，建议挂上代理（如果本地跑）
    #'proxies': {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'},
})

db = DataManager()

def download_history(symbol, timeframe, start_date_str):
    """
    从指定日期开始下载数据
    start_date_str: '2021-01-01T00:00:00Z'
    """
    print(f"⬇️ [{symbol}] Downloading {timeframe} from {start_date_str}...")
    
    since = exchange.parse8601(start_date_str)
    limit = 1000 # 尝试最大单次请求
    total_saved = 0
    
    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
            
            if not ohlcv:
                print(f"   Finished (No data returned).")
                break
            
            last_ts = ohlcv[-1][0]
            
            # 存入数据库
            db.save_data(symbol, timeframe, ohlcv)
            total_saved += len(ohlcv)
            
            print(f"   Saved {len(ohlcv)} rows. Last: {pd.to_datetime(last_ts, unit='ms')}")
            
            # 推进时间
            since = last_ts + 1
            
            # 如果追上了当前时间 (误差1小时内)
            if last_ts >= (time.time() * 1000) - 3600000:
                print("   Reached Present.")
                break
                
            time.sleep(exchange.rateLimit / 1000) # 遵守限频

        except Exception as e:
            print(f"   ⚠️ Error: {e}. Retrying in 5s...")
            time.sleep(5)
            
    print(f"✅ Total {total_saved} rows saved for {symbol} {timeframe}")

if __name__ == "__main__":
    start_time = '2021-01-01T00:00:00Z'
    
    print("=== 🏗️ Infrastructure Phase: Data Expansion ===")
    
    for sym in SYMBOLS:
        # 1. 下载 1h 数据 (Trend/Pair/Basket 核心) - 必须从 2021 开始
        # download_history(sym, '1h', start_time)
        # 2. 下载 5m 数据 (Sniper 核心)
        # download_history(sym, '5m', start_time)
        # 补全共振策略需要的数据
        # 1m 数据: 最近 3 天足够 (共振只看当前趋势)
        download_history(sym, '1m', 3) 
        # 30m 数据: 最近 30 天足够
        download_history(sym, '30m', 30)
        
    print("=== Data Expansion Complete ===")
