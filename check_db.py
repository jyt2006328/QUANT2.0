import sqlite3
import pandas as pd
import os
from datetime import datetime

# 锁定路径
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data/market_data.db')

def check_database():
    if not os.path.exists(DB_PATH):
        print("❌ Database not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    print(f"📂 Checking DB: {DB_PATH}")
    print(f"📏 File Size: {os.path.getsize(DB_PATH) / 1024 / 1024:.2f} MB")
    print("-" * 60)
    print(f"{'Table':<10} | {'Symbol':<15} | {'Count':<8} | {'Start Date':<20} | {'End Date':<20}")
    print("-" * 60)

    for tf in ['1h', '5m']:
        table = f"ohlcv_{tf}"
        try:
            # 获取所有币种
            cursor.execute(f"SELECT DISTINCT symbol FROM {table}")
            symbols = [row[0] for row in cursor.fetchall()]
            
            for sym in symbols:
                # 获取最早和最晚时间
                cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM {table} WHERE symbol=?", (sym,))
                min_ts, max_ts, count = cursor.fetchone()
                
                start_date = datetime.fromtimestamp(min_ts/1000).strftime('%Y-%m-%d %H:%M')
                end_date = datetime.fromtimestamp(max_ts/1000).strftime('%Y-%m-%d %H:%M')
                
                print(f"{tf:<10} | {sym:<15} | {count:<8} | {start_date:<20} | {end_date:<20}")
                
        except Exception as e:
            print(f"⚠️ Error reading {table}: {e}")

    conn.close()

if __name__ == "__main__":
    check_database()