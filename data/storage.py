import sqlite3
import os
import pandas as pd

class DataManager:
    """
    [基础设施] 本地数据仓库 (SQLite)
    """
    def __init__(self, db_name='market_data.db'):
        # 锁定路径
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, db_name)
        
        # 修改点 1: 增加 timeout (给你 30秒等待解锁，而不是直接报错)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
        
        # 修改点 2: 开启 WAL 模式 (这是解决 Dashboard 卡死的神器)
        try:
            self.conn.execute("PRAGMA journal_mode=WAL;")
        except:
            pass
            
        self._init_db()

    def _init_db(self):
        cursor = self.conn.cursor()
        # === 修复点: 增加 1m 和 30m ===
        for tf in ['1m', '5m', '30m', '1h']:
            table_name = f"ohlcv_{tf}"
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    symbol TEXT,
                    timestamp INTEGER,
                    open REAL, high REAL, low REAL, close REAL, volume REAL,
                    PRIMARY KEY (symbol, timestamp)
                )
            ''')
        self.conn.commit()

    def get_latest_timestamp(self, symbol, timeframe):
        table_name = f"ohlcv_{timeframe}"
        cursor = self.conn.cursor()
        # 检查表是否存在 (防止旧库报错)
        try:
            cursor.execute(f"SELECT MAX(timestamp) FROM {table_name} WHERE symbol=?", (symbol,))
            result = cursor.fetchone()[0]
            return result if result else None
        except sqlite3.OperationalError:
            return None

    def save_data(self, symbol, timeframe, ohlcv_list):
        if not ohlcv_list: return
        table_name = f"ohlcv_{timeframe}"
        data_to_insert = []
        for row in ohlcv_list:
            data_to_insert.append((symbol, row[0], row[1], row[2], row[3], row[4], row[5]))
        
        cursor = self.conn.cursor()
        # 确保存储时表已存在 (双重保险)
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                symbol TEXT,
                timestamp INTEGER,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                PRIMARY KEY (symbol, timestamp)
            )
        ''')
        cursor.executemany(f'''
            INSERT OR IGNORE INTO {table_name} (symbol, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
        self.conn.commit()

    def load_dataframe(self, symbol, timeframe, limit):
        table_name = f"ohlcv_{timeframe}"
        try:
            query = f"SELECT timestamp, open, high, low, close, volume FROM {table_name} WHERE symbol=? ORDER BY timestamp DESC LIMIT ?"
            df = pd.read_sql_query(query, self.conn, params=(symbol, limit))
            
            if df.empty: return pd.DataFrame()
            
            df = df.sort_values('timestamp')
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('date', inplace=True)
            
            # 映射列名 (适配策略层)
            df.rename(columns={'open':'o', 'high':'h', 'low':'l', 'close':'c', 'volume':'v'}, inplace=True)
            return df
        except:
            return pd.DataFrame()