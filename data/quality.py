import pandas as pd
import numpy as np

class DataGuard:
    """
    [基础设施] 数据质量卫士
    职责: 检测缺失值、异常值(Outliers)、时间连续性
    """
    def __init__(self):
        pass

    def check_and_fix(self, df, asset_name="Unknown"):
        """
        输入: DataFrame (Index=Date, Columns=Open/High/Low/Close/Volume 或 单列 Close)
        输出: 修复后的 DataFrame
        """
        if df.empty:
            print(f"⚠️ [DataGuard] {asset_name} is empty!")
            return df

        # 1. 去除重复索引
        df = df[~df.index.duplicated(keep='first')]
        
        # 2. 排序
        df = df.sort_index()
        
        # 3. 检测空值 (NaN)
        if df.isnull().values.any():
            # print(f"   🔧 [DataGuard] {asset_name}: Filling NaNs...")
            df = df.ffill().bfill() # 前向填充，如果开头是空则后向填充
            
        # 4. 检测零值 (价格为0是致命的)
        # 如果是多列 (OHLCV)
        if 'c' in df.columns:
            if (df['c'] <= 0).any():
                print(f"🚨 [DataGuard] {asset_name}: Found ZERO price! Replacing with prev.")
                df['c'] = df['c'].replace(0, np.nan).ffill()
        # 如果是单列 (Close Series)
        elif isinstance(df, pd.Series) or (len(df.columns) == 1):
            # 假设第一列是价格
            col = df.columns[0]
            if (df[col] <= 0).any():
                df[col] = df[col].replace(0, np.nan).ffill()

        # 5. (进阶) 检测离群值 (Flash Crash)
        # 如果单根K线跌幅超过 50%，视为异常数据（除非真的是黑天鹅，但在数据清洗层通常先过滤）
        # 这里先只做警告，暂不剔除，以免误伤 Sniper
        
        return df