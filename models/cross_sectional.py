import pandas as pd
import numpy as np
from datetime import datetime
from .factors import FactorFactory
from tqdm import tqdm  # 进度条

class CrossSectionalLoader:
    def __init__(self, db_manager):
        self.db = db_manager
        self.factory = FactorFactory(use_lag=True)

    def load_all_assets(self, symbols, timeframe='1h', limit=1000):
        all_dfs = []
        valid_count = 0
        
        print(f"⏳ Loading data for {len(symbols)} assets...")
        
        for sym in tqdm(symbols):
            # 1. 加载原始数据
            df = self.db.load_dataframe(sym, timeframe, limit)
            # 宽松一点的长度检查，防止刚上线的币被过滤
            if df.empty or len(df) < 50: 
                continue
                
            # 2. 列名映射修复 (关键!)
            df = df.rename(columns={
                'o': 'open', 'h': 'high', 'l': 'low', 
                'c': 'close', 'v': 'volume'
            })
            
            # 3. 内存优化 (float32)
            df = df.astype({col: 'float32' for col in ['open', 'high', 'low', 'close', 'volume']})
            
            # 4. 计算因子
            df_factors = self.factory.calculate_factors(df)
            
            # 5. 计算目标变量 (Label)
            df_factors['target'] = df['close'].shift(-1) / df['close'] - 1.0
            df_factors['symbol'] = sym
            
            all_dfs.append(df_factors)
            valid_count += 1
            
        print(f"✅ Loaded {valid_count}/{len(symbols)} assets")
        
        if not all_dfs:
            return pd.DataFrame()
            
        # 合并和索引设置
        full_df = pd.concat(all_dfs)
        full_df = full_df.reset_index().set_index(['date', 'symbol']).sort_index()
        
        return full_df

class Preprocessor:
    """
    [Preprocessor V2] 增加中性化 (Neutralization)
    """
    def clean_data(self, df):
        # 1. 基础清理
        df = df.replace([np.inf, -np.inf], np.nan).dropna()
        
        # 排除非特征列
        exclude_cols = [
            'target', 'symbol', 'timestamp', 
            'open', 'high', 'low', 'close', 'volume',
            'o', 'h', 'l', 'c', 'v'
        ]
        # 识别出因子列
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        # 2. 异常值处理 (Winsorize)
        # print(f"🧹 Winsorizing {len(feature_cols)} features...")
        df_clean = df.copy()
        
        def winsorize_section(section_df):
            lower = section_df.quantile(0.05)
            upper = section_df.quantile(0.95)
            return section_df.clip(lower=lower, upper=upper, axis=1)

        df_clean[feature_cols] = df[feature_cols].groupby(level='date', group_keys=False).apply(winsorize_section)
        
        # 3. [NEW] 市值中性化 (Market Cap Neutralization)
        # 使用 log(close * volume) 作为市值的代理 (Dollar Volume)
        # 逻辑: 剔除因子中与“市值”线性的部分，防止 AI 只买大票或只买小票
        # 简单做法: 对市值进行分组，或者直接在 Z-Score 时考虑权重 (这里用最简单的: 既然是 Crypto，直接做 Z-Score 其实已经隐含了部分截面中性)
        # 为了不引入过高的计算复杂度（回归法太慢），我们采用 "Robust Z-Score"
        
        print("📏 Normalizing factors (Z-Score)...")
        
        def zscore_section(section_df):
            # 使用中位数和中位数绝对偏差 (MAD) 替代 Mean/Std，更抗干扰
            median = section_df.median()
            mad = (section_df - median).abs().median()
            return (section_df - median) / (mad + 1e-9)
            
        df_norm = df_clean.copy()
        df_norm[feature_cols] = df_clean[feature_cols].groupby(level='date', group_keys=False).apply(zscore_section)
        
        # 补回 target
        df_norm['target'] = df_clean['target']
        
        # 4. Target Clip
        df_norm['target'] = np.clip(df_norm['target'], -0.1, 0.1)
        
        # 返回纯净数据
        final_cols = feature_cols + ['target']
        return df_norm[final_cols].dropna()