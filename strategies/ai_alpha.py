import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import os
from models.factors import FactorFactory

class AIAlphaStrategy:
    """
    [AI Strategy] 实盘策略 (V18.5 Fixed)
    修复: 
    1. 列名映射 (c->close)
    2. 特征对齐 (剔除原始 OHLCV，与训练集保持一致)
    """
    def __init__(self, model_path='models/xgb_model.pkl', top_n=2):
        self.model_path = model_path
        self.top_n = top_n
        self.factory = FactorFactory(use_lag=True)
        self.model = self._load_model()
        self.last_longs = []  # 上一轮持有的多单
        self.last_shorts = [] # 上一轮持有的空单
        self.buffer = 0.2     # [新增] 20% 的换仓阈值
        
    def _load_model(self):
        if os.path.exists(self.model_path):
            print(f"🧠 [AI] Model loaded from {self.model_path}")
            return joblib.load(self.model_path)
        print("⚠️ [AI] Model not found! Strategy will sleep.")
        return None

    def preprocess_live(self, df_factors):
        """
        数据清洗与标准化
        """
        # [FIX] 重置索引，确保数据格式统一
        if hasattr(df_factors.index, 'name') and df_factors.index.name is not None:
            df_factors = df_factors.reset_index(drop=True)
        
        # [CRITICAL FIX] 严格对齐特征列
        # 必须排除所有非因子的原始列，这列表必须与 models/cross_sectional.py 保持完全一致
        exclude_cols = [
            'symbol', 'target', 'timestamp',
            'open', 'high', 'low', 'close', 'volume',
            'o', 'h', 'l', 'c', 'v'
        ]
        
        # 只保留不在排除列表里的列 (即纯因子列)
        valid_cols = [c for c in df_factors.columns if c not in exclude_cols]
        df = df_factors[valid_cols].copy()
        
        # 2. 截面 Winsorize (去极值)
        def winsorize_col(col):
            lower = col.quantile(0.05)
            upper = col.quantile(0.95)
            return col.clip(lower, upper)
            
        df = df.apply(winsorize_col, axis=0)
        
        # 3. 截面 Z-Score (标准化)
        df = (df - df.mean()) / (df.std() + 1e-9)
        
        return df.fillna(0.0)

    def generate_signals(self, data_dict):
        if not self.model: return pd.Series()
        
        current_factors = []
        symbols = []
        
        for sym, df in data_dict.items():
            if df.empty or len(df) < 50: continue
            
            # [Fix 1] 列名映射
            df_renamed = df.rename(columns={
                'o': 'open', 'h': 'high', 'l': 'low', 
                'c': 'close', 'v': 'volume'
            })
            
            # 计算因子
            factors = self.factory.calculate_factors(df_renamed)
            
            latest = factors.iloc[[-1]].copy()
            current_factors.append(latest)
            symbols.append(sym)
            
        if not current_factors: return pd.Series()
        
        df_cross = pd.concat(current_factors)
        df_cross.index = symbols
        
        # [Fix 2] 清洗并对齐特征
        X_live = self.preprocess_live(df_cross)
        
        # [Debug] 打印一下特征数量，确保是 38 个
        # print(f"🔍 AI Features: {X_live.shape[1]}") 
        
        dtest = xgb.DMatrix(X_live)
        scores = self.model.predict(dtest)
        
        # 结果 Series: Index=Symbol, Value=Score
        current_scores = pd.Series(scores, index=symbols).sort_values(ascending=False)

        # === [Stability] 信号缓冲区逻辑 ===

        # 1. 初始状态 (第一次运行)
        if not self.last_longs and not self.last_shorts:
            long_candidates = current_scores.head(self.top_n).index.tolist()
            short_candidates = current_scores.tail(self.top_n).index.tolist()
        else:
            # 2. 缓冲逻辑
            long_candidates = self.last_longs.copy()
            short_candidates = self.last_shorts.copy()

            # 检查是否有更好的 Long
            # 现在的第 N 名
            current_top_n = current_scores.head(self.top_n).index.tolist()

            for new_cand in current_top_n:
                if new_cand not in long_candidates:
                    # 这是一个潜在的新多单
                    # 找到我要被替换掉的旧多单 (分数最低的那个)
                    worst_old = min(long_candidates, key=lambda x: current_scores.get(x, -999))

                    score_diff = current_scores[new_cand] - current_scores.get(worst_old, -999)

                    # 只有新币分数比旧币高出 buffer，才换！
                    # 假设分数范围是 -0.1 ~ 0.1, buffer 可以设为 0.005 (50bp)
                    # 或者用相对比例
                    if score_diff > 0.002: # 硬阈值，避免除法问题
                        print(f"🔄 [AI SWAP] Long: {worst_old} -> {new_cand} (Diff: {score_diff:.4f})")
                        long_candidates.remove(worst_old)
                        long_candidates.append(new_cand)

            # 检查是否有更好的 Short (逻辑同上，方向相反)
            current_bottom_n = current_scores.tail(self.top_n).index.tolist()
            for new_cand in current_bottom_n:
                if new_cand not in short_candidates:
                    worst_old = max(short_candidates, key=lambda x: current_scores.get(x, 999))
                    score_diff = current_scores.get(worst_old, 999) - current_scores[new_cand]

                    if score_diff > 0.002:
                        print(f"🔄 [AI SWAP] Short: {worst_old} -> {new_cand} (Diff: {score_diff:.4f})")
                        short_candidates.remove(worst_old)
                        short_candidates.append(new_cand)

        # 更新记忆
        self.last_longs = long_candidates
        self.last_shorts = short_candidates

        # 4. 生成权重
        weights = pd.Series(0.0, index=symbols)
        weight_per_asset = 1.0 / self.top_n

        for sym in long_candidates: 
            weights[sym] = weight_per_asset
        for sym in short_candidates: 
            weights[sym] = -weight_per_asset

        print(f"🧠 [AI Signal] Long: {long_candidates} | Short: {short_candidates}")
        return weights