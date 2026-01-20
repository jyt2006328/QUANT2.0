import pandas as pd
import numpy as np
import sqlite3
import os
import itertools
import matplotlib.pyplot as plt
from datetime import datetime
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

# 引入我们的模型和指标
from models.alpha import AlphaModel
from utils.indicators import calc_ema, calc_adx

# 消除 Warning
pd.set_option('future.no_silent_downcasting', True)

class CPCVEngine:
    def __init__(self, db_path='data/market_data.db'):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, db_path)
        self.conn = sqlite3.connect(self.db_path)
        
        # 验证对象
        self.target_symbol = 'BTC/USDT:USDT' 
        self.alpha_model = AlphaModel()
        
    def load_data(self):
        print(f"⏳ Loading 4-year history for {self.target_symbol}...")
        
        # === 升级 1: 加载完整 OHLCV 数据 (为了算 ADX) ===
        # 注意：storage.py 里的列名是全称，这里要对应
        query = f"SELECT timestamp, open, high, low, close FROM ohlcv_1h WHERE symbol='{self.target_symbol}' ORDER BY timestamp ASC"
        df = pd.read_sql(query, self.conn)
        
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('date', inplace=True)
        
        # 映射列名适配 utils
        df.rename(columns={'open':'o', 'high':'h', 'low':'l', 'close':'c'}, inplace=True)
        self.df = df
        
        print(f"✅ Loaded {len(self.df)} hours of data.")
        
        # === 升级 2: 预计算过滤器指标 ===
        print("   Computing Filters (EMA & ADX)...")
        self.ema200 = calc_ema(self.df['c'], 200)
        self.adx = calc_adx(self.df['h'], self.df['l'], self.df['c'], period=14)
        
        # 1. 预计算因子 (Features)
        print("   Computing Alphas (Features)...")
        # 构造假的多列 DataFrame 适配接口
        dummy_df = pd.DataFrame({self.target_symbol: self.df['c']})
        self.factors_dict = self.alpha_model.compute_signals(dummy_df)
        self.factors = self.factors_dict[self.target_symbol]
        
        # 2. 预计算目标 (Targets)
        self.targets = self.df['c'].pct_change().shift(-1).dropna()
        
        # 对齐所有数据 (Factor, Target, EMA, ADX)
        common_idx = self.factors.index.intersection(self.targets.index)
        # 还要确保 EMA/ADX 不为空 (前200个数据会是NaN)
        common_idx = common_idx[200:] 
        
        self.X = self.factors.loc[common_idx]
        self.y = self.targets.loc[common_idx]
        self.filter_ema = self.ema200.loc[common_idx]
        self.filter_adx = self.adx.loc[common_idx]
        self.price_close = self.df['c'].loc[common_idx]
        
        print(f"   Aligned Samples: {len(self.X)}")

    def run_cpcv(self, n_splits=5, n_test_splits=1):
        print(f"\n🚀 Starting CPCV (N={n_splits}, k={n_test_splits}) WITH FILTERS...")
        
        total_samples = len(self.X)
        chunk_size = total_samples // n_splits
        indices = np.arange(total_samples)
        chunks = [indices[i*chunk_size : (i+1)*chunk_size] for i in range(n_splits)]
        
        import itertools
        combos = list(itertools.combinations(range(n_splits), n_test_splits))
        
        results = []
        
        for i, test_chunk_ids in enumerate(combos):
            # 1. 切分数据
            test_idx = np.concatenate([chunks[i] for i in test_chunk_ids])
            train_chunk_ids = [x for x in range(n_splits) if x not in test_chunk_ids]
            train_idx = np.concatenate([chunks[i] for i in train_chunk_ids])
            
            X_train, y_train = self.X.iloc[train_idx], self.y.iloc[train_idx]
            X_test, y_test = self.X.iloc[test_idx], self.y.iloc[test_idx]
            
            # 2. 训练模型
            scaler = StandardScaler()
            X_train_s = scaler.fit_transform(X_train)
            X_test_s = scaler.transform(X_test)
            
            model = Ridge(alpha=1.0)
            model.fit(X_train_s, y_train)
            
            # 3. 预测 (Raw Signal)
            preds = model.predict(X_test_s)
            
            # === 升级 3: 应用过滤器逻辑 ===
            # 获取测试集对应时刻的指标
            ema_test = self.filter_ema.iloc[test_idx].values
            adx_test = self.filter_adx.iloc[test_idx].values
            price_test = self.price_close.iloc[test_idx].values
            
            # 原始持仓: 预测涨买(1)，预测跌空(-1)
            # 注意: Trend 策略通常是 Long-Only，这里我们模拟 Only Long
            # 如果预测 > 0 则买，否则空仓
            raw_positions = np.where(preds > 0, 1.0, 0.0)
            
            # 过滤器 A: EMA200 (趋势向下不做多)
            # 只有 Price > EMA200 才允许持有
            filter_mask_ema = (price_test > ema_test)
            
            # 过滤器 B: ADX (震荡不做单)
            # 只有 ADX > 25 才允许持有
            filter_mask_adx = (adx_test > 25)
            
            # 最终持仓 = 原始信号 * EMA过滤 * ADX过滤
            final_positions = raw_positions * filter_mask_ema * filter_mask_adx
            
            # 计算 PnL
            pnl = final_positions * y_test
            
            # 统计
            if pnl.std() == 0: sharpe = 0
            else: sharpe = (pnl.mean() / pnl.std()) * np.sqrt(365 * 24)
            
            total_ret = np.sum(pnl)
            
            results.append({
                "test_chunks": test_chunk_ids,
                "sharpe": sharpe,
                "return": total_ret,
                "trade_count": np.count_nonzero(final_positions) # 看看过滤后还剩多少交易
            })
            
            print(f"   Combo {i+1}/{len(combos)}: Sharpe={sharpe:.2f}, Trades={np.count_nonzero(final_positions)}")

        self.analyze_results(results)

    def analyze_results(self, results):
        df = pd.DataFrame(results)
        print("\n=== CPCV Analysis Report (With Trend+ADX Filter) ===")
        print(df.describe())
        
        mean_sharpe = df['sharpe'].mean()
        fail_rate = len(df[df['sharpe'] < 0.5]) / len(df)
        
        print(f"\nExpected Sharpe: {mean_sharpe:.2f}")
        print(f"Failure Rate: {fail_rate:.1%}")
        
        plt.figure(figsize=(10, 6))
        plt.hist(df['sharpe'], bins=10, alpha=0.7, color='green', edgecolor='black')
        plt.axvline(mean_sharpe, color='red', linestyle='dashed', linewidth=2, label=f'Mean: {mean_sharpe:.2f}')
        plt.title('CPCV Sharpe Distribution (Filtered)')
        plt.savefig('cpcv_result_v2.png')
        print("📸 Saved to cpcv_result_v2.png")

if __name__ == "__main__":
    engine = CPCVEngine()
    engine.load_data()
    engine.run_cpcv(n_splits=6, n_test_splits=1)