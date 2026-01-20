import pandas as pd
import numpy as np
import sqlite3
import os
import itertools
import matplotlib.pyplot as plt
from datetime import datetime

# 消除 Warning
pd.set_option('future.no_silent_downcasting', True)

class CPCVBasketEngine:
    def __init__(self, db_path='data/market_data.db'):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, db_path)
        self.conn = sqlite3.connect(self.db_path)
        
        # Basket 配置: 1 Long vs 3 Shorts
        self.main_symbol = 'BTC/USDT:USDT'
        self.alts = ['ETH/USDT:USDT', 'SOL/USDT:USDT', 'AVAX/USDT:USDT']
        
        # 策略参数
        self.lookback = 20
        self.threshold = 2.0
        
    def load_data(self):
        print(f"⏳ Loading 4-year history for Basket Check...")
        
        # 加载所有相关币种数据
        symbols = [self.main_symbol] + self.alts
        sym_str = "'" + "','".join(symbols) + "'"
        query = f"SELECT timestamp, symbol, close FROM ohlcv_1h WHERE symbol IN ({sym_str}) ORDER BY timestamp ASC"
        
        df = pd.read_sql(query, self.conn)
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        
        # 透视表
        self.prices = df.pivot(index='date', columns='symbol', values='close').ffill().dropna()
        print(f"✅ Loaded & Aligned: {len(self.prices)} rows.")

    def run_cpcv(self, n_splits=6, n_test_splits=1):
        print(f"\n🚀 Starting Basket CPCV (N={n_splits})...")
        
        # 计算全量策略信号 (向量化计算，速度快)
        # 1. 计算 Z-Scores
        z_scores_list = []
        for alt in self.alts:
            if alt not in self.prices.columns: continue
            ratio = self.prices[self.main_symbol] / self.prices[alt]
            mean = ratio.rolling(self.lookback).mean()
            std = ratio.rolling(self.lookback).std()
            z = (ratio - mean) / (std + 1e-9)
            z_scores_list.append(z)
            
        avg_z = pd.concat(z_scores_list, axis=1).mean(axis=1)
        
        # 2. 计算持仓权重 (1:1 对冲)
        # 信号: AvgZ > 2 -> Long BTC, Short Alts
        # 信号: AvgZ < -2 -> Short BTC, Long Alts
        
        # 权重容器
        w_main = pd.Series(0.0, index=self.prices.index)
        w_alts = pd.DataFrame(0.0, index=self.prices.index, columns=self.alts)
        
        # Case A: 吸血 (Long BTC / Short Alts)
        mask_long = avg_z > self.threshold
        w_main[mask_long] = 0.5
        for alt in self.alts:
            w_alts.loc[mask_long, alt] = -0.5 / len(self.alts)
            
        # Case B: 山寨季 (Short BTC / Long Alts)
        mask_short = avg_z < -self.threshold
        w_main[mask_short] = -0.5
        for alt in self.alts:
            w_alts.loc[mask_short, alt] = 0.5 / len(self.alts)
            
        # 3. 计算 PnL
        # 收益 = 权重(T-1) * 收益率(T)
        returns = self.prices.pct_change().shift(-1).fillna(0.0) # T+1 收益
        
        pnl_main = w_main * returns[self.main_symbol]
        pnl_alts = (w_alts * returns[self.alts]).sum(axis=1)
        total_pnl = pnl_main + pnl_alts
        
        # 扣除手续费估算 (每次变仓万六)
        # 简化：假设只要有持仓每天产生一点磨损，或者忽略
        # 这里为了看纯Alpha，先不扣费，或者给一个保守估计
        
        # 4. CPCV 切片统计
        total_len = len(total_pnl)
        chunk_size = total_len // n_splits
        results = []
        
        for i in range(n_splits):
            # 这是一个切片 (比如 2021上半年)
            start_idx = i * chunk_size
            end_idx = (i + 1) * chunk_size
            
            chunk_pnl = total_pnl.iloc[start_idx:end_idx]
            
            # 统计该切片的表现
            if chunk_pnl.std() == 0: sharpe = 0
            else: sharpe = (chunk_pnl.mean() / chunk_pnl.std()) * np.sqrt(365 * 24)
            
            ret = chunk_pnl.sum()
            
            results.append({
                "chunk_id": i,
                "sharpe": sharpe,
                "return": ret
            })
            print(f"   Chunk {i+1}/{n_splits} (Period ~8 months): Sharpe={sharpe:.2f}, Return={ret:.2%}")

        self.analyze_results(results, total_pnl)

    def analyze_results(self, results, total_pnl):
        df = pd.DataFrame(results)
        print("\n=== Basket Strategy Report ===")
        print(df.describe())
        
        mean_sharpe = df['sharpe'].mean()
        positive_chunks = len(df[df['sharpe'] > 0])
        
        print(f"\nAvg Sharpe: {mean_sharpe:.2f}")
        print(f"Win Rate (Chunks): {positive_chunks}/{len(df)}")
        
        if mean_sharpe > 0.5 and positive_chunks >= 4:
            print("✅ RESULT: PROMISING (Better than Trend)")
        else:
            print("⚠️ RESULT: ALSO UNSTABLE")

        plt.figure(figsize=(10, 6))
        total_pnl.cumsum().plot(title='Basket Strategy Cumulative Return (4 Years)')
        plt.savefig('basket_validation.png')
        print("📸 Saved to basket_validation.png")

if __name__ == "__main__":
    engine = CPCVBasketEngine()
    engine.load_data()
    engine.run_cpcv()