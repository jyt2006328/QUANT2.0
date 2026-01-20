import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from strategies.sniper import SniperStrategyV5
from models.risk import FactorRiskModel
from data.storage import DataManager
from utils.indicators import calc_ema, calc_adx, calc_atr
import talib

# 模拟配置 (在此调整权重以测试不同组合)
ALLOCATION = {
    "trend_weight": 0.5,   # 重点测 Trend
    "basket_weight": 0.5,  # 重点测 Basket
    "sniper_weight": 0.0,  # 暂时关掉 Sniper 排除干扰
    "ai_weight": 0.0
}
LEVERAGE_LIMIT = 5.0

class FullBacktest:
    def __init__(self, symbols, start_date, end_date):
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.db = DataManager()
        self.sniper_strat = SniperStrategyV5() # V5

    def load_data(self):
        print("⏳ Loading Data (Keeping short column names)...")
        self.data_5m = {}
        self.data_1h = {}
        
        for sym in tqdm(self.symbols):
            # 5m
            d5 = self.db.load_dataframe(sym, '5m', limit=10000)
            if not d5.empty:
                d5 = d5[~d5.index.duplicated()].sort_index()
                # Sniper V5 需要全称? 不，Sniper 内部用的是简写 c/v
                self.data_5m[sym] = d5
                
            # 1h
            d1 = self.db.load_dataframe(sym, '1h', limit=2000)
            if not d1.empty:
                d1 = d1[~d1.index.duplicated()].sort_index()
                self.data_1h[sym] = d1
                
        self.timesteps = None
        if self.symbols[0] in self.data_1h:
            idx = self.data_1h[self.symbols[0]].index
            self.timesteps = idx[(idx >= self.start_date) & (idx <= self.end_date)]

    def run(self):
        if self.timesteps is None: return
        print(f"🚀 Running Full Backtest on {len(self.timesteps)} hours...")
        
        cum_pnl = {'Trend': 0.0, 'Basket': 0.0, 'Total': 0.0}
        history = []
        equity = 100.0
        last_weights = pd.Series(0.0, index=self.symbols)
        
        for t_idx in tqdm(range(len(self.timesteps)-1)):
            curr_time = self.timesteps[t_idx]
            next_time = self.timesteps[t_idx+1]
            
            # --- 1. 数据切片 ---
            # Trend/Basket 用 1h 数据
            lf_data = {}
            for s in self.symbols:
                if s in self.data_1h:
                    # 必须取足够长的数据计算 EMA/ADX
                    lf_data[s] = self.data_1h[s].loc[:curr_time].tail(250) 
            
            # --- 2. 策略计算 ---
            tr_w = pd.Series(0.0, index=self.symbols)
            bk_w = pd.Series(0.0, index=self.symbols)
            
            if lf_data:
                # 构造 Close 矩阵
                closes = pd.DataFrame({s: df['c'] for s, df in lf_data.items()}).ffill()
                
                # === A. Trend Strategy (逻辑复刻) ===
                if ALLOCATION['trend_weight'] > 0:
                    # 计算指标
                    ema24 = calc_ema(closes, 24)
                    ema99 = calc_ema(closes, 99)
                    
                    for sym in self.symbols:
                        if sym not in lf_data: continue
                        df = lf_data[sym]
                        if len(df) < 100: continue
                        
                        price = df['c'].iloc[-1]
                        e24 = ema24[sym].iloc[-1]
                        e99 = ema99[sym].iloc[-1]
                        
                        # ADX 过滤
                        adx = talib.ADX(df['h'], df['l'], df['c'], timeperiod=14)[-1]
                        
                        sig = 0.0
                        # 简单的趋势逻辑: 价格 > EMA24 > EMA99 且 ADX > 25
                        if price > e24 and e24 > e99 and adx > 25:
                            sig = 1.0
                        elif price < e24 and e24 < e99 and adx > 25:
                            sig = -1.0
                            
                        tr_w[sym] = sig * ALLOCATION['trend_weight']

                # === B. Basket Strategy (逻辑复刻) ===
                if ALLOCATION['basket_weight'] > 0:
                    # 假设 BTC 是锚点
                    main_sym = self.symbols[0] # BTC
                    if main_sym in closes:
                        z_scores = []
                        for sym in self.symbols:
                            if sym == main_sym: continue
                            # Ratio = BTC / Alt
                            ratio = closes[main_sym] / closes[sym]
                            z = (ratio - ratio.rolling(24).mean()) / (ratio.rolling(24).std() + 1e-9)
                            z_scores.append(z.iloc[-1])
                        
                        if z_scores:
                            avg_z = np.mean(z_scores)
                            # 阈值 2.0
                            if avg_z > 2.0: # BTC 强 -> 多 BTC 空山寨
                                bk_w[main_sym] = 0.5
                                # 简单平均分给山寨
                                for sym in self.symbols[1:]: bk_w[sym] = -0.5 / (len(self.symbols)-1)
                            elif avg_z < -2.0: # BTC 弱 -> 空 BTC 多山寨
                                bk_w[main_sym] = -0.5
                                for sym in self.symbols[1:]: bk_w[sym] = 0.5 / (len(self.symbols)-1)
                            
                            bk_w = bk_w * ALLOCATION['basket_weight']

            # --- 3. 聚合与结算 ---
            final_w = tr_w.add(bk_w, fill_value=0)
            
            # 杠杆限制
            lev = final_w.abs().sum()
            if lev > LEVERAGE_LIMIT: final_w = final_w * (LEVERAGE_LIMIT / lev)
            
            # 计算 PnL
            hour_pnl_map = {'Trend': 0.0, 'Basket': 0.0, 'Total': 0.0}
            
            for s in self.symbols:
                if s not in self.data_1h: continue
                df = self.data_1h[s]
                if next_time not in df.index: continue
                
                p0 = df.loc[curr_time, 'c']
                p1 = df.loc[next_time, 'c']
                ret = (p1 - p0) / p0
                
                hour_pnl_map['Trend'] += tr_w.get(s, 0.0) * ret
                hour_pnl_map['Basket'] += bk_w.get(s, 0.0) * ret
                hour_pnl_map['Total'] += final_w.get(s, 0.0) * ret
                
            cum_pnl['Trend'] += hour_pnl_map['Trend']
            cum_pnl['Basket'] += hour_pnl_map['Basket']
            
            turnover = (final_w - last_weights).abs().sum()
            fee = turnover * 0.0004
            
            equity *= (1 + hour_pnl_map['Total'] - fee)
            
            history.append({
                'time': next_time,
                'Equity': equity,
                'Trend_Gross': cum_pnl['Trend'] * 100,
                'Basket_Gross': cum_pnl['Basket'] * 100,
                'Fee': fee
            })
            last_weights = final_w

        # 输出
        res_df = pd.DataFrame(history).set_index('time')
        print(f"\n📊 Final Equity: {res_df['Equity'].iloc[-1]:.2f}")
        print(f"Trend Contribution: {res_df['Trend_Gross'].iloc[-1]:.2f}")
        print(f"Basket Contribution: {res_df['Basket_Gross'].iloc[-1]:.2f}")
        
        plt.figure(figsize=(10,6))
        plt.plot(res_df.index, res_df['Equity'], label='Net Equity', linewidth=2, color='black')
        plt.plot(res_df.index, 100 + res_df['Trend_Gross'], label='Trend (Gross)', linestyle='--')
        plt.plot(res_df.index, 100 + res_df['Basket_Gross'], label='Basket (Gross)', linestyle='--')
        plt.legend(); plt.grid(True)
        plt.savefig('trend_basket_backtest.png')
        print("📈 Chart saved.")

if __name__ == "__main__":
    SYMBOLS = [
        'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
        'AVAX/USDT:USDT', 'DOGE/USDT:USDT', 'XRP/USDT:USDT',
        'BNB/USDT:USDT', 'AAVE/USDT:USDT'
    ]
    # 回测最近 300 小时
    engine = FullBacktest(SYMBOLS, start_date='2025-11-20', end_date='2025-12-02')
    engine.load_data()
    engine.run()