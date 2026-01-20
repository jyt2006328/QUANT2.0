import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import xgboost as xgb
from models.cross_sectional import CrossSectionalLoader
from strategies.ai_alpha import AIAlphaStrategy
from utils.indicators import calc_ema # 需要计算 EMA

class BacktestEngine:
    def __init__(self, symbols, start_date, end_date):
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        from data.storage import DataManager
        self.db = DataManager()
        self.loader = CrossSectionalLoader(self.db)
        self.ai_strat = AIAlphaStrategy() 

    def run(self):
        print("⏳ Loading Data...")
        full_data = self.loader.load_all_assets(self.symbols, timeframe='1h', limit=2000)
        if full_data.empty: return

        # 构造字典
        data_dict = {}
        for sym in self.symbols:
            try:
                df = full_data.xs(sym, level='symbol').sort_index()
                data_dict[sym] = df
            except: continue
        
        all_times = full_data.index.get_level_values('date').unique().sort_values()
        timesteps = all_times[(all_times >= self.start_date) & (all_times <= self.end_date)]
        
        print(f"🚀 Backtest: AI + QingYun Filter + Maker Fees")
        
        cumulative_returns = {'Strat': 0.0}
        equity_curve = []
        
        # 缓存上一小时的持仓，用于计算换手率（这里简化，假设每次都换）
        
        for current_time in tqdm(timesteps[:-1]):
            try:
                idx = all_times.get_loc(current_time)
                next_time = all_times[idx + 1]
            except: continue

            # --- 1. 计算青云 Bias (宏观方向) ---
            # 简单复现 QingYun 逻辑: 看 BTC 的 EMA 排列
            # 假设 BTC 是列表第一个
            anchor_sym = self.symbols[0] 
            if anchor_sym in data_dict:
                df_anchor = data_dict[anchor_sym]
                # 截取到当前时间
                hist = df_anchor.loc[:current_time].tail(100)
                if len(hist) > 99:
                    c = hist['close']
                    e24 = calc_ema(c, 24).iloc[-1]
                    e99 = calc_ema(c, 99).iloc[-1]
                    curr_p = c.iloc[-1]
                    
                    market_bias = 0
                    if curr_p > e24 and e24 > e99: market_bias = 1  # 多头市场
                    elif curr_p < e24 and e24 < e99: market_bias = -1 # 空头市场
                else:
                    market_bias = 0
            else:
                market_bias = 0

            # --- 2. AI 选币 ---
            longs = []; shorts = []
            try:
                current_slice = full_data.xs(current_time, level='date')
                if not current_slice.empty:
                    # 索引修复
                    if 'symbol' in current_slice.columns:
                        current_slice = current_slice.set_index('symbol')
                    
                    features = self.ai_strat.preprocess_live(current_slice)
                    if not features.empty:
                        # 索引再次检查
                        if isinstance(features.index, pd.RangeIndex) and len(features) == len(current_slice):
                             features.index = current_slice.index

                        dtest = xgb.DMatrix(features)
                        scores = self.ai_strat.model.predict(dtest)
                        score_series = pd.Series(scores, index=features.index).sort_values(ascending=False)
                        
                        # [CRITICAL] 结合青云 Bias 进行过滤
                        if market_bias == 1:
                            # 牛市：只做多 Top 2，不做空
                            longs = score_series.head(2).index.tolist()
                            shorts = [] 
                        elif market_bias == -1:
                            # 熊市：只做空 Bottom 2，不做多
                            longs = []
                            shorts = score_series.tail(2).index.tolist()
                        else:
                            # 震荡：不做，或者多空都做（这里选择空仓观望，稳健为主）
                            longs = []
                            shorts = []
                            # 或者选择原来的多空对冲:
                            # longs = score_series.head(1).index.tolist()
                            # shorts = score_series.tail(1).index.tolist()

            except Exception: pass

            # --- 3. 结算 ---
            pnl_hour = 0.0
            
            # 模拟 Maker 费率 (0.02%)
            fee = 0.0002 
            # 单边仓位权重 (如果只做一边，可以加大仓位到 0.5 或 1.0)
            weight = 0.5 
            
            for sym in self.symbols:
                if sym not in data_dict: continue
                df = data_dict[sym]
                if current_time not in df.index or next_time not in df.index: continue
                
                p0 = df.loc[current_time, 'close']
                p1 = df.loc[next_time, 'close']
                ret = (p1 - p0) / p0
                
                if sym in longs: 
                    pnl_hour += (ret - fee) * weight
                elif sym in shorts: 
                    pnl_hour -= (ret + fee) * weight
            
            cumulative_returns['Strat'] += pnl_hour
            equity_curve.append({'time': next_time, 'PnL': cumulative_returns['Strat']})

        # 输出
        if equity_curve:
            res_df = pd.DataFrame(equity_curve).set_index('time')
            print(f"\n💰 Total Return: {res_df['PnL'].iloc[-1]*100:.2f}%")
            
            # 画图
            try:
                plt.figure(figsize=(10, 5))
                plt.plot(res_df.index, res_df['PnL'], label='AI + QingYun Filter')
                plt.title(f'Backtest Result (Bias Filtered)\nFinal: {res_df["PnL"].iloc[-1]*100:.2f}%')
                plt.grid(True); plt.legend()
                plt.savefig('backtest_filtered.png')
                print("📈 Chart saved.")
            except: pass

if __name__ == "__main__":
    SYMBOLS = [
        'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
        'AVAX/USDT:USDT', 'DOGE/USDT:USDT', 'XRP/USDT:USDT',
        'BNB/USDT:USDT', 'AAVE/USDT:USDT'
    ]
    # 还是测这段时间
    engine = BacktestEngine(SYMBOLS, start_date='2025-11-20', end_date='2025-12-02')
    engine.run()