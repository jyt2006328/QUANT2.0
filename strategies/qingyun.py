import pandas as pd
import numpy as np
from utils.indicators import calc_ema

class QingYunStrategy:
    """
    [青云战法 V18.6] 实时架构版
    修复: 整点延迟问题，实现 Minute-Level 信号触发。
    """
    def __init__(self):
        # 宏观状态缓存 (只在整点更新)
        self.macro_trend = {} 
        # 信号冷却 (防止 14:01 开仓，14:02 又开)
        self.last_signal_time = {}

    def update_macro_trend(self, lf_data_dict):
        """
        [慢速层] 整点调用：计算 1H EMA 结构
        """
        trend_map = {}
        level_map = {}
        
        for sym, df in lf_data_dict.items():
            if len(df) < 100: continue
            
            c = df['c']
            ema24 = calc_ema(c, 24).iloc[-1]
            ema99 = calc_ema(c, 99).iloc[-1]
            
            # 定义趋势: 价格 > EMA24 > EMA99 = 多头
            bias = 0
            if c.iloc[-1] > ema24 and ema24 > ema99:
                bias = 1
            elif c.iloc[-1] < ema24 and ema24 < ema99:
                bias = -1
                
            trend_map[sym] = bias
            level_map[sym] = {'ema24': ema24, 'ema99': ema99}
            
        self.macro_trend = {'bias': trend_map, 'levels': level_map}
        print(f"☁️ [QingYun] Macro Updated. Bullish: {list(trend_map.values()).count(1)}")
        return trend_map

    def compute_realtime_signals(self, current_prices):
        """
        [快速层] 实时调用：检查当前价格是否回踩到位
        """
        signals = pd.Series(0.0, index=current_prices.keys())
        
        if not self.macro_trend: return signals
        
        bias_map = self.macro_trend.get('bias', {})
        level_map = self.macro_trend.get('levels', {})
        
        for sym, price in current_prices.items():
            # 1. 获取宏观方向
            trend = bias_map.get(sym, 0)
            levels = level_map.get(sym)
            if trend == 0 or not levels: continue
            
            # 2. 检查冷却时间 (1小时内不重复对同一个币报警，除非方向变了)
            # last_t = self.last_signal_time.get(sym, 0)
            # if time.time() - last_t < 3600: continue 
            
            ema24 = levels['ema24']
            
            # 3. 核心策略：回踩 EMA24 确认
            # 逻辑：趋势向上，但价格跌到了 EMA24 附近 (0.5% 范围内)
            # 这是一个绝佳的 "Dip Buy" 机会，而不是追高
            
            dist_pct = (price - ema24) / ema24
            
            if trend == 1: # 多头趋势
                # 价格在 EMA24 上方 0% ~ 0.5% 区间，或者 刚刚跌破一点点 (-0.2%)
                if -0.002 < dist_pct < 0.005:
                    signals[sym] = 1.0
                    print(f"⚡ [QingYun Realtime] {sym} Dip Detect! Price {price} near EMA24 {ema24:.2f}")
                    self.last_signal_time[sym] = time.time()
                    
            elif trend == -1: # 空头趋势
                # 价格在 EMA24 下方 0% ~ 0.5% 区间
                if -0.005 < dist_pct < 0.002:
                    signals[sym] = -1.0
                    print(f"⚡ [QingYun Realtime] {sym} Rebound Detect! Price {price} near EMA24 {ema24:.2f}")
                    self.last_signal_time[sym] = time.time()
                    
        return signals