import pandas as pd

class PairTradingStrategy:
    """
    [Pair Strategy V2] 严格配对交易
    改进: Z-Score 门槛大幅提高，只抓极值
    """
    def __init__(self, lookback=40, entry_threshold=3.0): # [Strict] 2.0 -> 3.0
        self.lookback = lookback
        self.entry_threshold = entry_threshold
        
    def compute_signals(self, prices_df, pairs_config):
        weights = pd.Series(0.0, index=prices_df.columns)
        
        for leg_a, leg_b in pairs_config:
            if leg_a not in prices_df or leg_b not in prices_df: continue
                
            # Ratio = A / B
            ratio = prices_df[leg_a] / prices_df[leg_b]
            mean = ratio.rolling(self.lookback).mean()
            std = ratio.rolling(self.lookback).std()
            
            z_score = (ratio - mean) / (std + 1e-9)
            current_z = z_score.iloc[-1]
            
            signal_a, signal_b = 0.0, 0.0
            
            # 只有偏离度达到 3个标准差才开仓
            if current_z > self.entry_threshold:
                # A 极度贵 -> 空 A 多 B
                signal_a = -0.5; signal_b = 0.5
                print(f"👯 [PAIR] {leg_a}/{leg_b} Z={current_z:.2f} > 3.0! SHORT A/LONG B")
                
            elif current_z < -self.entry_threshold:
                # A 极度便宜 -> 多 A 空 B
                signal_a = 0.5; signal_b = -0.5
                print(f"👯 [PAIR] {leg_a}/{leg_b} Z={current_z:.2f} < -3.0! LONG A/SHORT B")
            
            weights[leg_a] += signal_a
            weights[leg_b] += signal_b
            
        # 归一化
        total = weights.abs().sum()
        if total > 0: return weights / total
        return weights