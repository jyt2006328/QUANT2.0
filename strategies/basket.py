import pandas as pd
import numpy as np

class BasketTradingStrategy:
    """
    [Basket Strategy V2] 严格篮子对冲
    改进: 
    1. Z-Score 门槛提高 (2.0->2.8)
    2. 剔除更多极端值 (filter_top_n=2)
    3. 保留风险平价加权
    """
    def __init__(self, lookback=24, entry_threshold=2.8, filter_top_n=2):
        self.lookback = lookback
        self.entry_threshold = entry_threshold
        self.filter_top_n = filter_top_n 

    def compute_signals(self, prices_df, basket_config):
        weights = pd.Series(0.0, index=prices_df.columns)
        main_asset = basket_config.get('main')
        alts = basket_config.get('alts', [])
        
        available_alts = [a for a in alts if a in prices_df.columns]
        if main_asset not in prices_df or not available_alts: 
            return weights
            
        # 计算平均 Z-Score
        z_scores = []
        for alt in available_alts:
            ratio = prices_df[main_asset] / prices_df[alt]
            z = (ratio - ratio.rolling(self.lookback).mean()) / (ratio.rolling(self.lookback).std() + 1e-9)
            z_scores.append(z.iloc[-1])
            
        avg_z = np.mean(z_scores)
        
        # 辅助数据
        returns_24h = prices_df[available_alts].pct_change(24).iloc[-1]
        # 计算波动率用于风险平价加权
        volatility = prices_df[available_alts].pct_change().rolling(30).std().iloc[-1]
        
        # 信号生成
        if avg_z > self.entry_threshold:
            # BTC 吸血 (Long BTC, Short Alts)
            print(f"🧺 [BASKET] BTC Dominance Z={avg_z:.2f}. Executing.")
            weights[main_asset] += 0.5
            
            # 剔除涨幅最大的 N 个山寨（可能是妖币，不空它）
            sorted_alts = returns_24h.sort_values(ascending=False).index.tolist()
            target_alts = sorted_alts[self.filter_top_n:] 
            if not target_alts: 
                target_alts = available_alts
            
            # 使用风险平价加权分配空头仓位
            self._allocate_risk_parity(weights, target_alts, volatility, target_sum=-0.5)

        elif avg_z < -self.entry_threshold:
            # 山寨季 (Short BTC, Long Alts)
            print(f"🧺 [BASKET] Alt Season Z={avg_z:.2f}. Executing.")
            weights[main_asset] -= 0.5
            
            # 剔除跌幅最大的 N 个山寨（可能是归零币，不买它）
            sorted_alts = returns_24h.sort_values(ascending=True).index.tolist()
            target_alts = sorted_alts[self.filter_top_n:]
            if not target_alts: 
                target_alts = available_alts
            
            # 使用风险平价加权分配多头仓位
            self._allocate_risk_parity(weights, target_alts, volatility, target_sum=0.5)
                
        return weights

    def _allocate_risk_parity(self, weights, assets, volatility_series, target_sum):
        """
        根据波动率倒数分配权重 (风险平价)
        波动率越小的资产分配更多权重
        """
        if not assets:
            return
            
        inv_vol = 1.0 / (volatility_series[assets] + 1e-9)
        total_inv_vol = inv_vol.sum()
        
        if total_inv_vol == 0:
            # 如果算不出波动率，退化为等权
            weight_per_asset = target_sum / len(assets)
            for asset in assets:
                weights[asset] += weight_per_asset
        else:
            # 波动越小，分到的权重(绝对值)越大
            allocated_weights = (inv_vol / total_inv_vol) * target_sum
            for asset in assets:
                weights[asset] += allocated_weights[asset]