import numpy as np
import pandas as pd
from sklearn.covariance import LedoitWolf

class FactorRiskModel:
    """
    [基础设施] 因子风险模型
    核心功能: 使用 Ledoit-Wolf 收缩估计计算协方差矩阵 Σ，评估组合真实波动率。
    """
    def __init__(self, lookback=120):
        self.lookback = lookback

    def compute_portfolio_risk(self, weights, returns_df):
        """计算组合年化波动率"""
        if len(returns_df) < self.lookback:
            # 数据不足时的降级处理
            return weights.abs().sum() * 0.05 * np.sqrt(24*365)
        
        active = weights.index.intersection(returns_df.columns)
        if len(active) == 0:
            return 0.0
            
        # 使用 Ledoit-Wolf 估计协方差
        try:
            sigma = LedoitWolf().fit(returns_df[active].iloc[-self.lookback:].values).covariance_
        except:
            # 降级为样本协方差
            sigma = np.cov(returns_df[active].iloc[-self.lookback:].values, rowvar=False)
            
        # 计算组合方差: σ^2 = w^T * Σ * w
        var = weights[active].values.T @ sigma @ weights[active].values
        return np.sqrt(var) * np.sqrt(24*365)
    
    def get_covariance_matrix(self, returns_df, assets):
        """直接返回协方差矩阵 (供优化器使用)"""
        recent = returns_df[assets].iloc[-self.lookback:]
        if len(recent) < 10:
            return np.eye(len(assets)) * 1e-4
        try:
            return LedoitWolf().fit(recent.values).covariance_
        except:
            return np.cov(recent.values, rowvar=False) + np.eye(len(assets)) * 1e-6