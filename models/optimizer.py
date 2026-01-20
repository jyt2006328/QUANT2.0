import cvxpy as cp
import pandas as pd
import numpy as np

class PortfolioOptimizer:
    """
    [Optimizer] 基于 cvxpy 的均值-方差优化器
    目标: Max(w @ mu - lambda * risk - gamma * cost)
    """
    def __init__(self, lambda_risk=1.0, gamma_turnover=0.001):
        self.lambda_risk = lambda_risk # 风险厌恶系数
        self.gamma_turnover = gamma_turnover # 交易成本系数
        
    def optimize(self, alpha_vector, cov_matrix, current_weights=None, max_leverage=5.0):
        """
        alpha_vector: pd.Series (预期收益)
        cov_matrix: np.array (协方差矩阵)
        current_weights: pd.Series (当前持仓)
        """
        assets = alpha_vector.index
        n = len(assets)
        if n == 0: return pd.Series()
        
        # 准备数据
        mu = alpha_vector.values
        Sigma = cov_matrix
        
        # 定义优化变量
        w = cp.Variable(n)
        
        # 风险项: w.T * Sigma * w
        risk = cp.quad_form(w, Sigma)
        
        # 目标函数
        objective_expr = mu @ w - self.lambda_risk * risk
        
        # 交易成本惩罚 (L1 Norm of change)
        if current_weights is not None:
            # 强制类型转换，防止 Pandas Warning
            w_prev = current_weights.reindex(assets).fillna(0.0).astype(float).values
            turnover = cp.norm(w - w_prev, 1)
            objective_expr -= self.gamma_turnover * turnover
            
        objective = cp.Maximize(objective_expr)
        
        # 约束条件
        constraints = [
            cp.norm(w, 1) <= max_leverage # 总杠杆限制
        ]
        
        try:
            prob = cp.Problem(objective, constraints)
            prob.solve(solver=cp.OSQP) # 使用 OSQP 求解器
            
            if w.value is None:
                return None # 求解失败
                
            w_opt = pd.Series(w.value, index=assets)
            # 清洗微小权重 (小于 1bp 的视为 0)
            w_opt[w_opt.abs() < 1e-4] = 0.0 
            return w_opt
            
        except Exception as e:
            print(f"⚠️ Optimization Error: {e}")
            return None