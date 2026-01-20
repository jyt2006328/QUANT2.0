import pandas as pd
import numpy as np

class GeneticAlphaModel:
    """
    [Alpha] 遗传规划挖掘出的超级因子
    来源: research/gp_alphas.csv (Top IC Factors)
    """
    def __init__(self):
        pass

    def compute_alphas(self, df):
        """
        输入: DataFrame (必须包含 open, high, low, close)
        输出: DataFrame (包含 gp_f1, gp_f2...)
        """
        # 提取数据并转换为 numpy 以便快速计算
        o = df['o']
        h = df['h']
        l = df['l']
        c = df['c']
        
        # 预计算常用中间变量，防止除以零
        # eps = 1e-9
        # diff_ch = c - h (Close - High, 通常<=0)
        diff_ch = c - h
        diff_ch = diff_ch.replace(0, -0.0001) # 防止分母为0
        
        # === Factor 7 (IC -0.0455) ===
        # 原始公式: div(div(div(sub(X3, X1), div(X3, sub(X3, X1))), X3), div(X3, sub(X3, X1)))
        # 简化推导:
        # A = sub(X3, X1) = c - h
        # B = div(X3, A) = c / (c - h)
        # C = div(A, B) = (c - h)^2 / c
        # D = div(X3, A) = c / (c - h) (同 B)
        # E = div(C, X3) = (c - h)^2 / c^2
        # Final = div(E, D) = ((c - h)^2 / c^2) / (c / (c - h)) 
        #       = (c - h)^3 / c^3
        # 物理含义: (Close - High) / Close 的三次方。
        # 这是一个衡量 "收盘价距离最高价多远" 的强反转因子。
        
        gp_f7 = ((c - h) / c) ** 3
        
        # === Factor 10 (IC -0.0452) ===
        # 原始: div(div(div(log(div(X1, X3)), div(X3, sub(X3, X1))), div(X3, sub(X3, X1))), div(X3, sub(X3, X1)))
        # 简化: log(h/c) * ((c-h)/c)^3
        # 物理含义: 也是衡量最高价回撤幅度的，加了对数增强。
        
        gp_f10 = np.log(h / c) * ((c - h) / c) ** 3
        
        return pd.DataFrame({
            'gp_rev_1': gp_f7,
            'gp_rev_2': gp_f10
        }, index=df.index)