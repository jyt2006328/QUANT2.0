import pandas as pd
import numpy as np

def calc_ema(series, span):
    """指数移动平均"""
    return series.ewm(span=span, adjust=False).mean()

def calc_atr(high, low, close, period=14):
    """
    [新增] 计算真实波幅均值 (ATR)
    考虑了跳空缺口，更能反映真实波动率
    """
    # 1. 当前 K 线最高最低差
    tr1 = high - low
    # 2. 当前最高 与 前收盘 的差 (跳空高开风险)
    tr2 = (high - close.shift(1)).abs()
    # 3. 当前最低 与 前收盘 的差 (跳空低开风险)
    tr3 = (low - close.shift(1)).abs()
    
    # 取三者最大值作为 True Range
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # 计算移动平均 (通常使用 SMA 即可，也可以用 Wilder's Smoothing)
    atr = tr.rolling(period).mean()
    
    return atr

def calc_adx(high, low, close, period=14):
    """
    计算 ADX (平均趋向指标)
    衡量趋势强度：>25 代表强趋势，<20 代表震荡
    """
    # 1. True Range (ATR 的基础)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
    # 2. Directional Movement
    up_move = high - high.shift(1)
    down_move = low.shift(1) - low
    
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
    # 3. Smooth
    def wilder_smooth(series, n):
        return series.ewm(alpha=1/n, adjust=False).mean()

    atr = wilder_smooth(tr, period)
    plus_di = 100 * (wilder_smooth(pd.Series(plus_dm, index=high.index), period) / (atr + 1e-9))
    minus_di = 100 * (wilder_smooth(pd.Series(minus_dm, index=high.index), period) / (atr + 1e-9))
    
    # 4. DX & ADX
    dx = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di + 1e-9))
    adx = wilder_smooth(dx, period)
    
    return adx