import pandas as pd
import numpy as np
import talib

class FactorFactory:
    """
    [Factor Factory V3] Guru 级因子库 (Phase 24)
    集成 TA-Lib 核心指标库 + Alpha101 风格因子
    新增: 波动率因子, 量价相关性, K线形态因子
    强制集成 Lag Operator 防止未来函数
    """
    def __init__(self, use_lag=True):
        self.use_lag = use_lag

    def lag(self, series, n=1):
        """
        [滞后算子] Lag Operator
        T时刻的因子值只能包含 T-1及以前的信息
        """
        if self.use_lag:
            return series.shift(n)
        return series

    def calculate_factors(self, df):
        """
        生产全量因子
        df: 必须包含 open, high, low, close, volume (全小写)
        """
        if df.empty: return pd.DataFrame()
        
        # 复制数据，避免污染
        data = df.copy()
        
        # 提取基础序列 (转换为 float64 以便 TA-Lib 计算)
        # 注意: TA-Lib 对 float32 支持有时不稳定，建议计算时转 float64
        open_p = data['open'].astype('float64').values
        high   = data['high'].astype('float64').values
        low    = data['low'].astype('float64').values
        close  = data['close'].astype('float64').values
        volume = data['volume'].astype('float64').values
        
        # 同时保留 pandas Series 格式用于新因子计算
        open_series = data['open'].astype('float64')
        high_series = data['high'].astype('float64')
        low_series = data['low'].astype('float64')
        close_series = data['close'].astype('float64')
        volume_series = data['volume'].astype('float64')
        
        # ==========================================
        # 1. 动量类因子 (Momentum Indicators) - V2保留
        # ==========================================
        # RSI: 相对强弱指标
        data['RSI_6']  = self.lag(pd.Series(talib.RSI(close, timeperiod=6), index=df.index))
        data['RSI_12'] = self.lag(pd.Series(talib.RSI(close, timeperiod=12), index=df.index))
        data['RSI_24'] = self.lag(pd.Series(talib.RSI(close, timeperiod=24), index=df.index))
        
        # MOM: 动量
        data['MOM_10'] = self.lag(pd.Series(talib.MOM(close, timeperiod=10), index=df.index))
        
        # ROC: 变动率
        data['ROC_10'] = self.lag(pd.Series(talib.ROC(close, timeperiod=10), index=df.index))
        
        # CCI: 顺势指标
        data['CCI_14'] = self.lag(pd.Series(talib.CCI(high, low, close, timeperiod=14), index=df.index))
        
        # WILLR: 威廉指标
        data['WILLR_14'] = self.lag(pd.Series(talib.WILLR(high, low, close, timeperiod=14), index=df.index))
        
        # CMO: 钱德动量摆动指标
        data['CMO_14'] = self.lag(pd.Series(talib.CMO(close, timeperiod=14), index=df.index))
        
        # MFI: 资金流量指标 (结合了量价)
        data['MFI_14'] = self.lag(pd.Series(talib.MFI(high, low, close, volume, timeperiod=14), index=df.index))
        
        # STOCH_RSI: 随机相对强弱
        fastk, fastd = talib.STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
        data['STOCHRSI_K'] = self.lag(pd.Series(fastk, index=df.index))
        data['STOCHRSI_D'] = self.lag(pd.Series(fastd, index=df.index))

        # ==========================================
        # 2. 趋势类因子 (Trend Indicators) - V2保留
        # ==========================================
        # MACD: 指数平滑异同移动平均线
        macd, macdsignal, macdhist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        data['MACD'] = self.lag(pd.Series(macd, index=df.index))
        data['MACD_SIG'] = self.lag(pd.Series(macdsignal, index=df.index))
        data['MACD_HIST'] = self.lag(pd.Series(macdhist, index=df.index))
        
        # ADX: 平均趋向指数 (判断趋势强度)
        data['ADX_14'] = self.lag(pd.Series(talib.ADX(high, low, close, timeperiod=14), index=df.index))
        data['PLUS_DI'] = self.lag(pd.Series(talib.PLUS_DI(high, low, close, timeperiod=14), index=df.index))
        data['MINUS_DI'] = self.lag(pd.Series(talib.MINUS_DI(high, low, close, timeperiod=14), index=df.index))
        
        # AROON: 阿隆指标
        aroondown, aroonup = talib.AROON(high, low, timeperiod=14)
        data['AROON_DOWN'] = self.lag(pd.Series(aroondown, index=df.index))
        data['AROON_UP']   = self.lag(pd.Series(aroonup, index=df.index))
        data['AROON_OSC']  = self.lag(pd.Series(talib.AROONOSC(high, low, timeperiod=14), index=df.index))
        
        # TRIX: 三重指数平滑平均线
        data['TRIX_30'] = self.lag(pd.Series(talib.TRIX(close, timeperiod=30), index=df.index))

        # ==========================================
        # 3. 波动率因子 (Volatility Indicators) - V2保留
        # ==========================================
        # ATR: 真实波幅
        data['ATR_14'] = self.lag(pd.Series(talib.ATR(high, low, close, timeperiod=14), index=df.index))
        data['NATR_14'] = self.lag(pd.Series(talib.NATR(high, low, close, timeperiod=14), index=df.index)) # 归一化ATR
        data['TRANGE']  = self.lag(pd.Series(talib.TRANGE(high, low, close), index=df.index))
        
        # BBANDS: 布林带宽度
        u, m, l = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
        u_s = pd.Series(u, index=df.index)
        m_s = pd.Series(m, index=df.index)
        l_s = pd.Series(l, index=df.index)
        
        # 1. Band Width (带宽)
        data['BB_WIDTH'] = self.lag((u_s - l_s) / m_s)
        # 2. %B (价格在布林带的位置)
        data['BB_PCT']   = self.lag((pd.Series(close, index=df.index) - l_s) / (u_s - l_s))

        # ==========================================
        # 4. 成交量因子 (Volume Indicators) - V2保留
        # ==========================================
        # OBV: 能量潮
        data['OBV'] = self.lag(pd.Series(talib.OBV(close, volume), index=df.index))
        
        # AD: 累积/派发线
        data['AD'] = self.lag(pd.Series(talib.AD(high, low, close, volume), index=df.index))
        
        # ADOSC: 柴金摆动指标
        data['ADOSC'] = self.lag(pd.Series(talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10), index=df.index))

        # ==========================================
        # 5. Guru 特色 / 统计类因子 - V2保留
        # ==========================================
        # KDJ (TA-Lib 没有直接的 KDJ，需要手搓，或者用 STOCH 替代)
        slowk, slowd = talib.STOCH(high, low, close, fastk_period=9, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        data['KDJ_K'] = self.lag(pd.Series(slowk, index=df.index))
        data['KDJ_D'] = self.lag(pd.Series(slowd, index=df.index))
        data['KDJ_J'] = self.lag(3 * pd.Series(slowk, index=df.index) - 2 * pd.Series(slowd, index=df.index))
        
        # Return Lag (动量回归)
        data['RET_1H'] = self.lag(data['close'].pct_change(1))
        data['RET_3H'] = self.lag(data['close'].pct_change(3))
        data['RET_6H'] = self.lag(data['close'].pct_change(6))
        data['RET_12H'] = self.lag(data['close'].pct_change(12))
        data['RET_24H'] = self.lag(data['close'].pct_change(24))
        
        # Volatility Ratio (短期波动/长期波动)
        std_5 = data['close'].pct_change().rolling(5).std()
        std_20 = data['close'].pct_change().rolling(20).std()
        data['VOL_RATIO'] = self.lag(std_5 / (std_20 + 1e-9))

        # ==========================================
        # 6. V3 新增 Alpha101 风格因子
        # ==========================================
        
        # [Alpha] 动量反转组合
        # (Close - Open) / (High - Low) -> K线实体力度
        data['ALPHA_BODY'] = self.lag((close_series - open_series) / ((high_series - low_series) + 1e-9))
        
        # [Alpha] 高低点相对位置
        # (High - Close) / (High - Low) -> 上影线力度 (抛压)
        data['ALPHA_SHADOW_UP'] = self.lag((high_series - close_series) / ((high_series - low_series) + 1e-9))
        
        # [Alpha] 下影线力度 (支撑)
        data['ALPHA_SHADOW_DOWN'] = self.lag((close_series - low_series) / ((high_series - low_series) + 1e-9))
        
        # [Alpha] 量价相关性 (Volume-Price Correlation)
        # 过去 10 根 K 线，价格和成交量的相关系数
        data['CORR_PV_10'] = self.lag(close_series.rolling(10).corr(volume_series))
        
        # [Alpha] 乖离率 (Bias)
        # 价格偏离 MA20 的程度
        ma20 = talib.SMA(close, 20)
        data['BIAS_20'] = self.lag((close_series - pd.Series(ma20, index=df.index)) / (pd.Series(ma20, index=df.index) + 1e-9))
        
        # [Alpha] 换手率代理 (Turnover Proxy)
        # Amihud Illiquidity 变体: 收益率绝对值 / 成交金额
        ret_abs = close_series.pct_change().abs()
        dollar_vol = volume_series * close_series
        data['ILLIQ'] = self.lag(ret_abs / (dollar_vol + 1e-9))

        # [Alpha] Log Return (对数收益率，统计特性更好)
        data['LOG_RET'] = self.lag(np.log(close_series / close_series.shift(1)))

        # [Alpha] 价格加速度 (Price Acceleration)
        # 二阶差分，捕捉价格变化的速度变化
        data['PRICE_ACCEL'] = self.lag(close_series.diff().diff())

        # [Alpha] 成交量异常 (Volume Anomaly)
        # 当前成交量与过去20期平均成交量的比率
        vol_ma20 = volume_series.rolling(20).mean()
        data['VOLUME_ANOMALY'] = self.lag(volume_series / (vol_ma20 + 1e-9))

        # === 清洗 NaN ===
        # 计算这么多指标，前面的行肯定会有大量 NaN (取决于最大的 timeperiod)
        # 通常前 30-50 行都是无效的，交给下游处理，这里不 drop，保持长度一致
        
        return data

    def get_feature_names(self):
        """返回所有因子列名"""
        base_features = [
            # 动量类
            'RSI_6', 'RSI_12', 'RSI_24', 'MOM_10', 'ROC_10', 'CCI_14', 
            'WILLR_14', 'CMO_14', 'MFI_14', 'STOCHRSI_K', 'STOCHRSI_D',
            # 趋势类
            'MACD', 'MACD_SIG', 'MACD_HIST', 'ADX_14', 'PLUS_DI', 'MINUS_DI',
            'AROON_DOWN', 'AROON_UP', 'AROON_OSC', 'TRIX_30',
            # 波动率类
            'ATR_14', 'NATR_14', 'TRANGE', 'BB_WIDTH', 'BB_PCT',
            # 成交量类
            'OBV', 'AD', 'ADOSC',
            # 特色因子
            'KDJ_K', 'KDJ_D', 'KDJ_J', 'RET_1H', 'RET_3H', 'RET_6H', 
            'RET_12H', 'RET_24H', 'VOL_RATIO'
        ]
        
        alpha_features = [
            # V3 新增 Alpha 因子
            'ALPHA_BODY', 'ALPHA_SHADOW_UP', 'ALPHA_SHADOW_DOWN', 
            'CORR_PV_10', 'BIAS_20', 'ILLIQ', 'LOG_RET', 
            'PRICE_ACCEL', 'VOLUME_ANOMALY'
        ]
        
        return base_features + alpha_features