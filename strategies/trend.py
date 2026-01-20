import pandas as pd
from utils.indicators import calc_ema, calc_adx
import talib

class TrendFilter:
    """
    [Trend Strategy V2] 严格趋势跟踪
    改进: ADX 门槛提高 + RSI 动量确认
    """
    def __init__(self, ema_period=200, base_adx_threshold=30): # [Strict] 25 -> 30
        self.ema_period = ema_period
        self.base_adx_threshold = base_adx_threshold
        
    def filter_signals(self, weights, prices_data, macro_score=0):
        filtered_weights = weights.copy()
        
        # 宏观调整: 宏观好时稍微放宽，宏观差时更严
        current_adx_threshold = self.base_adx_threshold - (macro_score * 5)
        
        # 准备数据
        closes = pd.DataFrame({s: df['c'] for s, df in prices_data.items()}).ffill()
        ema = calc_ema(closes, self.ema_period)
        
        for asset, w in weights.items():
            if w == 0 or asset not in prices_data: continue
            
            df = prices_data[asset]
            c = df['c']
            current_price = c.iloc[-1]
            current_ema = ema[asset].iloc[-1]
            
            # [Filter 1] EMA 方向过滤
            if w > 0 and current_price < current_ema:
                filtered_weights[asset] = 0.0; continue
            if w < 0 and current_price > current_ema:
                filtered_weights[asset] = 0.0; continue

            # [Filter 2] ADX 强度过滤
            adx_series = calc_adx(df['h'], df['l'], df['c'])
            if adx_series.iloc[-1] < current_adx_threshold:
                filtered_weights[asset] = 0.0; continue
                
            # [Filter 3] RSI 动量确认 (新增 Strict 逻辑)
            # 防止在趋势末端接盘
            rsi = talib.RSI(c, timeperiod=14).iloc[-1]
            if w > 0 and rsi < 50: # 做多要求 RSI 处于强势区
                filtered_weights[asset] = 0.0; continue
            if w < 0 and rsi > 50: # 做空要求 RSI 处于弱势区
                filtered_weights[asset] = 0.0; continue
                
        return filtered_weights

class VolatilityTrailingStop:
    """
    [Risk Manager] 双向移动止损 (兼容版)
    修复: 类型比较错误 (String vs Int)
    """
    def __init__(self, multiplier=3.0):
        self.multiplier = multiplier
        self.hwm = {} # High Water Mark
        self.lwm = {} # Low Water Mark

    def update_and_check(self, sym, price, vol, side):
        """
        side: 支持 'long'/'short' 或 1/-1
        返回: True (触发止损), False (安全)
        """
        # 1. 统一类型判断逻辑
        is_long = False
        is_short = False
        
        # 安全的类型转换与判断
        try:
            side_str = str(side).lower()
            if side_str == 'long': is_long = True
            elif side_str == 'short': is_short = True
            elif isinstance(side, (int, float)):
                if side > 0: is_long = True
                elif side < 0: is_short = True
        except:
            pass # 无法识别类型，视为无持仓

        # 2. 如果无持仓，清理状态
        if not is_long and not is_short:
            if sym in self.hwm: self.hwm.pop(sym)
            if sym in self.lwm: self.lwm.pop(sym)
            return False

        # 3. 多单逻辑 (Long)
        if is_long:
            if sym in self.lwm: self.lwm.pop(sym)
            
            if sym not in self.hwm:
                self.hwm[sym] = price
                return False
            
            if price > self.hwm[sym]:
                self.hwm[sym] = price
            
            stop_price = self.hwm[sym] - (vol * self.multiplier)
            if price < stop_price:
                print(f"🛡️ [STOP LONG] {sym} Price {price:.4f} < Stop {stop_price:.4f} (DD)")
                self.hwm.pop(sym)
                return True

        # 4. 空单逻辑 (Short)
        elif is_short:
            if sym in self.hwm: self.hwm.pop(sym)
            
            if sym not in self.lwm:
                self.lwm[sym] = price
                return False
            
            if price < self.lwm[sym]:
                self.lwm[sym] = price
                
            stop_price = self.lwm[sym] + (vol * self.multiplier)
            if price > stop_price:
                print(f"🛡️ [STOP SHORT] {sym} Price {price:.4f} > Stop {stop_price:.4f} (DD)")
                self.lwm.pop(sym)
                return True
                
        return False

    def export_state(self):
        return {'hwm': self.hwm, 'lwm': self.lwm}
        
    def import_state(self, data):
        self.hwm = {k: float(v) for k, v in data.get('hwm', {}).items()}
        self.lwm = {k: float(v) for k, v in data.get('lwm', {}).items()}