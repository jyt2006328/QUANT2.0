import pandas as pd
import numpy as np
from datetime import datetime
from utils.indicators import calc_ema, calc_adx, calc_atr # [Fix] 导入 ATR

class ResonanceStrategy:
    """
    [Resonance V2] 改进版共振策略 (ADX + Volume + ATR Stop)
    """
    def __init__(self):
        self.adx_threshold = 25.0 
        
    def _analyze_trend(self, df):
        if len(df) < 70: return 0
        
        c = df['c']; h = df['h']; l = df['l']; v = df['v']
        
        e5 = calc_ema(c, 5).iloc[-1]
        e13 = calc_ema(c, 13).iloc[-1]
        e21 = calc_ema(c, 21).iloc[-1]
        e60 = calc_ema(c, 60).iloc[-1]
        price = c.iloc[-1]
        
        # [Filter 1] 均线纠缠过滤
        if abs(e21 - e60) / price < 0.003: return 0
            
        # [Filter 2] ADX 趋势强度过滤
        adx_series = calc_adx(h, l, c, period=14)
        if adx_series.iloc[-1] < self.adx_threshold: return 0
            
        # [Filter 3] 成交量过滤
        vol_ma20 = v.rolling(20).mean().iloc[-1]
        if v.iloc[-1] < vol_ma20: return 0

        # 信号判定
        if e5 > e13 and e13 > e21 and e21 > e60 and price > e5: return 1
        if e5 < e13 and e13 < e21 and e21 < e60 and price < e5: return -1
            
        return 0

    def check_signals(self, df_30m, df_5m):
        trend_30m = self._analyze_trend(df_30m)
        if trend_30m == 0: return 0
        
        trend_5m = self._analyze_trend(df_5m)
        
        if trend_5m == trend_30m:
            return trend_30m
            
        return 0

class ResonanceManager:
    """
    [Resonance V2] 状态管理 (修复僵尸复活 Bug)
    """
    def __init__(self, leverage_scale=1.0):
        self.positions = {}
        self.leverage_scale = leverage_scale

    def export_state(self):
        data = {}
        for sym, pos in self.positions.items():
            data[sym] = pos.copy()
            data[sym]['entry_time'] = pos['entry_time'].isoformat()
        return data

    def import_state(self, data):
        if not isinstance(data, dict): return
        for sym, info in data.items():
            try:
                info['entry_time'] = datetime.fromisoformat(info['entry_time'])
                self.positions[sym] = info
            except: pass

    def check_signals(self, data_pack, current_positions=None):
        strategy = ResonanceStrategy()
        weights = {}
        active = False
        
        # [新增] 记录本轮被处决的幽灵，防止它当场复活
        ghosts_this_round = []

        # === 1. 幽灵清洗 (Ghost Buster) ===
        if current_positions is not None:
            for sym, pos in self.positions.items():
                real_val = abs(current_positions.get(sym, 0.0))
                
                if real_val < 5.0: 
                    entry_time = pos['entry_time']
                    elapsed_min = (datetime.now() - entry_time).total_seconds() / 60
                    
                    if elapsed_min > 3:
                        print(f"👻 [Resonance] Detected GHOST position: {sym}. Cleaning up.")
                        ghosts_this_round.append(sym)
            
            # 统一清理
            for g in ghosts_this_round:
                self.positions.pop(g)

        # === 2. 管理现有持仓 ===
        assets_to_close = []
        for sym, pos in self.positions.items():
            if sym not in data_pack: continue
            if data_pack[sym]['5m'].empty: continue 

            df = data_pack[sym]['5m']
            curr_price = df['c'].iloc[-1]
            sl_price = pos['stop_loss']
            entry_price = pos['entry_price']
            side = pos['side']
            
            stop_hit = False
            if side == 1 and curr_price < sl_price: stop_hit = True
            if side == -1 and curr_price > sl_price: stop_hit = True
            
            roi = (curr_price - entry_price) / entry_price if side == 1 else (entry_price - curr_price) / entry_price
            if roi > 0.03 and not pos.get('breakeven_set', False):
                pos['stop_loss'] = entry_price * (1.002 if side == 1 else 0.998)
                pos['breakeven_set'] = True
                print(f"🌊 [RESONANCE] {sym} Set Breakeven Stop.")

            if stop_hit:
                print(f"🌊 [RESONANCE] {sym} STOPPED OUT. Price: {curr_price}, SL: {sl_price}")
                assets_to_close.append(sym)
                continue
                
            weights[sym] = side * self.leverage_scale
            active = True

        for sym in assets_to_close:
            self.positions.pop(sym)
            weights[sym] = 0.0

        # === 3. 扫描新机会 ===
        for sym, dfs in data_pack.items():
            if sym in self.positions: continue
            
            # [CRITICAL FIX] 如果刚刚被当作幽灵清理了，本轮禁止开仓！
            if sym in ghosts_this_round:
                continue

            if '5m' not in dfs or '30m' not in dfs: continue
            if dfs['5m'].empty or dfs['30m'].empty: continue
            
            signal = strategy.check_signals(dfs['30m'], dfs['5m'])
            
            if signal != 0:
                df = dfs['5m']
                atr_series = calc_atr(df['h'], df['l'], df['c'], period=14)
                atr = atr_series.iloc[-1]
                if pd.isna(atr): atr = (df['h'].iloc[-1] - df['l'].iloc[-1])
                
                curr_price = df['c'].iloc[-1]
                stop_loss = curr_price - (atr * 3.0) if signal == 1 else curr_price + (atr * 3.0)
                
                print(f"🌊 [RESONANCE V2] {sym} ENTRY ({signal}) | ATR: {atr:.4f} | Stop: {stop_loss:.2f}")
                
                self.positions[sym] = {
                    'side': signal,
                    'entry_price': curr_price,
                    'entry_time': datetime.now(),
                    'stop_loss': stop_loss,
                    'breakeven_set': False
                }
                weights[sym] = signal * self.leverage_scale
                active = True
                
        return pd.Series(weights), active