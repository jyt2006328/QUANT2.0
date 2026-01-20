import pandas as pd
import numpy as np
from datetime import datetime

class SniperStrategyV5:
    """
    [Sniper V5] 终极进化版
    集成: Z-Score + OBI (盘口) + Iceberg (冰山/资金流)
    """
    def __init__(self):
        self.base_L1_Z = 3.0
        self.base_L2_Z = 4.0
        self.min_vol = 2.5
        self.obi_threshold = 0.5 
        
    def check_signals(self, hf_data, obi_dict=None, micro_dict=None, z_adj=0.0):
        """
        micro_dict: { 'BTC...': {'net_flow': 50000, 'large_buy': 5, ...} }
        """
        sniper_weights = {}
        active = False
        level_tag = ""
        
        current_L1 = self.base_L1_Z + z_adj
        current_L2 = self.base_L2_Z + z_adj
        
        for sym, df in hf_data.items():
            if len(df) < 30: continue
            
            close = df['c']
            volume = df['v']
            
            # 1. 宏观指标 (Z-Score)
            z_score = (close - close.rolling(20).mean()) / (close.rolling(20).std() + 1e-9)
            vol_ratio = volume / (volume.rolling(20).mean() + 1e-9)
            
            cz = z_score.iloc[-1]
            cv = vol_ratio.iloc[-1]
            
            # 2. 微观指标 (Micro Structure)
            obi = obi_dict.get(sym, 0.0) if obi_dict else 0.0
            micro = micro_dict.get(sym, {}) if micro_dict else {}
            net_flow = micro.get('net_flow', 0)
            
            # --- 信号判定 ---
            signal = 0.0
            
            # [LONG] 抄底逻辑
            # 基础条件: 超跌 (Z < -L1) 且 放量 (Vol > 2.5)
            if cz < -current_L1 and cv > self.min_vol:
                
                # [Filter A] 盘口不恶化 (OBI > -0.5)
                # 如果卖盘还是碾压买盘，别接
                if obi > -self.obi_threshold:
                    
                    # [Filter B] 资金流验证 (Iceberg/Flow)
                    # 如果有显著净流入 (主力吸筹)，或者没有显著流出
                    if net_flow > -10000: # 只要不是大额流出就行
                        
                        # 评级
                        if cz < -current_L2:
                            signal = 1.0; level_tag = "WAR (Deep)"
                        else:
                            signal = 0.5; level_tag = "SKIRMISH"
                            
                        # [Bonus] 冰山确认
                        # 如果价格暴跌，但资金大幅净流入 -> 冰山吸筹 -> 加仓！
                        if net_flow > 50000:
                            signal = 1.0
                            level_tag = "ICEBERG LONG"

            # [SHORT] 摸顶逻辑
            elif cz > current_L1 and cv > self.min_vol:
                if obi < self.obi_threshold: # 买盘不强
                    if net_flow < 10000: # 没有大额流入
                        if cz > current_L2:
                            signal = -1.0; level_tag = "WAR (Top)"
                        else:
                            signal = -0.5; level_tag = "SKIRMISH"
                        
                        if net_flow < -50000: # 主力出货
                            signal = -1.0
                            level_tag = "ICEBERG SHORT"
            
            if signal != 0:
                sniper_weights[sym] = signal
                active = True
                
        return sniper_weights, active, level_tag

class SniperManagerV10:
    """
    [Sniper Manager] 保持 V4 的熔断逻辑，适配 V5 的接口
    """
    def __init__(self, sl=-0.03, time_limit=15):
        self.sl = sl 
        self.time_limit = time_limit
        self.positions = {} 

    def export_state(self):
        data = {}
        for sym, pos in self.positions.items():
            data[sym] = pos.copy()
            data[sym]['entry_time'] = pos['entry_time'].isoformat()
        return data
    
    def import_state(self, data):
        if not isinstance(data, dict): return
        for sym, info in data.items():
            try: info['entry_time'] = datetime.fromisoformat(info['entry_time']); self.positions[sym] = info
            except: pass

    def check_signals(self, hf_data, sniper_strategy, obi_dict=None, micro_dict=None, z_threshold_adjustment=0.0):
        # [Update] 传入 micro_dict
        new_signals, _, tag = sniper_strategy.check_signals(
            hf_data, 
            obi_dict=obi_dict,
            micro_dict=micro_dict, # 传入资金流
            z_adj=z_threshold_adjustment
        )
        
        sniper_weights = {}
        active_snipers = False
        assets_to_close = []
        
        # --- 1. 持仓管理 (保持原有的 熔断+止损+止盈 逻辑) ---
        for asset, pos in self.positions.items():
            if asset not in hf_data: continue
            
            df = hf_data[asset]
            curr_price = df['c'].iloc[-1]
            entry_price = pos['entry_price']
            
            ma20 = df['c'].rolling(20).mean().iloc[-1]
            std20 = df['c'].rolling(20).std().iloc[-1]
            curr_z = (curr_price - ma20) / (std20 + 1e-9)
            
            roi = (curr_price - entry_price) / entry_price if pos['side'] == 'long' else (entry_price - curr_price) / entry_price
            hold_mins = (datetime.now() - pos['entry_time']).total_seconds() / 60
            
            exit_reason = None
            
            if roi < self.sl: exit_reason = f"🩸 Hard SL ({roi*100:.2f}%)"
            elif hold_mins > self.time_limit and roi < 0: exit_reason = f"⏳ Time Stop"
            elif pos['side'] == 'short' and curr_price > ma20: exit_reason = f"🛡️ Trend Fuse (Short > MA20)"
            elif pos['side'] == 'long' and curr_price < ma20: exit_reason = f"🛡️ Trend Fuse (Long < MA20)"

            if exit_reason:
                print(f"🚨 [SNIPER EXIT] {asset}: {exit_reason}")
                assets_to_close.append(asset)
                continue
            
            # Z-Score 止盈
            current_level = 0
            if pos['side'] == 'long':
                if curr_z > 2.0: current_level = 3
                elif curr_z > 0.0: current_level = 2
                elif curr_z > -2.0: current_level = 1
            else:
                if curr_z < -2.0: current_level = 3
                elif curr_z < 0.0: current_level = 2
                elif curr_z < 2.0: current_level = 1
            
            if current_level > pos['level_reached']:
                ratio = 0.4 if current_level == 2 else 0.3
                print(f"💰 [TP] {asset} Z={curr_z:.2f}. Lock {ratio*100}%.")
                pos['remain_ratio'] -= ratio; pos['level_reached'] = current_level
            
            if pos['remain_ratio'] <= 0.1:
                assets_to_close.append(asset); continue

            sniper_weights[asset] = pos['initial_w'] * pos['remain_ratio']
            active_snipers = True

        for asset in assets_to_close:
            self.positions.pop(asset)
            sniper_weights[asset] = 0.0
        
        # --- 2. 新开仓 ---
        for asset, w in new_signals.items():
            if w != 0 and asset not in self.positions:
                side = 'long' if w > 0 else 'short'
                
                # 日志带上资金流信息
                micro_val = micro_dict.get(asset, {}).get('net_flow', 0) if micro_dict else 0
                print(f"⚡ [SNIPER {tag}] {asset} ({side}) w={w} | Flow: {micro_val:.0f}")
                
                self.positions[asset] = {
                    'entry_price': hf_data[asset]['c'].iloc[-1], 
                    'entry_time': datetime.now(), 
                    'side': side, 
                    'initial_w': w, 
                    'remain_ratio': 1.0, 
                    'level_reached': 0
                }
                sniper_weights[asset] = w
                active_snipers = True
                
        return pd.Series(sniper_weights), active_snipers