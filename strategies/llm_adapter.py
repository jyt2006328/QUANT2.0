import json
import os
from datetime import datetime

class LLMAdapter:
    """
    [LLM 适配器]
    职责: 读取外部输入的 SOP 指令 (regime.json)，将自然语言逻辑转化为量化参数。
    """
    def __init__(self, config_file='regime.json'):
        self.config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', config_file)
        self.default_regime = {
            "market_mode": "chop",
            "risk_preference": "neutral",
            "focus_assets": [],
            "leverage_suggestion": 1.0
        }

    def get_sop_config(self):
        """读取并解析 SOP 指令"""
        try:
            if not os.path.exists(self.config_path):
                return self._apply_rules(self.default_regime)
            
            with open(self.config_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                
            return self._apply_rules(raw_data)
        except Exception as e:
            print(f"⚠️ LLM Adapter Error: {e}")
            return self._apply_rules(self.default_regime)

    def _apply_rules(self, data):
        """
        核心逻辑: 将自然语言状态映射为具体的参数乘数
        """
        mode = data.get('market_mode', 'chop')
        risk = data.get('risk_preference', 'neutral')
        
        # 1. 策略权重调整 (Allocation Multipliers)
        # 默认全为 1.0
        multipliers = {
            'trend_w_mult': 1.0,
            'pair_w_mult': 1.0,
            'basket_w_mult': 1.0,
            'sniper_threshold_adj': 0.0, # 阈值调整
            'stop_loss_mult': 1.0        # 止损宽窄
        }
        
        # 2. 根据 Market Mode 调整
        if mode == 'bull_trend':
            # 牛市: 重 Trend，Sniper 顺势接多更容易
            multipliers['trend_w_mult'] = 1.5 
            multipliers['basket_w_mult'] = 0.5 # 牛市少做对冲
            multipliers['sniper_threshold_adj'] = -0.5 # 接针更容易
            
        elif mode == 'bear_trend':
            # 熊市: Trend (做空需谨慎，目前Trend主要是LongOnly，所以可能要降权), Basket 重仓
            multipliers['trend_w_mult'] = 0.5 
            multipliers['basket_w_mult'] = 1.5 # 熊市对冲为主
            multipliers['sniper_threshold_adj'] = 0.5 # 接针更难 (防飞刀)
            
        elif mode == 'chop':
            # 震荡: 关 Trend，靠 Sniper 和 Pair
            multipliers['trend_w_mult'] = 0.0 
            multipliers['pair_w_mult'] = 1.2
            multipliers['sniper_threshold_adj'] = 0.0
            
        elif mode == 'crisis':
            # 危机模式: 全停，或者只留 Sniper 抓黑天鹅
            multipliers['trend_w_mult'] = 0.0
            multipliers['pair_w_mult'] = 0.0
            multipliers['basket_w_mult'] = 0.0
            multipliers['sniper_threshold_adj'] = -1.0 # 极度恐慌时，Sniper 应该贪婪
            
        # 3. 根据 Risk Preference 调整
        if risk == 'aggressive':
            multipliers['stop_loss_mult'] = 1.5 # 宽止损
        elif risk == 'conservative':
            multipliers['stop_loss_mult'] = 0.8 # 窄止损
            
        return {
            "raw": data,
            "multipliers": multipliers
        }