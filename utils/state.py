import json
import os
from datetime import datetime

class StateManager:
    """
    [State] 状态持久化管理器
    负责保存和加载 Sniper, Trend Stop, Resonance 的内部状态。
    """
    def __init__(self, state_file='bot_state.json'):
        # 确保 state_file 路径在程序根目录
        self.state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', state_file)
        
    def save_state(self, sniper_mgr, stop_monitor, res_mgr=None):
        """
        保存当前状态到 JSON 文件
        新增: res_mgr (共振策略管理器)
        """
        state = {
            "timestamp": datetime.now().isoformat(),
            "sniper_positions": sniper_mgr.export_state(),
            "trailing_stop_hwm": stop_monitor.export_state(),
            # === 新增: 保存共振策略持仓 ===
            "resonance_positions": res_mgr.export_state() if res_mgr else {}
        }
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            print(f"⚠️ State save failed: {e}")
        
    def load_state(self, sniper_mgr, stop_monitor, res_mgr=None):
        """
        从 JSON 文件加载状态
        新增: res_mgr (共振策略管理器)
        """
        if not os.path.exists(self.state_file): return
        try: 
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            
            # 1. 恢复 Sniper
            sniper_mgr.import_state(state.get("sniper_positions", {}))
            
            # 2. 恢复 Trend Stop
            stop_monitor.import_state(state.get("trailing_stop_hwm", {}))
            
            # === 3. 新增: 恢复 Resonance (你问的那句就在这里) ===
            if res_mgr:
                res_mgr.import_state(state.get("resonance_positions", {}))
            # =================================================
            
            print(f"✅ State Restored (Saved: {state.get('timestamp')})")
        except Exception as e:
            print(f"⚠️ State load failed: {e}")