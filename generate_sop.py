import json
import os
from datetime import datetime

def save_sop(mode, risk, assets, reason="Manual Update"):
    data = {
        "timestamp": datetime.now().isoformat(),
        "market_mode": mode,
        "risk_preference": risk,
        "focus_assets": assets,
        "reasoning": reason
    }
    
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regime.json')
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    print(f"✅ SOP Updated: {mode} / {risk}")

if __name__ == "__main__":
    print("=== QuantBot SOP Commander ===")
    print("1. Bull Trend (Trend++, Basket--)")
    print("2. Bear Trend (Trend--, Basket++)")
    print("3. Chop (Trend OFF, Pair++)")
    print("4. Crisis (All OFF, Sniper++)")
    
    choice = input("Select Market Mode (1-4): ")
    mode_map = {'1': 'bull_trend', '2': 'bear_trend', '3': 'chop', '4': 'crisis'}
    selected_mode = mode_map.get(choice, 'chop')
    
    risk_choice = input("Risk Preference (1.Aggressive 2.Neutral 3.Conservative): ")
    risk_map = {'1': 'aggressive', '2': 'neutral', '3': 'conservative'}
    selected_risk = risk_map.get(risk_choice, 'neutral')
    
    print(f"🚀 Writing instructions: {selected_mode.upper()} + {selected_risk.upper()}")
    save_sop(selected_mode, selected_risk, [])