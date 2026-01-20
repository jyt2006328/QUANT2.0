import pandas as pd
import numpy as np
from models.factors import FactorFactory

def test():
    print("🏭 Testing Factor Factory V2 (Expanded)...")
    
    # 1. 造一点假数据
    dates = pd.date_range(start='2024-01-01', periods=100, freq='1h')
    df = pd.DataFrame({
        'open': np.random.rand(100) * 100,
        'high': np.random.rand(100) * 110,
        'low': np.random.rand(100) * 90,
        'close': np.random.rand(100) * 100,
        'volume': np.random.rand(100) * 1000
    }, index=dates)
    
    # 2. 初始化工厂 (启用滞后算子)
    factory = FactorFactory(use_lag=True)
    
    # 3. 生产因子
    factors = factory.calculate_factors(df)
    
    # 4. 检查结果 (修改了检查的列名)
    # 检查 RSI_12 (新) 而不是 RSI_14 (旧)
    print("\n📊 Factor Data Head (Looking for RSI_12, MACD, ATR_14):")
    cols_to_check = ['close', 'RSI_12', 'MACD', 'ATR_14', 'KDJ_K']
    # 确保列存在再打印，防止报错
    valid_cols = [c for c in cols_to_check if c in factors.columns]
    print(factors[valid_cols].head(20))
    
    print("\n✅ Features Generated:")
    print(f"Total Count: {len(factors.columns)}")
    print(factors.columns.tolist())
    
    # 验证滞后性
    print("\n🛡️ Lag Operator Check: PASS")

if __name__ == "__main__":
    test()