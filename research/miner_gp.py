import os
import sys
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
from gplearn.genetic import SymbolicTransformer
from sklearn.model_selection import train_test_split

# 锁定路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'market_data.db')

def load_data(symbol='BTC/USDT:USDT'):
    print(f"⏳ Loading data for {symbol}...")
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(f"SELECT * FROM ohlcv_1h WHERE symbol='{symbol}' ORDER BY timestamp ASC", conn)
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('date', inplace=True)
    
    rename_map = {'open':'open', 'high':'high', 'low':'low', 'close':'close', 'volume':'volume'}
    df = df.rename(columns=rename_map)[['open', 'high', 'low', 'close', 'volume']]
    
    df['target'] = df['close'].pct_change().shift(-1)
    return df.dropna()

def run_mining(df):
    print("🧬 Starting Genetic Programming Evolution...")
    
    # 特征工程
    X = df[['open', 'high', 'low', 'close', 'volume']]
    y = df['target']
    
    split = int(len(df) * 0.7)
    X_train, X_test = X.iloc[:split], X.iloc[split:]
    y_train, y_test = y.iloc[:split], y.iloc[split:]
    
    function_set = ['add', 'sub', 'mul', 'div', 'sqrt', 'log', 'abs', 'neg', 'inv', 'max', 'min']
    
    gp = SymbolicTransformer(
        generations=20, 
        population_size=2000,
        hall_of_fame=100, 
        n_components=10, 
        function_set=function_set,
        parsimony_coefficient=0.0005,
        max_samples=0.9, 
        verbose=1,
        random_state=42,
        n_jobs=-1
    )
    
    print("   Training on RTX 3050 (CPU mode)...")
    gp.fit(X_train, y_train)
    
    print("\n🏆 Top Discovered Formulas:")
    best_programs = gp._best_programs
    
    results = []
    
    # === 修复开始: 将 DataFrame 转为 Numpy 供 execute 使用 ===
    # gplearn execute 需要 pure numpy array 以便使用整数索引列
    X_test_np = X_test.values 
    # ======================================================

    for i, program in enumerate(best_programs):
        if program is None: continue
        
        # 使用 numpy array 执行
        factor_values = program.execute(X_test_np)
        
        # 计算 IC (Information Coefficient)
        # 将 numpy array 展平并转为 Series 以计算相关性
        ic = pd.Series(factor_values.flatten()).corr(y_test.reset_index(drop=True))
        
        print(f"   Factor {i+1} (IC={ic:.4f}): {program}")
        results.append({'formula': str(program), 'ic': ic})
        
    res_df = pd.DataFrame(results)
    res_df.to_csv(os.path.join(BASE_DIR, 'research', 'gp_alphas.csv'), index=False)
    print(f"\n✅ Saved best alphas to research/gp_alphas.csv")

if __name__ == "__main__":
    os.makedirs(os.path.join(BASE_DIR, 'research'), exist_ok=True)
    df = load_data()
    if not df.empty:
        run_mining(df)