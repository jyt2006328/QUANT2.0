import pandas as pd
import numpy as np
import xgboost as xgb
import joblib
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

# 添加根目录到路径，以便导入 models
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models.cross_sectional import CrossSectionalLoader, Preprocessor
from data.storage import DataManager

# 你的币种
SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
    'AVAX/USDT:USDT', 'DOGE/USDT:USDT', 'XRP/USDT:USDT',
    'BNB/USDT:USDT', 'AAVE/USDT:USDT'
]

def analyze():
    print("🔍 Analyzing Model & Factors...")
    
    # 1. 加载模型
    model_path = 'models/xgb_model.pkl'
    if not os.path.exists(model_path):
        print("❌ Model not found!")
        return
        
    model = joblib.load(model_path)
    
    # 2. 获取特征重要性
    importance = model.get_score(importance_type='gain')
    imp_df = pd.DataFrame(list(importance.items()), columns=['Feature', 'Gain'])
    imp_df = imp_df.sort_values('Gain', ascending=False)
    
    print("\n🏆 Top 10 Most Important Factors (by Gain):")
    print(imp_df.head(10))
    
    # 3. 准备数据进行单因子 IC 分析
    print("\n⏳ Loading data for IC Analysis...")
    db = DataManager()
    loader = CrossSectionalLoader(db)
    raw_df = loader.load_all_assets(SYMBOLS, timeframe='1h', limit=1000)
    
    if raw_df.empty: return

    processor = Preprocessor()
    df = processor.clean_data(raw_df)
    
    # 4. 计算单因子 IC (Rank IC)
    print("📊 Calculating Single Factor IC...")
    ic_results = []
    feature_cols = [c for c in df.columns if c not in ['target']]
    
    for col in feature_cols:
        # 按时间分组计算相关性
        ic_list = df.groupby(level='date').apply(
            lambda x: x[col].corr(x['target'], method='spearman')
        )
        ic_mean = ic_list.mean()
        ic_std = ic_list.std()
        ir = ic_mean / (ic_std + 1e-9)
        
        ic_results.append({
            'Feature': col,
            'IC': ic_mean,
            'IR': ir
        })
        
    ic_df = pd.DataFrame(ic_results).sort_values('IC', ascending=False) # 绝对值排序更合理，但这里先看正负
    
    print("\n💎 Top 5 Positive Alphas (正相关):")
    print(ic_df.head(5))
    
    print("\n💣 Top 5 Negative Alphas (负相关):")
    print(ic_df.tail(5))
    
    # 保存报告
    ic_df.to_csv('factor_analysis_report.csv', index=False)
    print("\n💾 Report saved to 'factor_analysis_report.csv'")

if __name__ == "__main__":
    analyze()