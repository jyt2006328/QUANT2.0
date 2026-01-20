from models.cross_sectional import CrossSectionalLoader, Preprocessor
from data.storage import DataManager
import pandas as pd

# 模拟配置里的币种
SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
    'AVAX/USDT:USDT', 'DOGE/USDT:USDT'
]

def test():
    print("🔬 Testing Cross-Sectional Pipeline...")
    
    # 1. 初始化
    db = DataManager()
    loader = CrossSectionalLoader(db)
    processor = Preprocessor()
    
    # 2. 加载并生产因子
    # 假设你数据库里有数据 (populator 跑过)
    # 如果没数据，这个测试会打印 Empty
    raw_df = loader.load_all_assets(SYMBOLS, timeframe='1h', limit=500)
    
    if raw_df.empty:
        print("⚠️ No data found in DB. Please run populator.py first.")
        return
        
    print(f"\n📦 Raw Data Shape: {raw_df.shape}")
    print(raw_df.head())
    
    # 3. 清洗与标准化
    clean_df = processor.clean_data(raw_df)
    
    print(f"\n✨ Clean Data Shape: {clean_df.shape}")
    print(clean_df.head())
    
    # 验证标准化效果 (理论上每个时间点的均值应接近0)
    sample_date = clean_df.index.get_level_values('date')[10]
    sample_slice = clean_df.xs(sample_date, level='date')
    print(f"\n🧐 Z-Score Check at {sample_date}:")
    print(f"   RSI_14 Mean: {sample_slice['RSI_14'].mean():.4f} (Should be ~0)")
    print(f"   RSI_14 Std:  {sample_slice['RSI_14'].std():.4f} (Should be ~1)")

if __name__ == "__main__":
    test()