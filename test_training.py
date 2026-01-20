from models.cross_sectional import CrossSectionalLoader, Preprocessor
from models.trainer import ModelTrainer
from data.storage import DataManager
import pandas as pd

# 你的币种列表
SYMBOLS = [
    'BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT', 
    'AVAX/USDT:USDT', 'DOGE/USDT:USDT', 'XRP/USDT:USDT',
    'BNB/USDT:USDT', 'AAVE/USDT:USDT'
]

def test():
    print("🧠 Starting AI Training Pipeline...")
    
    # 1. 准备数据
    db = DataManager()
    loader = CrossSectionalLoader(db)
    
    # 加载多一点数据，比如最近 1000 小时
    raw_df = loader.load_all_assets(SYMBOLS, timeframe='1h', limit=1000)
    
    if raw_df.empty:
        print("❌ No data found. Run populator.py first.")
        return

    # 2. 清洗
    processor = Preprocessor()
    clean_df = processor.clean_data(raw_df)
    
    print(f"📦 Data ready: {clean_df.shape}")
    
    # 3. 训练
    trainer = ModelTrainer()
    trainer.train(clean_df)
    
    # 4. 模拟一次实盘预测
    print("\n🔮 Inference Test:")
    # 取最后一个时间截面作为"当前市场状态"
    latest_date = clean_df.index.get_level_values('date')[-1]
    current_market = clean_df.xs(latest_date, level='date')
    
    # 预测需要去掉 target 列
    features = current_market.drop(columns=['target'])
    scores = trainer.predict(features)
    
    # 打印排名
    ranking = pd.Series(scores, index=features.index).sort_values(ascending=False)
    print(ranking)

if __name__ == "__main__":
    test()