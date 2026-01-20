import time
import json
from data.stream import BinanceDataStream

# 模拟你的 Config 里的符号列表
SYMBOLS = [
    'BTC/USDT:USDT', 
    'ETH/USDT:USDT',
    'SOL/USDT:USDT',
    'AVAX/USDT:USDT',
    'DOGE/USDT:USDT'
]

def test():
    print("🧪 Testing Hybrid Architecture Data Stream...")
    
    # 1. 初始化数据流
    stream = BinanceDataStream(SYMBOLS)
    
    # 2. 启动
    stream.start()
    
    # 等待几秒钟让数据预热
    print("⏳ Warming up (5s)...")
    time.sleep(5)
    
    # 3. 检查数据质量
    print("\n📊 Real-time Data Check:")
    print(f"{'Symbol':<15} | {'Binance Price':<15} | {'OBI (Alpha)':<15}")
    print("-" * 50)
    
    for sym in SYMBOLS:
        price = stream.get_latest_price(sym)
        obi = stream.get_order_imbalance(sym)
        
        # OBI > 0.3 显示为红色(买压), < -0.3 显示为绿色(卖压)
        tag = ""
        if obi > 0.3: tag = "🔥 Bullish"
        elif obi < -0.3: tag = "❄️ Bearish"
            
        print(f"{sym:<15} | {price:<15.4f} | {obi:<15.4f} {tag}")
    
    print("\n✅ Test Complete. Press Ctrl+C to stop.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        stream.stop()

if __name__ == "__main__":
    test()