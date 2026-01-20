import time
from data.stream import BinanceDataStream

SYMBOLS = ['BTC/USDT:USDT', 'ETH/USDT:USDT', 'SOL/USDT:USDT']

def test():
    print("🔬 Testing Microstructure Data Stream...")
    stream = BinanceDataStream(SYMBOLS)
    stream.start()
    
    print("⏳ Collecting ticks (15s)...")
    time.sleep(15)
    
    print("\n📊 Micro Structure Snapshot:")
    print(f"{'Symbol':<10} | {'Net Flow ($)':<15} | {'Big Buy':<10} | {'Big Sell':<10}")
    print("-" * 60)
    
    for sym in SYMBOLS:
        factors = stream.get_micro_factors(sym)
        net_flow = factors['net_flow']
        
        flow_tag = ""
        if net_flow > 50000: flow_tag = "🔥 Inflow"
        elif net_flow < -50000: flow_tag = "❄️ Outflow"
            
        print(f"{sym.split('/')[0]:<10} | {net_flow:<15.1f} | {factors['large_buy']:<10} | {factors['large_sell']:<10} {flow_tag}")

    print("\n✅ Test Complete.")
    stream.stop()

if __name__ == "__main__":
    test()