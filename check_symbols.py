import requests
import json
import pandas as pd

# Bitget V2 获取合约列表的端点
# productType: USDT-FUTURES (USDT专业合约)
URL = "https://api.bitget.com/api/v2/mix/market/contracts"
PARAMS = {
    "productType": "USDT-FUTURES"
}

def check_symbols():
    print(f"🔍 Querying Bitget V2 API: {URL} ...")
    try:
        resp = requests.get(URL, params=PARAMS, timeout=10)
        data = resp.json()
        
        if data['code'] != '00000':
            print(f"❌ API Error: {data}")
            return

        symbol_list = data['data']
        print(f"✅ Successfully fetched {len(symbol_list)} symbols.")
        
        # 提取关键信息并展示前 10 个
        preview_data = []
        for item in symbol_list:
            preview_data.append({
                "symbol": item['symbol'],       # 内部交易对名称
                "baseCoin": item['baseCoin'],   # 基础币种
                "quoteCoin": item['quoteCoin']  # 计价币种
            })
            
            # 重点检查我们关心的 BTC
            if item['baseCoin'] == 'BTC' and item['quoteCoin'] == 'USDT':
                print("\n🎯 === FOUND BTC TARGET ===")
                print(f"   instId (Symbol): {item['symbol']}")
                print(f"   baseCoin:        {item['baseCoin']}")
                print(f"   quoteCoin:       {item['quoteCoin']}")
                print("==========================")

        df = pd.DataFrame(preview_data)
        print("\n📋 Top 5 Active Symbols Example:")
        print(df.head(5).to_string())
        
        # 保存一份完整的列表到本地，方便你查阅
        df.to_csv("bitget_v2_symbols.csv", index=False)
        print("\n💾 Full list saved to 'bitget_v2_symbols.csv'")

    except Exception as e:
        print(f"❌ Network Error: {e}")

if __name__ == "__main__":
    check_symbols()