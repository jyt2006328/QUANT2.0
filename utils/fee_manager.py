import ccxt
import time

class FeeDiscountManager:
    """
    [后勤] 手续费自动补充模块
    逻辑: 检测平台币 (BGB) 余额，不足时自动买入，确保持续享受 20% 手续费折扣。
    """
    def __init__(self, exchange, coin='BGB', min_balance=5, replenish_usdt=10):
        self.exchange = exchange
        self.coin = coin 
        self.min_balance = min_balance 
        self.replenish_usdt = replenish_usdt 
        self.symbol = f"{coin}/USDT" 

    def check_and_replenish(self):
        try:
            # 1. 获取余额
            balance = self.exchange.fetch_balance()
            coin_balance = balance.get(self.coin, {}).get('free', 0)
            usdt_balance = balance.get('USDT', {}).get('free', 0)
            
            # 2. 判断是否需要补充
            if coin_balance < self.min_balance:
                print(f"   ⚠️ {self.coin} low ({coin_balance:.2f} < {self.min_balance}). Replenishing...")
                
                if usdt_balance < self.replenish_usdt:
                    print(f"   ❌ Not enough USDT to buy {self.coin}. Skipping.")
                    return

                # 3. 获取价格
                ticker = self.exchange.fetch_ticker(self.symbol)
                price = ticker['last']
                amount = self.replenish_usdt / price
                
                # 最小下单检查
                if amount * price < 6:
                    print("   ❌ Amount too small to trade.")
                    return

                print(f"   🛒 Buying ~${self.replenish_usdt} of {self.coin} for fees...")
                
                # === Bitget 市价买单特殊处理 ===
                params = {
                    'createMarketBuyOrderRequiresPrice': False
                }
                
                # 注意：这里传入的是 USDT 金额 (cost)
                self.exchange.create_order(self.symbol, 'market', 'buy', self.replenish_usdt, params=params)
                
                print(f"   ✅ {self.coin} replenished.")
                
        except Exception as e:
            print(f"   ⚠️ Fee Manager Error: {e}")
