import time
import ccxt

class MakerManager:
    """
    [Smart Execution] 智能挂单执行器
    策略: Limit Chase (限价追单) -> Market Fallback (市价兜底)
    """
    def __init__(self, exchange, max_attempts=3, wait_seconds=5):
        self.exchange = exchange
        self.max_attempts = max_attempts # 尝试挂单次数
        self.wait_seconds = wait_seconds # 每次挂单等待秒数

    def execute_order(self, symbol, side, amount, params={}):
        """
        执行智能下单流程
        返回: (success: bool, filled_amount: float, avg_price: float)
        """
        remaining = amount
        filled = 0.0
        total_cost = 0.0
        
        print(f"🤖 [Maker Algo] Start {symbol} {side} {amount}...")

        # --- Phase A: 尝试 Maker 挂单 (Chase) ---
        for i in range(self.max_attempts):
            try:
                # 1. 获取最新盘口价格 (Bitget)
                ticker = self.exchange.fetch_ticker(symbol)
                # 买入挂买一价(bid)，卖出挂卖一价(ask)
                price = ticker['bid'] if side == 'buy' else ticker['ask']
                
                # 2. 价格精度修正
                price = float(self.exchange.price_to_precision(symbol, price))
                
                print(f"   👉 Attempt {i+1}/{self.max_attempts}: Limit {side} @ {price}")
                
                # 3. 下限价单
                order = self.exchange.create_order(symbol, 'limit', side, remaining, price, params)
                order_id = order['id']
                
                # 4. 等待成交
                time.sleep(self.wait_seconds)
                
                # 5. 查询订单状态
                # Bitget 查单可能需要 try-catch 防止短暂找不到订单
                try:
                    updated_order = self.exchange.fetch_order(order_id, symbol)
                except:
                    # 如果查不到，保守起见等待一下再查，或者假设未成交
                    time.sleep(1)
                    updated_order = self.exchange.fetch_order(order_id, symbol)

                status = updated_order['status']
                batch_filled = float(updated_order['filled'])
                
                # 更新统计
                # 注意：这里我们只记录增量，但 fetch_order 返回的是累积量。
                # 简化处理：因为这是单次循环，order 是新的，所以 filled 就是这笔单子的成交量
                if batch_filled > 0:
                    cost = float(updated_order['cost']) if updated_order['cost'] else batch_filled * price
                    total_cost += cost
                    filled += batch_filled
                    remaining -= batch_filled
                
                # 6. 判断结果
                if status == 'closed' or remaining <= 0:
                    # 全部成交！
                    avg_price = total_cost / filled if filled > 0 else price
                    print(f"   ✅ Limit Order Filled! Avg: {avg_price:.4f}")
                    return True
                
                # 7. 未全部成交 -> 撤单，准备下一轮追单
                print(f"   ⏳ Partial/No fill ({batch_filled}/{amount}). Cancelling...")
                try:
                    self.exchange.cancel_order(order_id, symbol)
                except Exception as e:
                    # 此时可能正好成交了，或者订单已关闭
                    print(f"   ⚠️ Cancel failed (Order might be filled): {e}")
                
                # 检查精度：如果剩余量太小（比如 0.0001），就算完成了，不再追单
                if remaining < amount * 0.05: # 剩余不足 5%
                    return True

            except Exception as e:
                print(f"   ❌ Maker Attempt {i+1} Error: {e}")
                time.sleep(1)

        # --- Phase B: 市价兜底 (Taker Fallback) ---
        # 如果尝试了 N 次还没买够，为了保证策略执行，剩下的直接吃单
        if remaining > 0:
            print(f"   🚀 [Fallback] Taker Market Order for remaining {remaining:.4f}...")
            try:
                # 再次精度修正
                final_amt = float(self.exchange.amount_to_precision(symbol, remaining))
                order = self.exchange.create_order(symbol, 'market', side, final_amt, params=params)
                print("   ✅ Taker Filled.")
                return True
            except Exception as e:
                print(f"   ❌ Taker Failed: {e}")
                return False
        
        return True