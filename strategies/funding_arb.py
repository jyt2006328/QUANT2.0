import time
import pandas as pd
from datetime import datetime

class FundingArbitrageStrategy:
    """
    [Phase 26 Core] 资金费率套利引擎 (Final Version)
    逻辑: 
    1. 监控费率 -> 触发阈值
    2. Spot Leg: Taker 买入 (确保持有资产)
    3. Transfer: 划转至合约账户
    4. Swap Leg: Maker 做空 (降低磨损)
    """
    def __init__(self, spot_exchange, swap_exchange, transfer_agent, maker_agent):
        self.spot_exchange = spot_exchange   # 现货客户端 (用于 Taker)
        self.swap_exchange = swap_exchange   # 合约客户端 (用于查询)
        self.transfer = transfer_agent       # 划转代理
        self.maker = maker_agent             # 执行代理 (用于合约 Maker)
        
        # 核心参数
        self.entry_threshold = 0.0005  # 开仓门槛 (0.05%)
        self.exit_threshold = 0.0001   # 平仓门槛
        self.trade_amount_usdt = 15.0  # 单次套利金额 (U)
        
        # 记录套利对状态
        self.arb_positions = {} 

    def scan_and_execute(self, funding_rates, current_prices, is_dry_run=True):
        """
        扫描市场并执行套利
        """
        if not funding_rates: return

        # 1. 寻找最高费率
        sorted_rates = sorted(funding_rates.items(), key=lambda x: x[1], reverse=True)
        best_sym_raw, best_rate = sorted_rates[0] # e.g., 'BTCUSDT'
        
        # 2. 格式化 Symbol
        # Bitget/CCXT 格式转换
        base_coin = best_sym_raw.replace('USDT', '') # BTC
        spot_symbol = f"{base_coin}/USDT"            # 现货
        perp_symbol = f"{base_coin}/USDT:USDT"       # 合约

        # 3. 检查是否已有持仓 (平仓逻辑)
        if base_coin in self.arb_positions:
            current_rate = funding_rates.get(best_sym_raw, 0)
            if current_rate < self.exit_threshold:
                print(f"📉 [Arb] {base_coin} Rate dropped to {current_rate*100:.4f}%. Closing...")
                self._execute_arbitrage_close(base_coin, spot_symbol, perp_symbol, is_dry_run)
            return

        # 4. 开仓逻辑
        if best_rate > self.entry_threshold:
            print(f"🤑 [Arb Opportunity] {base_coin} Rate: {best_rate*100:.4f}%! Executing...")
            self._execute_arbitrage_open(base_coin, spot_symbol, perp_symbol, best_rate, is_dry_run)

    def _execute_arbitrage_open(self, base_coin, spot_symbol, perp_symbol, rate, is_dry_run):
        try:
            print(f"   🌊 Starting Delta-Neutral Arb for {base_coin}...")
            
            spot_qty = 0.0
            avg_price = 0.0
            
            # --- Step 1: Spot Buy (Taker - Speed First) ---
            # 现货端必须先成交，否则没有资产去做空，风险极大。
            print(f"   1. Buying Spot {spot_symbol} (Taker)...")
            
            if is_dry_run:
                # 模拟
                ticker = self.spot_exchange.fetch_ticker(spot_symbol)
                avg_price = ticker['last']
                spot_qty = self.trade_amount_usdt / avg_price
                spot_qty = spot_qty * 0.999 # 模拟扣手续费
                print(f"      [Dry Run] Simulated Buy {spot_qty:.4f} @ {avg_price}")
            else:
                # 实盘
                params = {'createMarketBuyOrderRequiresPrice': False}
                # 市价买入 (按金额)
                order = self.spot_exchange.create_order(spot_symbol, 'market', 'buy', self.trade_amount_usdt, params=params)
                time.sleep(1) # 等待成交
                
                # 获取实际成交量
                o_res = self.spot_exchange.fetch_order(order['id'], spot_symbol)
                filled_amt = float(o_res['filled'])
                # 简单估算扣除手续费后的到账数量 (更精确做法是查 trades)
                spot_qty = filled_amt * 0.999 
                avg_price = float(o_res.get('average', 0) or o_res.get('price', 0))
                
                print(f"      ✅ Bought {spot_qty:.4f} {base_coin} @ {avg_price}")

            if spot_qty <= 0: return

            # --- Step 2: Transfer (Spot -> Swap) ---
            print(f"   2. Transferring {spot_qty:.4f} {base_coin} Spot -> Swap...")
            if not is_dry_run:
                success = self.transfer.transfer_to_futures(base_coin, spot_qty)
                if not success:
                    print("      ❌ Transfer failed. (Manual Intervention Needed: Sell Spot?)")
                    return
                time.sleep(2) # 等待划转到账
            else:
                print("      [Dry Run] Simulated Transfer")

            # --- Step 3: Perp Short (Maker - Cost First) ---
            # 资产已在合约账户，现在开启 Smart Maker 挂空单
            print(f"   3. Shorting Perp {perp_symbol} (Maker)...")
            
            if is_dry_run:
                print(f"      [Dry Run] Simulated Maker Short {spot_qty:.4f}")
            else:
                # 使用 maker_agent 执行! (这是监工指出的关键缺失)
                success = self.maker.execute_order(perp_symbol, 'sell', spot_qty)
                
                if not success:
                    print("      ❌ Maker Short Failed! (Risk Alert: Long Exposure Only!)")
                    # 这里可以加兜底逻辑：如果 Maker 彻底失败，是否市价强平？
                    # 考虑到 maker_agent 内部已有市价兜底 (Fallback)，这里失败通常意味着 API 挂了
                    return 
                
                print(f"      ✅ Short Executed via Maker.")

            # --- 记录状态 ---
            self.arb_positions[base_coin] = {
                'status': 'OPEN',
                'qty': spot_qty,
                'entry_price': avg_price,
                'entry_rate': rate,
                'time': datetime.now().isoformat()
            }
            print("   🎉 Arbitrage Position Established (Delta Neutral)!")

        except Exception as e:
            print(f"   ❌ Execution Error: {e}")

    def _execute_arbitrage_close(self, base_coin, spot_symbol, perp_symbol, is_dry_run):
        try:
            pos = self.arb_positions.get(base_coin)
            if not pos: return
            
            qty = pos['qty']
            print(f"   👋 Closing Arb Position for {base_coin}...")
            
            # --- Step 1: Close Short (Maker) ---
            # 平空单，先挂单买回
            print(f"   1. Closing Short {perp_symbol} (Maker)...")
            if not is_dry_run:
                success = self.maker.execute_order(perp_symbol, 'buy', qty)
                if not success:
                    print("      ❌ Close Short Failed.")
                    return
            else:
                 print(f"      [Dry Run] Maker Buy {qty}")
            
            # --- Step 2: Transfer (Swap -> Spot) ---
            print(f"   2. Transferring {base_coin} Swap -> Spot...")
            if not is_dry_run:
                # 假设平仓后币都在，转回现货
                self.transfer.transfer_to_spot(base_coin, qty)
                time.sleep(2)
            
            # --- Step 3: Sell Spot (Taker) ---
            print(f"   3. Selling Spot {spot_symbol} (Taker)...")
            if not is_dry_run:
                 self.spot_exchange.create_order(spot_symbol, 'market', 'sell', qty)
                 print("      ✅ Spot Sold.")
            
            del self.arb_positions[base_coin]
            print("   🏁 Position Closed.")
            
        except Exception as e:
            print(f"   ❌ Close Error: {e}")