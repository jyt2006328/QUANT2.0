import ccxt
import time

class ExecutionGate:
    """
    [执行卫士] 
    灵感来源: 大佬的 insufficient_balance.py 和 change_leverage.py
    职责: 在真正下单前，进行最后一道安全检查。
    """
    def __init__(self, exchange, leverage_limit=5):
        self.exchange = exchange
        self.leverage_limit = leverage_limit

    def check_and_set_leverage(self, symbol):
        """
        [安全检查] 强制重置杠杆
        防止交易所重置或误操作导致杠杆过高
        """
        try:
            # Bitget 特有的设置杠杆 API
            # 这是一个防御性操作，确保无论何时，杠杆都被锁死在限制内
            self.exchange.set_leverage(self.leverage_limit, symbol)
            # 同时设置为单向持仓模式 (One-Way)，防止多空双开导致的保证金占用
            try:
                self.exchange.set_position_mode(hedged=False, symbol=symbol)
            except:
                pass # 有些交易所不支持或已设置，跳过
            return True
        except Exception as e:
            print(f"⚠️ [Gate] Leverage Set Failed for {symbol}: {e}")
            return False

    def get_valid_amount(self, symbol, target_amount, price):
        """
        [资金检查] 智能降仓
        如果计算出的数量超过了可用余额，自动按比例缩减，而不是报错。
        """
        try:
            balance = self.exchange.fetch_balance()
            usdt_free = balance['USDT']['free']
            
            # 计算这就交易需要的保证金 (考虑到杠杆)
            # Cost = (Amount * Price) / Leverage
            required_margin = (target_amount * price) / self.leverage_limit
            
            # 加上一点缓冲 (Buffer)，防止扣除手续费后不足
            required_margin *= 1.02 
            
            if required_margin > usdt_free:
                # 钱不够！开始计算最大可买数量
                max_buy_value = usdt_free * self.leverage_limit * 0.98
                max_amount = max_buy_value / price
                
                print(f"⚠️ [Gate] Insufficient Balance! Downgrading: {target_amount:.4f} -> {max_amount:.4f}")
                return max_amount
            
            return target_amount
            
        except Exception as e:
            print(f"⚠️ [Gate] Balance Check Error: {e}")
            return 0.0 # 安全起见，查不到余额就不买

    def check_min_notional(self, symbol, amount, price):
        """
        [最小名义价值检查]
        防止因为金额太小(如 0.5U) 被交易所拒绝
        """
        notional = amount * price
        if notional < 5.1: # Bitget 通常最小 5U
            return False
        return True