import ccxt
import time

class AssetTransferAgent:
    """
    [资金搬运工]
    逻辑源自: transfer_within_accounts.py
    职责: 在 Spot (现货) 和 Swap (U本位合约) 之间划转资金，为套利做准备。
    """
    def __init__(self, exchange):
        self.exchange = exchange

    def transfer_to_futures(self, currency, amount):
        """
        现货 -> 合约 (用于建立套利头寸)
        Type 1 in Binance API
        """
        return self._safe_transfer(currency, amount, 'spot', 'swap')

    def transfer_to_spot(self, currency, amount):
        """
        合约 -> 现货 (用于平仓后归集)
        Type 2 in Binance API
        """
        return self._safe_transfer(currency, amount, 'swap', 'spot')

    def _safe_transfer(self, currency, amount, from_type, to_type):
        try:
            # Bitget/Binance 通用 CCXT 接口
            # params 里的 type 可能需要根据具体交易所微调，CCXT 做了封装通常不需要
            print(f"💸 [Transfer] Moving {amount} {currency} from {from_type} to {to_type}...")
            
            # 精度截断，防止因为小数位过多导致划转失败
            amount = float(self.exchange.amount_to_precision(f"{currency}/USDT", amount))
            
            if amount <= 0:
                print("⚠️ [Transfer] Amount too small.")
                return False

            self.exchange.transfer(currency, amount, from_type, to_type)
            print("✅ Transfer Success.")
            return True
            
        except Exception as e:
            print(f"❌ [Transfer] Failed: {e}")
            return False
            
    def check_spot_balance(self, currency):
        try:
            bal = self.exchange.fetch_balance({'type': 'spot'})
            return float(bal.get(currency, {}).get('free', 0.0))
        except:
            return 0.0