import websocket
import threading
import json
import time
from collections import deque
import pandas as pd
import numpy as np

class BinanceDataStream:
    """
    [Data Core V3] Hybrid Stream (Ticker + Depth + AggTrade)
    新增: 逐笔成交 (Tick) 监控，计算资金流向微观因子
    """
    def __init__(self, symbols, max_len=1000):
        self.symbols = [s.split('/')[0].lower() + 'usdt' for s in symbols]
        self.base_url = "wss://fstream.binance.com/stream?streams="
        self.ws = None
        self.wst = None
        self.running = False
        self.reconnect_count = 0
        self.max_reconnect = 10
        
        # 基础数据
        self.tickers = {s: 0.0 for s in self.symbols}
        self.orderbooks = {s: {'bids': [], 'asks': []} for s in self.symbols}
        
        # [新增] 微观结构数据容器
        # 存储最近 N 笔成交，用于计算 VPIN / 大单流向
        self.trades = {s: deque(maxlen=2000) for s in self.symbols} 
        
        # [新增] 实时因子缓存
        self.micro_factors = {s: {'net_flow': 0.0, 'large_buy': 0, 'large_sell': 0} for s in self.symbols}

    def start(self):
        if self.running: 
            return
            
        self.running = True
        self._connect()

    def _connect(self):
        """独立的连接方法便于重连"""
        streams = []
        for s in self.symbols:
            streams.append(f"{s}@ticker")
            streams.append(f"{s}@depth5")
            streams.append(f"{s}@aggTrade") # [新增] 订阅逐笔成交
            streams.append(f"{s}@markPrice") # [新增] 订阅标记价格(含费率)
            
        conn_str = '/'.join(streams)
        full_url = self.base_url + conn_str
        
        print(f"🌊 [DataStream] Connecting... Monitoring {len(self.symbols)} assets (Deep View).")
        
        self.ws = websocket.WebSocketApp(
            full_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        self.wst = threading.Thread(target=self.ws.run_forever)
        self.wst.daemon = True
        self.wst.start()

    def stop(self):
        self.running = False
        if self.ws: 
            self.ws.close()

    def _on_open(self, ws):
        self.reconnect_count = 0  # 重置重连计数
        print("🟢 [DataStream] Connected (Tick Level).")

    def _on_message(self, ws, message):
        try:
            payload = json.loads(message)
            stream_name = payload.get('stream', '')
            data = payload.get('data', {})
            
            if not stream_name: 
                return
                
            symbol = stream_name.split('@')[0]
            event = stream_name.split('@')[1]
            
            # 1. Ticker
            if event == 'ticker':
                self.tickers[symbol] = float(data['c'])
                
            # 2. Depth
            elif event == 'depth5':
                # 验证数据格式
                if 'b' in data and 'a' in data:
                    self.orderbooks[symbol]['bids'] = data['b']
                    self.orderbooks[symbol]['asks'] = data['a']
            
            # 3. [新增] AggTrade (逐笔成交)
            elif event == 'aggTrade':
                self._process_trade(symbol, data)

            # 4. [新增] Mark Price & Funding Rate
            elif event == 'markPrice':
                # payload['r'] 是资金费率, 'P' 是标记价格
                funding_rate = float(payload.get('r', 0))
                # 我们把它存起来，之后策略要用
                self.tickers[symbol + '_funding'] = funding_rate
                    
        except Exception as e:
            print(f"⚠️ [DataStream] Message error: {e}")

    def _process_trade(self, symbol, trade):
        """
        处理每一笔成交，计算微观因子
        Trade Data: {
            "p": "Price",
            "q": "Quantity",
            "m": true(Buyer is Maker = 主动卖单) / false(Buyer is Taker = 主动买单)
        }
        """
        price = float(trade['p'])
        qty = float(trade['q'])
        is_buyer_maker = trade['m'] # True=卖单(由于买方挂单被吃), False=买单(主动吃挂单)
        
        # 定义方向: False(主动买) -> 1, True(主动卖) -> -1
        side = -1 if is_buyer_maker else 1
        volume_usd = price * qty
        
        # 1. 存入队列
        self.trades[symbol].append({
            'ts': time.time(),
            'side': side,
            'vol': volume_usd
        })
        
        # 2. 实时因子更新 (简单滑动窗口逻辑，比如最近1分钟)
        # 这里为了性能，我们做累加更新，或者简单的定期清洗
        # 简化版：统计大单
        if volume_usd > 10000: # >1万U 定义为大单
            if side == 1:
                self.micro_factors[symbol]['large_buy'] += 1
            else:
                self.micro_factors[symbol]['large_sell'] += 1
                
        # 计算净资金流 (Net Flow)
        self.micro_factors[symbol]['net_flow'] += (volume_usd * side)

    def _on_error(self, ws, error):
        print(f"❌ [DataStream] Error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"🔴 [DataStream] Closed. Code: {code}, Msg: {msg}")
        
        if self.running and self.reconnect_count < self.max_reconnect:
            self.reconnect_count += 1
            wait_time = min(30, 5 * self.reconnect_count)  # 指数退避
            print(f"🔄 [DataStream] Reconnecting in {wait_time}s... ({self.reconnect_count}/{self.max_reconnect})")
            time.sleep(wait_time)
            self._connect()
        else:
            print("🚫 [DataStream] Max reconnections reached.")

    # --- 接口方法 ---
    
    def get_latest_price(self, symbol):
        binance_sym = symbol.split('/')[0].lower() + 'usdt'
        return self.tickers.get(binance_sym, 0.0)

    def get_order_imbalance(self, symbol):
        binance_sym = symbol.split('/')[0].lower() + 'usdt'
        book = self.orderbooks.get(binance_sym)
        if not book or not book['bids'] or not book['asks']:
            return 0.0
        try:
            bid_qty = float(book['bids'][0][1])
            ask_qty = float(book['asks'][0][1])
            return (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-9)
        except: 
            return 0.0

    def get_micro_factors(self, symbol):
        """[新增] 获取微观因子"""
        binance_sym = symbol.split('/')[0].lower() + 'usdt'
        return self.micro_factors.get(binance_sym, {'net_flow': 0, 'large_buy': 0, 'large_sell': 0})
    
    def reset_micro_factors(self):
        """[新增] 重置微观统计 (用于每个周期的重新计数)"""
        for s in self.symbols:
            self.micro_factors[s] = {'net_flow': 0.0, 'large_buy': 0, 'large_sell': 0}
        # print("🧹 Micro factors reset.")

    def get_funding_rate(self, symbol):
        binance_sym = symbol.split('/')[0].lower() + 'usdt'
        return self.tickers.get(binance_sym + '_funding', 0.0)