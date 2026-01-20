import websocket
import threading
import json
import time
from datetime import datetime

# Binance U本位合约 WebSocket 地址
# 注意: 如果连不上，可能需要换成 wss://fstream.binance.com (全球) 
# 或者 wss://fstream-auth.binance.com
WS_URL = "wss://fstream.binance.com/ws"

# 订阅流名称 (小写)
# btcusdt@kline_1m (1分钟K线)
# btcusdt@depth5 (5档深度)
STREAMS = [
    "btcusdt@kline_1m",
    "btcusdt@depth5",
    "ethusdt@kline_1m"
]

def on_open(ws):
    print("🟢 [Binance] Connected.")
    
    # 构造订阅请求 (Binance 格式非常标准)
    req = {
        "method": "SUBSCRIBE",
        "params": STREAMS,
        "id": 1
    }
    ws.send(json.dumps(req))
    print(f"🚀 Sent Subscribe: {STREAMS}")

def on_message(ws, message):
    try:
        data = json.loads(message)
        
        # 1. 订阅回执
        if "result" in data and data["id"] == 1:
            print("✅ Subscription Successful!")
            return

        # 2. 处理数据流
        stream = data.get('s', '').lower() # symbol
        event = data.get('e') # event type
        
        ts = datetime.now().strftime("%H:%M:%S")

        # K线数据 (event = kline)
        if event == 'kline':
            k = data['k']
            is_closed = k['x'] # 是否收盘
            close_p = k['c']
            print(f"📊 [{ts}] {stream.upper()} [1m]: {close_price_format(close_p)} {'(Closed)' if is_closed else ''}")

        # 深度数据 (event = depthUpdate) 
        # 注意: depth5 推送没有 event 字段，直接是数据
        elif 'b' in data and 'a' in data:
            # 这是一个 depth 推送
            bid1 = data['b'][0][0]
            ask1 = data['a'][0][0]
            # 为了不刷屏，我们只打印 BTC 的深度
            if 'btc' in json.dumps(data).lower(): 
                 print(f"🌊 [{ts}] BINANCE_BOOK: Bid {bid1} | Ask {ask1}")

    except Exception as e:
        pass # 忽略解析错误

def close_price_format(price):
    return f"{float(price):.2f}"

def on_error(ws, error):
    print(f"❌ Error: {error}")

def on_close(ws, code, msg):
    print("🔴 Closed.")

if __name__ == "__main__":
    # 组合 URL: wss://fstream.binance.com/ws/stream1/stream2... 
    # 或者用单连接 + SUBSCRIBE 模式。这里用 SUBSCRIBE 模式。
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    print("⏳ Connecting to Binance Futures WS...")
    ws.run_forever()