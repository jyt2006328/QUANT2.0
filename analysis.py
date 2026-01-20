import pandas as pd
import re
import os

# === 修正: 指向 Systemd 的标准日志路径 ===
LOG_PATHS = [
    '/var/log/quantbot.log',      # Systemd 产生的日志 (优先)
    '/root/quant2.0/bot.log'      # 之前 nohup 产生的日志 (备选)
]
OUTPUT_CSV = '/root/quant2.0/trade_history.csv'

def parse_logs():
    trades = []
    # 匹配日志格式: 18:30:05 🚀 [NORMAL] BTC/USDT:USDT: BUY 0.0010 ($90.20)
    pattern = re.compile(r'(\d{2}:\d{2}:\d{2})\s+🚀\s+\[(.*?)\]\s+(.*?):\s+(BUY|SELL)\s+([\d.]+)\s+\(\$([\d.]+)\)')

    # 遍历所有可能的日志文件
    for log_file in LOG_PATHS:
        if not os.path.exists(log_file): continue
        print(f"📖 Reading log: {log_file}...")
        
        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    time_str, mode, symbol, side, amount, value = match.groups()
                    trades.append({
                        'time': time_str,
                        'mode': mode,
                        'symbol': symbol,
                        'side': side,
                        'amount': float(amount),
                        'value': float(value)
                    })

    if not trades:
        print("⚠️ No trades found in logs.")
        return

    df = pd.DataFrame(trades)
    print(f"✅ Found {len(df)} trades.")
    
    # 保存 CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"📁 Saved to {OUTPUT_CSV}")

    # 打印简单统计
    print("\n=== Summary ===")
    print(f"Total Turnover: ${df['value'].sum():.2f}")
    print("Trades by Strategy:")
    print(df['mode'].value_counts())

if __name__ == "__main__":
    parse_logs()
