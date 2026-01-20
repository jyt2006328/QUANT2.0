import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

# 缓存宏观数据
CACHE_FILE = 'data/macro_data.csv'

# === 新增：配置本地代理 ===
# 只有在本地运行(中国大陆)时需要，云服务器(海外)通常不需要
# 如果你的端口不是 7890，请自行修改
PROXY_URL = "http://127.0.0.1:7890" 

def update_macro_data():
    """
    下载宏观指标：
    ^IXIC: 纳斯达克
    DX-Y.NYB: 美元指数
    GC=F: 黄金期货
    """
    # 临时设置环境变量，强制 yfinance 走代理
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL
    
    tickers = ['^IXIC', 'DX-Y.NYB', 'GC=F']
    print("🌍 Fetching Macro Data from Yahoo Finance (via Proxy)...")
    
    try:
        # 增加 auto_adjust=True 消除警告
        # 增加 threads=False 防止并发过快被封
        df = yf.download(tickers, period="1y", interval="1d", progress=False, auto_adjust=True, threads=False)
        
        if df.empty:
            print("⚠️ Downloaded empty dataframe.")
            return pd.DataFrame()

        # yfinance 返回的是 MultiIndex，我们需要扁平化
        # 如果下载了多个ticker，'Close' 下面会有二级列名
        if isinstance(df.columns, pd.MultiIndex):
            # 尝试提取 Close 列
            try:
                closes = df['Close']
            except KeyError:
                # 新版 yfinance 可能直接返回混合列，或者 Close 就是顶层
                # 这是一个兼容性处理
                closes = df
        else:
            closes = df['Close'] if 'Close' in df.columns else df
        
        # 保存
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        path = os.path.join(base_dir, CACHE_FILE)
        closes.to_csv(path)
        print("✅ Macro data updated.")
        return closes
        
    except Exception as e:
        print(f"⚠️ Macro Fetch Error: {e}")
        return pd.DataFrame()

def get_macro_regime():
    """
    返回宏观状态评分 (-1 到 1)
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(base_dir, CACHE_FILE)
    
    if not os.path.exists(path):
        df = update_macro_data()
    else:
        # 如果文件太旧 (超过 12小时)，更新
        mtime = os.path.getmtime(path)
        if (datetime.now().timestamp() - mtime) > 43200:
            df = update_macro_data()
        else:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            
    if df is None or df.empty: return 0 
    
    score = 0
    
    # 纳指 (^IXIC)
    if '^IXIC' in df.columns:
        nasdaq = df['^IXIC'].ffill()
        ma20 = nasdaq.rolling(20).mean()
        if nasdaq.iloc[-1] > ma20.iloc[-1]:
            score += 1 
        else:
            score -= 1
            
    # 美元指数 (DX-Y.NYB)
    if 'DX-Y.NYB' in df.columns:
        dxy = df['DX-Y.NYB'].ffill()
        ma20 = dxy.rolling(20).mean()
        if dxy.iloc[-1] > ma20.iloc[-1]:
            score -= 1 # 美元强，币圈弱
        else:
            score += 1
            
    return score / 2.0

if __name__ == "__main__":
    update_macro_data()
    print(f"Current Macro Regime Score: {get_macro_regime()}")