import time
from ccxt import NetworkError, RateLimitExceeded, ExchangeError, RequestTimeout

def retry_request(func, *args, max_retries=5, **kwargs):
    """
    [通讯兵] 智能重试装饰器/包装器
    策略:
    1. RateLimit -> 休眠并指数退避
    2. Timeout -> 立即重试
    3. Critical Error -> 抛出异常
    """
    retries = 0
    delay = 1.0
    
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
            
        except RateLimitExceeded:
            # 触发限频，休眠时间加倍
            sleep_time = delay * (2 ** retries)
            print(f"   ⚠️ Rate Limit! Cooling down {sleep_time}s...")
            time.sleep(sleep_time)
            retries += 1
            
        except (RequestTimeout, NetworkError) as e:
            # 网络波动，温和重试
            print(f"   ⚠️ Network/Timeout: {e}. Retrying ({retries+1}/{max_retries})...")
            time.sleep(1)
            retries += 1
            
        except ExchangeError as e:
            # 交易所逻辑错误 (如参数不对)，通常重试没用，直接抛出
            print(f"   ❌ Exchange Error: {e}")
            raise e
            
        except Exception as e:
            print(f"   ❌ Unknown Error: {e}")
            raise e
            
    print("   ❌ Max retries exceeded.")
    return None