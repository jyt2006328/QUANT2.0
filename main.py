import ccxt
import pandas as pd
import numpy as np
import time
import json
import os
import re
import traceback
from datetime import datetime, timedelta

# ================= 模块导入 =================
from strategies.sniper import SniperManagerV10, SniperStrategyV5
from strategies.trend import TrendFilter, VolatilityTrailingStop
from strategies.pair import PairTradingStrategy
from strategies.basket import BasketTradingStrategy
from strategies.qingyun import QingYunStrategy
from strategies.llm_adapter import LLMAdapter
from strategies.resonance import ResonanceManager
from strategies.ai_alpha import AIAlphaStrategy
from strategies.funding_arb import FundingArbitrageStrategy
from data.stream import BinanceDataStream

from models.alpha import AlphaModel, PredictionModel
from models.risk import FactorRiskModel
from models.optimizer import PortfolioOptimizer

from data.loaders import fetch_data_ohlcv, StrategyConfig
from utils.state import StateManager
from utils.dingtalk import DingTalkBot
from utils.execution_gate import ExecutionGate
from utils.fee_manager import FeeDiscountManager
from utils.maker import MakerManager
from utils.transfer import AssetTransferAgent

pd.set_option('future.no_silent_downcasting', True)

# ================= 全局配置 =================
API_KEY = os.getenv('BITGET_API_KEY', '')
SECRET = os.getenv('BITGET_SECRET', '')
PASSWORD = os.getenv('BITGET_PASSWORD', '')

if not API_KEY:
    print("⚠️ 警告: 未找到 API Key，请检查环境变量设置！")

# 核弹级过滤门槛 (USDT)
MIN_TRADE_VAL = 5.5

# 1. 现货实例
exchange = ccxt.bitget({
    'apiKey': API_KEY, 'secret': SECRET, 'password': PASSWORD,
    'enableRateLimit': True,
    'options': {'defaultType': 'spot', 'adjustForTimeDifference': True},
    'createMarketBuyOrderRequiresPrice': False 
})

# 2. 合约实例
exchange_swap = ccxt.bitget({
    'apiKey': API_KEY, 'secret': SECRET, 'password': PASSWORD,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

# ================= 辅助函数 =================
def get_real_positions(exchange_obj, config_symbols):
    """
    [CRITICAL FIX] 智能匹配逻辑
    1. 优先精准匹配 (针对 CCXT 标准化符号)
    2. 再次模糊匹配 (针对 原始符号)
    """
    position_map = {}
    try:
        raw_positions = exchange_obj.fetch_positions()
        
        for pos in raw_positions:
            if float(pos['contracts']) == 0:
                continue
            
            raw_sym = pos['symbol'] # e.g. 'ETH/USDT:USDT' 或 'ETHUSDT_UMCBL'
            matched_conf_sym = None
            
            # --- 逻辑 A: 精准匹配 (优先!) ---
            # 如果交易所返回的直接就在我们的配置里，直接通过！
            if raw_sym in config_symbols:
                matched_conf_sym = raw_sym
            
            # --- 逻辑 B: 模糊匹配 (备用) ---
            # 只有 A 失败了，才进行清洗
            else:
                clean_exch = raw_sym
                # 如果包含 '/'，说明是标准化格式但没匹配上（罕见），提取前半部分
                if '/' in raw_sym:
                    clean_exch = raw_sym.split('/')[0]
                else:
                    # 如果是 'ETHUSDT_UMCBL' 这种原始格式，才执行去后缀逻辑
                    clean_exch = raw_sym.split('_')[0].replace('USDT', '')
                
                # 遍历配置寻找对应的主币
                for conf_sym in config_symbols:
                    clean_conf = conf_sym.split('/')[0] # 提取 ETH
                    if clean_exch == clean_conf:
                        matched_conf_sym = conf_sym
                        break
            
            if matched_conf_sym:
                qty = float(pos['contracts'])
                if pos['side'] == 'short':
                    qty = -qty
                position_map[matched_conf_sym] = qty * float(pos['markPrice'])
                # print(f"   ✅ [POS LOADED] {raw_sym} -> {matched_conf_sym}")
            else:
                print(f"   ⚠️ [POS IGNORED] Unmatched: {raw_sym}")

        return position_map
    except Exception as e:
        print(f"❌ Position Fetch Error: {e}")
        return {}

# ================= 执行层 (Maker 升级版) =================
last_broadcast_weights = {}

# ================= 执行层 (防粉尘骚扰版) =================
last_broadcast_weights = {}
dust_alert_tracker = {}  # [新增] 用于记录上次报警时间

def execute_orders(target_weights, equity, current_positions, gatekeeper, ding_bot, maker_agent, mode="NORMAL", is_dry_run=True):
    global last_broadcast_weights, dust_alert_tracker
    prefix = f"🚀 [{mode}]"
    
    current_fingerprint = {k: round(v, 4) for k, v in target_weights.items() if abs(v) > 1e-5}
    last_fingerprint = {k: round(v, 4) for k, v in last_broadcast_weights.items() if abs(v) > 1e-5}
    
    # 只有在仓位真的有变化时才执行
    if is_dry_run and current_fingerprint == last_fingerprint: return 
    last_broadcast_weights = target_weights.copy()
    
    orders_buffer = []

    # --- Phase 1: 订单计算 ---
    for sym, w in target_weights.items():
        try:
            tgt_val = equity * w
            cur_val = current_positions.get(sym, 0.0)
            diff = tgt_val - cur_val
            
            # 核弹级过滤 (价值过滤)
            if abs(diff) < MIN_TRADE_VAL: 
                continue 

            ticker = exchange.fetch_ticker(sym)
            price = ticker['last']
            if price <= 0: continue

            raw_amt = abs(diff) / price
            side = 'buy' if diff > 0 else 'sell'
            
            # 标记：是否是"清仓/减仓"操作
            # 如果目标价值很小 (<5U)，且当前有持仓，说明是想平仓
            is_closing = (abs(tgt_val) < 5.0) and (abs(cur_val) > 5.0)
            is_close = (w == 0)  # 同时保留原来的清仓标记
            
            orders_buffer.append({
                'sym': sym, 'side': side, 'raw_amt': raw_amt,
                'price': price, 'val_usd': raw_amt * price,
                'is_closing': is_closing,  # 用于智能粉尘检测
                'is_close': is_close,       # 用于清理成功后的记录移除
                'diff': diff                # 保留用于其他逻辑
            })
        except Exception as e:
            print(f"⚠️ Pre-calc Error {sym}: {e}")

    # --- Phase 2: 排序 (Sell First) ---
    orders_buffer.sort(key=lambda x: (0 if x['side'] == 'sell' else 1, -x['val_usd']))

    # --- Phase 3: 执行 (Maker) ---
    for order in orders_buffer:
        sym = order['sym']
        price = order['price']
        side = order['side']
        raw_amt = order['raw_amt']
        is_closing = order['is_closing']
        is_close = order['is_close']
        
        try:
            # A. 卫士检查
            if not is_dry_run:
                gatekeeper.check_and_set_leverage(sym)
                if side == 'buy':
                    valid_amt = gatekeeper.get_valid_amount(sym, raw_amt, price)
                else:
                    valid_amt = raw_amt
            else:
                valid_amt = raw_amt

            # B. 最小名义价值检查 (USDT维度)
            if not gatekeeper.check_min_notional(sym, valid_amt, price): continue

            # C. [CRITICAL FIX] 智能粉尘处理逻辑
            try:
                market = exchange.market(sym)
                min_amount = market.get('limits', {}).get('amount', {}).get('min')
                
                if min_amount and valid_amt < min_amount:
                    # 只有当这是"清仓单"时，我们才视为粉尘滞留问题
                    if is_closing:
                        current_time = time.time()
                        last_alert = dust_alert_tracker.get(sym, 0)
                        
                        # 冷却时间 1小时 (3600秒)
                        if current_time - last_alert > 3600:
                            msg = f"🧹 [Dust Stuck] {sym} 持仓 {valid_amt:.4f} 小于最小交易量 {min_amount}，机器无法卖出，请手动平仓！"
                            print(msg)  # 控制台打印
                            if not is_dry_run:
                                ding_bot.send_markdown("⚠️ 粉尘滞留警报", msg)
                            dust_alert_tracker[sym] = current_time
                        else:
                            # 冷却期内，完全静默，不打印任何日志
                            pass 
                    else:
                        # 如果是开仓或加仓，普通打印即可，不用发钉钉
                        print(f"   ⚠️ Ignored {sym}: Amount {valid_amt:.4f} < Min {min_amount}")
                    
                    continue  # 跳过下单
            except Exception as e:
                print(f"   ⚠️ Market Info Error {sym}: {e}")
                pass  # 获取失败则跳过检查

            # D. 精度修正 (关键防崩点)
            try:
                final_amt = float(exchange.amount_to_precision(sym, valid_amt))
            except Exception as e:
                print(f"   ⚠️ Precision Error {sym}: {e} (Amount too small?)")
                continue  # 跳过此订单，不要 Crash！

            if final_amt <= 0: continue
            
            val_usd = final_amt * price
            log_icon = "💸"
            if val_usd > 100: log_icon = "💰 BIG"
            
            timestamp = datetime.now().strftime("%H:%M:%S")
            print(f"{timestamp} {log_icon} {prefix} {sym}: {side.upper()} {final_amt:.4f} (${val_usd:.2f})")
            
            if not is_dry_run:
                try:
                    # [核心升级] 调用 MakerManager
                    success = maker_agent.execute_order(sym, side, final_amt)
                    
                    if success:
                        print(f"   ✅ Maker Executed: {sym}")
                        # 如果成功执行了清仓单，从粉尘警报记录中移除
                        if is_close and sym in dust_alert_tracker:
                            del dust_alert_tracker[sym]
                            print(f"   🧹 {sym} 粉尘已清理，移除警报记录")
                            
                        ding_bot.send_trade_alert(sym, side, price, final_amt, val_usd, mode + " (Maker)")
                    else:
                        print(f"   ❌ Maker Failed: {sym}, fallback to market order")
                        # Maker 失败时回退到市价单
                        exchange.create_order(sym, 'market', side, final_amt)
                        ding_bot.send_trade_alert(sym, side, price, final_amt, val_usd, mode + " (Market Fallback)")
                    
                    # 串行执行，稍微休息一下防止 API 拥堵
                    time.sleep(0.5)
                    
                except Exception as order_e:
                    print(f"   ❌ Order Fail ({sym}): {order_e}")
                    # 只有资金不足时才严重警告，其他错误(如API抖动)忽略
                    if "Insufficient balance" in str(order_e):
                        ding_bot.send_markdown("⚠️ 资金不足", f"无法执行 {sym} {side}，请检查余额。")

        except Exception as e:
            # 捕获单个订单的所有其他未知错误，确保循环不中断
            print(f"❌ Process Error ({sym}): {e}")

# ================= 主程序 =================
def run_bot():
    print("🔥 QuantBot V18 (AI + Hybrid Data) Initialized.")
    
    # 1. 初始化
    strat_config = StrategyConfig()
    state_manager = StateManager()
    
    sniper_strat = SniperStrategyV5()
    sniper_manager = SniperManagerV10(sl=-0.03, time_limit=15)
    resonance_manager = ResonanceManager(leverage_scale=0.5) 
    qingyun_arbiter = QingYunStrategy()
    # 初始化组件 (传入不同的 exchange 对象)
    # MakerManager 用 swap 对象 (因为大部分策略还是合约)
    maker_agent = MakerManager(exchange_swap, max_attempts=2, wait_seconds=5)
    
    # 资金划转用 spot 对象 (通常 API 在 spot 端更全，或者无所谓)
    transfer_agent = AssetTransferAgent(exchange_spot)
    
    # 套利策略：同时持有 现货剑 和 合约盾
    funding_arb = FundingArbitrageStrategy(
        spot_exchange=exchange_spot, 
        swap_exchange=exchange_swap, 
        transfer_agent=transfer_agent,
        maker_agent=maker_agent
    )
    
    # [新增] 初始化 AI 策略
    ai_strategy = AIAlphaStrategy(model_path='models/xgb_model.pkl', top_n=2)

    # [新增] 初始化 Binance 数据流 (Hybrid Architecture)
    # 我们监控 Config 里的币种
    initial_cfg = strat_config.load()
    SYMBOLS = initial_cfg.get('symbols', [])
    binance_stream = BinanceDataStream(SYMBOLS)
    try:
        binance_stream.start() # 启动子线程
        print("🌊 Binance Data Stream Started.")
    except Exception as e:
        print(f"⚠️ Binance Stream Start Failed: {e}")
    
    trend_filter = TrendFilter()
    stop_monitor = VolatilityTrailingStop(multiplier=4.0)
    pair_model = PairTradingStrategy()
    basket_model = BasketTradingStrategy()
    
    alpha_model = AlphaModel()
    pred_model = PredictionModel(train_window=700)
    risk_engine = FactorRiskModel()
    portfolio_opt = PortfolioOptimizer()
    
    initial_cfg = strat_config.load()
    ding_bot = DingTalkBot(initial_cfg.get('dingtalk', {}))
    llm_brain = LLMAdapter('regime.json')
    fee_manager = FeeDiscountManager(exchange, coin='BGB')
    gatekeeper = ExecutionGate(exchange_swap, leverage_limit=int(initial_cfg.get('leverage_limit', 5)))

    state_manager.load_state(sniper_manager, stop_monitor, resonance_manager)

    sniper_strat = SniperStrategyV5()
    
    # 缓存
    last_low_freq_weights = pd.Series()
    last_qingyun_weights = pd.Series()
    last_sop_mode = None
    # [Cache] 青云偏置缓存
    cached_qingyun_bias = 0
    last_ai_weights = pd.Series()  # [新增] AI 信号缓存
    
    # 配置
    SYMBOLS = initial_cfg.get('symbols', [])
    TIMEFRAME_LOW = '1h'
    TIMEFRAME_HIGH = '5m'
    
    while True:
        try:
            now = datetime.now()
            cfg = strat_config.load()
            SYMBOLS = cfg.get('symbols', SYMBOLS)
            dry_run = cfg.get('dry_run', True)
            
            if cfg.get('system_status') != 'active':
                print("💤 Paused."); time.sleep(60); continue

            # --- 1. LLM & SOP ---
            sop_data = llm_brain.get_sop_config()
            mults = sop_data['multipliers']
            current_mode = sop_data['raw'].get('market_mode')
            
            if current_mode != last_sop_mode:
                ding_bot.send_sop_update(current_mode, sop_data['raw'].get('risk_preference'), mults['trend_w_mult'])
                last_sop_mode = current_mode
            
            # --- 2. 账户同步 ---
            try:
                balance = exchange.fetch_balance()
                equity = float(balance['total']['USDT'])
                current_positions = get_real_positions(exchange, SYMBOLS)
                if now.minute == 0 and not dry_run: fee_manager.check_and_replenish()
            except:
                print("⚠️ Account Sync Failed"); time.sleep(5); continue

            # --- 3. 数据层 ---
            # A. 5m 数据 (Sniper 核心)
            hf_data = fetch_data_ohlcv(SYMBOLS, TIMEFRAME_HIGH, limit=120, exchange=exchange)
            
            # [新增] 收集实时盘口数据 (用于 Sniper V4)
            obi_data = {}
            for sym in SYMBOLS:
                obi = binance_stream.get_order_imbalance(sym)
                obi_data[sym] = obi

            micro_data = {}
            for sym in SYMBOLS:
                micro_data[sym] = binance_stream.get_micro_factors(sym)

            # 可选: 打印一下看看 (调试用，稳定后可删)
            if now.second < 5 and now.minute % 5 == 0:
                print(f"🔬 OBI Sample: {str({k: round(v, 2) for k,v in obi_data.items() if v!=0})}")

            # [新增] 实时监控 Binance 微观数据 (日志展示)
            # 这里的 OBI (Order Book Imbalance) 是未来 Sniper 进化的关键
            try:
                # 随便取一个币打印，证明数据流活着
                sample_sym = SYMBOLS[0]
                binance_price = binance_stream.get_latest_price(sample_sym)
                binance_obi = binance_stream.get_order_imbalance(sample_sym)
                # 只有在非整点的时候打印，避免日志太乱
                if now.minute % 5 == 0 and now.second < 10:
                    print(f"👀 [Hybrid View] {sample_sym} Binance: {binance_price} | OBI: {binance_obi:.4f}")
            except: pass

            # B. [优化] 青云偏置准实时更新
            # 策略：每小时拉取所有币种 1h 数据(重)，每分钟拉取 BTC 1h 数据(轻)来更新 Bias
            # 这样既有宏观数据，又有实时 Trend 判断
            
            lf_data_dict = {}
            need_full_refresh = (now.minute == 0 or last_low_freq_weights.empty)
            
            if need_full_refresh:
                # 整点：全量更新
                lf_data_dict = fetch_data_ohlcv(SYMBOLS, TIMEFRAME_LOW, limit=1200, exchange=exchange, only_close=False)
                # 更新缓存
                cached_qingyun_bias = qingyun_arbiter.get_market_bias(lf_data_dict)
                print(f"\n⏰ [Hourly] Full Refresh. QingYun Bias: {cached_qingyun_bias}")
            else:
                # 非整点：只更新 BTC (锚点) 1h 数据来刷新 Bias
                # 假设配置里的第一个是主力 BTC
                anchor_sym = SYMBOLS[0]
                try:
                    anchor_1h = fetch_data_ohlcv([anchor_sym], TIMEFRAME_LOW, limit=200, exchange=exchange, only_close=False)
                    # 临时构造一个只包含 BTC 的字典传给 arbiter，只要 arbiter 逻辑允许
                    # qingyun.py 的 get_market_bias 默认 target_symbol='BTC/USDT:USDT'
                    # 我们传入包含最新 BTC 数据的字典即可
                    temp_dict = {anchor_sym: anchor_1h[anchor_sym]}
                    cached_qingyun_bias = qingyun_arbiter.get_market_bias(temp_dict, target_symbol=anchor_sym)
                except Exception as e:
                    pass # 如果更新失败，沿用旧的 cached_qingyun_bias

            # C. 共振数据 (按需拉取)
            focus_symbols = [s for s in sop_data['raw'].get('focus_assets', []) if s in SYMBOLS]
            if not focus_symbols: focus_symbols = [s for s in SYMBOLS if 'BTC' in s or 'ETH' in s]
            
            d1m_map = fetch_data_ohlcv(focus_symbols, '1m', limit=100, exchange=exchange)
            d30m_map = fetch_data_ohlcv(focus_symbols, '30m', limit=100, exchange=exchange)
            
            res_data_pack = {}
            for sym in focus_symbols:
                if sym in d1m_map and sym in d30m_map and sym in hf_data:
                    res_data_pack[sym] = {'1m': d1m_map[sym], '5m': hf_data[sym], '30m': d30m_map[sym]}

            # --- 4. 策略计算 ---
            
            # A. Sniper (使用最新的 bias 和 OBI 数据)
            z_adj = mults['sniper_threshold_adj']
            # [Change] 传入 obi_dict=obi_data
            sniper_w, sniper_active = sniper_manager.check_signals(
                hf_data, 
                sniper_strat, 
                obi_dict=obi_data,  # <-- 注入微观数据
                micro_dict=micro_data,  # <-- [Update] 传入资金流
                z_threshold_adjustment=z_adj
            )
            
            # B. Resonance
            res_alloc = cfg['strategy_allocation'].get('resonance_weight', 0.0)
            
            # 只有当权重 > 0 时才运行策略，节省计算资源
            if res_alloc > 0:
                raw_res_w, res_active = resonance_manager.check_signals(
                    res_data_pack, 
                    current_positions=current_positions
                )
                res_w = raw_res_w * res_alloc
            else:
                res_w = pd.Series(0.0, index=SYMBOLS)
                res_active = False
            
            # [Phase 26 Fusion] 获取资金费率数据
            # 注意：stream.py 需要确保 funding_rates 字典是实时更新的
            funding_data = binance_stream.funding_rates 
            
            # 执行套利扫描 (独立于 Trend 策略)
            # 这部分是额外的 Alpha，不占用 Trend 的权重
            funding_arb.scan_and_execute(funding_data, current_prices)

            # [CRITICAL FIX] C. 唤醒 QingYun (带缓存)
            qingyun_active = False  # 标记
            # 1. 准备数据: 1H 用于定方向 (使用缓存数据，非整点也可以用)
            # 只要 lf_data_dict 里有数据就行 (非整点时它保存的是上个整点的数据，这是可以接受的 Bias)
            if lf_data_dict:
                lf_prices = pd.DataFrame({s: df['c'] for s, df in lf_data_dict.items()}).ffill()
                
                # 2. 实时计算: 传入最新的 5m 数据 (hf_data)
                # 这会检查当前的 5m K线是否触碰了 EMA
                raw_qingyun = qingyun_arbiter.compute_signals(hf_data, lf_prices)
                
                qy_alloc = cfg['strategy_allocation'].get('qingyun_weight', 0.5)
                
                # 不再缓存! 每一分钟都是新的判断!
                qingyun_w = raw_qingyun * qy_alloc
                
                if qingyun_w.abs().sum() > 0:
                    qingyun_active = True
                    # 打印日志
                    print(f"☁️ [QingYun SIGNAL] {qingyun_w[qingyun_w!=0].to_dict()}")
            else:
                qingyun_w = pd.Series(0.0, index=SYMBOLS)
                lf_prices = pd.DataFrame({s: df['c'] for s, df in lf_data_dict.items()}).ffill()
                raw_qingyun = qingyun_arbiter.compute_signals(hf_data, lf_prices)
                qy_alloc = cfg['strategy_allocation'].get('qingyun_weight', 0.5)

                # 更新缓存
                last_qingyun_weights = raw_qingyun * qy_alloc
                if last_qingyun_weights.abs().sum() > 0:
                    print(f"☁️ [QingYun UPDATE] Signals: {last_qingyun_weights[last_qingyun_weights!=0].to_dict()}")

            qingyun_w = last_qingyun_weights
            if qingyun_w.abs().sum() > 0:
                qingyun_active = True


            # D. Low Freq (Trend/Pair/Basket/AI) - 仅在整点或全量刷新时计算
            if need_full_refresh:
                # === [NEW] AI Alpha Strategy ===
                # 获取 AI 权重建议 (传入 lf_data_dict - 1h 数据)
                try:
                    current_ai_alloc = cfg['strategy_allocation'].get('ai_weight', 0.0)
                    if current_ai_alloc > 0:
                        raw_ai = ai_strategy.generate_signals(lf_data_dict)
                        
                        # === [Phase 26] 微观资金流融合 (Micro-Flow Fusion) ===
                        # 逻辑: 如果资金流向与 AI 信号反向，则降权
                        
                        adjusted_ai = raw_ai.copy()
                        
                        for sym, w in raw_ai.items():
                            if w == 0: continue
                            
                            # 获取该币种的微观因子
                            micro = binance_stream.get_micro_factors(sym)
                            net_flow = micro['net_flow']
                            
                            # 定义流向阈值 (例如 1分钟净流出 5万U 算显著)
                            FLOW_THRESHOLD = 50000 
                            
                            # Case A: AI 做多，但主力在卖 (背离)
                            if w > 0 and net_flow < -FLOW_THRESHOLD:
                                print(f"🛡️ [AI FILTER] {sym} LONG signal weakened by Outflow: {net_flow:.0f} U")
                                adjusted_ai[sym] = w * 0.5 # 降权 50%
                                
                            # Case B: AI 做空，但主力在买 (背离)
                            elif w < 0 and net_flow > FLOW_THRESHOLD:
                                print(f"🛡️ [AI FILTER] {sym} SHORT signal weakened by Inflow: +{net_flow:.0f} U")
                                adjusted_ai[sym] = w * 0.5 # 降权 50%
                        
                        # 注意：这里只缓存原始信号，不乘权重！
                        last_ai_raw_signal = adjusted_ai 
                        
                        if last_ai_raw_signal.abs().sum() > 0:
                            # 打印时带上资金流标记
                            print(f"🧠 [AI + FLOW] Signals: {last_ai_raw_signal[last_ai_raw_signal!=0].to_dict()}")
                            
                    else:
                        last_ai_raw_signal = pd.Series(0.0, index=SYMBOLS)
                except Exception as e:
                    print(f"⚠️ AI Strategy Error: {e}")
                    last_ai_raw_signal = pd.Series(0.0, index=SYMBOLS)
                
                # 1. Trend
                trend_w = pd.Series(0.0, index=SYMBOLS)
                t_alloc = cfg['strategy_allocation'].get('trend_weight', 0.0)
                
                if t_alloc > 0:
                    # 依然使用 AI 的因子(AlphaModel)做基础预测，但用 TrendFilter 严格过滤
                    f = alpha_model.compute_signals_with_genetic(lf_data_dict)
                    p = pred_model.predict(f, pd.DataFrame({s:df['c'] for s,df in lf_data_dict.items()}).ffill())
                    
                    if p.sum() > 0:
                        raw_trend = (p.clip(lower=0) / p.sum()) * t_alloc
                        # 传入实时 bias
                        trend_w = trend_filter.filter_signals(raw_trend, lf_data_dict, macro_score=float(cached_qingyun_bias))
                        # 止损检查
                        curr_vols = pd.DataFrame({s:df['c'] for s,df in lf_data_dict.items()}).pct_change().rolling(30).std().iloc[-1]
                        for sym in SYMBOLS:
                             if stop_monitor.update_and_check(sym, lf_data_dict[sym]['c'].iloc[-1], curr_vols.get(sym, 0.01), trend_w.get(sym,0)>0):
                                 trend_w[sym] = 0.0

                # 2. Basket
                basket_w = pd.Series(0.0, index=SYMBOLS)
                b_alloc = cfg['strategy_allocation'].get('basket_weight', 0.0)
                if b_alloc > 0:
                    basket_w = basket_model.compute_signals(pd.DataFrame({s:df['c'] for s,df in lf_data_dict.items()}).ffill(), cfg['basket_config']) * b_alloc

                # 3. Pair
                pair_w = pd.Series(0.0, index=SYMBOLS)
                p_alloc = cfg['strategy_allocation'].get('pair_weight', 0.0)
                if p_alloc > 0:
                    pair_w = pair_model.compute_signals(pd.DataFrame({s:df['c'] for s,df in lf_data_dict.items()}).ffill(), cfg['pairs_config']) * p_alloc

            # [FIX] 实时读取最新 AI 权重并计算最终权重
            current_ai_alloc = cfg['strategy_allocation'].get('ai_weight', 0.0)
            # 每分钟实时计算最终权重 (Signal * Current Weight)
            # 这样你把权重改成 0，下一分钟 ai_w 就立刻变 0
            ai_w = last_ai_raw_signal * current_ai_alloc
            
            # --- [新增] AI 策略的每分钟移动止损 ---
            # 即使不是整点，我们也要监控 AI 的持仓风险
            # 我们复用 stop_monitor (VolatilityTrailingStop)
            
            # 1. 计算当前波动率 (用于止损阈值)
            # 简单的 ATR 估算或者直接用最近 1h 的 std
            # 这里为了性能，我们每分钟只取最近 60 个 5m K线算一下波动
            vol_map = {}
            for sym in SYMBOLS:
                if sym in hf_data and not hf_data[sym].empty:
                    # 使用 5m 数据的最近 20 根计算标准差作为波动率基准
                    vol_map[sym] = hf_data[sym]['c'].pct_change().rolling(20).std().iloc[-1]
            
            # 2. 遍历所有 AI 持仓进行检查
            for sym, w in ai_w.items():
                if w == 0: 
                    # 如果 AI 没持仓，也要告诉 stop_monitor 清理旧状态
                    stop_monitor.update_and_check(sym, 0, 0, side='none')
                    continue
                
                # 获取当前价格和波动率
                if sym not in hf_data: continue
                current_price = hf_data[sym]['c'].iloc[-1]
                current_vol = vol_map.get(sym, 0.01) # 默认 1% 波动
                if pd.isna(current_vol): current_vol = 0.01
                
                # [关键] 检查止损
                # update_and_check 返回 True 表示触发止损
                
                # 确定方向
                side = 'long' if w > 0 else 'short'
                
                # 调用更新后的接口
                is_stop_hit = stop_monitor.update_and_check(sym, current_price, current_vol, side=side)
                
                if is_stop_hit:
                    print(f"🛡️ [AI STOP] {sym} ({side}) Trailing Stop Hit! Force Closing.")
                    # 强制将当次循环的权重置 0 (平仓)
                    # 注意：这里不修改 last_ai_raw_signal 缓存，只修改当前的 ai_w
                    # 这样下一小时重算时，如果 AI 还看多，会重新尝试（除非再次止损）
                    ai_w[sym] = 0.0
                    
                    # [新增] 既然止损了，就要告诉 AI 策略层"忘掉这个币"
                    # 否则下一个整点它可能又选进来了
                    # (这需要给 AI 策略加一个 remove_position 接口，暂时先靠下一轮 Buffer 逻辑)
            
            # ----------------------------------------

            # [聚合] 
            # 此时 ai_w 是经过止损检查的安全权重
            raw_combined = ai_w.add(trend_w, fill_value=0)\
                               .add(basket_w, fill_value=0)\
                               .add(pair_w, fill_value=0)
                
            active_assets = raw_combined[raw_combined.abs() > 1e-5].index.tolist()
                
            if active_assets:
                lf_ret = pd.DataFrame({s: df['c'] for s, df in lf_data_dict.items()}).pct_change().fillna(0)
                port_vol = risk_engine.compute_portfolio_risk(raw_combined, lf_ret)
                scaler = min(cfg['leverage_limit'], cfg['target_vol'] / (port_vol + 1e-9))
                last_low_freq_weights = raw_combined * scaler
            else:
                last_low_freq_weights = pd.Series(0.0, index=SYMBOLS)
            binance_stream.reset_micro_factors()
            # === [CRITICAL FIX] 资金分仓风控 ===
            # 获取各策略的资金上限 (默认 1.0 即 100%)
            alloc_cfg = cfg['strategy_allocation']
            
            def apply_cap(weights, cap_ratio):
                if weights.abs().sum() == 0: return weights
                # 当前策略占用的总杠杆倍数
                current_lev = weights.abs().sum()
                # 允许的最大杠杆 = 总杠杆限制 * 资金占比
                allowed_lev = cfg['leverage_limit'] * cap_ratio
                
                if current_lev > allowed_lev:
                    scaler = allowed_lev / current_lev
                    return weights * scaler
                return weights

            # 对每个策略施加紧箍咒
            sniper_w = apply_cap(sniper_w, alloc_cfg.get('sniper_cap', 1.0))
            res_w = apply_cap(res_w, alloc_cfg.get('resonance_cap', 1.0))
            qingyun_w = apply_cap(qingyun_w, alloc_cfg.get('qingyun_cap', 1.0))
            
            # [更新] AI 分仓限制 - 使用最新的 ai_w
            ai_w = apply_cap(ai_w, alloc_cfg.get('ai_cap', 1.0))

            # 5. 聚合
            final_weights = last_low_freq_weights.add(sniper_w, fill_value=0)\
                                                 .add(res_w, fill_value=0)\
                                                 .add(qingyun_w, fill_value=0)\
                                                 .add(ai_w, fill_value=0)\
                                                 .fillna(0.0)
            
            # [Phase 28 Final] 全局总杠杆熔断 (Global Leverage Cap)
            # 防止多个策略叠加导致总仓位爆炸
            # 假设我们只允许总持仓价值不超过本金的 3 倍 (保守) 或 5 倍 (激进)
            GLOBAL_MAX_LEVERAGE = 3.0 
            
            current_total_lev = final_weights.abs().sum()
            
            if current_total_lev > GLOBAL_MAX_LEVERAGE:
                scale_down = GLOBAL_MAX_LEVERAGE / current_total_lev
                print(f"🛡️ [RISK CONTROL] Total leverage {current_total_lev:.2f} > {GLOBAL_MAX_LEVERAGE}. Scaling down by {scale_down:.2f}")
                final_weights = final_weights * scale_down

            # [FIX] 标签显示逻辑修正 (修复 UnboundLocalError)
            exec_mode = "NORMAL"
            
            # 计算各策略的贡献度 (绝对值之和)
            total_w = final_weights.abs().sum()
            ai_w_sum = ai_w.abs().sum()
            sniper_w_sum = sniper_w.abs().sum()
            
            if total_w > 0:
                # === A. 有开仓信号 ===
                # 如果 AI 的权重占比超过 50%，或者 AI 有信号且 Sniper 没信号
                if ai_w_sum > 0.01 and ai_w_sum >= sniper_w_sum:
                    exec_mode = "AI-ALPHA"
                elif sniper_active:
                    exec_mode = "SNIPER"
                elif qingyun_active:
                    exec_mode = "QINGYUN"
                elif res_active:
                    exec_mode = "RESONANCE"
                else:
                    exec_mode = "NORMAL" # 默认 (如 Trend/Basket)
            else:
                # === B. 无开仓信号 (总权重为0) ===
                # 检查当前是否还有持仓 (判断是空仓还是正在平仓)
                has_pos = any([abs(v) > 5 for v in current_positions.values()])
                
                if has_pos:
                    exec_mode = "CLOSING" # 正在平仓
                else:
                    exec_mode = "IDLE"    # 空仓观望
            
            # [Call] 执行函数
            execute_orders(
                final_weights, 
                equity, 
                current_positions, 
                gatekeeper, 
                ding_bot, 
                maker_agent, 
                mode=exec_mode,  # <--- 确保这里传入了新的 exec_mode
                is_dry_run=dry_run
            )

            state_manager.save_state(sniper_manager, stop_monitor, resonance_manager)
            
            print(".", end="", flush=True)
            time.sleep(cfg['heartbeat'])

        except KeyboardInterrupt:
            binance_stream.stop()  # [新增] 优雅退出
            print("\n🛑 Stopped."); break
        except Exception as e:
            print(f"\n💥 Crash: {traceback.format_exc()}")
            ding_bot.send_markdown("CRASH", str(e))
            time.sleep(30)

if __name__ == "__main__":
    run_bot()