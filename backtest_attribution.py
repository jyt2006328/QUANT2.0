import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import json
import os
from datetime import datetime, timedelta

# 消除 Pandas Warning
pd.set_option('future.no_silent_downcasting', True)

# 导入策略模块
from strategies.sniper import SniperManagerV10, SniperStrategyV3
from strategies.trend import TrendFilter, VolatilityTrailingStop
from strategies.pair import PairTradingStrategy
from strategies.basket import BasketTradingStrategy
from models.alpha import AlphaModel, PredictionModel
from models.risk import FactorRiskModel
from models.optimizer import PortfolioOptimizer
from data.loaders import StrategyConfig

# ================= 虚拟交易所 (含滑点与费率) =================

class VirtualExchange:
    def __init__(self, initial_capital=10000.0, fee_rate=0.0006, slippage=0.001):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.fee_rate = fee_rate   
        self.slippage = slippage   
        self.positions = {}        
        self.equity_curve = []
        self.trade_log = []

    def update_mark_prices(self, current_prices):
        equity = self.cash
        for sym, amt in self.positions.items():
            if sym in current_prices and not np.isnan(current_prices[sym]):
                equity += amt * current_prices[sym]
        return equity

    def execute_orders(self, target_weights, current_prices, timestamp):
        total_equity = self.update_mark_prices(current_prices)
        
        for sym, w in target_weights.items():
            if sym not in current_prices or np.isnan(current_prices[sym]): continue
            
            raw_price = current_prices[sym]
            target_val = total_equity * w
            current_amt = self.positions.get(sym, 0.0)
            current_val = current_amt * raw_price
            diff_val = target_val - current_val
            
            if abs(diff_val) < 5.0: continue
            
            if diff_val > 0: 
                exec_price = raw_price * (1 + self.slippage)
            else: 
                exec_price = raw_price * (1 - self.slippage)
            
            amt_change = diff_val / exec_price
            trade_value = abs(amt_change * exec_price)
            fee = trade_value * self.fee_rate
            
            self.cash -= fee
            self.positions[sym] = current_amt + amt_change
            self.cash -= (amt_change * exec_price)
            
            self.trade_log.append({
                "time": timestamp, "symbol": sym, "action": "BUY" if diff_val > 0 else "SELL",
                "price": exec_price, "amount": amt_change, "fee": fee
            })

        final_equity = self.update_mark_prices(current_prices)
        self.equity_curve.append({"time": timestamp, "equity": final_equity})
        return final_equity

# ================= 回测引擎 =================

class BacktestEngine:
    def __init__(self, start_date, end_date, db_path='data/market_data.db'):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        base_dir = os.path.dirname(os.path.abspath(__file__))
        self.db_path = os.path.join(base_dir, db_path)
        print(f"📂 Backtest DB Path: {self.db_path}")
        
        self.conn = sqlite3.connect(self.db_path)
        self.cfg = StrategyConfig().load()
        
        self.risk_engine = FactorRiskModel()
        self.alpha_model = AlphaModel()
        self.pred_model = PredictionModel()
        self.pair_model = PairTradingStrategy()
        self.basket_model = BasketTradingStrategy()
        self.trend_filter = TrendFilter()
        self.stop_monitor = VolatilityTrailingStop()
        self.sniper_strat = SniperStrategyV3()
        self.sniper_manager = SniperManagerV10()
        self.optimizer = PortfolioOptimizer()
        
        self.exchange = VirtualExchange(slippage=0.001, fee_rate=0.0006)
        self.attribution_log = [] 

    def load_data(self):
        print("⏳ Loading historical data from DB...")
        warmup_start = self.start_date - timedelta(days=60)
        
        active_symbols = self.cfg['symbols']
        if not active_symbols: raise ValueError("Config symbols list is empty!")
        sym_str = "'" + "','".join(active_symbols) + "'"
        
        query_1h = f"SELECT * FROM ohlcv_1h WHERE symbol IN ({sym_str}) AND timestamp >= {warmup_start.timestamp()*1000} AND timestamp <= {self.end_date.timestamp()*1000}"
        df_1h = pd.read_sql(query_1h, self.conn)
        self.data_1h = self._pivot_data(df_1h)
        
        query_5m = f"SELECT * FROM ohlcv_5m WHERE symbol IN ({sym_str}) AND timestamp >= {warmup_start.timestamp()*1000} AND timestamp <= {self.end_date.timestamp()*1000}"
        df_5m = pd.read_sql(query_5m, self.conn)
        self.data_5m_dict = self._pivot_data_dict(df_5m)
        
        self.price_matrix_5m = self._pivot_data(df_5m).reindex(columns=active_symbols)
        print(f"✅ Data Loaded (Filtered). 1H Rows: {len(self.data_1h)}, 5M Rows: {len(self.price_matrix_5m)}")

    def _pivot_data(self, df):
        if df.empty: return pd.DataFrame()
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        pivot = df.pivot(index='date', columns='symbol', values='close')
        pivot = pivot.ffill() 
        pivot = pivot.dropna(axis=1, how='all')
        return pivot

    def _pivot_data_dict(self, df):
        if df.empty: return {}
        data_dict = {}
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.rename(columns={'open':'o', 'high':'h', 'low':'l', 'close':'c', 'volume':'v'})
        for sym, group in df.groupby('symbol'):
            data_dict[sym] = group.set_index('date').sort_index()
            data_dict[sym] = data_dict[sym][~data_dict[sym].index.duplicated(keep='first')]
        return data_dict

    def _run_logic(self, silent=False):
        """核心回测逻辑，供 run() 和 run_silent() 复用"""
        timeline = self.price_matrix_5m.index
        timeline = timeline[(timeline >= self.start_date) & (timeline <= self.end_date)]
        
        current_weights = {
            'trend': pd.Series(dtype=float), 'pair': pd.Series(dtype=float),
            'basket': pd.Series(dtype=float), 'sniper': pd.Series(dtype=float)
        }
        last_low_freq_weights = pd.Series()
        
        for current_time in timeline:
            # 1. Sniper
            hf_slice = {}
            for sym, df in self.data_5m_dict.items():
                if df.empty: continue
                loc = df.index.searchsorted(current_time)
                if loc > 0:
                    if loc < len(df) and df.index[loc] == current_time: end_loc = loc + 1
                    else: end_loc = loc
                    slice_df = df.iloc[max(0, end_loc-120):end_loc]
                    if not slice_df.empty: hf_slice[sym] = slice_df
            
            sniper_w, _ = self.sniper_manager.check_signals(hf_slice, self.sniper_strat)
            current_weights['sniper'] = sniper_w

            # 2. Low Freq
            if current_time.minute == 0:
                if not self.data_1h.empty:
                    loc_1h = self.data_1h.index.searchsorted(current_time)
                    if loc_1h > 500:
                        lf_slice = self.data_1h.iloc[:loc_1h+1]
                        alloc = self.cfg['strategy_allocation']
                        
                        trend_w = pd.Series(0, index=lf_slice.columns)
                        if alloc['trend_weight'] > 0:
                            f = self.alpha_model.compute_signals(lf_slice)
                            p = self.pred_model.predict(f, lf_slice)
                            raw_t = p.clip(lower=0)
                            if raw_t.sum() > 0:
                                trend_w = (raw_t/raw_t.sum()) * alloc['trend_weight']
                                trend_w = self.trend_filter.filter_signals(trend_w, lf_slice)
                            curr_vols = lf_slice.pct_change(fill_method=None).rolling(30).std().iloc[-1]
                            for sym in trend_w.index:
                                if self.stop_monitor.update_and_check(sym, lf_slice[sym].iloc[-1], curr_vols.get(sym,0.01), trend_w.get(sym,0)>0):
                                    trend_w[sym] = 0.0
                        current_weights['trend'] = trend_w

                        valid_pairs = [p for p in self.cfg['pairs_config'] if p[0] in lf_slice and p[1] in lf_slice]
                        current_weights['pair'] = self.pair_model.compute_signals(lf_slice, valid_pairs) * alloc['pair_weight']
                        current_weights['basket'] = self.basket_model.compute_signals(lf_slice, self.cfg['basket_config']) * alloc['basket_weight']
                        
                        # Optimization
                        raw_comb = current_weights['trend'].add(current_weights['pair'], fill_value=0).add(current_weights['basket'], fill_value=0)
                        valid_assets = raw_comb.dropna().index
                        
                        if len(valid_assets) > 0:
                            lf_ret = lf_slice[valid_assets].pct_change(fill_method=None).fillna(0.0)
                            cov = self.risk_engine.get_covariance_matrix(lf_ret, valid_assets)
                            opt_w = self.optimizer.optimize(raw_comb[valid_assets], cov, last_low_freq_weights, 5.0)
                            
                            if opt_w is not None:
                                last_low_freq_weights = opt_w
                                raw_sum = raw_comb.abs().sum()
                                opt_sum = opt_w.abs().sum()
                                scaler = opt_sum / raw_sum if raw_sum > 1e-6 else 0.0
                            else:
                                port_vol = self.risk_engine.compute_portfolio_risk(raw_comb, lf_ret)
                                scaler = min(5.0, 2.0 / (port_vol + 1e-9))
                                last_low_freq_weights = raw_comb * scaler
                            
                            current_weights['trend'] *= scaler
                            current_weights['pair'] *= scaler
                            current_weights['basket'] *= scaler

            # 3. Record Attribution (Only if not silent to save memory)
            if not silent:
                record = {'time': current_time}
                for strat_name, w_series in current_weights.items():
                    for asset, weight in w_series.items():
                        if abs(weight) > 1e-6: record[f"{strat_name}_{asset}"] = weight
                self.attribution_log.append(record)
            
            # 4. Execute
            final_weights = current_weights['trend'].add(current_weights['pair'], fill_value=0).add(current_weights['basket'], fill_value=0).add(current_weights['sniper'], fill_value=0).fillna(0.0)
            try:
                curr_prices_snapshot = self.price_matrix_5m.loc[current_time].to_dict()
                self.exchange.execute_orders(final_weights, curr_prices_snapshot, current_time)
            except KeyError: pass

            if not silent and current_time.hour == 0 and current_time.minute == 0:
                print(f"📅 Analyzing... {current_time.date()}")

    def run(self):
        print("🚀 Starting Attribution Backtest (With Slippage)...")
        self._run_logic(silent=False)
        self.generate_detailed_report()

    def run_silent(self):
        # 优化器专用: 不打印、不记录归因、不画图，只跑净值
        self._run_logic(silent=True)

    def get_stats(self):
        if not self.exchange.equity_curve:
            return {'sharpe': 0.0, 'total_return': 0.0, 'max_drawdown': 0.0}
        
        df = pd.DataFrame(self.exchange.equity_curve).set_index('time')
        df['returns'] = df['equity'].pct_change().fillna(0.0)
        
        total_ret = (df['equity'].iloc[-1] / self.exchange.initial_capital) - 1
        std = df['returns'].std()
        sharpe = (df['returns'].mean() / std) * np.sqrt(365 * 24 * 12) if std != 0 else 0
        
        roll_max = df['equity'].cummax()
        drawdown = df['equity'] / roll_max - 1
        max_dd = drawdown.min()
        
        return {'sharpe': float(sharpe), 'total_return': float(total_ret), 'max_drawdown': float(max_dd)}

    def generate_detailed_report(self):
        print("\n=== Attribution Analysis (With Slippage) ===")
        if not self.attribution_log:
            print("No positions recorded.")
            return

        w_df = pd.DataFrame(self.attribution_log).set_index('time').fillna(0.0)
        prices = self.price_matrix_5m.reindex(w_df.index)
        returns = prices.pct_change(fill_method=None).fillna(0.0)
        
        strategies = ['trend', 'pair', 'basket', 'sniper']
        strat_curves = pd.DataFrame(index=w_df.index)
        
        for strat in strategies:
            cols = [c for c in w_df.columns if c.startswith(f"{strat}_")]
            if not cols:
                strat_curves[strat] = 0.0
                continue
            strat_pnl = pd.Series(0.0, index=w_df.index)
            for col in cols:
                asset = col.replace(f"{strat}_", "")
                if asset in returns.columns:
                    w = w_df[col].shift(1).fillna(0.0)
                    r = returns[asset]
                    strat_pnl += w * r
            
            strat_curves[strat] = strat_pnl.cumsum()
            print(f"{strat.capitalize()} Gross Return: {strat_pnl.sum():.2%}")

        eq_df = pd.DataFrame(self.exchange.equity_curve).set_index('time')
        
        plt.figure(figsize=(14, 10))
        plt.subplot(2, 1, 1)
        if not eq_df.empty:
            eq_curve = eq_df['equity'] / self.exchange.initial_capital
            plt.plot(eq_curve, label='Net Equity (Real Account)', color='green', linewidth=2)
            final_ret = eq_curve.iloc[-1] - 1
            plt.title(f'Real Account Net Return: {final_ret:.2%}')
        else:
            plt.title('No Trades Executed')
        plt.grid(True)
        plt.legend()
        
        plt.subplot(2, 1, 2)
        for strat in strategies:
            plt.plot(strat_curves[strat], label=f'{strat.capitalize()} (Gross)')
        plt.title('Strategy Attribution (Theoretical Gross Contribution)')
        plt.grid(True)
        plt.legend()
        
        plt.tight_layout()
        plt.savefig('attribution_result.png')
        print("\n📸 Detailed report saved to attribution_result.png")

# ================= 必须添加的入口 =================
if __name__ == "__main__":
    # 默认回测过去 30 天
    end = datetime.now()
    start = end - timedelta(days=30) 
    
    bt = BacktestEngine(start, end)
    bt.load_data()
    bt.run()