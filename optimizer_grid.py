import itertools
import pandas as pd
import numpy as np
from backtest_attribution import BacktestEngine
from datetime import datetime, timedelta
import json
import os

# 消除 Warning
pd.set_option('future.no_silent_downcasting', True)

class GridSearch:
    def __init__(self):
        # 定义我们要搜索的参数空间
        self.param_grid = {
            # 1. Trend 策略参数
            'trend_window': [500, 700],       # 训练窗口
            
            # 2. Sniper 策略参数
            'sniper_threshold_l2': [3.5, 4.0], # 大针阈值
            
            # 3. 风控参数
            'trailing_stop_mult': [2.0, 3.0, 4.0] # 移动止损宽窄
        }
        
        self.results = []
        
    def run(self):
        # 生成所有参数组合
        keys, values = zip(*self.param_grid.items())
        combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]
        
        print(f"🔥 Starting Grid Search. Total combinations: {len(combinations)}")
        
        # 设定回测时间：过去 60 天 (数据需已通过 populator 下载)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=60)
        
        for i, params in enumerate(combinations):
            print(f"\n=== Test {i+1}/{len(combinations)}: {params} ===")
            
            try:
                # 1. 动态修改策略参数 (这里需要 BacktestEngine 支持传参，或者我们就地修改 Config)
                # 为了简单，我们假设 BacktestEngine 可以读取临时修改的 Config
                # 这里我们使用一种更这种的方式：继承并重写 BacktestEngine 的配置加载
                
                engine = BacktestEngine(start_date, end_date)
                
                # --- 强行注入参数 ---
                # Trend Window (影响 PredictionModel)
                engine.pred_model.train_window = params['trend_window']
                
                # Sniper Threshold (影响 SniperStrategyV3)
                engine.sniper_strat.L2_Z = params['sniper_threshold_l2']
                # 对应调整 L1 (比如 L1 = L2 - 1.0)
                engine.sniper_strat.L1_Z = params['sniper_threshold_l2'] - 1.0
                
                # Trailing Stop
                engine.stop_monitor.multiplier = params['trailing_stop_mult']
                
                # 2. 运行回测 (静默模式，不画图)
                engine.load_data()
                engine.run_silent() # 我们需要在 BacktestEngine 里加一个不画图的方法
                
                # 3. 收集结果
                stats = engine.get_stats()
                result_record = params.copy()
                result_record.update(stats)
                self.results.append(result_record)
                
                print(f"   -> Sharpe: {stats['sharpe']:.2f}, Return: {stats['total_return']:.2%}")
                
            except Exception as e:
                print(f"   ❌ Error: {e}")

        # 4. 保存结果
        df = pd.DataFrame(self.results)
        df = df.sort_values('sharpe', ascending=False)
        print("\n🏆 Top 5 Parameter Sets:")
        print(df.head(5).to_string())
        df.to_csv('grid_search_results.csv', index=False)

if __name__ == "__main__":
    gs = GridSearch()
    gs.run()