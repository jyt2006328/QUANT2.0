import pandas as pd
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from strategies.alpha_genetic import GeneticAlphaModel

class AlphaModel:
    """
    [Trend] 因子生产工厂
    负责计算 Momentum, Reversal, Volatility, 并融合 Genetic Alphas
    """
    def __init__(self, use_pca=True, n_components=2):
        self.use_pca = use_pca
        self.n_components = n_components
        self.genetic = GeneticAlphaModel()
        
    def compute_signals(self, prices_df):
        """兼容旧接口 (只用 Close) - 保留以防万一"""
        returns = prices_df.pct_change().fillna(0.0)
        mom = returns.rolling(120).mean()
        rev = -returns.rolling(5).mean()
        vol = returns.rolling(30).std()
        
        out = {}
        for asset in prices_df.columns:
            df = pd.DataFrame({'mom': mom[asset], 'rev': rev[asset], 'vol': vol[asset]}).dropna()
            if len(df) < 50: 
                out[asset] = None
                continue
            try: 
                out[asset] = pd.DataFrame(
                    PCA(self.n_components).fit_transform(StandardScaler().fit_transform(df)), 
                    index=df.index
                ) if self.use_pca else df
            except: 
                out[asset] = df
        return out

    def compute_signals_with_genetic(self, data_dict):
        """[新接口] 支持 OHLCV，融合遗传因子"""
        out = {}
        for asset, df in data_dict.items():
            if len(df) < 120: 
                out[asset] = None
                continue
                
            # 1. 基础因子 (Base Factors)
            returns = df['c'].pct_change().fillna(0.0)
            mom = returns.rolling(120).mean()
            rev = -returns.rolling(5).mean()
            vol = returns.rolling(30).std()
            
            base_factors = pd.DataFrame({'mom': mom, 'rev': rev, 'vol': vol}, index=df.index)
            
            # 2. 遗传因子 (Genetic Factors)
            gp_factors = self.genetic.compute_alphas(df)
            
            # 3. 合并因子
            combined = base_factors.join(gp_factors).dropna()
            
            # 4. PCA 降维 (因为因子变多了，必须降维防止共线性)
            if self.use_pca:
                try:
                    scaler = StandardScaler()
                    X_scaled = scaler.fit_transform(combined.values)
                    # 增加主成分数量，捕捉更多信息 (2 -> 3)
                    pca = PCA(n_components=3) 
                    X_orth = pca.fit_transform(X_scaled)
                    out[asset] = pd.DataFrame(X_orth, index=combined.index, columns=[f'f{i}' for i in range(3)])
                except:
                    out[asset] = combined
            else:
                out[asset] = combined
                
        return out

class PredictionModel:
    """
    [Trend] 收益率预测模型 (Ridge Regression)
    """
    def __init__(self, train_window=500):
        self.train_window = train_window
        
    def predict(self, factors_dict, prices_df):
        predictions = {}
        target_returns = prices_df.pct_change().shift(-1) # Y = T+1 Returns
        
        for asset, factors_df in factors_dict.items():
            if factors_df is None:
                predictions[asset] = 0.0
                continue
            
            # 数据对齐 X 和 Y
            y_series = target_returns[asset].rename('target')
            dataset = factors_df.join(y_series).dropna()
            
            # 滚动窗口截取
            if len(dataset) > self.train_window:
                dataset = dataset.iloc[-self.train_window:]
                
            if len(dataset) < 50:
                predictions[asset] = 0.0
                continue
            
            # 训练模型
            X = dataset.iloc[:, :-1].values
            y = dataset.iloc[:, -1].values
            
            model = Ridge(alpha=1.0)
            model.fit(X, y)
            
            # 预测最新一期
            current_features = factors_df.iloc[[-1]].values
            pred_return = model.predict(current_features)[0]
            predictions[asset] = pred_return
            
        return pd.Series(predictions)