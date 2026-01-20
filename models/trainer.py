import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.metrics import mean_squared_error
from scipy.stats import spearmanr
import joblib
import os

class ModelTrainer:
    """
    [AI Core] XGBoost 滚动训练器
    参考: Guru 2 Lesson 6
    """
    def __init__(self, model_path='models/xgb_model.pkl'):
        self.model_path = model_path
        self.model = None
        self.params = {
            'objective': 'reg:squarederror',
            'eta': 0.05,             # 学习率
            'max_depth': 6,          # 树深
            'subsample': 0.8,        # 样本采样
            'colsample_bytree': 0.8, # 特征采样
            'eval_metric': 'rmse',
            'nthread': 4             # 多线程
        }

    def split_data(self, df, train_ratio=0.8):
        """
        按时间序列切分 (严禁 Shuffle)
        """
        # 获取所有唯一的时间点，排序
        dates = df.index.get_level_values('date').unique().sort_values()
        split_idx = int(len(dates) * train_ratio)
        split_date = dates[split_idx]
        
        print(f"✂️ Splitting data at {split_date}")
        
        # 切分
        train_df = df[df.index.get_level_values('date') < split_date]
        valid_df = df[df.index.get_level_values('date') >= split_date]
        
        return train_df, valid_df

    def train(self, df):
        """
        训练模型
        """
        train_df, valid_df = self.split_data(df)
        
        # 准备特征 (排除 target)
        features = [c for c in df.columns if c != 'target']
        target = 'target'
        
        print(f"🏋️ Training XGBoost on {len(train_df)} samples...")
        print(f"   Features: {len(features)}")
        
        dtrain = xgb.DMatrix(train_df[features], label=train_df[target])
        dvalid = xgb.DMatrix(valid_df[features], label=valid_df[target])
        
        # 训练
        evals = [(dtrain, 'train'), (dvalid, 'eval')]
        self.model = xgb.train(
            self.params, 
            dtrain, 
            num_boost_round=500, 
            evals=evals,
            early_stopping_rounds=50,
            verbose_eval=50
        )
        
        # 保存
        self.save_model()
        
        # 评估 IC
        self.evaluate(valid_df, features)

    def evaluate(self, df, features):
        """
        计算 IC (Information Coefficient)
        """
        print("📊 Evaluating Rank IC...")
        dtest = xgb.DMatrix(df[features])
        preds = self.model.predict(dtest)
        
        # 将预测值拼回去
        res_df = df.copy()
        res_df['pred'] = preds
        
        # 按时间分组计算 Rank IC (预测排名 vs 真实排名 的相关性)
        ic_list = []
        for date, group in res_df.groupby(level='date'):
            if len(group) < 2: continue
            ic, _ = spearmanr(group['pred'], group['target'])
            ic_list.append(ic)
            
        mean_ic = np.mean(ic_list)
        ic_ir = mean_ic / (np.std(ic_list) + 1e-9)
        
        print(f"🏆 Test Results:")
        print(f"   Mean Rank IC: {mean_ic:.4f}")
        print(f"   IC IR:        {ic_ir:.4f}")
        
        if mean_ic > 0.03:
            print("🚀 Model is Good to Go!")
        else:
            print("⚠️ Model is weak. Need better factors.")

    def save_model(self):
        joblib.dump(self.model, self.model_path)
        print(f"💾 Model saved to {self.model_path}")

    def load_model(self):
        if os.path.exists(self.model_path):
            self.model = joblib.load(self.model_path)
            return True
        return False

    def predict(self, current_factors_df):
        """
        实盘预测接口
        """
        if not self.model:
            if not self.load_model():
                return None
        
        dtest = xgb.DMatrix(current_factors_df)
        return self.model.predict(dtest)