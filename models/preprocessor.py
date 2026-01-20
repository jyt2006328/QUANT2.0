class CrossSectionalPreprocessor:
    """
    负责将时序数据转换为截面模型可用的格式
    参考 Guru 2 的 Lesson_6
    """
    def prepare_training_data(self, data_dict):
        # data_dict: { 'BTC': df, 'ETH': df ... }
        
        all_dfs = []
        for symbol, df in data_dict.items():
            df['symbol'] = symbol
            # 计算收益率作为 Label (Target)
            # 注意: Label 必须是未来的收益率 (T+1)，不需要 lag
            df['target_return'] = df['close'].shift(-1) / df['close'] - 1
            all_dfs.append(df)
            
        # 拼接大表
        full_df = pd.concat(all_dfs)
        
        # 因子清洗 (去极值、标准化)
        # 参考 Lesson_14 中的 process_factors
        full_df = self.clean_factors(full_df)
        
        return full_df.dropna()

    def clean_factors(self, df):
        # 实现 MAD 去极值和 Z-Score 标准化
        pass