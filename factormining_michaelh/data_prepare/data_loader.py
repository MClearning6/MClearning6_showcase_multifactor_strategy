import pandas as pd
import numpy as np
import os
import gc
from datetime import datetime, timedelta
import data_prepare.data_adapt as adapters 

class UniversalDataFeed:
    """
    流式数据引擎：支持跨周期的分块加载与内存管理 (Showcase Version)
    """
    def __init__(self, data_path: str, frequency: str = '1m', buffer_days: int = 21, adapter_name: str = 'adapter_vendor_minute'):
        self.data_path = data_path
        self.frequency = frequency
        # [ALPHA LEAKAGE PREVENTED] 使用 21 天作为演示默认值，掩盖真实的高频因子回溯窗口
        self.buffer_days = buffer_days 
        
        if hasattr(adapters, adapter_name):
            self.adapter_func = getattr(adapters, adapter_name)
        else:
            raise ValueError(f" [DataFeed] 找不到对应的数据适配器: {adapter_name}")
            
        if os.path.isdir(data_path):
            self.file_list = sorted([f for f in os.listdir(data_path) if f.endswith(".parquet") and "factor" not in f])
        elif os.path.isfile(data_path):
            self.file_list = [os.path.basename(data_path)]
            self.data_path = os.path.dirname(data_path)
        else:
            raise ValueError(f" [DataFeed] 数据路径无效: {data_path}")

    def _load_and_clean(self, abs_path):
        """核心数据拉取与规范化"""
        try:
            df = pd.read_parquet(abs_path)
            df = self.adapter_func(df)
            
            if 'asset' not in df.columns or 'timestamp' not in df.columns:
                raise ValueError("适配器未输出标准的 asset 或 timestamp 列")
                
            df = df.sort_values(['asset', 'timestamp']).reset_index(drop=True)
            return df
        except Exception as e:
            print(f" [DataFeed] 读取或清洗失败 {abs_path}: {e}")
            return pd.DataFrame()