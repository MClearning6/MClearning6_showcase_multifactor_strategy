import pandas as pd
import numpy as np
import warnings
import sys
import os
import itertools
import gc

# ==========================================
# 1. 基础设置与导入
# ==========================================
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_path))
if project_root not in sys.path:
    sys.path.append(project_root)

import config
from processor import utils_lib as lib  # 仅保留标准日频算子库
from processor.cleaner import FactorCleaner
from data_prepare.data_loader import UniversalDataFeed

warnings.filterwarnings('ignore')

def expand_params(params_config: dict):
    """展开超参数网格，用于因子遍历搜索"""
    if not params_config: 
        yield {}
        return
    keys, values = params_config.keys(), params_config.values()
    list_values = [v if isinstance(v, list) else [v] for v in values]
    for combinations in itertools.product(*list_values):
        yield dict(zip(keys, combinations))

# ==========================================
# 2. 因子挖掘主流程 (Daily Frequency Pipeline), 由AI剥离了高频因子计算逻辑
# ==========================================
def main():
    print(" [System] 日频因子挖掘引擎启动...")
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # 1. 初始化数据管家 (仅加载日频 EOD 数据)
    feed = UniversalDataFeed(
        data_path=config.DATA_PATH, 
        frequency='1d',  # 强制锁定为日频
        buffer_days=config.BUFFER_DAYS,
        adapter_name=config.DATA_ADAPTER 
    )

    # 2. 遍历数据块计算
    for df, cut_timestamp in feed.yield_chunks():
        print(f"\n 成功加载截面数据块 | 时间范围: {df['timestamp'].min().date()} -> {df['timestamp'].max().date()}")

        # -----------------------------------
        # 前置计算：日频基础特征
        # -----------------------------------
        # 使用安全的 groupby pct_change 计算日收益率，自动处理不同标的的对齐问题
        df['ret_1d'] = df.groupby('asset')['close'].pct_change()
        
        # 演示用：提取简单的量价特征作为截面正交化的基底 (Dummy Exposures)
        pca_conf = config.PCA_SETTINGS
        pca_exposure_cols = ['volume', 'ret_1d'] if pca_conf.get('enabled') else []

        # -----------------------------------
        # 因子计算引擎 (基于表达式解析)
        # -----------------------------------
        for conf in config.FACTOR_LIST:
            factor_base_name = conf['name']
            raw_params = conf.get('params', {})
            shift_steps = conf.get('shift', 0)

            expression_template = conf.get('expr')
            if not expression_template: 
                continue

            for single_params in expand_params(raw_params):
                try:
                    suffix = "_" + "_".join(str(v) for v in single_params.values()) if single_params else ""
                    raw_col = f"factor_{factor_base_name}{suffix}"
                    alpha_col = f"alpha_{factor_base_name}{suffix}"
                    
                    save_dir = os.path.join(config.OUTPUT_DIR, f"{factor_base_name}{suffix}")
                    os.makedirs(save_dir, exist_ok=True)
                    
                    chunk_name = cut_timestamp.strftime('%Y-%m') if cut_timestamp else "all_data"
                    save_file = os.path.join(save_dir, f"{chunk_name}.parquet")
                    
                    if os.path.exists(save_file):
                        print(f"    跳过已存在因子: {save_file}")
                        continue

                    print(f"    计算因子: {raw_col} ...")
                    current_expr = expression_template.format(**single_params) if single_params else expression_template
                    
                    # 动态执行因子表达式
                    df[raw_col] = eval(current_expr, {'pd': pd, 'np': np, 'lib': lib}, {'df': df})

                    # 严格的 T+1 滞后对齐
                    if shift_steps > 0:
                        df[raw_col] = df.groupby('asset')[raw_col].shift(shift_steps)

                    # 截面清洗管道
                    df[alpha_col] = FactorCleaner.process_factor(
                        df, raw_col, 
                        winsorize=pca_conf.get('winsorize', True),    
                        standardize=pca_conf.get('standardize', True),  
                        pca_neutralize=pca_conf.get('enabled', False),
                        exposure_cols=pca_exposure_cols,
                        n_components=pca_conf.get('n_components', 1),
                        neutralize=False, sector_col=None
                    )

                    # 瘦身与持久化存储
                    if cut_timestamp:
                        mask = df['timestamp'] >= cut_timestamp
                        df_save = df.loc[mask, ['asset', 'timestamp', alpha_col]].copy()
                    else:
                        df_save = df[['asset', 'timestamp', alpha_col]].copy()

                    df_save.rename(columns={alpha_col: 'factor_value'}, inplace=True)
                    df_save['factor_value'] = df_save['factor_value'].astype('float32') # 降维节省磁盘
                    df_save.to_parquet(save_file, index=False)
                    
                    # 及时释放内存
                    df.drop(columns=[raw_col, alpha_col], inplace=True)
                    del df_save

                except Exception as e:
                    print(f"    计算失败 [{factor_base_name} {single_params}]: {e}")
                    # 异常状态下的安全清理
                    cols_to_drop = [c for c in [raw_col, alpha_col] if c in df.columns]
                    if cols_to_drop:
                        df.drop(columns=cols_to_drop, inplace=True)

        # 块级垃圾回收
        del df
        gc.collect()

    print("\n 所有因子计算与清洗任务完成！")

if __name__ == "__main__":
    main()