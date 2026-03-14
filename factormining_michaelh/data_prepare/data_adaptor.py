import pandas as pd
import numpy as np
import os

def adapter_daily_grid(df: pd.DataFrame, halt_limit: int = 5) -> pd.DataFrame:
    """
    底层物理日历网格对齐 (Reindex 笛卡尔积)
    halt_limit: 停牌前推填充阈值，生产环境参数由外部 config 注入
    """
    print("   [Adapter] 启动底层物理日历网格对齐 (Reindex 笛卡尔积)...")
    df = df.copy()
    df = df.rename(columns={'code': 'asset', 'pctChg': 'change'}, errors='ignore')
    
    time_col = 'date' if 'date' in df.columns else 'timestamp'
    df['timestamp'] = pd.to_datetime(df[time_col])
    df = df.drop(columns=['date'], errors='ignore').drop_duplicates(subset=['timestamp', 'asset'])
    
    price_cols = [c for c in ['open', 'high', 'low', 'close', 'vwap'] if c in df.columns]
    vol_cols = [c for c in ['volume', 'amount'] if c in df.columns]
    
    unique_times = np.sort(df['timestamp'].unique()) 
    unique_assets = df['asset'].unique()
    
    full_idx = pd.MultiIndex.from_product(
        [unique_times, unique_assets], 
        names=['timestamp', 'asset']
    )
    
    df_grid = df.set_index(['timestamp', 'asset']).reindex(full_idx)
    
    cols_to_ffill = [c for c in df_grid.columns if c not in vol_cols]
    if cols_to_ffill:
        # 使用传入的参数代替硬编码
        df_grid[cols_to_ffill] = df_grid.groupby(level='asset')[cols_to_ffill].ffill(limit=halt_limit) 
        
    if vol_cols:
        df_grid[vol_cols] = df_grid[vol_cols].fillna(0)
        
    df_clean = df_grid.reset_index()
    if 'close' in df_clean.columns:
        df_clean = df_clean.dropna(subset=['close'])
        
    df_clean = df_clean.sort_values(['asset', 'timestamp']).reset_index(drop=True)
    return df_clean