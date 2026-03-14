# 已由AI剥离高频计算逻辑
import pandas as pd
import numpy as np
import os
import sys
import warnings
from tqdm import tqdm
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings('ignore')

# 确保能导入项目根目录的配置
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_path))
if project_root not in sys.path:
    sys.path.append(project_root)

import config
from data_prepare.data_loader import UniversalDataFeed

# ==========================================
# 模块 1：统计风险模型构建 (Statistical Risk Model)
# ==========================================
def build_statistical_risk_model(df_price: pd.DataFrame) -> pd.DataFrame:
    """
    基于滚动主成分分析 (Rolling PCA) 提取市场内生风险因子。
    用于后续剥离隐性共性波动 (Latent Common Risk Exposures)。
    """
    lookback = config.PCA_SETTINGS.get('lookback', 120)
    n_components = config.PCA_SETTINGS.get('n_components', 5)
    
    print(f" [Risk Model] 正在构建统计风险模型 (Lookback: {lookback}d, PCs: {n_components})...")
    
    df_price = df_price.sort_values(['asset', 'timestamp'])
    
    # 强制计算标准日收益率作为风险基底
    if 'ret_1d' not in df_price.columns:
        if 'close' in df_price.columns:
            df_price['ret_1d'] = df_price.groupby('asset')['close'].pct_change()
        elif 'change' in df_price.columns:
            df_price['ret_1d'] = df_price['change']
        else:
            raise ValueError("缺少价格或涨跌幅数据用于计算风险暴露。")
            
    df_price['date'] = df_price['timestamp'].dt.date
    wide_rets = df_price.pivot(index='date', columns='asset', values='ret_1d')
    
    all_dates = wide_rets.index.tolist()
    risk_exposures = []

    for i in tqdm(range(lookback, len(all_dates)), desc="Rolling PCA Extraction"):
        current_date = all_dates[i]
        
        # 截取历史窗口，严格防止前视偏差
        historical_rets = wide_rets.iloc[i-lookback : i]
        X = historical_rets.T
        
        # 剔除无效标的
        X = X.dropna(how='all')
        if len(X) <= n_components:
            continue
            
        X_mat = X.values
        
        # 截面均值对齐 (市场组合收益代理) 填充缺失值
        col_means = np.nanmean(X_mat, axis=0) 
        col_means = np.nan_to_num(col_means, nan=0.0) 
        
        inds = np.where(np.isnan(X_mat))
        X_mat[inds] = col_means[inds[1]]

        try:
            X_std = StandardScaler().fit_transform(X_mat)
            pca = PCA(n_components=n_components)
            U = pca.fit_transform(X_std)
            
            df_u = pd.DataFrame(U, index=X.index, columns=[f'PC{k+1}' for k in range(n_components)])
            df_u = df_u.reset_index()
            df_u['date'] = current_date
            risk_exposures.append(df_u)
        except Exception:
            continue

    df_risk = pd.concat(risk_exposures, ignore_index=True)
    
    # 确保存储目录存在
    os.makedirs(os.path.dirname(config.RISK_CACHE_PATH), exist_ok=True)
    df_risk.to_parquet(config.RISK_CACHE_PATH, index=False)
    
    print(f" [Risk Model] 统计风险模型构建完成！缓存路径: {config.RISK_CACHE_PATH}")
    return df_risk

# ==========================================
# 模块 2：纯净因子提取 (Factor Neutralization)
# ==========================================
def neutralize_daily_factors(df_risk: pd.DataFrame):
    """
    利用提取的主成分矩阵，对原始截面因子进行正交化处理 (Orthogonalization)，
    提取纯净 Alpha 残差。
    """
    print("\n [Neutralization] 开始执行因子横截面正交化...")
    os.makedirs(config.NEUTRAL_FACTOR_DIR, exist_ok=True)
    
    if not os.path.exists(config.OUTPUT_DIR):
        print(f" [警告] 因子输出目录 {config.OUTPUT_DIR} 不存在，请先运行因子挖掘脚本。")
        return

    factor_names = [d for d in os.listdir(config.OUTPUT_DIR) if os.path.isdir(os.path.join(config.OUTPUT_DIR, d))]
    
    for f_name in factor_names:
        raw_path = os.path.join(config.OUTPUT_DIR, f_name, "all_data.parquet")
        out_dir = os.path.join(config.NEUTRAL_FACTOR_DIR, f_name)
        out_path = os.path.join(out_dir, "all_data.parquet")
        
        if not os.path.exists(raw_path): 
            continue
        if os.path.exists(out_path):
            print(f"     跳过 {f_name}: 已存在中性化缓存")
            continue
            
        try:
            os.makedirs(out_dir, exist_ok=True)
            df_factor = pd.read_parquet(raw_path)
            
            if 'date' not in df_factor.columns:
                df_factor['date'] = df_factor['timestamp'].dt.date
                
            # 因子与风险模型对齐
            df_merged = pd.merge(df_factor, df_risk, on=['date', 'asset'], how='inner')
            if df_merged.empty: 
                continue
            
            pc_cols = [c for c in df_risk.columns if c.startswith('PC')]
            
            def cross_section_ols(group):
                y = group['factor_value'].values
                X = group[pc_cols].values
                
                # 最小二乘法提取残差 (纯净 Alpha)
                beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
                residual = y - X @ beta
                
                # 残差再标准化
                std = residual.std()
                if std > 1e-8:
                    residual = (residual - residual.mean()) / std
                else:
                    residual = residual - residual.mean()
                    
                group['neutral_factor'] = residual
                return group

            # 按截面日分组执行正交回归
            df_neutralized = df_merged.groupby('date', group_keys=False).apply(cross_section_ols)
            
            df_save = df_neutralized[['asset', 'timestamp', 'neutral_factor']].rename(columns={'neutral_factor': 'factor_value'})
            df_save.to_parquet(out_path, index=False)
            print(f"    完成中性化: {f_name}")
            
        except Exception as e:
            print(f"    中性化失败 {f_name}: {e}")

# ==========================================
# 主程序
# ==========================================
def main():
    # 强制校验执行频率
    if config.FREQ != '1d':
        print(" [系统拦截] 当前展示框架仅支持日频 (EOD) 逻辑执行，请检查 config.FREQ 设置。")
        sys.exit(1)

    # 1. 加载或构建风险模型
    if os.path.exists(config.RISK_CACHE_PATH):
        print(" 发现风险模型缓存，正在加载...")
        df_risk = pd.read_parquet(config.RISK_CACHE_PATH)
    else:
        print(" 未发现风险缓存，启动全量基底数据计算...")
        # 使用统一的数据管道读取基础行情
        feed = UniversalDataFeed(
            data_path=config.DATA_PATH, 
            frequency='1d',
            buffer_days=0, 
            adapter_name=config.DATA_ADAPTER
        )
        
        # 提取全量历史数据构建风险矩阵
        chunks = []
        for df, _ in feed.yield_chunks():
            chunks.append(df)
        
        if not chunks:
            print(" 数据管道返回为空，请检查数据源配置。")
            sys.exit(1)
            
        df_full_price = pd.concat(chunks, ignore_index=True)
        df_risk = build_statistical_risk_model(df_full_price)

    # 2. 执行正交化流水线
    neutralize_daily_factors(df_risk)
    
    print(f"\n 全部处理完成！中性化因子已存放至: {config.NEUTRAL_FACTOR_DIR}")

if __name__ == "__main__":
    main()