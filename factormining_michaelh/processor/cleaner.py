import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

class FactorCleaner:
    """
    量化截面因子清洗与预处理管道 (Cross-Sectional Processing Pipeline)
    """
    @staticmethod
    def clean_inf(series: pd.Series) -> pd.Series:
        return series.replace([np.inf, -np.inf], np.nan)

    @staticmethod
    def winsorize(series: pd.Series, limits=(0.01, 0.01)) -> pd.Series:
        """MAD 或分位数缩尾处理"""
        valid_series = series.dropna()
        if len(valid_series) == 0:
            return series
            
        q_min = valid_series.quantile(limits[0])
        q_max = valid_series.quantile(1.0 - limits[1])
        return series.clip(lower=q_min, upper=q_max)

    @staticmethod
    def z_score(series: pd.Series) -> pd.Series:
        """截面标准化"""
        std = series.std()
        if std == 0 or np.isnan(std): 
            return pd.Series(0, index=series.index)
        return (series - series.mean()) / std

    @staticmethod
    def neutralize(df_group: pd.DataFrame, factor_col: str, sector_col: str) -> pd.Series:
        """分类变量（如行业/市值组）正交化"""
        if sector_col not in df_group.columns:
            return df_group[factor_col]
        
        sector_means = df_group.groupby(sector_col)[factor_col].transform('mean')
        return df_group[factor_col] - sector_means
    
    @staticmethod
    def pca_neutralize(df_group: pd.DataFrame, factor_col: str, exposure_cols: list, n_components: int = 1) -> pd.Series:
        """
        基于主成分分析(PCA)的统计特征正交化
        用于剥离高频维度的隐性共性风险 (Latent Common Risk Factors)
        """
        X = df_group[exposure_cols].fillna(0).values
        y = df_group[factor_col].values
        
        if X.shape[0] <= n_components + 1 or X.shape[1] < n_components:
            return df_group[factor_col]

        X_std = StandardScaler().fit_transform(X)
        pca = PCA(n_components=n_components)
        U = pca.fit_transform(X_std)

        # OLS 提取残差作为纯净 Alpha
        beta, _, _, _ = np.linalg.lstsq(U, y, rcond=None)
        return pd.Series(y - U @ beta, index=df_group.index)

    @classmethod
    def process_factor(
        cls,
        df: pd.DataFrame,
        col_name: str,
        winsorize: bool = False,
        standardize: bool = False,
        pca_neutralize: bool = False,  
        exposure_cols: list = None,   
        n_components: int = 3,
        neutralize: bool = False,     
        sector_col: str = None        
    ) -> pd.Series:
        
        s = cls.clean_inf(df[col_name])
        cols_needed = ['timestamp'] 
        
        if pca_neutralize and exposure_cols:
            cols_needed.extend(exposure_cols)
        if neutralize and sector_col:
            cols_needed.append(sector_col)
            
        temp_df = df[list(set(cols_needed))].copy()
        temp_df['val'] = s

        def cross_sectional_process(group):
            mean_val = group['val'].mean()
            group['val'] = group['val'].fillna(mean_val if not np.isnan(mean_val) else 0)

            if winsorize:
                group['val'] = cls.winsorize(group['val'])
            if neutralize and sector_col:
                group['val'] = cls.neutralize(group, 'val', sector_col)
            if pca_neutralize and exposure_cols:
                group['val'] = cls.pca_neutralize(
                    group, 
                    factor_col='val', 
                    exposure_cols=exposure_cols, 
                    n_components=n_components
                )
            if standardize:
                group['val'] = cls.z_score(group['val'])
                
            return group['val']

        result = temp_df.groupby('timestamp', group_keys=False).apply(cross_sectional_process)
        return result.reindex(df.index)

# ==========================================
# 2. 高频数据特征准备 (Showcase Version)
# ==========================================
def prepare_hf_data_dummy(df: pd.DataFrame, ret_1m_col: str = 'ret_1m'):
    """
    构造多时间尺度的收益率特征展示 (Dummy Timeframes)
    注意：生产环境中的实际期限结构参数由外部配置注入。
    """
    df = df.sort_values(['asset', 'datetime']).copy()
    
    # [ALPHA LEAKAGE PREVENTED] 替换为标准的演示性窗口，掩盖真实的期限结构逻辑
    dummy_windows = [10, 30, 60] 
    exposure_cols = []
    
    for w in dummy_windows:
        col_name = f'ret_{w}window'
        
        # 时序平移防止前视偏差 (Look-ahead bias)
        df[col_name] = df.groupby('asset')[ret_1m_col].transform(
            lambda x: x.shift(1).rolling(window=w, min_periods=w//2).sum()
        )
        exposure_cols.append(col_name)
        
    return df, exposure_cols