import pandas as pd
import numpy as np
import os
import sys
import warnings

# 引用项目路径
current_path = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_path))
if project_root not in sys.path:
    sys.path.append(project_root)

import config

warnings.filterwarnings('ignore')

class AlphaComposer:
    """
    多因子线性合成器：将多个中性化因子合成最终 Alpha Score。
    支持 IC/ICIR 加权逻辑。
    """
    def __init__(self):
        self.report_path = config.REPORT_PATH
        self.pool_path = config.NEUTRAL_FACTOR_DIR
        self.weight_metric = getattr(config, 'COMBINE_METRIC', 'ICIR') # 建议默认用 ICIR

    def get_weights(self, factor_list: list) -> dict:
        if not os.path.exists(self.report_path):
            raise FileNotFoundError(f"找不到评估报告: {self.report_path}")
            
        df_report = pd.read_csv(self.report_path)
        report_lookup = dict(zip(df_report['Factor'].astype(str), df_report[self.weight_metric]))
        
        weights = {f: report_lookup[f] for f in factor_list if f in report_lookup}
        
        # 权重归一化处理 (Sum of Abs Weights = 1)
        total_abs_weight = sum(abs(v) for v in weights.values())
        if total_abs_weight > 0:
            weights = {k: v / total_abs_weight for k, v in weights.items()}
            
        return weights

    def compose(self, factor_list: list):
        print(f" [Composer] 正在合成 Alpha 信号 (因子数: {len(factor_list)})...")
        
        weights = self.get_weights(factor_list)
        df_list = []
        
        # 优化：先收集所有 DataFrame，最后一次性合并
        for f_name in factor_list:
            f_path = os.path.join(self.pool_path, f_name, "all_data.parquet")
            if not os.path.exists(f_path): continue
            
            df_f = pd.read_parquet(f_path)
            df_f = df_f.rename(columns={'factor_value': f_name}).set_index(['asset', 'timestamp'])
            df_list.append(df_f[f_name])

        if not df_list: return pd.DataFrame()

        # 一次性 Join，效率远高于循环 Merge
        df_master = pd.concat(df_list, axis=1).fillna(0)
        
        # 加权求和
        df_master['alpha_score'] = 0.0
        for name, w in weights.items():
            df_master['alpha_score'] += df_master[name] * w
            
        return df_master.reset_index()

def main():
    # 从 config 中读取手动指定的因子组合
    factors = getattr(config, 'SELECTED_ALPHAS', [])
    if not factors:
        print(" 未在 config 中找到 SELECTED_ALPHAS，请检查配置。")
        return

    composer = AlphaComposer()
    df_alpha = composer.compose(factors)

    if not df_alpha.empty:
        # 统一处理回测时间范围
        start_date = getattr(config, 'BACKTEST_START', '2010-01-01')
        df_alpha = df_alpha[df_alpha['timestamp'] >= start_date]
        
        os.makedirs(os.path.dirname(config.FINAL_ALPHA_PATH), exist_ok=True)
        df_alpha.to_parquet(config.FINAL_ALPHA_PATH, index=False)
        print(f" 合成完成！Alpha 轨迹已存至: {config.FINAL_ALPHA_PATH}")

if __name__ == '__main__':
    main()