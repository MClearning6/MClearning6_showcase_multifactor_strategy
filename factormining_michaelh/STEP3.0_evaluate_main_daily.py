import pandas as pd
import numpy as np
import os
import sys
import warnings

# ==========================================
# 1. 环境与依赖设置
# ==========================================
sys.path.append(os.getcwd())
import config

warnings.filterwarnings('ignore')

# ==========================================
# 2. 日频评估引擎 (Factor Evaluator)
# ==========================================
class FactorEvaluatorDaily:
    @staticmethod
    def preprocess_data(df: pd.DataFrame, ret_col='next_ret', horizon=1) -> pd.DataFrame:
        """
        前处理与远期收益率计算。
        为保护真实交易执行逻辑，此处仅展示最基础的 T+1 理想收益率计算。
        真实生产环境支持动态执行锚点 (VWAP, TWAP, 尾盘等)。
        """
        df = df.copy()
        if ret_col in df.columns:
            df = df.drop(columns=[ret_col])
        
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values(['asset', 'timestamp'])
        
        # 基础上市天数计算
        df['days_since_ipo'] = df.groupby('asset').cumcount()

        # [ALPHA LEAKAGE PREVENTED] 
        # 替换为标准的演示性收益率计算逻辑，隐藏真实的微观执行假设
        df[ret_col] = df.groupby('asset')['close'].shift(-horizon) / df['close'] - 1

        df[ret_col] = df[ret_col].replace([np.inf, -np.inf], np.nan)
        return df.dropna(subset=[ret_col])

    @staticmethod
    def calc_ic_series(df: pd.DataFrame, factor_col: str, ret_col: str) -> pd.Series:
        def daily_ic(group):
            if len(group) < 10: return np.nan 
            return group[factor_col].corr(group[ret_col], method='spearman')
        return df.groupby('date').apply(daily_ic)

    @staticmethod
    def calc_group_returns(df: pd.DataFrame, factor_col: str, ret_col: str, n_bins=5):
        def get_group_ret(sub_df):
            try:
                if len(sub_df) < n_bins * 2: return pd.Series(np.nan, index=range(n_bins))
                labels = list(range(n_bins))
                sub_df['group'] = pd.qcut(sub_df[factor_col], n_bins, labels=labels, duplicates='drop')
                return sub_df.groupby('group')[ret_col].mean()
            except Exception:
                return pd.Series(np.nan, index=range(n_bins))
        return df.groupby('date').apply(get_group_ret)

    @staticmethod
    def calc_stability_metrics(df: pd.DataFrame, factor_col: str) -> dict:
        """计算截面自相关性 (Auto-correlation) 与绝对换手率"""
        if df.empty: return {}
        pivot = df.pivot(index='date', columns='asset', values=factor_col)
        pivot_prev = pivot.shift(1)
        
        ac_cs = pivot.corrwith(pivot_prev, axis=1).mean()
        delta = np.nansum((pivot - pivot_prev).abs(), axis=1).sum()
        exposure = np.nansum(pivot.abs(), axis=1).sum()
        turnover = delta / exposure if exposure > 1e-6 else np.nan

        return {'Turnover_Avg': turnover, 'Rank_Stability': ac_cs}

    @staticmethod
    def filter_universe(df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤不可交易的股票 (Trading Universe Definition)
        注：出于 Showcase 目的，具体的过滤阈值已参数化为虚拟默认值。
        """
        print(f"   [Universe] 原始数据池规模: {len(df)}")
        df = df.copy()
        
        # 1. 基础流动性过滤 (去除一字板/停牌)
        if 'high' in df.columns and 'low' in df.columns:
            df = df[df['high'] != df['low']]
            
        # 2. [ALPHA LEAKAGE PREVENTED] 模糊化价格与上市天数阈值
        min_price = getattr(config, 'UNIVERSE_MIN_PRICE', 1.0)
        min_ipo_days = getattr(config, 'UNIVERSE_MIN_IPO_DAYS', 30)
        
        if 'close' in df.columns:
            df = df[df['close'] > min_price]
            
        if 'days_since_ipo' in df.columns:
            df = df[df['days_since_ipo'] >= min_ipo_days]
            df = df.drop(columns=['days_since_ipo'])
        
        # 3. 基础风控过滤
        if 'isst' in df.columns:
            df = df[df['isst'].astype(str) != '1']

        # 4. [ALPHA LEAKAGE PREVENTED] 模糊化流动性分位数截断值
        if 'amount' in df.columns:
            liquidity_quantile = getattr(config, 'UNIVERSE_LIQUIDITY_QUANTILE', 0.05)
            threshold = df.groupby('date')['amount'].transform(lambda x: x.quantile(liquidity_quantile))
            df = df[df['amount'] > threshold]
            
        print(f"   [Universe] 过滤后有效标的规模: {len(df)}")
        return df

# ==========================================
# 3. 主流程：增量评估流水线 (Incremental Pipeline)
# ==========================================
def run_daily_evaluation_pipeline():
    print("🚀 System Init: Daily Factor Evaluation Stream (Incremental Mode)")
    
    if not os.path.exists(config.NEUTRAL_FACTOR_DIR):
        print(f"❌ 未找到中性化因子目录: {config.NEUTRAL_FACTOR_DIR}")
        return

    factor_names_on_disk = [d for d in os.listdir(config.NEUTRAL_FACTOR_DIR) if os.path.isdir(os.path.join(config.NEUTRAL_FACTOR_DIR, d))]
    
    df_history = pd.DataFrame()
    factors_to_eval = factor_names_on_disk
    
    # 增量评估逻辑
    if os.path.exists(config.REPORT_PATH):
        try:
            df_history = pd.read_csv(config.REPORT_PATH)
            evaluated_factors = set(df_history['Factor'].tolist())
            factors_to_eval = [f for f in factor_names_on_disk if f not in evaluated_factors]
            print(f"📂 发现历史报告，已包含 {len(evaluated_factors)} 个因子。本次需增量评估 {len(factors_to_eval)} 个。")
        except Exception as e:
            print(f"⚠️ 读取历史报告失败 ({e})，将执行全量重新评估。")
    else:
        print(f"✨ 未发现历史报告，执行全量评估 (共 {len(factors_to_eval)} 个因子)。")

    if not factors_to_eval:
        print("\n✅ 所有因子均已评估，无需重复计算！")
        return

    # 加载行情基底数据
    print(f"\n📥 [IO] Loading base market data...")
    df_price = pd.read_parquet(config.DATA_PATH)
    df_price = FactorEvaluatorDaily.preprocess_data(df_price, ret_col='next_ret', horizon=config.return_horizon)
    df_price['date'] = df_price['timestamp'].dt.date.astype(str)
    
    # 加载宏观/市场状态标签 (若有)
    df_states = None
    if getattr(config, 'MARKET_STATE_PATH', None) and os.path.exists(config.MARKET_STATE_PATH):
        df_states = pd.read_csv(config.MARKET_STATE_PATH)
        df_states.columns = [c.lower() for c in df_states.columns]
        if 'timestamp' in df_states.columns and 'date' not in df_states.columns:
            df_states['date'] = pd.to_datetime(df_states['timestamp']).dt.date.astype(str)
        if 'state_name' not in df_states.columns and 'state' in df_states.columns:
            df_states['state_name'] = df_states['state']
        df_states = df_states[['date', 'state_name']].rename(columns={'state_name': 'market_state'}).drop_duplicates('date', keep='last')

    new_report_data = []

    # 因子遍历评估
    for f_name in factors_to_eval:
        print(f"  ⚙️ Processing {f_name} ...")
        f_path = os.path.join(config.NEUTRAL_FACTOR_DIR, f_name, "all_data.parquet")
        
        if not os.path.exists(f_path):
            continue
        
        try:
            df_factor = pd.read_parquet(f_path)
            df_eval = pd.merge(df_price, df_factor, on=['asset', 'timestamp'], how='inner')
            if df_eval.empty: continue
            
            df_eval = FactorEvaluatorDaily.filter_universe(df_eval)

            if df_states is not None:
                df_eval = pd.merge(df_eval, df_states, on=['date'], how='left')
                if 'market_state' in df_eval.columns:
                    df_eval['market_state'] = df_eval['market_state'].fillna('Unknown')

            s_ic = FactorEvaluatorDaily.calc_ic_series(df_eval, 'factor_value', 'next_ret')
            df_groups = FactorEvaluatorDaily.calc_group_returns(df_eval, 'factor_value', 'next_ret', n_bins=5)
            
            ic_mean = s_ic.mean()
            ic_std = s_ic.std()
            icir = (ic_mean / ic_std) if ic_std != 0 else 0
            
            # 收益率年化换算 (假设每年 242 交易日)
            if not df_groups.empty and 4 in df_groups.columns and 0 in df_groups.columns:
                ls_ret_daily = (df_groups[4] - df_groups[0]).mean()
                ls_ret_annual = ls_ret_daily * 242 / config.return_horizon
                long_ret_annual = df_groups[4].mean() * 242 / config.return_horizon
            else:
                ls_ret_annual = long_ret_annual = 0

            stability = FactorEvaluatorDaily.calc_stability_metrics(df_eval, 'factor_value')

            summary = {
                'Factor': f_name,
                'IC_Mean': ic_mean,
                'ICIR': icir,
                'Long_Ret_Annual': long_ret_annual,
                'LongShort_Ret_Annual': ls_ret_annual,
                'Turnover_Avg': stability.get('Turnover_Avg', np.nan),
                'Rank_Stability': stability.get('Rank_Stability', np.nan)
            }
            
            # 状态条件 IC 统计
            if 'market_state' in df_eval.columns:
                df_eval['ic_global'] = df_eval['date'].map(s_ic)
                state_stats = df_eval.groupby('market_state')['ic_global'].mean()
                for state, val in state_stats.items():
                    if state != 'Unknown': summary[f"IC_{state}_Mean"] = val

            new_report_data.append(summary)
            print(f"    ✅ Done. IC: {ic_mean:.4f}, ICIR: {icir:.4f}")
            
        except Exception as e:
            print(f"    ❌ Error on {f_name}: {e}")

    # 合并输出
    if new_report_data:
        df_new = pd.DataFrame(new_report_data)
        df_final = pd.concat([df_history, df_new], ignore_index=True) if not df_history.empty else df_new
            
        df_final.fillna(0, inplace=True) 
        df_final.sort_values('ICIR', ascending=False, inplace=True)
        
        os.makedirs(os.path.dirname(config.REPORT_PATH), exist_ok=True)
        df_final.to_csv(config.REPORT_PATH, index=False)
        print(f"\n🎯 增量评估完成！最新报告库已更新至: {config.REPORT_PATH}")

if __name__ == '__main__':
    run_daily_evaluation_pipeline()