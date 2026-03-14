import os

# ==========================================
# 1. 路径管理 (使用相对路径，彻底脱敏)
# ==========================================
# 自动获取项目根目录，确保代码在任何机器上克隆后都能直接跑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 数据输入路径 (建议在仓库中放一个 sample_data.parquet 用于演示)
DATA_PATH = os.path.join(BASE_DIR, "data/daily_sample.parquet")

# 因子输出目录
OUTPUT_DIR = os.path.join(BASE_DIR, "output/raw_factors")
NEUTRAL_FACTOR_DIR = os.path.join(BASE_DIR, "output/neutral_factors")

# 评估报告与风险矩阵缓存
REPORT_PATH = os.path.join(BASE_DIR, "output/evaluation_report.csv")
RISK_CACHE_PATH = os.path.join(BASE_DIR, "output/risk_matrix.parquet")
FINAL_ALPHA_PATH = os.path.join(BASE_DIR, "output/alpha_scores/combined_alpha.parquet")

# 外部状态参考 (如市场择时信号，可选)
MARKET_STATE_PATH = os.path.join(BASE_DIR, "data/market_states.csv")

# ==========================================
# 2. 核心基建配置 (Infrastructure)
# ==========================================
FREQ = '1d'              # 锁定日频展示，保护高频逻辑
BUFFER_DAYS = 21         # 因子计算回溯 Buffer 天数
DATA_ADAPTER = "adapter_daily_grid" # 对应 data_adapt.py 中的函数名

# ==========================================
# 3. 风险模型与中性化 (Risk & Neutralization)
# ==========================================
# 统计风险模型参数 ( Connor-Korajczyk PCA 逻辑 )
RISK_LOOKBACK = xxx       # 风险模型滚动窗口
N_COMPONENTS = xxx          # 提取的隐性风格因子数量

PCA_SETTINGS = {
    "enabled": True,
    "winsorize": True,
    "standardize": True,
    "lookback": xxx,      # 与 RISK_LOOKBACK 保持同步
    "n_components": xxx
}

# ==========================================
# 4. 评估参数 (Evaluation)
# ==========================================
return_horizon = 5        # 预测目标：未来 N 日超额收益
COMBINE_METRIC = 'ICIR'   # 因子合成权重基准

# --- Universe 过滤阈值 (Alpha 护城河，建议模糊化处理) ---
UNIVERSE_MIN_PRICE = 2.0         # 过滤低价股
UNIVERSE_MIN_IPO_DAYS = 60       # 过滤次新股
UNIVERSE_LIQUIDITY_QUANTILE = 0.1 # 剔除截面成交额最后 10% 的股票
BACKTEST_START = '2018-01-01'    # 策略/回测起始时间

# ==========================================
# 5. 清理与自动化 (OpSec & Cleanup)
# ==========================================
CLEANUP_DRY_RUN = True           # 默认开启模拟清理，防止误删
GARBAGE_IC_THRESHOLD = 0.01      # IC 低于此值的因子被视为无效

# ==========================================
# 6. 因子池配置 (Alpha Library)
# ==========================================
# 最终参与合成的 Alpha 名单
SELECTED_ALPHAS = [
    'factor_momentum_20',
    'factor_reversal_5',
    'factor_volatility_std_10'
]

# 因子挖掘任务清单 (表达式引擎调用),只有需要复杂的中继变量时用到算字库，否则直接写expr
FACTOR_LIST = [
    {
        "name": "Alpha_041",
        "desc": "(((high * low)^0.5) - vwap)",
        "expr": "((df['high'] * df['low'])**0.5) - (df['amount'] / (df['volume'] + 1e-8))",
        "params": {}, "shift": 0
    },
    # ... 更多因子定义
]