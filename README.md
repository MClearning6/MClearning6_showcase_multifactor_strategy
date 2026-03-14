# MClearning6_showcase_multifactor_strategy
# readme由AI生成
Quantitative Factor Mining Infrastructure (Showcase)
📌 项目概述 (Overview)
本项目展示了一套工业级的日频（EOD）多因子量化研究与挖掘基建。该框架实现了从原始数据适配、向量化因子并行计算、统计风险中性化到因子增量评估的全流程自动化。

核心目标：提供一个高可扩展性、逻辑严谨且具备生产环境抗风险能力的因子挖掘 Pipeline。

🔒 知识产权声明 (IP & OpSec Disclaimer)
本仓库仅作为技术能力展示（Showcase）。出于 Alpha 隔离与合规要求，所有高频（Minute/Tick）计算模块、核心盈利因子公式、以及具体的交易执行参数（如具体的 Universe 过滤阈值、T+1 冲击成本假设）均已剥离或进行模糊化处理。

⚠️ 免责声明 (Non-Investment Advice Disclaimer)
本项目所提供的所有内容（包括代码、数据及回测结果）仅用于技术研究与学术交流，不构成任何形式的投资建议。回测收益不代表未来表现。据此操作，风险自担。

🛠 核心架构 (Architecture)
项目采用模块化设计，确保每一层逻辑均可独立测试与复用：

data_prepare/: 数据治理层。通过物理日历网格对齐（Reindex），消除数据缺失导致的截面偏移。

processor/utils_lib.py: 算子库。封装了高性能的向量化时间序列与截面算子（Moving Window / Cross-Sectional）。

processor/cleaner.py: 信号清洗管道。集成 MAD 缩尾、Z-score 标准化以及基于 Connor-Korajczyk 逻辑的统计中性化。

factor_mining_infra/: 计算调度核心。

main_daily.py: 基于表达式引擎的因子并行量产系统。

neutralize_engine.py: 滚动 PCA 风险暴露剔除引擎。

evaluate_main_daily.py: 增量式评价系统，支持 IC/ICIR、换手率及市场状态条件 IC 分析（因子切割理论）。

post_pruning.py: 清理质量差的因子。

simple_concat.py: 简单信号合成，从因子库中取出目标因子并使用ICIR加权线性合成。

config.py: 全局参数中枢。管理研究路径、风险阈值与 Universe 定义。

strategy_run_v2_dualtrack.py: 采用未复权和复权价双轨制回测，因子计算和实盘交易逻辑解耦，优化器具体逻辑已隐藏。

🚀 关键技术特性 (Key Features)
1. 统计风险模型与中性化 (Statistical Neutralization)

不同于传统的行业/风格中性化，框架内置了基于滚动 PCA 的隐性风险剥离模块。通过提取市场内生共性风险因子（Latent Factors），确保产出的 Alpha 信号具有极低的市场相关性。

2. 表达式计算引擎 (Expression-based Engine)

支持通过 config 定义因子表达式，实现因子逻辑与计算逻辑的解耦。支持多参数组合自动扩展，极大提升了因子挖掘的迭代效率。

3. 增量评估流水线 (Incremental Evaluation)

针对大规模因子库设计的增量评价逻辑。系统会自动记录已评估因子的指纹，仅对新挖掘的因子进行回测分析，大幅节省算力资源。

4. 严谨的 Universe 治理

内置多维度过滤逻辑（Liquidity, Price, IPO Days, ST），并在收益率计算中严格遵守 T+1 交易规则，有效防止回测中的“虚假繁荣”。

📊 产出示例 (Sample Outputs)
完成流水线后，系统将在 output/ 目录下生成可视化报告：

Factor Report: 包含各因子的 IC Mean, ICIR, Turnover, 以及在不同市场状态下的表现。

Risk Matrix: 缓存的滚动主成分暴露矩阵，防止隐秘的未来函数。

Alpha Scores: 用于回测引擎消费的最终加权信号流。



📈 策略回测框架 (Backtesting Framework)
回测模块并非简单的矩阵运算，而是一个高度模拟真实物理世界的执行系统。它旨在验证 Alpha 信号在考虑交易成本、流动性约束及公司行为后的真实捕获能力。

核心设计原则 (Core Mechanics)

物理时空对齐 (Strict Grid Alignment): 基于基准指数（Benchmark）的交易日历构建全市场笛卡尔积网格，强制处理所有停牌与退市节点，彻底杜绝回测中的“幸存者偏差”。

精密除权处理 (Corporate Actions Adjustment): 由于缺乏实际分红数据，框架通过复权因子动态逆推交易所的除权参考价，解耦送股（Bonus Shares）与现金分红（Cash Dividends），确保账户权益的计算符合真实会计准则。​	
 
物理撮合限制 (T+1 Execution Boundary):

买入约束: 严格检查目标标的是否处于涨停状态（Limit-up）或停牌，涨停板无法建立新头寸。

卖出约束: 严格检查跌停状态（Limit-down）或停牌，跌停板头寸将被强制锁定，直至流动性恢复。

交易摩擦模拟: 支持自定义佣金（Commission）、印花税（Stamp Duty）及滑点（Slippage）模型，支持基于成交量权重的订单撮合。

模块说明 (Module Breakdown)

ExecutionEngine: 模拟券商柜台逻辑，管理现金流、持仓头寸及订单流水（Order Ledger）。

PortfolioOptimizer: 组合优化器接口。支持从简单的等权分配到复杂的凸优化（CVXOPT）权重求解。

Analyzer: 绩效归因中心。自动生成交割单、持仓日志，并输出包含最大回撤（Drawdown）填充的高清收益曲线。

📊 绩效展示 (Performance Showcase)
系统运行结束后，会在 output/ 目录下生成完整的绩效包：

backtest_orders.csv: 详尽的交割单，包含每笔交易的价格、费率及成交后的现金水位。

backtest_positions.csv: 每日持仓快照，记录组合的市值分布。

equity_curve.png: 高清收益曲线对比图，直观展示策略相对于基准的超额收益（Alpha）及风险回撤特征。

📌 回测示例输出：

Date: 2026-01-12 | Equity: 4007407 | Position: {'600668.SH': 67800, '002722.SZ': 49600, '000779.SZ': 93300, '600449.SH': 55000, '600051.SH': 104200, '000877.SZ': 19100, '002815.SZ': 300, '000823.SZ': 1900}

Date: 2026-01-19 | Equity: 4058821 | Position: {'600668.SH': 71000, '000779.SZ': 91400, '600449.SH': 55000, '600051.SH': 103900, '000877.SZ': 19100, '000823.SZ': 400, '601002.SH': 152600}

Date: 2026-01-26 | Equity: 4204948 | Position: {'600668.SH': 70000, '000779.SZ': 95300, '600051.SH': 103800, '601002.SH': 150700, '603118.SH': 4300, '002752.SZ': 122200}

Date: 2026-02-02 | Equity: 4093334 | Position: {'600668.SH': 75100, '000779.SZ': 94800, '600051.SH': 103600, '002752.SZ': 126700, '603869.SH': 84300, '002212.SZ': 900}

Date: 2026-02-09 | Equity: 4266284 | Position: {'600668.SH': 76600, '000779.SZ': 92800, '600051.SH': 103200, '002752.SZ': 124600, '603869.SH': 83400, '002212.SZ': 4200}

回测区间2020-02-11 ~ 2026-02-10 

   回测结果摘要

总收益率: 329.74%

最大回撤: -21.66%

[Report] 收益曲线图已保存至: /xxx/backtest_equity_dualtrack.png

交割单已导出至: /xxx/backtest_orders.csv

持仓明细已导出至: /xxx/backtest_positions.csv

💡 回测说明 (Backtest Notes)
示例输出中的高额累计收益部分归因于特定的回测区间与资产池，且未由于个人资金未考虑冲击成本（Impact Cost）在大规模资金下的衰减效应，仅使用千1滑点模拟。实际生产环境中的滑点表现可能因流动性挤压而劣于回测预估。
