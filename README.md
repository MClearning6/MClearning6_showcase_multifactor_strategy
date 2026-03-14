# MClearning6_showcase_multifactor_strategy
# readme由AI生成
Quantitative Factor Mining Infrastructure (Showcase)
📌 项目概述 (Overview)
本项目展示了一套工业级的日频（EOD）多因子量化研究与挖掘基建。该框架实现了从原始数据适配、向量化因子并行计算、统计风险中性化到因子增量评估的全流程自动化。

核心目标：提供一个高可扩展性、逻辑严谨且具备生产环境抗风险能力的因子挖掘 Pipeline。

🔒 知识产权声明 (IP & OpSec Disclaimer)
本仓库仅作为技术能力展示（Showcase）。出于 Alpha 隔离与合规要求，所有高频（Minute/Tick）计算模块、核心盈利因子公式、以及具体的交易执行参数（如具体的 Universe 过滤阈值、T+1 冲击成本假设）均已剥离或进行模糊化处理。

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
