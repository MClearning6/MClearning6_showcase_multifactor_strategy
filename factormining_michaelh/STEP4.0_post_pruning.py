import pandas as pd
import os
import shutil
import config

# ================= 配置路径 =================
# 建议通过 config 统一管理
OUTPUT_DIR = config.OUTPUT_DIR
NEUTRAL_FACTOR_DIR = getattr(config, 'NEUTRAL_FACTOR_DIR', os.path.join(os.path.dirname(OUTPUT_DIR), "neutral_factors"))
DRY_RUN = getattr(config, 'CLEANUP_DRY_RUN', True) # 默认开启预防模式，体现严谨性
# ============================================

def main():
    report_path = config.REPORT_PATH
    if not os.path.exists(report_path):
        print(f" [Error] 找不到评估报告: {report_path}")
        return

    # 1. 加载研究评估报告
    df_report = pd.read_csv(report_path)
    report_factors = set(df_report['Factor'].tolist())
    
    # 扫描磁盘现状
    disk_factors = set([
        d for d in os.listdir(OUTPUT_DIR)
        if os.path.isdir(os.path.join(OUTPUT_DIR, d))
    ])

    # 2. [ALPHA LEAKAGE PREVENTED] 使用配置化的动态阈值
    # 避免在代码中暴露具体的 IC 筛选底线
    ic_threshold = getattr(config, 'GARBAGE_IC_THRESHOLD', 0.01)
    
    garbage_mask = df_report['IC_Mean'].abs() < ic_threshold
    garbage_factors_list = df_report[garbage_mask]['Factor'].tolist()

    # 找出孤儿因子 (在磁盘上但未被评估)
    absent_factors = disk_factors - report_factors
    all_to_delete = list(set(garbage_factors_list) | absent_factors)
    
    print(f" [Cleanup Stats] 总测试因子: {len(df_report)}")
    print(f" [Cleanup Stats] 待清理因子 (IC < {ic_threshold}): {len(garbage_factors_list)}")
    print(f" [Cleanup Stats] 孤儿因子 (未归档): {len(absent_factors)}")
    print(f" [Action] 拟物理删除总计: {len(all_to_delete)} 个因子目录\n")

    if DRY_RUN:
        print(" [Mode: DRY_RUN] 仅显示清理计划，不执行实际删除。请在 config 中关闭 CLEANUP_DRY_RUN 以应用更改。")

    freed_space_mb = 0

    # 3. 执行物理清理过程
    for f_name in all_to_delete:
        factor_path = os.path.join(OUTPUT_DIR, f_name)
        cache_path = os.path.join(NEUTRAL_FACTOR_DIR, f_name)

        # 统计并移除路径
        target_paths = [p for p in [factor_path, cache_path] if os.path.exists(p)]
        
        for path in target_paths:
            # 计算待释放空间
            for root, dirs, files in os.walk(path):
                freed_space_mb += sum(os.path.getsize(os.path.join(root, name)) for name in files) / (1024 * 1024)
            
            if not DRY_RUN:
                shutil.rmtree(path)
                print(f"    [Deleted] {path}")
            else:
                print(f"    [Plan] Should delete: {path}")

    status = "计划释放" if DRY_RUN else "实际释放"
    print(f"\n 清理作业完成！{status}空间: {freed_space_mb:.2f} MB")

if __name__ == "__main__":
    main()