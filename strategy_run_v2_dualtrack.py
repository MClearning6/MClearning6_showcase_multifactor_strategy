import os
import sys
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from datetime import datetime

# ==========================================
# 1. 全局配置中心 (Config)
# ==========================================
class Config:
    """
    [脱敏说明]: 所有个人路径已替换为相对项目根目录的路径。
    """
    # 路径管理
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "data")
    
    # 输入文件 (需确保这些文件存在于 data 目录下)
    FACTOR_PATH = os.path.join(DATA_DIR, "alpha_scores.parquet")
    PRICE_PATH = os.path.join(DATA_DIR, "market_daily.parquet")
    BENCHMARK_PATH = os.path.join(DATA_DIR, "benchmark_000852.parquet") 
    
    # 账户初始化
    INITIAL_CAPITAL = 1000000.0
    BACKTEST_START_DATE = '2020-01-01'
    
    # 交易成本 (已调为行业标准)
    COMMISSION_RATE = 0.0003 
    STAMP_DUTY = 0.0005      
    SLIPPAGE = 0.001        
    
    # 策略参数
    BENCHMARK_CODE = '000852'
    EVICTION_RANK = 100      # 因子排名超过此值则踢出组合
    TARGET_POS_COUNT = 50    # 目标持仓数量
    
    # 优化器配置
    OPT_CONFIG = {
        'lambda_risk': 0.1,
        'lambda_turnover': 1.25, 
        'max_pos_limit': 0.15    
    }

# ==========================================
# 2. 数据处理与对齐 (DataFeed)
# ==========================================
class DataFeed:
    def __init__(self, config):
        self.config = config
        self.real_price_df = None 
        self.adj_price_df = None  
        self.adj_factor_df = None 
        self.volume_df = None
        self.bench_df = None
        self.factor_df = None 
        self.valid_dates = []

    def load_and_align_data(self):
        print(">>> [DataFeed] 启动严密物理日历对齐 (Grid Alignment)...")
        
        # 1. 加载全量数据
        df_factor = pd.read_parquet(self.config.FACTOR_PATH)
        df_price = pd.read_parquet(self.config.PRICE_PATH)
        df_bench = pd.read_parquet(self.config.BENCHMARK_PATH)

        # 2. 构造绝对时空网格
        trading_calendar = pd.DatetimeIndex(df_bench['date'].unique()).sort_values()
        universe_assets = sorted(df_price['asset'].unique())
        
        multi_idx = pd.MultiIndex.from_product(
            [trading_calendar, universe_assets], names=['date', 'asset']
        )

        # 3. 向量化重排至绝对网格 (Unstack)
        df_price_grid = df_price.set_index(['date', 'asset']).reindex(multi_idx)
        self.real_price_df = df_price_grid['close'].unstack()
        self.adj_price_df = df_price_grid['adj_close'].unstack()
        self.adj_factor_df = df_price_grid['adj_factor'].unstack()
        self.volume_df = df_price_grid['volume'].unstack().fillna(0)
        self.high_df = df_price_grid.get('high', df_price_grid['close']).unstack()
        self.low_df = df_price_grid.get('low', df_price_grid['close']).unstack()
        
        # 处理因子评分对齐
        self.factor_df = df_factor.set_index(['date', 'asset']).reindex(multi_idx).reset_index()
        self.bench_df = df_bench.set_index('date').reindex(trading_calendar)['close'].sort_index()

        self.valid_dates = trading_calendar.tolist()
        self._precalc_status()

    def _precalc_status(self):
        """逆推 A 股真实的涨跌停价格边界"""
        # 昨日复权价 / 今日复权因子 = 今日真实世界的基准参考价
        ex_ref_price = self.adj_price_df.shift(1) / self.adj_factor_df
        
        # 构建涨跌停乘数 (已脱敏：整合为板块逻辑)
        limit_up_ratio = pd.DataFrame(0.10, index=ex_ref_price.index, columns=ex_ref_price.columns)
        
        # 处理科创/创业板特殊比例
        is_star_gem = (limit_up_ratio.columns.str.startswith('688')) | (limit_up_ratio.columns.str.startswith('300'))
        limit_up_ratio.loc[:, is_star_gem] = 0.20
        
        # 计算理论价格并取 2 位精度
        limit_up_price = (ex_ref_price * (1 + limit_up_ratio)).round(2)
        limit_down_price = (ex_ref_price * (1 - limit_up_ratio)).round(2)

        self.is_limit_up = (self.real_price_df >= limit_up_price - 0.001) & (self.real_price_df == self.high_df)
        self.is_limit_down = (self.real_price_df <= limit_down_price + 0.001) & (self.real_price_df == self.low_df)
        self.is_suspended = self.volume_df < 1e-4

# ==========================================
# 3. 撮合与结算引擎 (ExecutionEngine)
# ==========================================
class ExecutionEngine:
    def __init__(self, config):
        self.config = config
        self.cash = config.INITIAL_CAPITAL
        self.positions = {} # {code: volume}
        self.history = []
        self.order_ledger = []

    def process_corporate_actions(self, today_factors, prev_factors, yesterday_real_prices):
        """
        [逻辑核心]: 通过复权因子逆推送股与分红。
        $P_{ref} = P_{prev} / (Factors_{today} / Factors_{prev})$
        """
        for code in list(self.positions.keys()):
            tf, pf = today_factors.get(code, 0), prev_factors.get(code, 0)
            if pf > 0 and (tf / pf) > 1.0001:
                ratio = tf / pf
                p_yest = yesterday_real_prices.get(code, 0)
                if p_yest <= 0: continue
                
                p_ref = p_yest / ratio
                # 逆推送股比例 S (假设以 0.1 为步长)
                est_s = math.floor((ratio - 1) * 10) / 10.0
                if est_s > 0:
                    self.positions[code] += int(self.positions[code] * est_s)
                
                # 现金分红逻辑 (扣除送股占用的价值)
                div_cash = self.positions[code] * (p_yest - p_ref * (1 + est_s))
                if div_cash > 0: self.cash += div_cash

    def execute_and_settle(self, date, target_money_dict, real_prices, status):
        """
        物理层结算：处理 T+1 涨跌停限制与佣金。
        """
        # 1. 锁定停牌和跌停无法卖出的持仓
        for code in list(self.positions.keys()):
            if status['susp'].get(code, False) or status['down'].get(code, False):
                target_money_dict[code] = self.positions[code] * real_prices.get(code, 0)

        # 2. 生成交易清单
        all_codes = set(list(self.positions.keys()) + list(target_money_dict.keys()))
        
        # [卖出流]
        for code in all_codes:
            current_vol = self.positions.get(code, 0)
            target_vol = int(target_money_dict.get(code, 0) / real_prices.get(code, 1) / 100) * 100 if real_prices.get(code, 0) > 0 else 0
            
            if target_vol < current_vol:
                if not status['susp'].get(code, False) and not status['down'].get(code, False):
                    sell_vol = current_vol - target_vol
                    exec_price = real_prices[code] * (1 - self.config.SLIPPAGE)
                    amount = sell_vol * exec_price
                    fee = amount * (self.config.COMMISSION_RATE + self.config.STAMP_DUTY)
                    self.cash += (amount - fee)
                    self.positions[code] -= sell_vol
                    if self.positions[code] <= 0: del self.positions[code]
                    self._record_order(date, code, 'SELL', sell_vol, exec_price, amount, fee)

        # [买入流] (按评分排序，此处简化处理)
        for code, t_money in target_money_dict.items():
            current_vol = self.positions.get(code, 0)
            target_vol = int(t_money / real_prices.get(code, 1) / 100) * 100
            
            if target_vol > current_vol:
                if not status['susp'].get(code, False) and not status['up'].get(code, False):
                    buy_vol = target_vol - current_vol
                    exec_price = real_prices[code] * (1 + self.config.SLIPPAGE)
                    cost = buy_vol * exec_price
                    fee = max(cost * self.config.COMMISSION_RATE, 5.0)
                    if self.cash >= (cost + fee):
                        self.cash -= (cost + fee)
                        self.positions[code] = self.positions.get(code, 0) + buy_vol
                        self._record_order(date, code, 'BUY', buy_vol, exec_price, cost, fee)

    def _record_order(self, date, code, direction, vol, price, amt, fee):
        self.order_ledger.append({
            'date': date.date(), 'code': code, 'direction': direction,
            'volume': vol, 'price': round(price, 3), 'amount': round(amt, 2),
            'fee': round(fee, 2), 'cash': round(self.cash, 2)
        })

# ==========================================
# 4. 核心调度与分析 (Controller)
# ==========================================
class BacktestController:
    def __init__(self):
        self.config = Config()
        self.data_feed = DataFeed(self.config)
        self.execution = ExecutionEngine(self.config)

    def run(self):
        self.data_feed.load_and_align_data()
        dates = [d for d in self.data_feed.valid_dates if d >= pd.to_datetime(self.config.BACKTEST_START_DATE)]
        
        print(f">>> [Backtest] 启动双轨回测 | 范围: {dates[0].date()} ~ {dates[-1].date()}")
        
        prev_adj_factors = None
        prev_real_prices = None
        
        for i, today in enumerate(dates):
            if i == 0: continue
            yesterday = dates[i-1]
            
            # 获取当日市场快照
            real_prices = self.data_feed.real_price_df.loc[today]
            adj_factors = self.data_feed.adj_factor_df.loc[today]
            status = {
                'up': self.data_feed.is_limit_up.loc[today],
                'down': self.data_feed.is_limit_down.loc[today],
                'susp': self.data_feed.is_suspended.loc[today]
            }
            
            # 1. 处理分红送股
            if prev_adj_factors is not None:
                self.execution.process_corporate_actions(adj_factors, prev_adj_factors, prev_real_prices)
            
            # 2. 生成目标持仓 (此处 Mock 了 Optimizer 的输出逻辑)
            # [Showcase Note]: 实际生产中此处调用 self.optimizer.solve()
            df_factor_today = self.data_feed.factor_df[self.data_feed.factor_df['date'] == yesterday].dropna()
            top_picks = df_factor_today.sort_values('total_score', ascending=False).head(self.config.TARGET_POS_COUNT)
            
            target_money = {}
            if not top_picks.empty:
                # 简化的等权分配演示
                current_equity = self.execution.cash + sum(v * real_prices.get(c, 0) for c, v in self.execution.positions.items())
                per_stock_money = current_equity / self.config.TARGET_POS_COUNT
                for code in top_picks['asset']:
                    target_money[code] = per_stock_money
            
            # 3. 执行交易与结算
            equity = self.execution.execute_and_settle(today, target_money, real_prices, status)
            
            # 记录历史
            self.execution.history.append({
                'date': today, 'equity': equity, 'benchmark': self.data_feed.bench_df.loc[today]
            })
            
            prev_adj_factors, prev_real_prices = adj_factors, real_prices
            if i % 20 == 0:
                print(f"   Progress: {today.date()} | Equity: {equity:,.0f}")

        self.generate_report()

    def generate_report(self):
        res = pd.DataFrame(self.execution.history).set_index('date')
        res['strategy_nv'] = res['equity'] / self.config.INITIAL_CAPITAL
        res['bench_nv'] = res['benchmark'] / res['benchmark'].iloc[0]
        
        print("\n" + "="*30)
        print("📊 回测报告摘要")
        print(f"最终净值: {res['strategy_nv'].iloc[-1]:.4f}")
        print(f"超额收益: {res['strategy_nv'].iloc[-1] - res['bench_nv'].iloc[-1]:.4f}")
        print("="*30)
        
        res[['strategy_nv', 'bench_nv']].plot(title='Backtest Showcase: Strategy vs Benchmark')
        plt.show()

if __name__ == "__main__":
    controller = BacktestController()
    controller.run()
