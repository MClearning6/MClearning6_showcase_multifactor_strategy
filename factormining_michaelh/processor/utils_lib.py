# 日频算子库
import pandas as pd
import numpy as np

def ts_rank(s, window=10):
    """时序滚动排序"""
    return s.rolling(window, min_periods=1).rank(pct=True)