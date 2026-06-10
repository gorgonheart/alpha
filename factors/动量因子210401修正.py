# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from factors.c_ext.momentum_wrapper import compute_momentum_c

try:
    from core.model.backtest_config import BacktestConfig as _BacktestConfig
    def get_lookback(param) -> int:
        """含 rolling(26×168=4368) 硬编码大窗口，保留全量数据不裁剪。"""
        return _BacktestConfig.get_kline_num()
except ImportError:
    def get_lookback(param) -> int:  # type: ignore[misc]
        return 52560

TRUNC_LEN = 52560


def signal_multi_params(df, param_list) -> dict:
    close = np.ascontiguousarray(df['close'].values, dtype=np.float64)
    high  = np.ascontiguousarray(df['high'].values,  dtype=np.float64)
    low   = np.ascontiguousarray(df['low'].values,   dtype=np.float64)
    ret = {}
    for param in param_list:
        n = min(int(param), len(close))
        out = compute_momentum_c(close, high, low, n=n, trunc_len=TRUNC_LEN)
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
