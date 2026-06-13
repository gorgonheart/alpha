#!/usr/bin/python3
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from factors.c_ext.cci_wrapper import compute_cci_c

WINDOW = 15000


def signal_multi_params(df, param_list) -> dict:
    open_ = np.ascontiguousarray(df["open"].values,  dtype=np.float64)
    high  = np.ascontiguousarray(df["high"].values,  dtype=np.float64)
    low   = np.ascontiguousarray(df["low"].values,   dtype=np.float64)
    close = np.ascontiguousarray(df["close"].values, dtype=np.float64)
    ret = {}
    for param in param_list:
        out = compute_cci_c(open_, high, low, close, n=int(param), window=WINDOW)
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
