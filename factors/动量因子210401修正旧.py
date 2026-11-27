# -*- coding: utf-8 -*-
"""
中性策略框架 | 邢不行 | 2024分享会
author: 邢不行
微信: xbx6660
"""
import pandas as pd
import numpy as np

from core.model.backtest_config import BacktestConfig
from core.utils.cpp_shim import (
    ewm as _ewm,
    rolling_min as _rolling_min,
    rolling_max as _rolling_max,
    rolling_mean as _rolling_mean,
    rolling_std as _rolling_std,
)

def get_lookback(param) -> int:
    """含 rolling(26×168=4368) 硬编码大窗口，保留全量数据不裁剪。"""
    return BacktestConfig.get_kline_num()

# def signal(*args):
#     df = args[0]
#     n = args[1]
#     factor_name = args[2]

#     source_cls = df.columns.tolist()

#     df['tp'] = (df['close'] + df['high'] + df['low']) / 3
#     df['tp2'] = df['tp'].ewm(span=n, adjust=False).mean()
#     df['diff'] = df['tp'] - df['tp2']
#     df['min'] = df['diff'].rolling(n, min_periods=1).min()
#     df['max'] = df['diff'].rolling(n, min_periods=1).max()

#     df[factor_name] = (df['diff'] - df['min']) / (df['max'] - df['min'])

#     df.drop(columns=list(set(df.columns.values).difference(set(source_cls + [factor_name]))), inplace=True)

#     return df


def signal_multi_params(df, param_list) -> dict:
    tp = (df['close'] + df['high'] + df['low']) / 3
    tpnmax = df['high']
    tpnmin = df['low']
    if (len(tp) >= 2):
        tp.iloc[0] = tp.iloc[1]
        tpnmax.iloc[0] = tpnmax.iloc[1]
        tpnmin.iloc[0] = tpnmin.iloc[1]


    tpn = tp[-52560:]
    tpnmax = pd.Series(
        _rolling_min(tpnmax.values, 4, 4), index=tpnmax.index
    )[-52560:]
    tpnmin = pd.Series(
        _rolling_max(tpnmin.values, 4, 4), index=tpnmin.index
    )[-52560:]

  
    ret = dict()
    for param in param_list:
        n = min(int(param), tpn.shape[0])
        # weights = np.concatenate(([1], 2**np.arange(n-1)))  
        # # tp_ma = tp.ewm(span=n, adjust=False).mean()

        # 1. 检测异常值（使用Z-score方法）
        pctchange = (tpnmax - tpnmin)/tpnmin
        pctma = pd.Series(_rolling_mean(pctchange.values, n, 1), index=pctchange.index)
        pctstd = pd.Series(_rolling_std(pctchange.values, n, 1), index=pctchange.index)
        pctzscore = (pctchange - pctma) / pctstd
        threshold = 25
        is_anomaly = (np.abs(pctzscore) > threshold) & ((pctchange > 0.2) | (pctchange < -0.5))#

        #将异常值替换为nan
        # print(df[is_anomaly].shape[0])
        anomaly_indices = tpn[is_anomaly].index.values.ravel()

        # close.loc[anomaly_indices, 'value'] = np.nan
        tpn.loc[anomaly_indices] = np.nan

        split_indices = np.where(is_anomaly)[0] + 1
        dfs = np.split(tpn, split_indices)

        subfactor = []
        for i, sub_df in enumerate(dfs):
            if sub_df.shape[0] > 168:
      

                sub_tp_ma = pd.Series(_ewm(sub_df.values, n), index=sub_df.index)
                # tp_ma = tp.rolling(n, min_periods=1).apply(lambda x: x.ewm(n, adjust=False).mean().to_list()[-1])
                # print(pd.concat([tp_ma, tp], axis=1, keys=['tp_ma', 'tp']).head(1000))

                sub_diff = (sub_df - sub_tp_ma)
                sub_diff_min = pd.Series(_rolling_min(sub_diff.values, n, 167), index=sub_diff.index)
                sub_diff_max = pd.Series(_rolling_max(sub_diff.values, n, 167), index=sub_diff.index)

            
                subfactor.append((sub_diff - sub_diff_min) / (sub_diff_max - sub_diff_min))
            else:
                subfactor.append(pd.Series(np.full(sub_df.shape[0], np.nan)))
        


    #补长对齐df以免计算错误
        factor = pd.concat(subfactor,ignore_index=True)

        max_high = pd.Series(_rolling_max(tpnmax.values, 26*168, 1), index=tpnmax.index)
        min_low = pd.Series(_rolling_min(tpnmin.values, 26*168, 1), index=tpnmin.index)
        factor = factor + (tpn - min_low) / (max_high - min_low)

        if (len(tp) != len(factor)):
            factor = pd.concat([pd.Series([np.nan] * (len(tp) - len(factor))), factor])


        ret[str(param)] = factor
    return ret
