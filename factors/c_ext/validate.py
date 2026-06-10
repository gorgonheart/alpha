"""
Validate C output matches Python original for BTCUSDT real data.

Run:
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/validate.py
"""
import sys
import os

# ensure project root on path
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd
from factors.c_ext.momentum_wrapper import compute_momentum_c

BTCUSDT_PATH = os.path.join(
    _ROOT, "data", "binance_spot_1h_resample", "40m", "BTCUSDT.pkl"
)
PARAMS = [168, 336]
TRUNC_LEN = 52560


def signal_multi_params_py(df, param_list):
    """Python original (copied verbatim from 动量因子210401修正.py, no C dependency)."""
    tp = (df['close'] + df['high'] + df['low']) / 3
    tpnmax = df['high'].copy()
    tpnmin = df['low'].copy()
    if len(tp) >= 2:
        tp.iloc[0] = tp.iloc[1]
        tpnmax.iloc[0] = tpnmax.iloc[1]
        tpnmin.iloc[0] = tpnmin.iloc[1]

    tpn    = tp[-TRUNC_LEN:]
    tpnmax = tpnmax.rolling(4).min()[-TRUNC_LEN:]
    tpnmin = tpnmin.rolling(4).max()[-TRUNC_LEN:]

    ret = {}
    for param in param_list:
        n = min(int(param), tpn.shape[0])
        pctchange = (tpnmax - tpnmin) / tpnmin
        pctma     = pctchange.rolling(n, min_periods=1).mean()
        pctstd    = pctchange.rolling(n, min_periods=1).std()
        pctzscore = (pctchange - pctma) / pctstd
        threshold = 25
        is_anomaly = (np.abs(pctzscore) > threshold) & (
            (pctchange > 0.2) | (pctchange < -0.5)
        )
        tpn_copy = tpn.copy()
        anomaly_indices = tpn_copy[is_anomaly].index.values.ravel()
        tpn_copy.loc[anomaly_indices] = np.nan
        split_indices = np.where(is_anomaly)[0] + 1
        dfs = np.split(tpn_copy, split_indices)
        subfactor = []
        for sub_df in dfs:
            if sub_df.shape[0] > 168:
                sub_tp_ma    = sub_df.ewm(span=n, adjust=False).mean()
                sub_diff     = sub_df - sub_tp_ma
                sub_diff_min = sub_diff.rolling(n, min_periods=167).min()
                sub_diff_max = sub_diff.rolling(n, min_periods=167).max()
                subfactor.append(
                    (sub_diff - sub_diff_min) / (sub_diff_max - sub_diff_min)
                )
            else:
                subfactor.append(pd.Series(np.full(sub_df.shape[0], np.nan)))
        factor = pd.concat(subfactor, ignore_index=True)
        max_high = tpnmax.rolling(26 * 168, min_periods=1).max()
        min_low  = tpnmin.rolling(26 * 168, min_periods=1).min()
        factor   = factor + (tpn_copy - min_low) / (max_high - min_low)
        if len(tp) != len(factor):
            factor = pd.concat(
                [pd.Series([np.nan] * (len(tp) - len(factor))), factor]
            )
        ret[str(param)] = factor.values
    return ret


def main():
    if not os.path.exists(BTCUSDT_PATH):
        print(f"Data file not found: {BTCUSDT_PATH}")
        print("Skipping validation (no test data available).")
        sys.exit(0)

    print(f"Loading {BTCUSDT_PATH} ...")
    df = pd.read_pickle(BTCUSDT_PATH).sort_values("candle_begin_time").reset_index(drop=True)
    print(f"Rows: {len(df)}")

    py_results = signal_multi_params_py(df, PARAMS)

    all_pass = True
    for param in PARAMS:
        py_arr = np.asarray(py_results[str(param)], dtype=np.float64)
        c_arr  = compute_momentum_c(
            df["close"].values, df["high"].values, df["low"].values,
            n=param, trunc_len=TRUNC_LEN,
        )

        py_nan = np.isnan(py_arr)
        c_nan  = np.isnan(c_arr)
        nan_match = np.array_equal(py_nan, c_nan)

        if nan_match:
            valid = ~py_nan
            val_match = bool(np.allclose(py_arr[valid], c_arr[valid], atol=1e-9))
        else:
            val_match = False
            mismatch_nan = np.where(py_nan != c_nan)[0]
            print(f"  NaN mismatch at indices: {mismatch_nan[:10]}")

        status = "PASS" if (nan_match and val_match) else "FAIL"
        if status == "FAIL":
            all_pass = False
            if nan_match:
                bad = np.where(~py_nan & (np.abs(py_arr - c_arr) > 1e-9))[0]
                if len(bad):
                    print(f"  val mismatch at: {bad[:5]}")
                    print(f"    py={py_arr[bad[:3]]}  c={c_arr[bad[:3]]}")

        print(f"param={param:>5}  NaN-match={nan_match}  val-match={val_match}  [{status}]")

    if all_pass:
        print("\nAll validations PASSED.")
        sys.exit(0)
    else:
        print("\nValidation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
