# factors/c_ext/illq_validate.py
"""
Validate C output matches Python for fixture data.
Run: conda run -n alpha311 python factors/c_ext/illq_validate.py
"""
import sys
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd
from factors.c_ext.illq_wrapper import compute_illq_c

FIXTURE = os.path.join(_ROOT, "tests", "fixtures", "module6", "input_btcusdt_candles.pkl")
PARAM = 10000


def signal_multi_params_py(df, param_list):
    """Python reference — no C dependency."""
    zhenfu  = (df["high"] - df["low"]) / df["open"]
    ilq2    = df["quote_volume"] / zhenfu
    ilq     = df["quote_volume"] / (
        (2 * (df["high"] - df["low"]) - (df["open"] - df["close"]).abs()) / df["open"]
    )
    ilq_sum = ilq + ilq2
    ret = {}
    for param in param_list:
        n = int(param)
        bbi = (df["close"].rolling(n,     min_periods=1).mean()
             + df["close"].rolling(2*n,   min_periods=1).mean()
             + df["close"].rolling(4*n,   min_periods=1).mean()
             + df["close"].rolling(6*n,   min_periods=1).mean()) / 4
        bbi_bias = df["close"] / bbi
        ret[str(param)] = np.asarray(ilq_sum.rolling(n, min_periods=1).std() * bbi_bias,
                                      dtype=np.float64)
    return ret


def main():
    if not os.path.exists(FIXTURE):
        print(f"Fixture not found: {FIXTURE}")
        sys.exit(0)

    df = pd.read_pickle(FIXTURE).sort_values("candle_begin_time").reset_index(drop=True)
    print(f"Rows: {len(df)}")

    py_arr = signal_multi_params_py(df, [PARAM])[str(PARAM)]
    c_arr  = compute_illq_c(
        df["open"].values, df["high"].values, df["low"].values,
        df["close"].values, df["quote_volume"].values, n=PARAM,
    )

    py_nan = np.isnan(py_arr)
    c_nan  = np.isnan(c_arr)
    nan_ok = np.array_equal(py_nan, c_nan)
    val_ok = bool(np.allclose(py_arr[~py_nan], c_arr[~c_nan], atol=1e-9)) if nan_ok else False
    status = "PASS" if (nan_ok and val_ok) else "FAIL"
    print(f"param={PARAM}  nan_match={nan_ok}  val_match={val_ok}  [{status}]")

    if not (nan_ok and val_ok):
        if not nan_ok:
            diff = np.where(py_nan != c_nan)[0]
            print(f"  NaN mismatch at indices: {diff[:5]}")
        else:
            bad = np.where(~py_nan & (np.abs(py_arr - c_arr) > 1e-9))[0]
            print(f"  value mismatch at indices: {bad[:5]}")
            print(f"  py={py_arr[bad[:3]]}  c={c_arr[bad[:3]]}")
        sys.exit(1)

    print("Validation PASSED.")


if __name__ == "__main__":
    main()
