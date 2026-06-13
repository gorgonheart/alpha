# factors/c_ext/cci_validate.py
"""
Validate C output matches Python for fixture data.
Run: conda run -n alpha311 python factors/c_ext/cci_validate.py
"""
import sys
import os

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import numpy as np
import pandas as pd
from factors.c_ext.cci_wrapper import compute_cci_c

FIXTURE = os.path.join(_ROOT, "tests", "fixtures", "module6", "input_btcusdt_candles.pkl")
PARAM  = 10000
WINDOW = 15000


def ewm_fixed_py(arr, window, span, min_periods):
    alpha = 2 / (span + 1)
    y = arr.ewm(alpha=alpha, adjust=False).mean()
    residual = (1 - alpha)**window * y + (alpha - 1) * (1 - alpha)**(window - 1) * arr.shift(-1)
    residual = residual.shift(window).fillna(0)
    result = y - residual
    result.iloc[:min_periods - 1] = np.nan
    return result


def signal_multi_params_py(df, param_list):
    """Python reference -- no C dependency."""
    ret = {}
    for param in param_list:
        n   = int(param)
        oma = ewm_fixed_py(df["open"],  WINDOW, span=n, min_periods=2)
        hma = ewm_fixed_py(df["high"],  WINDOW, span=n, min_periods=2)
        lma = ewm_fixed_py(df["low"],   WINDOW, span=n, min_periods=2)
        cma = ewm_fixed_py(df["close"], WINDOW, span=n, min_periods=2)
        tp  = (oma + hma + lma + cma) / 4
        ma  = ewm_fixed_py(tp,               WINDOW, span=n, min_periods=2)
        md  = ewm_fixed_py((cma - ma).abs(), WINDOW, span=n, min_periods=2)
        ret[str(param)] = np.asarray((tp - ma) / md, dtype=np.float64)
    return ret


def main():
    if not os.path.exists(FIXTURE):
        print(f"Fixture not found: {FIXTURE}")
        sys.exit(0)

    df = pd.read_pickle(FIXTURE).sort_values("candle_begin_time").reset_index(drop=True)
    print(f"Rows: {len(df)}")

    py_arr = signal_multi_params_py(df, [PARAM])[str(PARAM)]
    c_arr  = compute_cci_c(
        df["open"].values, df["high"].values,
        df["low"].values,  df["close"].values,
        n=PARAM, window=WINDOW,
    )

    py_nan = np.isnan(py_arr)
    c_nan  = np.isnan(c_arr)
    nan_ok = np.array_equal(py_nan, c_nan)
    val_ok = bool(np.allclose(py_arr[~py_nan], c_arr[~c_nan], atol=1e-9)) if nan_ok else False
    status = "PASS" if (nan_ok and val_ok) else "FAIL"
    print(f"param={PARAM}  window={WINDOW}  nan_match={nan_ok}  val_match={val_ok}  [{status}]")

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
