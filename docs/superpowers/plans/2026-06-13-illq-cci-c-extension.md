# ILLQ + CciV3 C Extension Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port `ILLQStdBbi45208` and `CciV3` factors to C via cffi/MSVC, wire both into the preprocess `gen_factor_data` pipeline, and verify at param=10000 that C output matches Python exactly.

**Architecture:** Two independent C extensions following the established momentum pattern — each factor gets its own `.c` → `.pyd` → wrapper → validate → updated Python module. Golden `.npy` snapshots are captured once from Python and committed; regression tests assert the C output matches exactly. `config.py` wires both into the existing `gen_factor_data` pipeline without any changes to `core/preprocess.py`.

**Tech Stack:** Python 3.11, cffi 1.15, MSVC (conda alpha311), pandas, numpy, pytest

---

## File Map

| File | Action |
|------|--------|
| `tests/fixtures/capture_illq_cci.py` | Create — one-time golden generator |
| `tests/fixtures/illq/golden_10000.npy` | Create — committed golden (ILLQ) |
| `tests/fixtures/cci/golden_10000.npy` | Create — committed golden (CciV3) |
| `tests/support/illq_harness.py` | Create — fixture loader + Python reference |
| `tests/test_module_illq.py` | Create — 4 regression tests |
| `factors/c_ext/illq_core.h` | Create — C declaration |
| `factors/c_ext/illq_core.c` | Create — rolling_mean + rolling_std + compute_illq |
| `factors/c_ext/illq_build.py` | Create — cffi build → `_illq_core.pyd` |
| `factors/c_ext/illq_wrapper.py` | Create — numpy ↔ C bridge |
| `factors/c_ext/illq_validate.py` | Create — Python vs C pairwise check |
| `factors/ILLQStdBbi45208.py` | Modify — delegate to C wrapper |
| `tests/support/cci_harness.py` | Create — fixture loader + Python reference |
| `tests/test_module_cci.py` | Create — 4 regression tests |
| `factors/c_ext/cci_core.h` | Create — C declaration |
| `factors/c_ext/cci_core.c` | Create — ewm_fixed_inner + compute_cci |
| `factors/c_ext/cci_build.py` | Create — cffi build → `_cci_core.pyd` |
| `factors/c_ext/cci_wrapper.py` | Create — numpy ↔ C bridge |
| `factors/c_ext/cci_validate.py` | Create — Python vs C pairwise check |
| `factors/CciV3.py` | Modify — delegate to C wrapper, remove ewm_fixed |
| `config.py` | Modify — add both to precompute_factor_configs |
| `.gitignore` | Modify — exclude cffi-generated .c files |

All Python commands use: `conda run -n alpha311 python ...`

---

## Task 1: Golden Snapshot Fixtures

**Files:**
- Create: `tests/fixtures/capture_illq_cci.py`
- Create: `tests/fixtures/illq/golden_10000.npy` (generated)
- Create: `tests/fixtures/cci/golden_10000.npy` (generated)

Run the ORIGINAL Python implementations to capture ground-truth output. These files are committed and serve as the regression baseline. Must be generated BEFORE the factor files are updated to use C.

- [ ] **Step 1: Write capture script**

```python
# tests/fixtures/capture_illq_cci.py
"""
One-time golden snapshot generator for ILLQ and CciV3 at param=10000.
Run BEFORE updating factor files to use C, then commit the .npy outputs:
    conda run -n alpha311 python tests/fixtures/capture_illq_cci.py
"""
import sys
import importlib
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

FIXTURE_INPUT = ROOT / "tests" / "fixtures" / "module6" / "input_btcusdt_candles.pkl"
ILLQ_DIR = ROOT / "tests" / "fixtures" / "illq"
CCI_DIR  = ROOT / "tests" / "fixtures" / "cci"
PARAM = 10000


def main():
    ILLQ_DIR.mkdir(parents=True, exist_ok=True)
    CCI_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_pickle(FIXTURE_INPUT).sort_values("candle_begin_time").reset_index(drop=True)
    print(f"Loaded {len(df)} rows from fixture")

    illq_mod = importlib.import_module("factors.ILLQStdBbi45208")
    illq_out = illq_mod.signal_multi_params(df, [PARAM])
    np.save(str(ILLQ_DIR / f"golden_{PARAM}.npy"),
            np.asarray(illq_out[str(PARAM)], dtype=np.float64))
    print(f"Saved tests/fixtures/illq/golden_{PARAM}.npy")

    cci_mod = importlib.import_module("factors.CciV3")
    cci_out = cci_mod.signal_multi_params(df, [PARAM])
    np.save(str(CCI_DIR / f"golden_{PARAM}.npy"),
            np.asarray(cci_out[str(PARAM)], dtype=np.float64))
    print(f"Saved tests/fixtures/cci/golden_{PARAM}.npy")

    print("Done. Commit tests/fixtures/illq/ and tests/fixtures/cci/")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run capture script**

```powershell
cd d:\alpha
conda run -n alpha311 python tests/fixtures/capture_illq_cci.py
```

Expected output:
```
Loaded 7200 rows from fixture
Saved tests/fixtures/illq/golden_10000.npy
Saved tests/fixtures/cci/golden_10000.npy
Done. Commit tests/fixtures/illq/ and tests/fixtures/cci/
```

- [ ] **Step 3: Verify files exist**

```powershell
dir tests\fixtures\illq\golden_10000.npy
dir tests\fixtures\cci\golden_10000.npy
```

Expected: both files present, each ~57 KB (7200 float64 values).

- [ ] **Step 4: Commit**

```bash
git add tests/fixtures/capture_illq_cci.py tests/fixtures/illq/ tests/fixtures/cci/
git commit -m "test: add golden fixtures for ILLQ and CciV3 at param=10000"
```

---

## Task 2: ILLQ Test Harness + Regression Tests (initially failing)

**Files:**
- Create: `tests/support/illq_harness.py`
- Create: `tests/test_module_illq.py`

Write tests first so they fail with `ImportError` (red phase). Tests pass after Task 5.

- [ ] **Step 1: Write harness**

```python
# tests/support/illq_harness.py
from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_ILLQ_FIXTURE    = _ROOT / "tests" / "fixtures" / "illq"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "ILLQStdBbi45208"
BATCH_KEY   = "spot_dict_batch1"


def load_btcusdt_candles() -> pd.DataFrame:
    return pd.read_pickle(_MODULE6_FIXTURE / "input_btcusdt_candles.pkl").reset_index(drop=True)


def load_golden(param: int) -> np.ndarray:
    return np.load(str(_ILLQ_FIXTURE / f"golden_{param}.npy"))


def signal_multi_params_py(df: pd.DataFrame, param_list: list) -> dict:
    """Pure-Python reference — never calls C, valid after factor file is updated to C."""
    zhenfu  = (df["high"] - df["low"]) / df["open"]
    ilq2    = df["quote_volume"] / zhenfu
    ilq     = df["quote_volume"] / (
        (2 * (df["high"] - df["low"]) - (df["open"] - df["close"]).abs()) / df["open"]
    )
    ilq_sum = ilq + ilq2
    ret: dict[str, np.ndarray] = {}
    for param in param_list:
        n = int(param)
        bbi = (
            df["close"].rolling(n,     min_periods=1).mean()
          + df["close"].rolling(2 * n, min_periods=1).mean()
          + df["close"].rolling(4 * n, min_periods=1).mean()
          + df["close"].rolling(6 * n, min_periods=1).mean()
        ) / 4
        bbi_bias = df["close"] / bbi
        ret[str(param)] = np.asarray(
            ilq_sum.rolling(n, min_periods=1).std() * bbi_bias, dtype=np.float64
        )
    return ret


def make_factor_handle(tmp_path: Path, df: pd.DataFrame, run_time):
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")
    mgr.save_data_file(BATCH_KEY, {"BTCUSDT": df.copy(deep=True)})
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: mgr},
            num_batch=256,
        ),
    )
    return handle, mgr
```

- [ ] **Step 2: Write regression tests**

```python
# tests/test_module_illq.py
"""Regression tests for ILLQStdBbi45208 C extension at param=10000."""
from __future__ import annotations
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from factors.c_ext.illq_wrapper import compute_illq_c  # ImportError until Task 5
from tests.support.illq_harness import (
    FACTOR_NAME, BATCH_KEY,
    load_btcusdt_candles, load_golden, signal_multi_params_py, make_factor_handle,
)
from core.preprocess import gen_factor_data

PARAM = 10000


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path


@pytest.fixture()
def df():
    return load_btcusdt_candles()


def _c_out(df: pd.DataFrame) -> np.ndarray:
    return compute_illq_c(
        df["open"].values.astype("float64"),
        df["high"].values.astype("float64"),
        df["low"].values.astype("float64"),
        df["close"].values.astype("float64"),
        df["quote_volume"].values.astype("float64"),
        n=PARAM,
    )


def test_output_length(df):
    assert len(_c_out(df)) == len(df)


def test_golden(df):
    np.testing.assert_array_equal(
        _c_out(df), load_golden(PARAM),
        err_msg="C output differs from committed golden. "
                "Re-run tests/fixtures/capture_illq_cci.py if algorithm changed intentionally.",
    )


def test_c_vs_python(df):
    c_arr  = _c_out(df)
    py_arr = signal_multi_params_py(df, [PARAM])[str(PARAM)]
    assert np.array_equal(np.isnan(c_arr), np.isnan(py_arr)), \
        "NaN positions differ between C and Python"
    valid = ~np.isnan(py_arr)
    np.testing.assert_allclose(
        c_arr[valid], py_arr[valid], atol=1e-9,
        err_msg="C and Python values differ beyond atol=1e-9",
    )


def test_signal_multi_params_golden(tmp, df, monkeypatch):
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, mgr = make_factor_handle(tmp, df, run_ts)
    monkeypatch.setattr(
        "core.preprocess.bmac_config",
        SimpleNamespace(precompute_factor_configs={FACTOR_NAME: [PARAM]}),
    )
    gen_factor_data(handle, run_ts, "spot")
    col = f"{FACTOR_NAME}_{PARAM}"
    df_result = mgr.read_candle(BATCH_KEY)["BTCUSDT"]
    assert col in df_result.columns, f"Column {col!r} not found"
    np.testing.assert_array_equal(
        df_result[col].values, load_golden(PARAM),
        err_msg=f"gen_factor_data column {col!r} differs from golden.",
    )
```

- [ ] **Step 3: Verify tests fail with ImportError**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_illq.py -v 2>&1 | Select-String -Pattern "ERROR|ImportError|PASSED|FAILED" | head -5
```

Expected: `ImportError: No module named 'factors.c_ext.illq_wrapper'`

- [ ] **Step 4: Commit**

```bash
git add tests/support/illq_harness.py tests/test_module_illq.py
git commit -m "test: ILLQ regression test harness (red phase — wrapper not yet built)"
```

---

## Task 3: ILLQ C Header + Build Script

**Files:**
- Create: `factors/c_ext/illq_core.h`
- Create: `factors/c_ext/illq_build.py`

- [ ] **Step 1: Write header**

```c
/* factors/c_ext/illq_core.h */
#ifndef ILLQ_CORE_H
#define ILLQ_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_illq: ILLQStdBbi45208 factor for one symbol, one param.
 *
 * open/high/low/close/qvol : price and quote_volume arrays, all length len
 * n                        : param (rolling window)
 * len                      : array length
 * out                      : output array, length len
 */
void compute_illq(
    const double *open,
    const double *high,
    const double *low,
    const double *close,
    const double *qvol,
    int n,
    int len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* ILLQ_CORE_H */
```

- [ ] **Step 2: Write build script**

```python
# factors/c_ext/illq_build.py
"""
Run:
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/illq_build.py

Output: factors/c_ext/_illq_core.cp311-win_amd64.pyd (or platform suffix)
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_illq(
        const double *open,
        const double *high,
        const double *low,
        const double *close,
        const double *qvol,
        int n,
        int len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "illq_core.c")
with open(_src_path, "r") as f:
    _src = f.read()

ffi.set_source(
    "_illq_core",
    _src,
    extra_compile_args=["/O2"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
```

- [ ] **Step 3: Commit**

```bash
git add factors/c_ext/illq_core.h factors/c_ext/illq_build.py
git commit -m "feat: illq_core.h and cffi build script"
```

---

## Task 4: ILLQ C Implementation + Compile

**Files:**
- Create: `factors/c_ext/illq_core.c`

### Algorithm notes

`ilq_sum` is element-wise (no rolling), computed once per call from `open, high, low, close, qvol`.

`rolling_mean(w, min_periods=1)` — circular sum buffer, O(1) per element:
- Add `x[i]` to running sum; subtract `x[i-w]` when `i >= w`. `count` tracks non-NaN elements.
- Return `sum/count` when `count >= 1`, else NaN.

`rolling_std(w, ddof=1, min_periods=1)` — Welford sum+sum² buffer:
- Return NaN when `count < 2` (matches pandas ddof=1 behaviour), else `sqrt((sum² - sum²/count) / (count-1))`.

- [ ] **Step 1: Write illq_core.c**

```c
/* factors/c_ext/illq_core.c */
#include <math.h>
#include <stdlib.h>
#include "illq_core.h"

/* rolling_mean: window w, min_periods=1, skip NaN — matches pandas rolling(w,min_periods=1).mean() */
static void roll_mean(const double *x, int n, int w, double *out) {
    double sum = 0.0;
    int    cnt = 0;
    for (int i = 0; i < n; i++) {
        if (!isnan(x[i])) { sum += x[i]; cnt++; }
        if (i >= w && !isnan(x[i - w])) { sum -= x[i - w]; cnt--; }
        out[i] = (cnt >= 1) ? sum / cnt : NAN;
    }
}

/* rolling_std: window w, ddof=1, min_periods=1 — matches pandas rolling(w,min_periods=1).std() */
static void roll_std(const double *x, int n, int w, double *out) {
    double sum = 0.0, sumsq = 0.0;
    int    cnt = 0;
    for (int i = 0; i < n; i++) {
        if (!isnan(x[i])) { sum += x[i]; sumsq += x[i] * x[i]; cnt++; }
        if (i >= w && !isnan(x[i - w])) {
            sum -= x[i - w]; sumsq -= x[i - w] * x[i - w]; cnt--;
        }
        if (cnt < 2) {
            out[i] = NAN;
        } else {
            double var = (sumsq - sum * sum / cnt) / (cnt - 1);
            out[i] = (var > 0.0) ? sqrt(var) : 0.0;
        }
    }
}

void compute_illq(
    const double *open, const double *high, const double *low,
    const double *close, const double *qvol,
    int n, int len, double *out
) {
    double *ilq_sum  = (double *)malloc(len * sizeof(double));
    double *bbi1     = (double *)malloc(len * sizeof(double));
    double *bbi2     = (double *)malloc(len * sizeof(double));
    double *bbi3     = (double *)malloc(len * sizeof(double));
    double *bbi4     = (double *)malloc(len * sizeof(double));
    double *bbi      = (double *)malloc(len * sizeof(double));
    double *bbi_bias = (double *)malloc(len * sizeof(double));
    double *std_ilq  = (double *)malloc(len * sizeof(double));

    /* ilq_sum = ilq + ilq2 element-wise (IEEE 754: x/0 = ±Inf, 0/0 = NaN — same as pandas) */
    for (int i = 0; i < len; i++) {
        double zhenfu = (high[i] - low[i]) / open[i];
        double ilq2   = qvol[i] / zhenfu;
        double route  = (2.0 * (high[i] - low[i]) - fabs(open[i] - close[i])) / open[i];
        double ilq    = qvol[i] / route;
        ilq_sum[i]    = ilq + ilq2;
    }

    /* BBI = average of rolling means at windows n, 2n, 4n, 6n */
    roll_mean(close, len, n,       bbi1);
    roll_mean(close, len, 2 * n,   bbi2);
    roll_mean(close, len, 4 * n,   bbi3);
    roll_mean(close, len, 6 * n,   bbi4);
    for (int i = 0; i < len; i++) {
        bbi[i]      = (bbi1[i] + bbi2[i] + bbi3[i] + bbi4[i]) / 4.0;
        bbi_bias[i] = close[i] / bbi[i];
    }

    /* factor = rolling_std(ilq_sum, n) * bbi_bias */
    roll_std(ilq_sum, len, n, std_ilq);
    for (int i = 0; i < len; i++)
        out[i] = std_ilq[i] * bbi_bias[i];

    free(ilq_sum); free(bbi1); free(bbi2); free(bbi3); free(bbi4);
    free(bbi); free(bbi_bias); free(std_ilq);
}
```

- [ ] **Step 2: Compile**

```powershell
conda run -n alpha311 python factors/c_ext/illq_build.py
```

Expected: build output ending with `Build complete. Output in: ...factors\c_ext`  
Verify `.pyd` exists:
```powershell
dir factors\c_ext\_illq_core*.pyd
```

- [ ] **Step 3: Smoke test (no pytest)**

```powershell
conda run -n alpha311 python -c "import sys; sys.path.insert(0,'factors/c_ext'); import _illq_core; print('import ok'); import numpy as np; o=np.empty(5,dtype='float64'); _illq_core.lib.compute_illq(*[_illq_core.ffi.cast('double*',a.ctypes.data) for a in [np.ones(5)*1.0,np.ones(5)*1.1,np.ones(5)*0.9,np.ones(5)*1.0,np.ones(5)*1e6]], 2, 5, _illq_core.ffi.cast('double*',o.ctypes.data)); print('out:', o)"
```

Expected: import ok, `out:` array with some finite values (no crash).

- [ ] **Step 4: Commit**

```bash
git add factors/c_ext/illq_core.c
git commit -m "feat: illq_core.c - rolling_mean, rolling_std, compute_illq"
```

---

## Task 5: ILLQ Python Wrapper + Run Tests

**Files:**
- Create: `factors/c_ext/illq_wrapper.py`

- [ ] **Step 1: Write wrapper**

```python
# factors/c_ext/illq_wrapper.py
"""numpy bridge to _illq_core C extension."""
import os
import sys
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _illq_core as _mod
except ImportError as e:
    raise ImportError(
        f"C extension not found: {e}\n"
        f"Run:  conda run -n alpha311 python {os.path.join(_HERE, 'illq_build.py')}"
    ) from e

_lib = _mod.lib
_ffi = _mod.ffi


def _ptr(arr: np.ndarray):
    return _ffi.cast("double *", arr.ctypes.data)


def compute_illq_c(
    open_: np.ndarray,
    high:  np.ndarray,
    low:   np.ndarray,
    close: np.ndarray,
    qvol:  np.ndarray,
    n: int,
) -> np.ndarray:
    """
    Compute ILLQStdBbi45208 factor (C implementation).

    Parameters
    ----------
    open_, high, low, close, qvol : 1-D float64 ndarray, same length
    n : rolling window (param)

    Returns
    -------
    out : 1-D float64 ndarray, same length as input
    """
    open_ = np.ascontiguousarray(open_, dtype=np.float64)
    high  = np.ascontiguousarray(high,  dtype=np.float64)
    low   = np.ascontiguousarray(low,   dtype=np.float64)
    close = np.ascontiguousarray(close, dtype=np.float64)
    qvol  = np.ascontiguousarray(qvol,  dtype=np.float64)
    length = len(open_)
    out = np.empty(length, dtype=np.float64)
    _lib.compute_illq(
        _ptr(open_), _ptr(high), _ptr(low), _ptr(close), _ptr(qvol),
        int(n), int(length),
        _ptr(out),
    )
    return out
```

- [ ] **Step 2: Run ILLQ regression tests**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_illq.py -v
```

Expected:
```
PASSED tests/test_module_illq.py::test_output_length
PASSED tests/test_module_illq.py::test_golden
PASSED tests/test_module_illq.py::test_c_vs_python
PASSED tests/test_module_illq.py::test_signal_multi_params_golden
4 passed
```

If `test_c_vs_python` fails, the error output shows the first mismatch index and values. Common causes:
- `rolling_std` ddof issue: verify `cnt < 2 → NAN`
- Floating-point accumulation in large-window rolling: expected at atol=1e-9 with clean data

- [ ] **Step 3: Commit**

```bash
git add factors/c_ext/illq_wrapper.py
git commit -m "feat: illq_wrapper.py - numpy bridge to C extension"
```

---

## Task 6: ILLQ Validate Script + Update Factor Module

**Files:**
- Create: `factors/c_ext/illq_validate.py`
- Modify: `factors/ILLQStdBbi45208.py`

- [ ] **Step 1: Write validate script**

```python
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
```

- [ ] **Step 2: Run validate script**

```powershell
conda run -n alpha311 python factors/c_ext/illq_validate.py
```

Expected:
```
Rows: 7200
param=10000  nan_match=True  val_match=True  [PASS]
Validation PASSED.
```

- [ ] **Step 3: Replace ILLQStdBbi45208.py with C-backed version**

Replace the entire content of `factors/ILLQStdBbi45208.py` with:

```python
# -*- coding: utf-8 -*-
import numpy as np
import pandas as pd

from factors.c_ext.illq_wrapper import compute_illq_c


def signal_multi_params(df, param_list) -> dict:
    open_ = np.ascontiguousarray(df["open"].values,         dtype=np.float64)
    high  = np.ascontiguousarray(df["high"].values,         dtype=np.float64)
    low   = np.ascontiguousarray(df["low"].values,          dtype=np.float64)
    close = np.ascontiguousarray(df["close"].values,        dtype=np.float64)
    qvol  = np.ascontiguousarray(df["quote_volume"].values, dtype=np.float64)
    ret = {}
    for param in param_list:
        out = compute_illq_c(open_, high, low, close, qvol, n=int(param))
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
```

- [ ] **Step 4: Re-run regression tests to confirm still green**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_illq.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add factors/c_ext/illq_validate.py factors/ILLQStdBbi45208.py
git commit -m "feat: ILLQStdBbi45208 - replace inner loop with C wrapper"
```

---

## Task 7: CciV3 Test Harness + Regression Tests (initially failing)

**Files:**
- Create: `tests/support/cci_harness.py`
- Create: `tests/test_module_cci.py`

- [ ] **Step 1: Write harness**

```python
# tests/support/cci_harness.py
from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_CCI_FIXTURE     = _ROOT / "tests" / "fixtures" / "cci"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "CciV3"
BATCH_KEY   = "spot_dict_batch1"
WINDOW      = 15000


def load_btcusdt_candles() -> pd.DataFrame:
    return pd.read_pickle(_MODULE6_FIXTURE / "input_btcusdt_candles.pkl").reset_index(drop=True)


def load_golden(param: int) -> np.ndarray:
    return np.load(str(_CCI_FIXTURE / f"golden_{param}.npy"))


def _ewm_fixed_py(arr: pd.Series, window: int, span: int, min_periods: int) -> pd.Series:
    """Python ewm_fixed — exact copy of original CciV3.ewm_fixed, no C dependency."""
    alpha = 2 / (span + 1)
    y = arr.ewm(alpha=alpha, adjust=False).mean()
    residual = (1 - alpha) ** window * y + (alpha - 1) * (1 - alpha) ** (window - 1) * arr.shift(-1)
    residual = residual.shift(window).fillna(0)
    result = y - residual
    result.iloc[:min_periods - 1] = np.nan
    return result


def signal_multi_params_py(df: pd.DataFrame, param_list: list) -> dict:
    """Pure-Python reference — never calls C, valid after factor file is updated to C."""
    ret: dict[str, np.ndarray] = {}
    for param in param_list:
        n   = int(param)
        oma = _ewm_fixed_py(df["open"],  WINDOW, span=n, min_periods=2)
        hma = _ewm_fixed_py(df["high"],  WINDOW, span=n, min_periods=2)
        lma = _ewm_fixed_py(df["low"],   WINDOW, span=n, min_periods=2)
        cma = _ewm_fixed_py(df["close"], WINDOW, span=n, min_periods=2)
        tp  = (oma + hma + lma + cma) / 4
        ma  = _ewm_fixed_py(tp,               WINDOW, span=n, min_periods=2)
        md  = _ewm_fixed_py((cma - ma).abs(), WINDOW, span=n, min_periods=2)
        ret[str(param)] = np.asarray((tp - ma) / md, dtype=np.float64)
    return ret


def make_factor_handle(tmp_path: Path, df: pd.DataFrame, run_time):
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")
    mgr.save_data_file(BATCH_KEY, {"BTCUSDT": df.copy(deep=True)})
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: mgr},
            num_batch=256,
        ),
    )
    return handle, mgr
```

- [ ] **Step 2: Write regression tests**

```python
# tests/test_module_cci.py
"""Regression tests for CciV3 C extension at param=10000."""
from __future__ import annotations
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from factors.c_ext.cci_wrapper import compute_cci_c  # ImportError until Task 11
from tests.support.cci_harness import (
    FACTOR_NAME, BATCH_KEY, WINDOW,
    load_btcusdt_candles, load_golden, signal_multi_params_py, make_factor_handle,
)
from core.preprocess import gen_factor_data

PARAM = 10000


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path


@pytest.fixture()
def df():
    return load_btcusdt_candles()


def _c_out(df: pd.DataFrame) -> np.ndarray:
    return compute_cci_c(
        df["open"].values.astype("float64"),
        df["high"].values.astype("float64"),
        df["low"].values.astype("float64"),
        df["close"].values.astype("float64"),
        n=PARAM,
        window=WINDOW,
    )


def test_output_length(df):
    assert len(_c_out(df)) == len(df)


def test_golden(df):
    np.testing.assert_array_equal(
        _c_out(df), load_golden(PARAM),
        err_msg="C output differs from committed golden. "
                "Re-run tests/fixtures/capture_illq_cci.py if algorithm changed intentionally.",
    )


def test_c_vs_python(df):
    c_arr  = _c_out(df)
    py_arr = signal_multi_params_py(df, [PARAM])[str(PARAM)]
    assert np.array_equal(np.isnan(c_arr), np.isnan(py_arr)), \
        "NaN positions differ between C and Python"
    valid = ~np.isnan(py_arr)
    np.testing.assert_allclose(
        c_arr[valid], py_arr[valid], atol=1e-9,
        err_msg="C and Python values differ beyond atol=1e-9",
    )


def test_signal_multi_params_golden(tmp, df, monkeypatch):
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, mgr = make_factor_handle(tmp, df, run_ts)
    monkeypatch.setattr(
        "core.preprocess.bmac_config",
        SimpleNamespace(precompute_factor_configs={FACTOR_NAME: [PARAM]}),
    )
    gen_factor_data(handle, run_ts, "spot")
    col = f"{FACTOR_NAME}_{PARAM}"
    df_result = mgr.read_candle(BATCH_KEY)["BTCUSDT"]
    assert col in df_result.columns, f"Column {col!r} not found"
    np.testing.assert_array_equal(
        df_result[col].values, load_golden(PARAM),
        err_msg=f"gen_factor_data column {col!r} differs from golden.",
    )
```

- [ ] **Step 3: Verify tests fail with ImportError**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_cci.py -v 2>&1 | Select-String -Pattern "ERROR|ImportError" | Select-Object -First 3
```

Expected: `ImportError: No module named 'factors.c_ext.cci_wrapper'`

- [ ] **Step 4: Commit**

```bash
git add tests/support/cci_harness.py tests/test_module_cci.py
git commit -m "test: CciV3 regression test harness (red phase — wrapper not yet built)"
```

---

## Task 8: CciV3 C Header + Build Script

**Files:**
- Create: `factors/c_ext/cci_core.h`
- Create: `factors/c_ext/cci_build.py`

- [ ] **Step 1: Write header**

```c
/* factors/c_ext/cci_core.h */
#ifndef CCI_CORE_H
#define CCI_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_cci: CciV3 factor for one symbol, one param.
 *
 * open/high/low/close : price arrays, all length len
 * n                   : span parameter (alpha = 2/(n+1))
 * window              : ewm_fixed window (15000 in Python)
 * len                 : array length
 * out                 : output array, length len
 */
void compute_cci(
    const double *open,
    const double *high,
    const double *low,
    const double *close,
    int n,
    int window,
    int len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* CCI_CORE_H */
```

- [ ] **Step 2: Write build script**

```python
# factors/c_ext/cci_build.py
"""
Run:
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/cci_build.py

Output: factors/c_ext/_cci_core.cp311-win_amd64.pyd (or platform suffix)
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_cci(
        const double *open,
        const double *high,
        const double *low,
        const double *close,
        int n,
        int window,
        int len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "cci_core.c")
with open(_src_path, "r") as f:
    _src = f.read()

ffi.set_source(
    "_cci_core",
    _src,
    extra_compile_args=["/O2"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
```

- [ ] **Step 3: Commit**

```bash
git add factors/c_ext/cci_core.h factors/c_ext/cci_build.py
git commit -m "feat: cci_core.h and cffi build script"
```

---

## Task 9: CciV3 C Implementation + Compile

**Files:**
- Create: `factors/c_ext/cci_core.c`

### Algorithm: `ewm_fixed_inner`

Closed-form derivation. Let `y[t]` = standard EWM (adjust=False, init from first non-NaN),
`beta_w = (1-alpha)^window`.

```
t < window  OR  y[t-window] is NaN  OR  arr[t-window+1] is NaN:
    ewm_fixed[t] = y[t]   (residual = 0 from Python fillna(0))

t >= window, all valid:
    ewm_fixed[t] = y[t] - beta_w * y[t-window] + beta_w * arr[t-window+1]
```

NaN note: with `adjust=False`, EWM initializes from the first non-NaN value; subsequent NaN inputs propagate NaN in `y` but don't corrupt already-valid state once initialized.

**Note on test data:** The fixture has 7200 rows and `window=15000`, so `t < window` for all `t`, meaning the correction branch is never reached in tests. The correction path is exercised only on production data with >15000 rows.

- [ ] **Step 1: Write cci_core.c**

```c
/* factors/c_ext/cci_core.c */
#include <math.h>
#include <stdlib.h>
#include "cci_core.h"

/*
 * ewm_fixed_inner: matches Python ewm_fixed(arr, window, span=n, min_periods=2).
 *
 * EWM initializes from first non-NaN element of arr (adjust=False behaviour).
 * Correction formula (for t >= window when both y[t-window] and arr[t-window+1] are valid):
 *   out[t] = y[t] - beta_w * y[t-window] + beta_w * arr[t-window+1]
 * Otherwise:
 *   out[t] = y[t]   (residual = 0, i.e. Python fillna(0) applied)
 * min_periods=2: out[0] = NaN unconditionally.
 */
static void ewm_fixed_inner(
    const double *arr,
    double alpha, double beta_w, int window,
    int len, double *out
) {
    double *y = (double *)malloc(len * sizeof(double));

    /* Standard EWM with adjust=False: init from first non-NaN */
    double prev = NAN;
    for (int t = 0; t < len; t++) {
        if (isnan(arr[t])) {
            y[t] = NAN;
        } else if (isnan(prev)) {
            y[t] = prev = arr[t];            /* first valid value initialises EWM */
        } else {
            y[t] = prev = alpha * arr[t] + (1.0 - alpha) * prev;
        }
    }

    /* Apply windowed correction */
    for (int t = 0; t < len; t++) {
        if (isnan(y[t])) {
            out[t] = NAN;
        } else if (t >= window
                   && !isnan(y[t - window])
                   && !isnan(arr[t - window + 1])) {
            out[t] = y[t] - beta_w * y[t - window] + beta_w * arr[t - window + 1];
        } else {
            out[t] = y[t];   /* t < window or NaN in residual → fillna(0) */
        }
    }

    out[0] = NAN;   /* min_periods=2 */
    free(y);
}

void compute_cci(
    const double *open, const double *high, const double *low, const double *close,
    int n, int window, int len, double *out
) {
    double alpha  = 2.0 / (n + 1.0);
    double beta_w = pow(1.0 - alpha, (double)window);

    double *oma      = (double *)malloc(len * sizeof(double));
    double *hma      = (double *)malloc(len * sizeof(double));
    double *lma      = (double *)malloc(len * sizeof(double));
    double *cma      = (double *)malloc(len * sizeof(double));
    double *tp       = (double *)malloc(len * sizeof(double));
    double *ma       = (double *)malloc(len * sizeof(double));
    double *abs_diff = (double *)malloc(len * sizeof(double));
    double *md       = (double *)malloc(len * sizeof(double));

    ewm_fixed_inner(open,  alpha, beta_w, window, len, oma);
    ewm_fixed_inner(high,  alpha, beta_w, window, len, hma);
    ewm_fixed_inner(low,   alpha, beta_w, window, len, lma);
    ewm_fixed_inner(close, alpha, beta_w, window, len, cma);

    for (int i = 0; i < len; i++)
        tp[i] = (oma[i] + hma[i] + lma[i] + cma[i]) / 4.0;

    ewm_fixed_inner(tp, alpha, beta_w, window, len, ma);

    for (int i = 0; i < len; i++)
        abs_diff[i] = (isnan(cma[i]) || isnan(ma[i])) ? NAN : fabs(cma[i] - ma[i]);

    ewm_fixed_inner(abs_diff, alpha, beta_w, window, len, md);

    for (int i = 0; i < len; i++) {
        if (isnan(tp[i]) || isnan(ma[i]) || isnan(md[i]) || fabs(md[i]) < 1e-15)
            out[i] = NAN;
        else
            out[i] = (tp[i] - ma[i]) / md[i];
    }

    free(oma); free(hma); free(lma); free(cma);
    free(tp);  free(ma);  free(abs_diff); free(md);
}
```

- [ ] **Step 2: Compile**

```powershell
conda run -n alpha311 python factors/c_ext/cci_build.py
```

Expected: `Build complete. Output in: ...factors\c_ext`  
Verify `.pyd` exists:
```powershell
dir factors\c_ext\_cci_core*.pyd
```

- [ ] **Step 3: Smoke test**

```powershell
conda run -n alpha311 python -c "import sys; sys.path.insert(0,'factors/c_ext'); import _cci_core; print('import ok'); import numpy as np; n=5; o=np.empty(n,dtype='float64'); _cci_core.lib.compute_cci(*[_cci_core.ffi.cast('double*',a.ctypes.data) for a in [np.ones(n)*1.0,np.ones(n)*1.1,np.ones(n)*0.9,np.ones(n)*1.0]], 3, 15000, n, _cci_core.ffi.cast('double*',o.ctypes.data)); print('out:', o)"
```

Expected: import ok, `out:` array — index 0 is NaN, others are 0.0 (since open=high=low=close, md=0 → NaN) or finite values.

- [ ] **Step 4: Commit**

```bash
git add factors/c_ext/cci_core.c
git commit -m "feat: cci_core.c - ewm_fixed_inner, compute_cci"
```

---

## Task 10: CciV3 Python Wrapper + Run Tests

**Files:**
- Create: `factors/c_ext/cci_wrapper.py`

- [ ] **Step 1: Write wrapper**

```python
# factors/c_ext/cci_wrapper.py
"""numpy bridge to _cci_core C extension."""
import os
import sys
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _cci_core as _mod
except ImportError as e:
    raise ImportError(
        f"C extension not found: {e}\n"
        f"Run:  conda run -n alpha311 python {os.path.join(_HERE, 'cci_build.py')}"
    ) from e

_lib = _mod.lib
_ffi = _mod.ffi


def _ptr(arr: np.ndarray):
    return _ffi.cast("double *", arr.ctypes.data)


def compute_cci_c(
    open_:  np.ndarray,
    high:   np.ndarray,
    low:    np.ndarray,
    close:  np.ndarray,
    n:      int,
    window: int = 15000,
) -> np.ndarray:
    """
    Compute CciV3 factor (C implementation).

    Parameters
    ----------
    open_, high, low, close : 1-D float64 ndarray, same length
    n      : span parameter (alpha = 2/(n+1))
    window : ewm_fixed window; default 15000 matches Python constant

    Returns
    -------
    out : 1-D float64 ndarray, same length as input
    """
    open_ = np.ascontiguousarray(open_, dtype=np.float64)
    high  = np.ascontiguousarray(high,  dtype=np.float64)
    low   = np.ascontiguousarray(low,   dtype=np.float64)
    close = np.ascontiguousarray(close, dtype=np.float64)
    length = len(open_)
    out = np.empty(length, dtype=np.float64)
    _lib.compute_cci(
        _ptr(open_), _ptr(high), _ptr(low), _ptr(close),
        int(n), int(window), int(length),
        _ptr(out),
    )
    return out
```

- [ ] **Step 2: Run CciV3 regression tests**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_cci.py -v
```

Expected:
```
PASSED tests/test_module_cci.py::test_output_length
PASSED tests/test_module_cci.py::test_golden
PASSED tests/test_module_cci.py::test_c_vs_python
PASSED tests/test_module_cci.py::test_signal_multi_params_golden
4 passed
```

If `test_c_vs_python` fails on NaN positions: the likely cause is a difference in how pandas EWM initialises from a NaN first value vs the C `ewm_fixed_inner` behaviour. Debug steps:
1. Check `np.where(np.isnan(c_arr) != np.isnan(py_arr))[0][:5]` to find the first mismatch index
2. Check whether `py_arr[1]` is NaN — if so, pandas propagates NaN after index 0, while C initialises from index 1. Fix: change C to propagate NaN until a valid `arr[t]` arrives (already handled in `ewm_fixed_inner` by the `isnan(arr[t])` branch)

- [ ] **Step 3: Commit**

```bash
git add factors/c_ext/cci_wrapper.py
git commit -m "feat: cci_wrapper.py - numpy bridge to C extension"
```

---

## Task 11: CciV3 Validate Script + Update Factor Module

**Files:**
- Create: `factors/c_ext/cci_validate.py`
- Modify: `factors/CciV3.py`

- [ ] **Step 1: Write validate script**

```python
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
    """Python reference — no C dependency."""
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
```

- [ ] **Step 2: Run validate script**

```powershell
conda run -n alpha311 python factors/c_ext/cci_validate.py
```

Expected:
```
Rows: 7200
param=10000  window=15000  nan_match=True  val_match=True  [PASS]
Validation PASSED.
```

- [ ] **Step 3: Replace CciV3.py with C-backed version**

Replace the entire content of `factors/CciV3.py` with:

```python
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
```

- [ ] **Step 4: Re-run regression tests**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_cci.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add factors/c_ext/cci_validate.py factors/CciV3.py
git commit -m "feat: CciV3 - replace ewm_fixed with C wrapper"
```

---

## Task 12: Config Integration + Final Cleanup + Full Test Run

**Files:**
- Modify: `config.py` (line 72–74)
- Modify: `.gitignore`
- Commit: prebuilt `.pyd` files

- [ ] **Step 1: Update config.py precompute_factor_configs**

In `config.py`, replace lines 72–74:

```python
# 预计算因子配置：{因子模块名（不含 factors. 前缀）: [param 列表]}
# 留空 dict 则跳过因子预计算步骤
precompute_factor_configs = {
    '动量因子210401修正': [168, 336],
    'ILLQStdBbi45208':   [10000],
    'CciV3':             [10000],
}
```

- [ ] **Step 2: Verify config loads correctly**

```powershell
conda run -n alpha311 python -c "import config; print(config.precompute_factor_configs)"
```

Expected:
```
{'动量因子210401修正': [168, 336], 'ILLQStdBbi45208': [10000], 'CciV3': [10000]}
```

- [ ] **Step 3: Update .gitignore**

Append to `.gitignore` (after the existing cffi section):

```
factors/c_ext/_illq_core.c
factors/c_ext/_cci_core.c
```

- [ ] **Step 4: Run full regression suite**

```powershell
conda run -n alpha311 python -m pytest tests/test_module_illq.py tests/test_module_cci.py tests/test_module5_gen_factor_data.py tests/test_module6_momentum_factor.py -v
```

Expected: all 16 tests pass (4+4+5+3... adjust count based on current state — every test must be green).

- [ ] **Step 5: Commit everything**

```bash
git add config.py .gitignore factors/c_ext/_illq_core*.pyd factors/c_ext/_cci_core*.pyd
git commit -m "feat: wire ILLQStdBbi45208 and CciV3 into precompute_factor_configs; commit prebuilt pyd"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Covered by |
|---|---|
| `illq_core.c` with rolling_mean, rolling_std | Task 4 |
| `cci_core.c` with ewm_fixed_inner | Task 9 |
| cffi build scripts for both | Tasks 3, 8 |
| Python wrappers for both | Tasks 5, 10 |
| validate scripts for both | Tasks 6, 11 |
| Update ILLQStdBbi45208.py | Task 6 |
| Update CciV3.py, remove ewm_fixed | Task 11 |
| Golden fixtures at param=10000 | Task 1 |
| 4 regression tests per factor (8 total) | Tasks 2, 7 |
| config.py integration | Task 12 |
| gen_factor_data: no changes needed | N/A (already exists) |
| .gitignore for generated .c files | Task 12 |
| Commit prebuilt .pyd | Task 12 |

No gaps found.
