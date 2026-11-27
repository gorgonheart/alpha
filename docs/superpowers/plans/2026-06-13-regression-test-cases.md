# Regression Test Cases Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 8 regression tests — 5 for the momentum C extension (`compute_momentum_c` / `signal_multi_params`) and 3 for `gen_factor_data` — using committed golden snapshot fixtures to assert value-level stability across code changes.

**Architecture:** A one-time capture script reads live `data/` and writes committed golden fixture files. Module 6 gets a new test file for the C extension. Module 5 gets 3 new tests in the existing file, plus a one-line fix for an idempotency bug in `_calc_symbol_factors` that TDD exposes. Mixed fixture strategy: unit test input+golden are committed snapshots; only the capture script touches live `data/`.

**Tech Stack:** pytest, numpy, pandas, cffi (_momentum_core.pyd), Python 3.11, conda env `alpha311`

---

## File Map

| Action | Path |
|---|---|
| Create | `tests/fixtures/capture.py` |
| Create | `tests/support/module6_harness.py` |
| Create | `tests/support/module5_harness.py` |
| Create | `tests/test_module6_momentum_factor.py` |
| Modify | `tests/test_module5_gen_factor_data.py` (add 3 tests at bottom) |
| Modify | `core/preprocess.py:580-591` (fix `_calc_symbol_factors`) |
| Generated — track in git | `tests/fixtures/module6/input_btcusdt_candles.pkl` |
| Generated — track in git | `tests/fixtures/module6/golden_btcusdt_168.npy` |
| Generated — track in git | `tests/fixtures/module6/golden_btcusdt_336.npy` |
| Generated — track in git | `tests/fixtures/module5/golden_signal_btcusdt_168.npy` |
| Generated — track in git | `tests/fixtures/module5/golden_signal_btcusdt_336.npy` |

---

### Task 1: Create fixture capture script

**Files:**
- Create: `tests/fixtures/capture.py`

- [ ] **Step 1: Write the capture script**

```python
# tests/fixtures/capture.py
"""
One-time script: generates golden fixture files for module5 and module6 regression tests.
Reads live data/ and writes committed snapshots.

Run (from project root):
    conda run -n alpha311 python tests/fixtures/capture.py

Regenerate after intentional algorithm changes.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from factors import 动量因子210401修正 as _momentum  # noqa: E402
from factors.c_ext.momentum_wrapper import compute_momentum_c  # noqa: E402

DATA_DIR = ROOT / "data"
FIXTURE_DIR = Path(__file__).parent
MODULE5_DIR = FIXTURE_DIR / "module5"
MODULE6_DIR = FIXTURE_DIR / "module6"
MODULE5_DIR.mkdir(parents=True, exist_ok=True)
MODULE6_DIR.mkdir(parents=True, exist_ok=True)

NROWS = 7200
TRUNC_LEN = 52560


def _load_btcusdt(nrows: int) -> pd.DataFrame:
    path = DATA_DIR / "binance_spot_1h_resample" / "40m" / "BTCUSDT.pkl"
    df = pd.read_pickle(path).sort_values("candle_begin_time").reset_index(drop=True)
    return df.tail(nrows).reset_index(drop=True)


def main() -> None:
    print("Loading BTCUSDT spot 1h candles...")
    df = _load_btcusdt(NROWS)
    print(f"  {len(df)} rows  {df['candle_begin_time'].iloc[0]}  –  {df['candle_begin_time'].iloc[-1]}")

    # ── module6: input snapshot ──────────────────────────────────────────────
    df.to_pickle(MODULE6_DIR / "input_btcusdt_candles.pkl")
    print("  Saved module6/input_btcusdt_candles.pkl")

    # ── module6: golden C-extension outputs ──────────────────────────────────
    close = np.ascontiguousarray(df["close"].values, dtype=np.float64)
    high  = np.ascontiguousarray(df["high"].values,  dtype=np.float64)
    low   = np.ascontiguousarray(df["low"].values,   dtype=np.float64)

    for param in [168, 336]:
        out = compute_momentum_c(close, high, low, n=param, trunc_len=TRUNC_LEN)
        path = MODULE6_DIR / f"golden_btcusdt_{param}.npy"
        np.save(path, out)
        non_nan = int((~np.isnan(out)).sum())
        print(f"  Saved module6/golden_btcusdt_{param}.npy  ({non_nan}/{len(out)} non-NaN)")

    # ── module5: golden signal_multi_params outputs ──────────────────────────
    df_with_sym = df.copy()
    df_with_sym["symbol"] = "BTCUSDT"
    sigs = _momentum.signal_multi_params(df_with_sym, [168, 336])
    for param in [168, 336]:
        arr = np.asarray(sigs[str(param)].values, dtype=np.float64)
        path = MODULE5_DIR / f"golden_signal_btcusdt_{param}.npy"
        np.save(path, arr)
        non_nan = int((~np.isnan(arr)).sum())
        print(f"  Saved module5/golden_signal_btcusdt_{param}.npy  ({non_nan}/{len(arr)} non-NaN)")

    print("\nCapture complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the capture script**

```
conda run -n alpha311 python tests/fixtures/capture.py
```

Expected output (exact numbers vary):
```
Loading BTCUSDT spot 1h candles...
  7200 rows  2025-...  –  2026-...
  Saved module6/input_btcusdt_candles.pkl
  Saved module6/golden_btcusdt_168.npy  (XXXX/7200 non-NaN)
  Saved module6/golden_btcusdt_336.npy  (XXXX/7200 non-NaN)
  Saved module5/golden_signal_btcusdt_168.npy  (XXXX/7200 non-NaN)
  Saved module5/golden_signal_btcusdt_336.npy  (XXXX/7200 non-NaN)
Capture complete.
```

If `data/binance_spot_1h_resample/40m/BTCUSDT.pkl` is missing, start the data server first or copy the file. The script exits with a traceback — fix the path and rerun.

- [ ] **Step 3: Verify fixture files were created**

```
conda run -n alpha311 python -c "
from pathlib import Path
files = [
    'tests/fixtures/module6/input_btcusdt_candles.pkl',
    'tests/fixtures/module6/golden_btcusdt_168.npy',
    'tests/fixtures/module6/golden_btcusdt_336.npy',
    'tests/fixtures/module5/golden_signal_btcusdt_168.npy',
    'tests/fixtures/module5/golden_signal_btcusdt_336.npy',
]
for f in files:
    p = Path(f)
    print(p, p.stat().st_size, 'bytes' if p.exists() else 'MISSING')
"
```

All five files must exist with size > 0.

---

### Task 2: Create module6_harness.py

**Files:**
- Create: `tests/support/module6_harness.py`

- [ ] **Step 1: Write the harness**

```python
# tests/support/module6_harness.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "module6"
TRUNC_LEN = 52560

_MISSING_MSG = (
    "Fixture files not found. Run from project root:\n"
    "  conda run -n alpha311 python tests/fixtures/capture.py"
)


def load_btcusdt_candles() -> pd.DataFrame:
    path = FIXTURE_DIR / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return pd.read_pickle(path)


def load_golden(param: int) -> np.ndarray:
    path = FIXTURE_DIR / f"golden_btcusdt_{param}.npy"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return np.load(path)


def signal_multi_params_py(df: pd.DataFrame, param_list: list[int]) -> dict[str, np.ndarray]:
    """Python reference implementation (source: factors/c_ext/validate.py)."""
    tp = (df["close"] + df["high"] + df["low"]) / 3
    tpnmax = df["high"].copy()
    tpnmin = df["low"].copy()
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
        is_anomaly = (np.abs(pctzscore) > 25) & ((pctchange > 0.2) | (pctchange < -0.5))
        tpn_copy = tpn.copy()
        tpn_copy.loc[tpn_copy[is_anomaly].index] = np.nan
        split_indices = np.where(is_anomaly)[0] + 1
        subfactor = []
        for sub_df in np.split(tpn_copy, split_indices):
            if sub_df.shape[0] > 168:
                ewm    = sub_df.ewm(span=n, adjust=False).mean()
                diff   = sub_df - ewm
                d_min  = diff.rolling(n, min_periods=167).min()
                d_max  = diff.rolling(n, min_periods=167).max()
                subfactor.append((diff - d_min) / (d_max - d_min))
            else:
                subfactor.append(pd.Series(np.full(sub_df.shape[0], np.nan)))
        factor   = pd.concat(subfactor, ignore_index=True)
        max_high = tpnmax.rolling(26 * 168, min_periods=1).max()
        min_low  = tpnmin.rolling(26 * 168, min_periods=1).min()
        factor   = factor + (tpn_copy - min_low) / (max_high - min_low)
        if len(tp) != len(factor):
            factor = pd.concat([pd.Series([np.nan] * (len(tp) - len(factor))), factor])
        ret[str(param)] = np.asarray(factor, dtype=np.float64)
    return ret
```

---

### Task 3: Create test_module6_momentum_factor.py

**Files:**
- Create: `tests/test_module6_momentum_factor.py`

- [ ] **Step 1: Write the test file**

```python
# tests/test_module6_momentum_factor.py
"""
Regression tests for the momentum C extension.

Fixture files must be generated before running:
    conda run -n alpha311 python tests/fixtures/capture.py
"""
from __future__ import annotations

import numpy as np
import pytest

import factors.动量因子210401修正 as momentum_factor
from factors.c_ext.momentum_wrapper import compute_momentum_c
from tests.support.module6_harness import (
    TRUNC_LEN,
    load_btcusdt_candles,
    load_golden,
    signal_multi_params_py,
)


def test_compute_momentum_c_output_length_matches_input():
    """Output array must have the same length as the input arrays."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    out = compute_momentum_c(close, high, low, n=168, trunc_len=TRUNC_LEN)
    assert len(out) == len(close), f"Expected length {len(close)}, got {len(out)}"


@pytest.mark.parametrize("param", [168, 336])
def test_compute_momentum_c_golden(param):
    """C extension output must exactly match the committed golden array."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    result   = compute_momentum_c(close, high, low, n=param, trunc_len=TRUNC_LEN)
    expected = load_golden(param)
    np.testing.assert_array_equal(
        result, expected,
        err_msg=f"C extension output differs from golden for param={param}. "
                "Re-run tests/fixtures/capture.py if the algorithm changed intentionally.",
    )


def test_compute_momentum_c_vs_python_reference():
    """C and Python implementations must agree within atol=1e-9 on non-NaN positions."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    c_out  = compute_momentum_c(close, high, low, n=168, trunc_len=TRUNC_LEN)
    py_out = signal_multi_params_py(df, [168])["168"]
    nan_c  = np.isnan(c_out)
    nan_py = np.isnan(py_out)
    assert np.array_equal(nan_c, nan_py), (
        f"NaN positions differ: C has {nan_c.sum()} NaNs, Python has {nan_py.sum()} NaNs"
    )
    np.testing.assert_allclose(c_out[~nan_c], py_out[~nan_py], atol=1e-9)


def test_signal_multi_params_golden():
    """signal_multi_params() end-to-end output must match the committed golden array."""
    df = load_btcusdt_candles().copy()
    df["symbol"] = "BTCUSDT"
    result   = momentum_factor.signal_multi_params(df, [168])
    expected = load_golden(168)
    np.testing.assert_array_equal(
        result["168"].values, expected,
        err_msg="signal_multi_params output differs from golden for param=168.",
    )
```

- [ ] **Step 2: Run module6 tests (expect all 5 to pass)**

```
conda run -n alpha311 python -m pytest tests/test_module6_momentum_factor.py -v
```

Expected:
```
PASSED tests/test_module6_momentum_factor.py::test_compute_momentum_c_output_length_matches_input
PASSED tests/test_module6_momentum_factor.py::test_compute_momentum_c_golden[168]
PASSED tests/test_module6_momentum_factor.py::test_compute_momentum_c_golden[336]
PASSED tests/test_module6_momentum_factor.py::test_compute_momentum_c_vs_python_reference
PASSED tests/test_module6_momentum_factor.py::test_signal_multi_params_golden
5 passed
```

If any golden test fails: check whether the fixture files match what the current code produces by re-running `capture.py`. If the test fails with `FileNotFoundError`, step 2 of Task 1 was not completed.

---

### Task 4: Commit module6 work

**Files:** `tests/fixtures/capture.py`, `tests/support/module6_harness.py`, `tests/test_module6_momentum_factor.py`, all files under `tests/fixtures/`

- [ ] **Step 1: Stage and commit**

```bash
git add tests/fixtures/capture.py tests/support/module6_harness.py tests/test_module6_momentum_factor.py tests/fixtures/module5/ tests/fixtures/module6/
git commit -m "test: module6 momentum C extension regression (5 tests + capture script)"
```

---

### Task 5: Create module5_harness.py

**Files:**
- Create: `tests/support/module5_harness.py`

- [ ] **Step 1: Write the harness**

```python
# tests/support/module5_harness.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_MODULE5_FIXTURE = _ROOT / "tests" / "fixtures" / "module5"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "动量因子210401修正"
BATCH_KEY = "spot_dict_batch1"

_MISSING_MSG = (
    "Fixture files not found. Run from project root:\n"
    "  conda run -n alpha311 python tests/fixtures/capture.py"
)


def load_btcusdt_candles() -> pd.DataFrame:
    """Returns the 7200-row BTCUSDT snapshot (same file used by module6 tests)."""
    path = _MODULE6_FIXTURE / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return pd.read_pickle(path)


def load_golden_signal(param: int) -> np.ndarray:
    """Returns the golden signal_multi_params output for BTCUSDT at the given param."""
    path = _MODULE5_FIXTURE / f"golden_signal_btcusdt_{param}.npy"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return np.load(path)


def make_factor_handle(
    tmp_path: Path,
    df: pd.DataFrame,
    run_time,
    *,
    symbol: str = "BTCUSDT",
) -> tuple[SimpleNamespace, CandleFileManager]:
    """Build a minimal handle with a single-batch market_dict ready for gen_factor_data."""
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    df_with_sym = df.copy(deep=True)
    df_with_sym["symbol"] = symbol
    preprocess_mgr.save_data_file(BATCH_KEY, {symbol: df_with_sym})

    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: preprocess_mgr},
            num_batch=256,
        ),
    )
    return handle, preprocess_mgr
```

---

### Task 6: Add golden value tests to test_module5_gen_factor_data.py

**Files:**
- Modify: `tests/test_module5_gen_factor_data.py`

- [ ] **Step 1: Add harness imports to the existing import block and append 2 golden tests at the bottom**

In `tests/test_module5_gen_factor_data.py`, add to the existing import block (after `import numpy as np` at line 8):

```python
from tests.support.module5_harness import (
    BATCH_KEY as _M5_BATCH_KEY,
    FACTOR_NAME as _M5_FACTOR_NAME,
    load_btcusdt_candles as _load_m5_candles,
    load_golden_signal,
    make_factor_handle,
)
```

Then append these test functions at the bottom of the same file:


@pytest.mark.parametrize("params", [[168], [168, 336]])
def test_gen_factor_data_golden_values(tmp, monkeypatch, params):
    """Factor column values written by gen_factor_data must match golden signal_multi_params output."""
    df = _load_m5_candles()
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, preprocess_mgr = make_factor_handle(tmp / f"golden_{'_'.join(map(str, params))}", df, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={_M5_FACTOR_NAME: params})
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    result_dict = preprocess_mgr.read_candle(_M5_BATCH_KEY)
    df_result = result_dict["BTCUSDT"]

    for param in params:
        col = f"{_M5_FACTOR_NAME}_{param}"
        assert col in df_result.columns, f"Missing column {col!r}"
        expected = load_golden_signal(param)
        np.testing.assert_array_equal(
            df_result[col].values, expected,
            err_msg=f"gen_factor_data column {col!r} differs from golden. "
                    "Re-run tests/fixtures/capture.py if algorithm changed intentionally.",
        )
```

- [ ] **Step 2: Run the two new golden tests (expect both to pass)**

```
conda run -n alpha311 python -m pytest "tests/test_module5_gen_factor_data.py::test_gen_factor_data_golden_values[params0]" "tests/test_module5_gen_factor_data.py::test_gen_factor_data_golden_values[params1]" -v
```

Expected:
```
PASSED tests/test_module5_gen_factor_data.py::test_gen_factor_data_golden_values[params0]
PASSED tests/test_module5_gen_factor_data.py::test_gen_factor_data_golden_values[params1]
2 passed
```

---

### Task 7: Add idempotency test, fix _calc_symbol_factors, verify

**Context:** `_calc_symbol_factors` at `core/preprocess.py:580-591` uses `pd.concat([df, factor_df], axis=1)`. On the second call to `gen_factor_data`, df already contains the factor columns, and pd.concat appends them again — creating duplicate column names. The idempotency test exposes this; the fix replaces pd.concat with `df.assign()`.

**Files:**
- Modify: `tests/test_module5_gen_factor_data.py`
- Modify: `core/preprocess.py:580-591`

- [ ] **Step 1: Append idempotency test to test_module5_gen_factor_data.py**

Add after the last line:

```python
def test_gen_factor_data_idempotent(tmp, monkeypatch):
    """Calling gen_factor_data twice must produce identical output with no duplicate columns."""
    df = _load_m5_candles()
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, preprocess_mgr = make_factor_handle(tmp / "idempotent", df, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={_M5_FACTOR_NAME: [168]})
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")
    first_values = preprocess_mgr.read_candle(_M5_BATCH_KEY)["BTCUSDT"][f"{_M5_FACTOR_NAME}_168"].values.copy()

    gen_factor_data(handle, run_ts, "spot")
    df_second = preprocess_mgr.read_candle(_M5_BATCH_KEY)["BTCUSDT"]

    factor_cols = [c for c in df_second.columns if c == f"{_M5_FACTOR_NAME}_168"]
    assert len(factor_cols) == 1, f"Duplicate factor columns after second call: {factor_cols}"
    np.testing.assert_array_equal(df_second[f"{_M5_FACTOR_NAME}_168"].values, first_values)
```

- [ ] **Step 2: Run the idempotency test and confirm it FAILS**

```
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py::test_gen_factor_data_idempotent -v
```

Expected output (before fix):
```
FAILED tests/test_module5_gen_factor_data.py::test_gen_factor_data_idempotent
AssertionError: Duplicate factor columns after second call: ['动量因子210401修正_168', '动量因子210401修正_168']
```

- [ ] **Step 3: Fix _calc_symbol_factors in core/preprocess.py**

Replace lines 580–591 (the entire `_calc_symbol_factors` function):

```python
def _calc_symbol_factors(item, factor_modules_params):
    """Worker: compute all pre-configured factor columns for one symbol DataFrame.
    ctypes/cffi calls release the GIL, so ThreadPoolExecutor achieves true CPU parallelism.
    """
    symbol, df = item
    factor_cols: dict[str, pd.Series] = {}
    for module, factor_name, param_list in factor_modules_params:
        raw = module.signal_multi_params(df, param_list)
        for param_key, series in raw.items():
            factor_cols[f'{factor_name}_{param_key}'] = series
    return symbol, df.assign(**factor_cols)
```

`df.assign()` adds or overwrites named columns in a copy of df, preventing duplicate columns on repeated calls.

- [ ] **Step 4: Run the idempotency test again — expect PASS**

```
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py::test_gen_factor_data_idempotent -v
```

Expected:
```
PASSED tests/test_module5_gen_factor_data.py::test_gen_factor_data_idempotent
1 passed
```

- [ ] **Step 5: Run the full module5 test file to confirm no regressions**

```
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py -v
```

Expected: all 5 tests pass (2 pre-existing + 2 golden + 1 idempotency).

---

### Task 8: Final full-suite run and commit

**Files:** all modified files

- [ ] **Step 1: Run all new test files together**

```
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py tests/test_module6_momentum_factor.py -v
```

Expected: all 10 tests pass (5 module5: 2 pre-existing + 2 golden + 1 idempotency; 5 module6: 1 length + 2 golden + 1 py-vs-c + 1 signal). All must pass.

- [ ] **Step 2: Commit all module5 changes**

```bash
git add tests/support/module5_harness.py tests/test_module5_gen_factor_data.py core/preprocess.py
git commit -m "test: module5 gen_factor_data golden regression + idempotency fix in _calc_symbol_factors"
```
