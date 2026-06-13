# ILLQ & CciV3 C Extension + Preprocess Integration — Design Spec

**Date:** 2026-06-13

## Goal

Port `ILLQStdBbi45208.py` and `CciV3.py` to C (via cffi/MSVC), following the same pattern used for `动量因子210401修正`, and wire both into the preprocess `gen_factor_data` pipeline. Regression tests at param=10000 verify C output matches Python exactly.

## Architecture

Two independent C extensions, one per factor. Each follows the established pattern:

```
factors/c_ext/<name>_core.c   →   _<name>_core.pyd
                                        ↓
                                <name>_wrapper.py   (numpy ↔ C bridge)
                                        ↓
                               factors/<FactorName>.py  (signal_multi_params calls C)
                                        ↓
                                config.precompute_factor_configs
                                        ↓
                                core/preprocess.gen_factor_data  (already exists, no change)
```

**Tech Stack:** Python 3.11, cffi 1.15, MSVC (conda alpha311), pandas, numpy, pytest

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `factors/c_ext/illq_core.h` | Create | C function declaration for ILLQ |
| `factors/c_ext/illq_core.c` | Create | ILLQ numerical kernel |
| `factors/c_ext/illq_build.py` | Create | cffi build → `_illq_core.pyd` |
| `factors/c_ext/illq_wrapper.py` | Create | numpy ↔ C bridge |
| `factors/c_ext/illq_validate.py` | Create | Python vs C pairwise comparison |
| `factors/ILLQStdBbi45208.py` | Modify | `signal_multi_params` delegates to C |
| `factors/c_ext/cci_core.h` | Create | C function declaration for CciV3 |
| `factors/c_ext/cci_core.c` | Create | CciV3 numerical kernel |
| `factors/c_ext/cci_build.py` | Create | cffi build → `_cci_core.pyd` |
| `factors/c_ext/cci_wrapper.py` | Create | numpy ↔ C bridge |
| `factors/c_ext/cci_validate.py` | Create | Python vs C pairwise comparison |
| `factors/CciV3.py` | Modify | `signal_multi_params` delegates to C; `ewm_fixed` removed |
| `config.py` | Modify | Add both factors to `precompute_factor_configs` |
| `tests/fixtures/capture_illq_cci.py` | Create | One-time golden snapshot generator |
| `tests/fixtures/illq/golden_10000.npy` | Create | Committed golden output (ILLQ, param=10000) |
| `tests/fixtures/cci/golden_10000.npy` | Create | Committed golden output (CciV3, param=10000) |
| `tests/support/illq_harness.py` | Create | Fixture loader + preprocess handle builder |
| `tests/support/cci_harness.py` | Create | Fixture loader + preprocess handle builder |
| `tests/test_module_illq.py` | Create | 4 regression tests for ILLQ |
| `tests/test_module_cci.py` | Create | 4 regression tests for CciV3 |

`gen_factor_data` in `core/preprocess.py` requires **no changes** — it already iterates `precompute_factor_configs`.

---

## C Algorithm: ILLQStdBbi — `compute_illq`

### Python algorithm (reference)

```python
def signal_multi_params(df, param_list) -> dict:
    zhenfu   = (df['high'] - df['low']) / df['open']
    ilq2     = df['quote_volume'] / zhenfu
    ilq      = df['quote_volume'] / ((2*(df['high']-df['low']) - abs(df['open']-df['close'])) / df['open'])
    ilq_sum  = ilq + ilq2
    for param in param_list:
        n        = int(param)
        bbi      = (rolling_mean(close,n) + rolling_mean(close,2n) +
                    rolling_mean(close,4n) + rolling_mean(close,6n)) / 4
        bbi_bias = close / bbi
        factor   = rolling_std(ilq_sum, n) * bbi_bias
        ret[str(param)] = factor
```

### C function signature

```c
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
```

### C algorithm

1. **Element-wise (once per call):** compute `ilq_sum[i]` for all i:
   - `zhenfu = (high[i] - low[i]) / open[i]`
   - `ilq2 = qvol[i] / zhenfu`
   - `route = (2*(high[i]-low[i]) - fabs(open[i]-close[i])) / open[i]`
   - `ilq = qvol[i] / route`
   - `ilq_sum[i] = ilq + ilq2`  (NaN propagates naturally via IEEE 754 if denominator is 0)

2. **Rolling mean with circular sum buffer** (O(n) time, O(w) space), `min_periods=1`, applied four times for windows n, 2n, 4n, 6n → bbi1..bbi4.
   - `bbi[i] = (bbi1[i] + bbi2[i] + bbi3[i] + bbi4[i]) / 4`
   - `bbi_bias[i] = close[i] / bbi[i]`

3. **Rolling std (Welford, ddof=1)** on `ilq_sum`, window n, `min_periods=1`:
   - For `n_valid == 1`: std = NaN (matches pandas ddof=1 behaviour)
   - For `n_valid >= 2`: `std = sqrt((sum_sq - sum*sum/n_valid) / (n_valid - 1))`

4. `out[i] = std[i] * bbi_bias[i]`

**Memory allocation inside C:** `ilq_sum[len]`, `bbi1..bbi4[len]`, `bbi[len]`, `bbi_bias[len]`, `std_ilq[len]`. All freed before return.

---

## C Algorithm: CciV3 — `compute_cci`

### Python algorithm (reference)

```python
def ewm_fixed(arr, window, span, min_periods):
    alpha = 2/(span+1)
    y = arr.ewm(alpha=alpha, adjust=False).mean()
    residual = (1-alpha)**window * y + (alpha-1)*(1-alpha)**(window-1) * arr.shift(-1)
    residual = residual.shift(window).fillna(0)
    ewm_fixed = y - residual
    ewm_fixed[:min_periods-1] = NaN
    return ewm_fixed

def signal_multi_params(df, param_list):
    for param in param_list:
        oma = ewm_fixed(open,  15000, span=n, min_periods=2)
        hma = ewm_fixed(high,  15000, span=n, min_periods=2)
        lma = ewm_fixed(low,   15000, span=n, min_periods=2)
        cma = ewm_fixed(close, 15000, span=n, min_periods=2)
        tp  = (oma + hma + lma + cma) / 4
        ma  = ewm_fixed(tp,           15000, span=n, min_periods=2)
        md  = ewm_fixed(|cma - ma|,   15000, span=n, min_periods=2)
        ret[str(param)] = (tp - ma) / md
```

### Closed-form derivation of `ewm_fixed`

Let `y[t]` = standard EWM with `adjust=False`, `alpha = 2/(n+1)`, `beta = 1-alpha`, `beta_w = beta^window`.

Expanding the shift operations:

```
ewm_fixed[t] = y[t]                                              (t < window)
ewm_fixed[t] = y[t] - beta_w * y[t-window] + beta_w * arr[t-window+1]   (t >= window)
```

This reduces the implementation to:
1. One forward pass to compute `y[0..len-1]` (standard EWM recurrence)
2. One correction pass using already-computed `y[t-window]` and `arr[t-window+1]`
3. Set `out[0] = NaN` (min_periods=2 → first position always NaN)

**Note:** `arr.shift(-1)` in the Python formula is forward-looking in intermediate form, but the subsequent `shift(window)` makes it causal (`arr[t-window+1]` is past data).

### C function signature

```c
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
```

### Internal helper

```c
static void ewm_fixed_inner(
    const double *arr, double alpha, double beta_w,
    int window, int len, double *out
);
```

1. Compute standard EWM into `y[]`: `y[0]=arr[0]`, `y[t] = alpha*arr[t] + (1-alpha)*y[t-1]`
2. Apply correction: `out[t] = y[t]` for `t < window`; `out[t] = y[t] - beta_w*y[t-w] + beta_w*arr[t-w+1]` for `t >= window`
3. Set `out[0] = NaN` (min_periods=2)

### Full kernel flow

`ewm_fixed_inner` called 6 times: open→oma, high→hma, low→lma, close→cma, tp=(avg)→ma, |cma-ma|→md. Output = `(tp-ma)/md`; divide-by-near-zero (|md| < 1e-15) → NaN.

**Memory:** 7 temporary arrays of length `len` (oma, hma, lma, cma, tp, ma, md), plus one `y[]` inside `ewm_fixed_inner` (allocated/freed per call).

---

## Testing Strategy

### Data source

Reuse the existing committed fixture: `tests/fixtures/module6/input_btcusdt_candles.pkl`
(7200 rows of BTCUSDT 1h spot candles, includes `open`, `high`, `low`, `close`, `quote_volume`).

**Known limitation for CciV3:** `window=15000 > 7200 rows`, so the ewm correction term (`shift(15000)`) is never applied — `ewm_fixed` reduces to standard EWM throughout. The test still fully verifies C == Python; testing the correction path requires ≥ 15001 rows.

### Golden snapshot capture

`tests/fixtures/capture_illq_cci.py` — run once:
- Loads fixture, calls Python `signal_multi_params(df, [10000])` for each factor
- Saves `tests/fixtures/illq/golden_10000.npy` and `tests/fixtures/cci/golden_10000.npy`
- Committed to git as regression baseline

### Tests (4 per factor, 8 total)

| Test | What it checks |
|------|---------------|
| `test_output_length` | `len(C_output) == len(input)` |
| `test_golden` | C output `== golden .npy` exactly (`assert_array_equal`) |
| `test_c_vs_python` | C output matches Python `signal_multi_params` at `atol=1e-9`, NaN positions identical |
| `test_signal_multi_params_golden` | End-to-end: updated factor module → C → matches golden |

### gen_factor_data integration test

Existing `test_gen_factor_data_adds_factor_columns` pattern can be extended in each test file to verify the factor columns appear in preprocess output (monkeypatching `bmac_config`).

---

## Integration: `config.py`

Add both factors to `precompute_factor_configs`:

```python
precompute_factor_configs = {
    '动量因子210401修正': [168, 336],
    'ILLQStdBbi45208':   [10000],
    'CciV3':             [10000],
}
```

`gen_factor_data` in `core/preprocess.py` requires no changes.

---

## Updated Factor Files (interface unchanged)

### `factors/ILLQStdBbi45208.py`

```python
import numpy as np
import pandas as pd
from factors.c_ext.illq_wrapper import compute_illq_c

def signal_multi_params(df, param_list) -> dict:
    open_ = np.ascontiguousarray(df['open'].values,         dtype=np.float64)
    high  = np.ascontiguousarray(df['high'].values,         dtype=np.float64)
    low   = np.ascontiguousarray(df['low'].values,          dtype=np.float64)
    close = np.ascontiguousarray(df['close'].values,        dtype=np.float64)
    qvol  = np.ascontiguousarray(df['quote_volume'].values, dtype=np.float64)
    ret = {}
    for param in param_list:
        out = compute_illq_c(open_, high, low, close, qvol, n=int(param))
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
```

### `factors/CciV3.py`

```python
import numpy as np
import pandas as pd
from factors.c_ext.cci_wrapper import compute_cci_c

WINDOW = 15000

def signal_multi_params(df, param_list) -> dict:
    open_ = np.ascontiguousarray(df['open'].values,  dtype=np.float64)
    high  = np.ascontiguousarray(df['high'].values,  dtype=np.float64)
    low   = np.ascontiguousarray(df['low'].values,   dtype=np.float64)
    close = np.ascontiguousarray(df['close'].values, dtype=np.float64)
    ret = {}
    for param in param_list:
        out = compute_cci_c(open_, high, low, close, n=int(param), window=WINDOW)
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
```

The `ewm_fixed` helper is removed from `CciV3.py`.

---

## Build Instructions

```powershell
# ILLQ
cd d:\alpha
conda run -n alpha311 python factors/c_ext/illq_build.py

# CciV3
conda run -n alpha311 python factors/c_ext/cci_build.py
```

Each produces a `.pyd` in `factors/c_ext/`. Prebuilt `.pyd` files are committed to git for zero-build usage (same as `_momentum_core.cp311-win_amd64.pyd`).
