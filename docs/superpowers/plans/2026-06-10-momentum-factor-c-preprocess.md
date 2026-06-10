# 动量因子 C 扩展 + Preprocess 预计算 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将动量因子核心算法用 C 实现，通过 cffi/MSVC 编译为 .pyd，在 preprocess 阶段并行预计算全市场因子列，结果存入 market_dict。

**Architecture:** C 库（纯数值）→ Python cffi 包装层（ctypes 兼容接口）→ preprocess 新增 `gen_factor_data()` 步骤，在 `gen_batch_data_incremental` 完成后、`gen_pivot_incremental` 之前，用 `ThreadPoolExecutor` 并行计算全部 symbol × params，因子列直接写入各 symbol 的 DataFrame 中存回 `preprocess_mgr`。

**Tech Stack:** Python 3.11, cffi 1.15, MSVC (via conda alpha311), pandas, numpy, pytest

---

## 文件映射

| 文件 | 操作 | 职责 |
|------|------|------|
| `factors/c_ext/momentum_core.c` | 新建 | 全部数值算法 |
| `factors/c_ext/momentum_core.h` | 新建 | 函数声明 |
| `factors/c_ext/build.py` | 新建 | cffi 编译脚本，产出 `_momentum_core*.pyd` |
| `factors/c_ext/momentum_wrapper.py` | 新建 | cffi 加载 + numpy 接口封装 |
| `factors/c_ext/validate.py` | 新建 | Python vs C 输出逐元素对比 |
| `factors/动量因子210401修正.py` | 修改 | `signal_multi_params` 内部改调 wrapper |
| `config.py` | 修改 | 新增 `precompute_factor_configs` |
| `core/preprocess.py` | 修改 | 新增 `gen_factor_data()`，接入 `preprocess_type_data` |
| `tests/test_module5_gen_factor_data.py` | 新建 | `gen_factor_data` 单元测试 |

---

## Task 1：C 库头文件 + 编译脚本

**Files:**
- Create: `factors/c_ext/momentum_core.h`
- Create: `factors/c_ext/build.py`

- [ ] **Step 1: 写头文件**

```c
// factors/c_ext/momentum_core.h
#ifndef MOMENTUM_CORE_H
#define MOMENTUM_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * compute_momentum: 计算单个 symbol 单个 param 的动量因子
 *
 * close/high/low : 全量 K 线数组，长度 len
 * n              : span 参数（如 168）
 * len            : 数组长度（全量）
 * trunc_len      : 截取末尾条数（52560），若 len < trunc_len 则用 len
 * out            : 输出数组，长度 len，前 (len-trunc_len) 个填 NaN
 */
void compute_momentum(
    const double *close,
    const double *high,
    const double *low,
    int n,
    int len,
    int trunc_len,
    double *out
);

#ifdef __cplusplus
}
#endif

#endif /* MOMENTUM_CORE_H */
```

- [ ] **Step 2: 写 cffi 编译脚本**

```python
# factors/c_ext/build.py
"""
运行方式：
    cd d:\\alpha
    python factors/c_ext/build.py

产出：factors/c_ext/_momentum_core.cp311-win_amd64.pyd（或对应平台后缀）
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_momentum(
        const double *close,
        const double *high,
        const double *low,
        int n,
        int len,
        int trunc_len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "momentum_core.c")
with open(_src_path, "r") as f:
    _src = f.read()

ffi.set_source(
    "_momentum_core",
    _src,
    extra_compile_args=["/O2"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
```

- [ ] **Step 3: 确认 factors/c_ext 目录存在**

```powershell
New-Item -ItemType Directory -Force d:\alpha\factors\c_ext
```

- [ ] **Step 4: Commit**

```bash
git add factors/c_ext/momentum_core.h factors/c_ext/build.py
git commit -m "feat: add momentum_core.h and cffi build script"
```

---

## Task 2：C 库实现 — Phase 1（预处理 + 异常检测）

**Files:**
- Create: `factors/c_ext/momentum_core.c`

- [ ] **Step 1: 写 C 文件（Phase 1 + 辅助结构）**

```c
/* factors/c_ext/momentum_core.c */
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "momentum_core.h"

/* ── 单调双端队列（用于 rolling min/max） ─────────────────── */
#define DQ_CAP 65536

typedef struct {
    int    idx[DQ_CAP];
    double val[DQ_CAP];
    int    head, tail;
} Deque;

static void dq_clear(Deque *d)               { d->head = d->tail = 0; }
static int  dq_empty(const Deque *d)         { return d->head == d->tail; }
static void dq_push_back(Deque *d, int i, double v) {
    d->idx[d->tail] = i; d->val[d->tail] = v;
    d->tail = (d->tail + 1) % DQ_CAP;
}
static void dq_pop_back(Deque *d)  { d->tail = (d->tail - 1 + DQ_CAP) % DQ_CAP; }
static void dq_pop_front(Deque *d) { d->head = (d->head + 1) % DQ_CAP; }
static double dq_front_val(const Deque *d)   { return d->val[d->head]; }
static int    dq_front_idx(const Deque *d)   { return d->idx[d->head]; }
static double dq_back_val(const Deque *d) {
    return d->val[(d->tail - 1 + DQ_CAP) % DQ_CAP];
}

/* rolling min，window 大小 w，min_periods=1 */
static void roll_min(const double *x, int n, int w, double *out) {
    Deque dq; dq_clear(&dq);
    for (int i = 0; i < n; i++) {
        while (!dq_empty(&dq) && dq_front_idx(&dq) <= i - w) dq_pop_front(&dq);
        if (!isnan(x[i])) {
            while (!dq_empty(&dq) && dq_back_val(&dq) >= x[i]) dq_pop_back(&dq);
            dq_push_back(&dq, i, x[i]);
        }
        out[i] = dq_empty(&dq) ? NAN : dq_front_val(&dq);
    }
}

/* rolling max，window 大小 w，min_periods=1 */
static void roll_max(const double *x, int n, int w, double *out) {
    Deque dq; dq_clear(&dq);
    for (int i = 0; i < n; i++) {
        while (!dq_empty(&dq) && dq_front_idx(&dq) <= i - w) dq_pop_front(&dq);
        if (!isnan(x[i])) {
            while (!dq_empty(&dq) && dq_back_val(&dq) <= x[i]) dq_pop_back(&dq);
            dq_push_back(&dq, i, x[i]);
        }
        out[i] = dq_empty(&dq) ? NAN : dq_front_val(&dq);
    }
}

/* ── Phase 1：计算 tp, h4min, l4max, pctchange, is_anomaly ── */
static void phase1(
    const double *c, const double *h, const double *l,
    int tlen, int n,
    double *tp, double *h4min, double *l4max,
    double *pctchg, int *is_anom
) {
    /* tp = (c+h+l)/3, tp[0]=tp[1] */
    for (int i = 0; i < tlen; i++) tp[i] = (c[i] + h[i] + l[i]) / 3.0;
    if (tlen >= 2) tp[0] = tp[1];

    /* h4min = rolling(4).min(h), l4max = rolling(4).max(l) */
    roll_min(h, tlen, 4, h4min);
    roll_max(l, tlen, 4, l4max);

    /* pctchange = (h4min - l4max) / l4max */
    for (int i = 0; i < tlen; i++) {
        double denom = l4max[i];
        pctchg[i] = (isnan(denom) || denom == 0.0) ? NAN
                    : (h4min[i] - denom) / denom;
    }

    /* rolling mean + std of pctchange (Welford ring-buffer, window n) */
    double *ring  = (double *)malloc(n * sizeof(double));
    double  wsum  = 0.0, wsum2 = 0.0;
    int     wpos  = 0, wcount = 0;
    for (int i = 0; i < tlen; i++) {
        double x = pctchg[i];
        double mean_i = NAN, std_i = NAN;
        if (!isnan(x)) {
            if (wcount == n) {
                double old = ring[wpos];
                wsum -= old; wsum2 -= old * old; wcount--;
            }
            ring[wpos] = x; wpos = (wpos + 1) % n;
            wsum += x; wsum2 += x * x; wcount++;
            mean_i = wsum / wcount;
            if (wcount > 1) {
                double var = (wsum2 - wsum * wsum / wcount) / (wcount - 1);
                std_i = (var > 0.0) ? sqrt(var) : 0.0;
            } else {
                std_i = 0.0;
            }
        }
        /* anomaly: |zscore| > 25 AND (pct > 0.2 OR pct < -0.5) */
        is_anom[i] = 0;
        if (!isnan(mean_i) && !isnan(std_i) && std_i > 0.0) {
            double z = (x - mean_i) / std_i;
            if (fabs(z) > 25.0 && (x > 0.2 || x < -0.5)) {
                is_anom[i] = 1;
                tp[i] = NAN;   /* 与 Python tpn.loc[anomaly_indices]=NaN 一致 */
            }
        }
    }
    free(ring);
}
```

- [ ] **Step 2: Commit（仅 Phase 1，尚未完整）**

```bash
git add factors/c_ext/momentum_core.c
git commit -m "feat: momentum_core.c Phase 1 - tp, h4min, anomaly detection"
```

---

## Task 3：C 库实现 — Phase 2 + 3（分段 EWM + 长窗口）

**Files:**
- Modify: `factors/c_ext/momentum_core.c`

- [ ] **Step 1: 追加 Phase 2 辅助函数 + Phase 3 + 主函数到 momentum_core.c**

在文件末尾追加（接上 Task 2 的内容）：

```c
/* ── Phase 2：对一个 segment [start, end) 计算 EWM+归一化 ── */
static void process_segment(
    double *tp_trunc,   /* 已被 anomaly 修改过的 tp（anomaly 处为 NaN） */
    int start, int end,
    double alpha, int n,
    double *o           /* 输出指针，偏移已对齐 */
) {
    int seg_len = end - start;
    if (seg_len <= 168) {
        for (int i = start; i < end; i++) o[i] = NAN;
        return;
    }
    double ewm_val = NAN;
    int valid_count = 0;
    Deque min_dq, max_dq;
    dq_clear(&min_dq); dq_clear(&max_dq);

    for (int i = start; i < end; i++) {
        if (isnan(tp_trunc[i])) { o[i] = NAN; continue; }

        /* EWM(span=n, adjust=False): 首个有效值初始化为自身 */
        if (isnan(ewm_val)) ewm_val = tp_trunc[i];
        else                ewm_val = alpha * tp_trunc[i] + (1.0 - alpha) * ewm_val;

        double diff = tp_trunc[i] - ewm_val;

        /* rolling min（窗口 n）*/
        while (!dq_empty(&min_dq) && dq_front_idx(&min_dq) <= i - n) dq_pop_front(&min_dq);
        while (!dq_empty(&min_dq) && dq_back_val(&min_dq) >= diff)    dq_pop_back(&min_dq);
        dq_push_back(&min_dq, i, diff);

        /* rolling max（窗口 n）*/
        while (!dq_empty(&max_dq) && dq_front_idx(&max_dq) <= i - n) dq_pop_front(&max_dq);
        while (!dq_empty(&max_dq) && dq_back_val(&max_dq) <= diff)    dq_pop_back(&max_dq);
        dq_push_back(&max_dq, i, diff);

        valid_count++;
        if (valid_count >= 167) {  /* min_periods=167 */
            double dmin  = dq_front_val(&min_dq);
            double dmax  = dq_front_val(&max_dq);
            double range = dmax - dmin;
            o[i] = (range < 1e-10) ? NAN : (diff - dmin) / range;
        } else {
            o[i] = NAN;
        }
    }
}

/* ── 主函数 ────────────────────────────────────────────────── */
void compute_momentum(
    const double *close, const double *high, const double *low,
    int n, int len, int trunc_len, double *out
) {
    int offset = len - trunc_len;
    if (offset < 0) { offset = 0; trunc_len = len; }
    int tlen = trunc_len;

    /* 前段填 NaN */
    for (int i = 0; i < offset; i++) out[i] = NAN;
    double *o = out + offset;   /* o[i] 对应截断后的第 i 个点 */

    const double *c = close + offset;
    const double *h = high  + offset;
    const double *l = low   + offset;

    /* 分配工作数组 */
    double *tp     = (double *)malloc(tlen * sizeof(double));
    double *h4min  = (double *)malloc(tlen * sizeof(double));
    double *l4max  = (double *)malloc(tlen * sizeof(double));
    double *pctchg = (double *)malloc(tlen * sizeof(double));
    double *maxh   = (double *)malloc(tlen * sizeof(double));
    double *minl   = (double *)malloc(tlen * sizeof(double));
    int    *is_anom = (int *)calloc(tlen, sizeof(int));

    /* Phase 1 */
    phase1(c, h, l, tlen, n, tp, h4min, l4max, pctchg, is_anom);

    /* Phase 2：按 segment 处理
     *  anomaly 位置是前一段的最后一个元素（tp[i]=NaN），
     *  与 Python np.split(tpn, anomaly_idx+1) 逻辑一致 */
    double alpha = 2.0 / (n + 1.0);
    int seg_start = 0;
    for (int i = 0; i < tlen; i++) {
        if (is_anom[i]) {
            process_segment(tp, seg_start, i + 1, alpha, n, o);
            seg_start = i + 1;
        }
    }
    process_segment(tp, seg_start, tlen, alpha, n, o);  /* 最后一段 */

    /* Phase 3：长窗口分量
     *  max_high = rolling(4368).max(h4min)
     *  min_low  = rolling(4368).min(l4max)
     *  o[i] += (tp[i] - min_low[i]) / (max_high[i] - min_low[i]) */
    roll_max(h4min, tlen, 26 * 168, maxh);
    roll_min(l4max, tlen, 26 * 168, minl);
    for (int i = 0; i < tlen; i++) {
        if (!isnan(tp[i]) && !isnan(o[i]) && !isnan(minl[i]) && !isnan(maxh[i])) {
            double range = maxh[i] - minl[i];
            if (range > 1e-10)
                o[i] += (tp[i] - minl[i]) / range;
        }
    }

    free(tp); free(h4min); free(l4max); free(pctchg);
    free(maxh); free(minl); free(is_anom);
}
```

- [ ] **Step 2: 编译验证**

```powershell
cd d:\alpha
python factors/c_ext/build.py
```

预期输出：`Build complete. Output in: ...factors\c_ext`，目录下出现 `_momentum_core.cp311-win_amd64.pyd`（文件名含平台后缀）。

- [ ] **Step 3: 快速冒烟测试**

```python
# 在 d:\alpha 目录运行：python -c "..."
import sys; sys.path.insert(0, 'factors/c_ext')
import _momentum_core
import numpy as np
n = 200
close = np.random.rand(300).astype(np.float64)
high  = close + 0.01
low   = close - 0.01
out   = np.empty(300, dtype=np.float64)
p = _momentum_core.ffi.cast
lib = _momentum_core.lib
lib.compute_momentum(
    _momentum_core.ffi.cast('double*', close.ctypes.data),
    _momentum_core.ffi.cast('double*', high.ctypes.data),
    _momentum_core.ffi.cast('double*', low.ctypes.data),
    n, 300, 300,
    _momentum_core.ffi.cast('double*', out.ctypes.data),
)
print('smoke ok, non-nan count:', np.sum(~np.isnan(out)))
```

- [ ] **Step 4: Commit**

```bash
git add factors/c_ext/momentum_core.c
git commit -m "feat: momentum_core.c Phase 2+3 - segmented EWM, long-window, main function"
```

---

## Task 4：Python Wrapper

**Files:**
- Create: `factors/c_ext/momentum_wrapper.py`

- [ ] **Step 1: 写 wrapper**

```python
# factors/c_ext/momentum_wrapper.py
"""
ctypes 风格接口封装，将 numpy 数组桥接到 C 库。
加载顺序：优先从本文件同目录找已编译的 _momentum_core*.pyd，
找不到时抛出 ImportError 并提示运行 build.py。
"""
import os
import sys
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _momentum_core as _mod
except ImportError as e:
    raise ImportError(
        f"C extension not found: {e}\n"
        f"Run:  python {os.path.join(_HERE, 'build.py')}"
    ) from e

_lib = _mod.lib
_ffi = _mod.ffi


def _ptr(arr: np.ndarray):
    """返回 double* cffi 指针。arr 必须是 C-contiguous float64。"""
    return _ffi.cast("double *", arr.ctypes.data)


def compute_momentum_c(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    n: int,
    trunc_len: int = 52560,
) -> np.ndarray:
    """
    计算动量因子（C 实现）。

    Parameters
    ----------
    close, high, low : 1-D float64 ndarray，长度相同
    n                : span 参数
    trunc_len        : 截取末尾条数（默认 52560）

    Returns
    -------
    out : 1-D float64 ndarray，与输入等长，前端 NaN 填充
    """
    close = np.ascontiguousarray(close, dtype=np.float64)
    high  = np.ascontiguousarray(high,  dtype=np.float64)
    low   = np.ascontiguousarray(low,   dtype=np.float64)
    length = len(close)
    out = np.empty(length, dtype=np.float64)
    _lib.compute_momentum(
        _ptr(close), _ptr(high), _ptr(low),
        int(n), int(length), int(trunc_len),
        _ptr(out),
    )
    return out
```

- [ ] **Step 2: Commit**

```bash
git add factors/c_ext/momentum_wrapper.py
git commit -m "feat: momentum_wrapper.py - numpy bridge to C extension"
```

---

## Task 5：validate.py — 验证 C 与 Python 输出一致

**Files:**
- Create: `factors/c_ext/validate.py`

- [ ] **Step 1: 写 validate.py**

```python
# factors/c_ext/validate.py
"""
运行：cd d:\\alpha && python factors/c_ext/validate.py
对 BTCUSDT 真实数据验证 Python 原版与 C 版输出一致（atol=1e-9）。
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
import pandas as pd
from factors.c_ext.momentum_wrapper import compute_momentum_c

BTCUSDT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "binance_spot_1h_resample", "40m", "BTCUSDT.pkl",
)
PARAMS = [168, 336]
TRUNC_LEN = 52560


def signal_multi_params_py(df, param_list):
    """Python 原版（从因子文件复制，不依赖 wrapper）。"""
    import pandas as _pd
    import numpy as _np

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
        pctma   = pctchange.rolling(n, min_periods=1).mean()
        pctstd  = pctchange.rolling(n, min_periods=1).std()
        pctzscore = (pctchange - pctma) / pctstd
        threshold = 25
        is_anomaly = (_np.abs(pctzscore) > threshold) & (
            (pctchange > 0.2) | (pctchange < -0.5)
        )
        tpn_copy = tpn.copy()
        anomaly_indices = tpn_copy[is_anomaly].index.values.ravel()
        tpn_copy.loc[anomaly_indices] = _np.nan
        split_indices = _np.where(is_anomaly)[0] + 1
        dfs = _np.split(tpn_copy, split_indices)
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
                subfactor.append(_pd.Series(_np.full(sub_df.shape[0], _np.nan)))
        factor = _pd.concat(subfactor, ignore_index=True)
        max_high = tpnmax.rolling(26 * 168, min_periods=1).max()
        min_low  = tpnmin.rolling(26 * 168, min_periods=1).min()
        factor = factor + (tpn_copy - min_low) / (max_high - min_low)
        if len(tp) != len(factor):
            factor = _pd.concat(
                [_pd.Series([_np.nan] * (len(tp) - len(factor))), factor]
            )
        ret[str(param)] = factor.values
    return ret


def main():
    print(f"Loading {BTCUSDT_PATH} ...")
    df = pd.read_pickle(BTCUSDT_PATH).sort_values("candle_begin_time").reset_index(drop=True)
    print(f"Rows: {len(df)}")

    py_results = signal_multi_params_py(df, PARAMS)

    all_pass = True
    for param in PARAMS:
        py_out = py_results[str(param)]
        c_out  = compute_momentum_c(
            df["close"].values, df["high"].values, df["low"].values,
            n=param, trunc_len=TRUNC_LEN,
        )
        # Align: py_out 可能比 c_out 短（pad 前端 NaN）
        py_arr = np.asarray(py_out, dtype=np.float64)
        c_arr  = c_out

        py_nan = np.isnan(py_arr)
        c_nan  = np.isnan(c_arr)
        nan_match = np.array_equal(py_nan, c_nan)
        val_match = np.allclose(
            py_arr[~py_nan], c_arr[~c_nan], atol=1e-9, equal_nan=False
        ) if nan_match else False

        status = "PASS" if (nan_match and val_match) else "FAIL"
        if status == "FAIL":
            all_pass = False
            mismatch = np.where(~py_nan & (np.abs(py_arr - c_arr) > 1e-9))[0]
            print(f"  param={param} first mismatch idx={mismatch[:5]}")
            print(f"  py={py_arr[mismatch[:3]]}  c={c_arr[mismatch[:3]]}")
        print(f"param={param:>5}  NaN-match={nan_match}  val-match={val_match}  [{status}]")

    if all_pass:
        print("\nAll validations PASSED.")
        sys.exit(0)
    else:
        print("\nValidation FAILED.")
        sys.exit(1)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: 运行验证**

```powershell
cd d:\alpha
python factors/c_ext/validate.py
```

预期输出：
```
param=  168  NaN-match=True  val-match=True  [PASS]
param=  336  NaN-match=True  val-match=True  [PASS]

All validations PASSED.
```

若 FAIL，对比具体不一致位置，回查 C 算法与 Python 原版的差异（常见：rolling std ddof、min_periods 边界、ewm 首值初始化）。

- [ ] **Step 3: Commit**

```bash
git add factors/c_ext/validate.py
git commit -m "feat: validate.py - Python vs C output consistency check"
```

---

## Task 6：更新因子模块使用 C wrapper

**Files:**
- Modify: `factors/动量因子210401修正.py`

- [ ] **Step 1: 读取当前文件确认结构**（已知：`signal_multi_params(df, param_list) -> dict`）

- [ ] **Step 2: 替换实现，接口不变**

将 `factors/动量因子210401修正.py` 中 `signal_multi_params` 函数替换为：

```python
import numpy as np
import pandas as pd
from core.model.backtest_config import BacktestConfig
from factors.c_ext.momentum_wrapper import compute_momentum_c

TRUNC_LEN = 52560


def get_lookback(param) -> int:
    return BacktestConfig.get_kline_num()


def signal_multi_params(df, param_list) -> dict:
    close = df['close'].values.astype(np.float64)
    high  = df['high'].values.astype(np.float64)
    low   = df['low'].values.astype(np.float64)
    ret = {}
    for param in param_list:
        n = min(int(param), len(close))
        out = compute_momentum_c(close, high, low, n=n, trunc_len=TRUNC_LEN)
        # Python 原版对超出 TRUNC_LEN 的前端已是 NaN，Series 保持兼容
        ret[str(param)] = pd.Series(out, index=df.index)
    return ret
```

- [ ] **Step 3: 用 validate.py 的 `signal_multi_params_py` 回归确认**

validate.py 已对比过，这里只需确认因子文件能被 `factor_calc.py` 正常 import：

```powershell
cd d:\alpha
python -c "from factors.c_ext.momentum_wrapper import compute_momentum_c; print('import ok')"
python -c "import importlib; m = importlib.import_module('factors.动量因子210401修正'); print(dir(m))"
```

- [ ] **Step 4: Commit**

```bash
git add "factors/动量因子210401修正.py"
git commit -m "feat: 动量因子210401修正 - replace inner loop with C wrapper"
```

---

## Task 7：config.py 新增 precompute_factor_configs

**Files:**
- Modify: `config.py`

- [ ] **Step 1: 在 config.py 末尾（json 覆盖逻辑之前）添加**

在 `config.py` 第 69 行（`# 是否生成预处理数据`那段）之后、json 覆盖逻辑之前，添加：

```python
# 预计算因子配置：{因子模块名（不含 factors. 前缀）: [param 列表]}
# 留空 dict 则跳过因子预计算步骤
precompute_factor_configs = {
    '动量因子210401修正': [168, 336],
}
```

同时在 json 覆盖的 `allowed_attrs` 中追加 `'precompute_factor_configs'`：

```python
allowed_attrs = (
    'kline_count_1h_spot', 'kline_count_1h_swap', 'error_webhook_url', 'use_api',
    'data_api_uuid', 'data_api_key', 'enabled_hour_offsets', 'funding_rate',
    'precompute_factor_configs',   # ← 新增
)
```

- [ ] **Step 2: 验证 import**

```powershell
python -c "import config; print(config.precompute_factor_configs)"
```

预期：`{'动量因子210401修正': [168, 336]}`

- [ ] **Step 3: Commit**

```bash
git add config.py
git commit -m "feat: config.py - add precompute_factor_configs"
```

---

## Task 8：实现 gen_factor_data() + 接入 preprocess_type_data

**Files:**
- Modify: `core/preprocess.py`

- [ ] **Step 1: 在 preprocess.py 顶部 import 区域追加**

在现有 import 之后添加：

```python
import importlib
import os
from concurrent.futures import ThreadPoolExecutor
import config as bmac_config
```

（`time`, `ThreadPoolExecutor`, `logger`, `now_time_str` 已存在，只补缺的）

- [ ] **Step 2: 在 preprocess.py 末尾追加 gen_factor_data()**

```python
def _calc_symbol_factors(item, factor_modules_params):
    """
    worker 函数：对单个 symbol 的 DataFrame 计算所有预配置因子列。
    ctypes/cffi 调用自动释放 GIL，ThreadPoolExecutor 可实现真并行。

    factor_modules_params: list of (module, factor_name, param_list)
    """
    symbol, df = item
    result_dfs = [df]
    for module, factor_name, param_list in factor_modules_params:
        raw = module.signal_multi_params(df, param_list)
        factor_df = pd.DataFrame(raw).add_prefix(f'{factor_name}_')
        factor_df.index = df.index
        result_dfs.append(factor_df)
    return symbol, pd.concat(result_dfs, axis=1)


def gen_factor_data(handle: BmacHandle, run_time: datetime, trade_type: str):
    """
    在 market_dict 已写入后，对所有 batch 并行计算预配置因子列并写回。
    因子列命名规则：{factor_name}_{param}，与 factor_calc.py 保持一致。
    """
    factor_configs = getattr(bmac_config, 'precompute_factor_configs', {})
    if not factor_configs:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

    # 加载因子模块（一次性，所有 batch 复用）
    factor_modules_params = []
    for factor_name, param_list in factor_configs.items():
        mod = importlib.import_module(f'factors.{factor_name}')
        factor_modules_params.append((mod, factor_name, param_list))

    # 遍历所有已保存的 batch
    batch_idx = 1
    t_start = time.perf_counter()
    total_symbols = 0
    while True:
        batch_key = f'{trade_type}_dict_batch{batch_idx}'
        if not preprocess_mgr.has_symbol(batch_key):
            break
        market_dict: dict[str, pd.DataFrame] = preprocess_mgr.read_candle(batch_key)
        max_workers = min(len(market_dict), os.cpu_count() or 8, 16)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            updated = dict(
                pool.map(
                    lambda item: _calc_symbol_factors(item, factor_modules_params),
                    market_dict.items(),
                )
            )

        preprocess_mgr.set_candle(batch_key, run_time, updated)
        total_symbols += len(updated)
        batch_idx += 1

    elapsed = time.perf_counter() - t_start
    logger.ok(
        f'预计算因子 {trade_type} 完成，symbols={total_symbols}，'
        f'当前时间={now_time_str()}，耗时 {elapsed:.1f} 秒'
    )
```

- [ ] **Step 3: 在 preprocess_type_data 中接入（在 gen_pivot_incremental 之前）**

找到 `preprocess_type_data` 函数中的这一行：

```python
    t_start = time.perf_counter()
    gen_pivot_incremental(handle, run_time, trade_type, year_pivot_names)
```

在其前面插入：

```python
    gen_factor_data(handle, run_time, trade_type)
```

- [ ] **Step 4: Commit**

```bash
git add core/preprocess.py
git commit -m "feat: preprocess.py - gen_factor_data() with parallel symbol computation"
```

---

## Task 9：测试 gen_factor_data

**Files:**
- Create: `tests/test_module5_gen_factor_data.py`

- [ ] **Step 1: 写测试**

```python
# tests/test_module5_gen_factor_data.py
"""
验证 gen_factor_data 在 preprocess_mgr 中正确写入因子列。
使用与 module3 相同的 BTCUSDT spot 数据和 SimpleNamespace handle。
"""
from __future__ import annotations
import importlib
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.candle_manager import CandleFileManager
from core.preprocess import gen_factor_data
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import (
    RESAMPLE_INTERVAL,
    SPOT_CASE,
    load_btcusdt_spot,
)

FACTOR_NAME = '动量因子210401修正'
PARAMS = [168]
BATCH_KEY_TPL = 'spot_dict_batch1'


def make_handle_with_market_dict(tmp_path: Path, df_full: pd.DataFrame, run_time):
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    # 写一个 batch（模拟 gen_batch_data 的输出）
    df = df_full.copy(deep=True)
    df['symbol'] = 'BTCUSDT'
    market_dict = {'BTCUSDT': df}
    preprocess_mgr.save_data_file(BATCH_KEY_TPL, market_dict)
    preprocess_mgr.save_ready(BATCH_KEY_TPL, run_time)

    preprocess_handle = SimpleNamespace(
        preprocess_mgr_map={offset_str: preprocess_mgr},
        num_batch=256,
    )
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=preprocess_handle,
    )
    return handle, preprocess_mgr, offset_str


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path


def test_gen_factor_data_adds_factor_columns(tmp, monkeypatch):
    """gen_factor_data 应在 market_dict 的每个 symbol DataFrame 中写入因子列。"""
    df_full = load_btcusdt_spot()
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1]).to_pydatetime()

    handle, preprocess_mgr, offset_str = make_handle_with_market_dict(
        tmp, df_full, run_ts
    )

    # 注入 config
    fake_config = SimpleNamespace(
        precompute_factor_configs={FACTOR_NAME: PARAMS}
    )
    monkeypatch.setattr('core.preprocess.bmac_config', fake_config)

    gen_factor_data(handle, run_ts, 'spot')

    result = preprocess_mgr.read_candle(BATCH_KEY_TPL)
    df_result = result['BTCUSDT']

    expected_col = f'{FACTOR_NAME}_{PARAMS[0]}'
    assert expected_col in df_result.columns, (
        f"Expected column '{expected_col}' in {df_result.columns.tolist()}"
    )

    factor_vals = df_result[expected_col].values
    non_nan = factor_vals[~np.isnan(factor_vals)]
    assert len(non_nan) > 0, "All factor values are NaN"
    assert np.all(np.isfinite(non_nan)), "Factor values contain Inf"


def test_gen_factor_data_no_config_is_noop(tmp, monkeypatch):
    """precompute_factor_configs 为空时 gen_factor_data 不修改 market_dict。"""
    df_full = load_btcusdt_spot()
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1]).to_pydatetime()

    handle, preprocess_mgr, offset_str = make_handle_with_market_dict(
        tmp, df_full, run_ts
    )

    fake_config = SimpleNamespace(precompute_factor_configs={})
    monkeypatch.setattr('core.preprocess.bmac_config', fake_config)

    gen_factor_data(handle, run_ts, 'spot')

    result = preprocess_mgr.read_candle(BATCH_KEY_TPL)
    df_result = result['BTCUSDT']
    factor_cols = [c for c in df_result.columns if FACTOR_NAME in c]
    assert factor_cols == [], f"Unexpected columns: {factor_cols}"
```

- [ ] **Step 2: 运行测试**

```powershell
cd d:\alpha
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py -v
```

预期：
```
PASSED tests/test_module5_gen_factor_data.py::test_gen_factor_data_adds_factor_columns
PASSED tests/test_module5_gen_factor_data.py::test_gen_factor_data_no_config_is_noop
```

- [ ] **Step 3: 运行全量既有测试，确认无回归**

```powershell
conda run -n alpha311 python -m pytest tests/ -v --tb=short
```

预期：所有已有测试通过。

- [ ] **Step 4: Commit**

```bash
git add tests/test_module5_gen_factor_data.py
git commit -m "test: module5 gen_factor_data - factor column injection and noop"
```

---

## Task 10：端到端冒烟 + 预编译产物提交

**Files:**
- Modify: `.gitignore`（排除 cffi 临时文件）

- [ ] **Step 1: 更新 .gitignore**

在 `.gitignore` 中追加：

```
# cffi build artifacts
factors/c_ext/__pycache__/
factors/c_ext/*.o
factors/c_ext/*.exp
factors/c_ext/*.lib
```

保留 `factors/c_ext/_momentum_core*.pyd`（预编译产物，供其他人直接使用）。

- [ ] **Step 2: 端到端冒烟**

确认因子列可从 preprocess 结果中读出：

```python
# python -c "..."
import pandas as pd, pickle, glob, os
files = glob.glob('data/preprocess_1h_resample/*/spot_dict_batch1.pkl')
if files:
    d = pd.read_pickle(files[0])
    sym = list(d.keys())[0]
    cols = [c for c in d[sym].columns if '动量因子' in c]
    print(f'symbol={sym}, factor_cols={cols}')
    print(d[sym][cols].tail())
else:
    print('No preprocess data found - run a preprocess cycle first')
```

- [ ] **Step 3: 提交预编译 .pyd 和最终清理**

```bash
git add factors/c_ext/_momentum_core*.pyd .gitignore
git commit -m "chore: add prebuilt momentum_core.pyd and update .gitignore"
```

---

## 自检结论

| 设计需求 | 覆盖 Task |
|---|---|
| C 实现 Phase 1 异常检测 | Task 2 |
| C 实现 Phase 2 分段 EWM+归一化 | Task 3 |
| C 实现 Phase 3 长窗口分量 | Task 3 |
| cffi/MSVC 编译脚本 | Task 1 |
| Python wrapper（numpy 接口） | Task 4 |
| Python vs C 一致性验证 | Task 5 |
| 因子模块接口不变 | Task 6 |
| config.py precompute_factor_configs | Task 7 |
| gen_factor_data() 并行实现 | Task 8 |
| 接入 preprocess_type_data | Task 8 |
| 测试 gen_factor_data | Task 9 |
| 回归测试 | Task 9 |
