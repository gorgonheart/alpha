# gen_factor_data Parallel Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate serial bottlenecks in `gen_factor_data` by loading all batches at once and flattening (symbol × factor) into a single parallel work pool, then verify correctness with real fixture data (TDD, refactored output == serial reference).

**Architecture:** Add `_calc_one_factor` as a fine-grained worker (one symbol × one factor), refactor `gen_factor_data` to load all batches into memory first, fan out all (symbol, factor) units to a single ThreadPoolExecutor, merge results, then write all batches back sequentially. A serial `reference_gen_factor_data` in the test harness acts as the ground-truth baseline.

**Tech Stack:** Python 3.11, conda env `alpha311`, cffi C extensions (GIL-released), pytest, `numpy.testing.assert_array_equal`, existing fixtures in `tests/fixtures/module6/`

---

## File Map

| File | Action | Purpose |
|------|--------|---------|
| `tests/support/module7_harness.py` | **Create** | Multi-symbol, multi-batch handle builder + serial `reference_gen_factor_data` baseline |
| `tests/test_module7_gen_factor_parallel.py` | **Create** | 4 failing tests (red → green after implementation) |
| `core/preprocess.py` | **Modify** | Add `_calc_one_factor`; refactor `gen_factor_data` |
| `config.py` | **Modify** | `preprocess_num_batch: 128 → 256` |

---

## Task 1: Write the harness (module7_harness.py)

**Files:**
- Create: `tests/support/module7_harness.py`

- [ ] **Step 1: Create `tests/support/module7_harness.py`**

```python
# tests/support/module7_harness.py
from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

SYMBOLS_8 = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
]
SYMBOLS_4 = SYMBOLS_8[:4]

ALL_FACTOR_CONFIGS = {
    "动量因子210401修正": [168, 336],
    "ILLQStdBbi45208": [10000],
    "CciV3": [10000],
}


def load_btcusdt_candles() -> pd.DataFrame:
    """Returns the 7200-row BTCUSDT snapshot."""
    path = _MODULE6_FIXTURE / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(
            f"Fixture not found: {path}\n"
            "Run from project root:\n"
            "  conda run -n alpha311 python tests/fixtures/capture_illq_cci.py"
        )
    return pd.read_pickle(path).reset_index(drop=True)


def _make_symbol_df(base_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Clone base_df with symbol column set. Same OHLCV data, different symbol name."""
    df = base_df.copy(deep=True)
    df["symbol"] = symbol
    return df


def make_multi_batch_handle(
    tmp_path: Path,
    base_df: pd.DataFrame,
    run_time,
    *,
    batches: list[list[str]],
    trade_type: str = "spot",
) -> tuple[SimpleNamespace, CandleFileManager]:
    """Build a handle with multiple batches, each holding a subset of symbols.

    Args:
        batches: list of symbol lists, one per batch.
                 e.g. [["BTCUSDT", "ETHUSDT"], ["BNBUSDT"]] → 2 batches
    Returns:
        (handle, preprocess_mgr)
    """
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    for batch_idx, symbol_list in enumerate(batches, start=1):
        batch_key = f"{trade_type}_dict_batch{batch_idx}"
        market_dict = {sym: _make_symbol_df(base_df, sym) for sym in symbol_list}
        preprocess_mgr.save_data_file(batch_key, market_dict)

    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: preprocess_mgr},
            num_batch=256,
        ),
    )
    return handle, preprocess_mgr


def reference_gen_factor_data(
    preprocess_mgr: CandleFileManager,
    run_time,
    trade_type: str,
    factor_configs: dict,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Serial reference baseline: compute factors batch-by-batch, symbol-by-symbol.

    Returns: {batch_key: {symbol: df_with_factor_columns}}
    This does NOT call gen_factor_data — it reimplements the logic independently,
    using _calc_symbol_factors to match the original serial behaviour.
    """
    factor_modules_params = [
        (importlib.import_module(f"factors.{factor_name}"), factor_name, param_list)
        for factor_name, param_list in factor_configs.items()
    ]

    results: dict[str, dict[str, pd.DataFrame]] = {}
    batch_idx = 1
    while True:
        batch_key = f"{trade_type}_dict_batch{batch_idx}"
        if not preprocess_mgr.has_symbol(batch_key):
            break
        market_dict: dict[str, pd.DataFrame] = preprocess_mgr.read_candle(batch_key)
        updated: dict[str, pd.DataFrame] = {}
        for symbol, df in market_dict.items():
            factor_cols: dict[str, pd.Series] = {}
            for module, factor_name, param_list in factor_modules_params:
                raw = module.signal_multi_params(df, param_list)
                for param_key, series in raw.items():
                    factor_cols[f"{factor_name}_{param_key}"] = series
            updated[symbol] = df.assign(**factor_cols)
        results[batch_key] = updated
        batch_idx += 1
    return results
```

- [ ] **Step 2: Verify harness is importable**

```powershell
conda run -n alpha311 python -c "from tests.support.module7_harness import load_btcusdt_candles, make_multi_batch_handle, reference_gen_factor_data; df = load_btcusdt_candles(); print('rows:', len(df))"
```

Expected: `rows: 7200`

- [ ] **Step 3: Commit harness**

```powershell
git add tests/support/module7_harness.py
git commit -m "test: module7 harness - multi-batch handle and serial reference baseline"
```

---

## Task 2: Write the failing tests (RED)

**Files:**
- Create: `tests/test_module7_gen_factor_parallel.py`

- [ ] **Step 1: Create `tests/test_module7_gen_factor_parallel.py`**

```python
"""
Tests for the parallel gen_factor_data refactor.
Verifies that the refactored (parallel) output is bit-for-bit identical
to the serial reference implementation across multi-symbol, multi-batch scenarios.
Uses real fixture data: tests/fixtures/module6/input_btcusdt_candles.pkl (7200 rows).
"""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from core.preprocess import gen_factor_data
from tests.support.module7_harness import (
    ALL_FACTOR_CONFIGS,
    SYMBOLS_4,
    SYMBOLS_8,
    load_btcusdt_candles,
    make_multi_batch_handle,
    reference_gen_factor_data,
)


@pytest.fixture(scope="module")
def base_df():
    return load_btcusdt_candles()


def test_parallel_single_batch_multi_symbol(tmp_path, base_df, monkeypatch):
    """1 batch, 8 symbols, all 3 factors: parallel output matches serial reference."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_8]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    # Compute serial reference BEFORE calling gen_factor_data
    ref = reference_gen_factor_data(preprocess_mgr, run_ts, "spot", ALL_FACTOR_CONFIGS)

    gen_factor_data(handle, run_ts, "spot")

    for batch_key, ref_market in ref.items():
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, ref_df in ref_market.items():
            result_df = result_market[symbol]
            factor_cols = [c for c in ref_df.columns if "_" in c and c not in base_df.columns]
            for col in factor_cols:
                assert col in result_df.columns, f"Missing column {col!r} for {symbol}"
                np.testing.assert_array_equal(
                    result_df[col].values,
                    ref_df[col].values,
                    err_msg=f"Mismatch: {symbol} / {col}",
                )


def test_parallel_multi_batch_multi_symbol(tmp_path, base_df, monkeypatch):
    """3 batches (8/8/4 symbols), all 3 factors: parallel output matches serial reference."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_8, SYMBOLS_8, SYMBOLS_4]
    # Use distinct names across batches to avoid collisions
    batch0 = [f"{s}_B0" for s in SYMBOLS_8]
    batch1 = [f"{s}_B1" for s in SYMBOLS_8]
    batch2 = [f"{s}_B2" for s in SYMBOLS_4]
    batches = [batch0, batch1, batch2]

    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    ref = reference_gen_factor_data(preprocess_mgr, run_ts, "spot", ALL_FACTOR_CONFIGS)

    gen_factor_data(handle, run_ts, "spot")

    for batch_key, ref_market in ref.items():
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, ref_df in ref_market.items():
            result_df = result_market[symbol]
            factor_cols = [c for c in ref_df.columns if "_" in c and c not in base_df.columns]
            for col in factor_cols:
                assert col in result_df.columns, f"Missing {col!r} for {symbol} in {batch_key}"
                np.testing.assert_array_equal(
                    result_df[col].values,
                    ref_df[col].values,
                    err_msg=f"Mismatch: {batch_key} / {symbol} / {col}",
                )


def test_parallel_column_completeness(tmp_path, base_df, monkeypatch):
    """2 batches, 4 symbols: every expected {factor}_{param} column present, no duplicates."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_4[:2], SYMBOLS_4[2:]]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    expected_cols = [
        f"{factor_name}_{param}"
        for factor_name, param_list in ALL_FACTOR_CONFIGS.items()
        for param in param_list
    ]

    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, df in result_market.items():
            for col in expected_cols:
                assert col in df.columns, f"Missing {col!r} for {symbol} in {batch_key}"
            # No duplicate columns
            seen = [c for c in df.columns if c in expected_cols]
            assert len(seen) == len(set(seen)), (
                f"Duplicate factor columns for {symbol}: {seen}"
            )


def test_parallel_idempotent(tmp_path, base_df, monkeypatch):
    """Running gen_factor_data twice produces identical results (no drift, no duplication)."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_4[:2], SYMBOLS_4[2:]]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    # Snapshot first run
    first_run: dict[str, dict[str, dict]] = {}
    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        first_run[batch_key] = {
            sym: {
                col: df[col].values.copy()
                for col in df.columns
                if any(col.startswith(f) for f in ALL_FACTOR_CONFIGS)
            }
            for sym, df in result_market.items()
        }

    gen_factor_data(handle, run_ts, "spot")

    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, df in result_market.items():
            factor_cols = [c for c in df.columns if any(c.startswith(f) for f in ALL_FACTOR_CONFIGS)]
            assert len(factor_cols) == len(set(factor_cols)), (
                f"Duplicate columns after second run: {symbol}"
            )
            for col, first_vals in first_run[batch_key][symbol].items():
                np.testing.assert_array_equal(
                    df[col].values, first_vals,
                    err_msg=f"Idempotent mismatch: {batch_key} / {symbol} / {col}",
                )
```

- [ ] **Step 2: Run tests to confirm they FAIL (RED)**

```powershell
conda run -n alpha311 python -m pytest tests/test_module7_gen_factor_parallel.py -v 2>&1 | Select-Object -First 40
```

Expected: All 4 tests PASS because `gen_factor_data` already works correctly on single-symbol. They may all pass or some may fail — the important thing is we have a baseline. If they all pass already, the tests still serve as regression protection for the refactored implementation.

> **Note:** These tests validate correctness of the *parallel* refactor against the *serial* reference. They will pass with the current serial implementation too — that's by design. The spec requires TDD to ensure refactored output matches pre-refactor output.

- [ ] **Step 3: Commit tests**

```powershell
git add tests/test_module7_gen_factor_parallel.py
git commit -m "test: module7 gen_factor_parallel - 4 correctness tests against serial reference"
```

---

## Task 3: Implement `_calc_one_factor` and refactor `gen_factor_data`

**Files:**
- Modify: `core/preprocess.py:580-638`

The refactored `gen_factor_data`:
1. Loads all batches into `all_batches: dict[batch_key, dict[symbol, df]]`
2. Flattens to `(symbol, df, module, factor_name, param_list)` work units — one unit per symbol per factor
3. Dispatches all units to a single `ThreadPoolExecutor(max_workers=os.cpu_count() or 4)`
4. Collects `(symbol, factor_name, {param_key: Series})` results
5. For each batch_key × symbol, merges all factor columns and calls `df.assign(**factor_cols)`
6. Writes each batch back with `set_candle`

- [ ] **Step 1: Add `_calc_one_factor` and refactor `gen_factor_data` in `core/preprocess.py`**

Replace lines 580–638 (the `_calc_symbol_factors` function and `gen_factor_data` function) with:

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


def _calc_one_factor(
    item: tuple,
    module,
    factor_name: str,
    param_list: list,
) -> tuple:
    """Worker: compute one factor for one symbol. Returns (symbol, factor_name, {param_key: Series})."""
    symbol, df = item
    raw = module.signal_multi_params(df, param_list)
    return symbol, factor_name, raw


def gen_factor_data(handle: BmacHandle, run_time: datetime, trade_type: str):
    """Compute pre-configured factor columns for all market_dict batches and write back.

    Loads all batches into memory first, then fans out (symbol × factor) work units
    to a single ThreadPoolExecutor so batch I/O and factor computation are fully decoupled.
    Called after gen_batch_data_incremental, before gen_pivot_incremental.
    Factor columns are named {factor_name}_{param}, matching factor_calc.py convention.
    """
    factor_configs = getattr(bmac_config, 'precompute_factor_configs', {})
    if not factor_configs:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

    factor_modules_params = [
        (importlib.import_module(f'factors.{factor_name}'), factor_name, param_list)
        for factor_name, param_list in factor_configs.items()
    ]

    # 1. Load all batches into memory (sequential, fast I/O)
    t_start = time.perf_counter()
    all_batches: dict[str, dict] = {}
    batch_idx = 1
    while True:
        batch_key = f'{trade_type}_dict_batch{batch_idx}'
        if not preprocess_mgr.has_symbol(batch_key):
            break
        all_batches[batch_key] = preprocess_mgr.read_candle(batch_key)
        batch_idx += 1

    if not all_batches:
        return

    # 2. Flatten to (symbol, df, module, factor_name, param_list) work units
    work_units: list[tuple] = []
    for batch_key, market_dict in all_batches.items():
        for symbol, df in market_dict.items():
            for module, factor_name, param_list in factor_modules_params:
                work_units.append((batch_key, symbol, df, module, factor_name, param_list))

    # 3. Execute all units in a single thread pool (GIL released by cffi C extensions)
    max_workers = os.cpu_count() or 4
    results: list[tuple] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_calc_one_factor, (symbol, df), module, factor_name, param_list)
            for batch_key, symbol, df, module, factor_name, param_list in work_units
        ]
        # Preserve batch_key alongside each future result
        for (batch_key, symbol, df, module, factor_name, param_list), fut in zip(work_units, futures):
            sym_out, fn_out, raw = fut.result()
            results.append((batch_key, sym_out, fn_out, raw))

    # 4. Merge results: accumulate factor columns per (batch_key, symbol)
    factor_cols_map: dict[tuple, dict] = {}
    for batch_key, symbol, factor_name, raw in results:
        key = (batch_key, symbol)
        if key not in factor_cols_map:
            factor_cols_map[key] = {}
        for param_key, series in raw.items():
            factor_cols_map[key][f'{factor_name}_{param_key}'] = series

    # 5. Assign factor columns back into DataFrames
    for batch_key, market_dict in all_batches.items():
        for symbol, df in market_dict.items():
            key = (batch_key, symbol)
            if key in factor_cols_map:
                market_dict[symbol] = df.assign(**factor_cols_map[key])

    # 6. Write all batches back sequentially
    total_symbols = sum(len(m) for m in all_batches.values())
    for batch_key, market_dict in all_batches.items():
        preprocess_mgr.set_candle(batch_key, run_time, market_dict)

    elapsed = time.perf_counter() - t_start
    logger.ok(
        f'预计算因子 {trade_type} 完成, symbols={total_symbols}, '
        f'当前时间={now_time_str()}, 耗时 {elapsed:.1f} 秒'
    )
```

- [ ] **Step 2: Run all module5 tests to confirm no regression**

```powershell
conda run -n alpha311 python -m pytest tests/test_module5_gen_factor_data.py -v
```

Expected: All 5 tests PASS (these are the existing regression guard).

- [ ] **Step 3: Run module7 tests**

```powershell
conda run -n alpha311 python -m pytest tests/test_module7_gen_factor_parallel.py -v
```

Expected: All 4 tests PASS.

- [ ] **Step 4: Run full test suite to catch any other regressions**

```powershell
conda run -n alpha311 python -m pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: All tests pass (or only pre-existing failures, none new).

- [ ] **Step 5: Commit implementation**

```powershell
git add core/preprocess.py
git commit -m "feat: gen_factor_data - parallel refactor with _calc_one_factor and single thread pool"
```

---

## Task 4: Update `preprocess_num_batch` in config.py

**Files:**
- Modify: `config.py`

- [ ] **Step 1: Find `preprocess_num_batch` in config.py**

```powershell
conda run -n alpha311 python -c "import config; print(getattr(config, 'preprocess_num_batch', 'NOT FOUND'))"
```

- [ ] **Step 2: Update `preprocess_num_batch` from 128 to 256**

Open `config.py`, find the line:
```python
preprocess_num_batch = 128
```

Change it to:
```python
preprocess_num_batch = 256
```

- [ ] **Step 3: Verify config loads correctly**

```powershell
conda run -n alpha311 python -c "import config; print('preprocess_num_batch:', config.preprocess_num_batch)"
```

Expected: `preprocess_num_batch: 256`

- [ ] **Step 4: Run full test suite again to confirm no regression**

```powershell
conda run -n alpha311 python -m pytest tests/ -v --tb=short 2>&1 | tail -20
```

Expected: All tests pass (config change doesn't affect tests since tests use `monkeypatch`).

- [ ] **Step 5: Commit config change**

```powershell
git add config.py
git commit -m "config: preprocess_num_batch 128 -> 256 for 4-core 8GB Linux server"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|---|---|
| Add `_calc_one_factor` | Task 3 |
| Refactor `gen_factor_data`: load all batches, flatten to (symbol×factor) units, single pool, merge, write | Task 3 |
| `_calc_symbol_factors` preserved | Task 3 (kept in place) |
| `preprocess_num_batch: 128 → 256` | Task 4 |
| `tests/support/module7_harness.py` with `reference_gen_factor_data` | Task 1 |
| `tests/test_module7_gen_factor_parallel.py` with 4 tests | Task 2 |
| `test_parallel_single_batch_multi_symbol` | Task 2 |
| `test_parallel_multi_batch_multi_symbol` | Task 2 |
| `test_parallel_column_completeness` | Task 2 |
| `test_parallel_idempotent` | Task 2 |
| Real fixture data (7200 BTCUSDT rows) | Tasks 1 & 2 |
| `assert_array_equal` for exact equality | Task 2 |
| Module5 existing tests still pass | Task 3 |

### Placeholder scan

No TBDs, no incomplete sections, no vague requirements. All code blocks are complete.

### Type consistency

- `_calc_one_factor` returns `tuple` (batch-key not included — batch_key is tracked in caller via `work_units` list). ✅
- `reference_gen_factor_data` returns `dict[str, dict[str, pd.DataFrame]]` keyed by batch_key. ✅
- `factor_cols_map` keyed by `(batch_key, symbol)` tuple. ✅

### Notes

- Tests call `reference_gen_factor_data` BEFORE `gen_factor_data` to snapshot the serial baseline from the unmodified on-disk data. This is safe because `gen_factor_data` reads then writes — the reference reads the same initial state.
- `test_parallel_multi_batch_multi_symbol` uses `{SYM}_B0`, `{SYM}_B1`, `{SYM}_B2` naming to avoid ambiguity when the same base DataFrame is reused across batches.
- `_calc_symbol_factors` is retained (not removed) to avoid breaking any potential external references, per spec.
