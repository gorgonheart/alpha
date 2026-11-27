"""
Compare 动量因子210401修正.py (new, C ext) vs 动量因子210401修正旧.py (old, cpp_shim).

Stubs core.utils.cpp_shim with pandas equivalents so the old module is importable.

Run:
    conda run -n alpha311 python tests/compare_factor_versions.py
"""
from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# ── stub core.utils.cpp_shim before importing the old factor ──────────────────
_shim = types.ModuleType("core.utils.cpp_shim")

def _ewm(values, span):
    return pd.Series(values).ewm(span=span, adjust=False).mean().values

def _rolling_min(values, window, min_periods):
    return pd.Series(values).rolling(window, min_periods=min_periods).min().values

def _rolling_max(values, window, min_periods):
    return pd.Series(values).rolling(window, min_periods=min_periods).max().values

def _rolling_mean(values, window, min_periods):
    return pd.Series(values).rolling(window, min_periods=min_periods).mean().values

def _rolling_std(values, window, min_periods):
    return pd.Series(values).rolling(window, min_periods=min_periods).std().values

_shim.ewm = _ewm
_shim.rolling_min = _rolling_min
_shim.rolling_max = _rolling_max
_shim.rolling_mean = _rolling_mean
_shim.rolling_std = _rolling_std

# also stub BacktestConfig used in get_lookback
_bt_mod = types.ModuleType("core.model.backtest_config")
class _BacktestConfig:
    @staticmethod
    def get_kline_num():
        return 52560
_bt_mod.BacktestConfig = _BacktestConfig
sys.modules.setdefault("core.model.backtest_config", _bt_mod)
sys.modules["core.utils.cpp_shim"] = _shim

# patch parent packages into sys.modules so import works
for _pkg in ("core", "core.utils", "core.model"):
    if _pkg not in sys.modules:
        sys.modules[_pkg] = types.ModuleType(_pkg)

import importlib.util

def _load_factor(py_file: Path):
    spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

old_mod  = _load_factor(ROOT / "factors" / "动量因子210401修正旧.py")
new_mod  = _load_factor(ROOT / "factors" / "动量因子210401修正.py")
pure_mod = _load_factor(ROOT / "factors" / "动量因子210401修正纯python.py")

# ── load fixture data ─────────────────────────────────────────────────────────
FIXTURE = ROOT / "tests" / "fixtures" / "module6" / "input_btcusdt_candles.pkl"
df = pd.read_pickle(FIXTURE).sort_values("candle_begin_time").reset_index(drop=True)
print(f"Loaded fixture: {len(df)} rows")

# ── comparison helper ─────────────────────────────────────────────────────────
def compare(label: str, old_arr, new_arr):
    old_arr = np.asarray(old_arr, dtype=np.float64)
    new_arr = np.asarray(new_arr, dtype=np.float64)
    nan_match = np.array_equal(np.isnan(old_arr), np.isnan(new_arr))
    valid = ~np.isnan(old_arr) & ~np.isnan(new_arr)
    if nan_match and valid.sum() > 0:
        val_match = bool(np.allclose(old_arr[valid], new_arr[valid], atol=1e-9))
    else:
        val_match = False
    status = "MATCH" if (nan_match and val_match) else "DIFFER"
    old_nan = np.isnan(old_arr).sum()
    new_nan = np.isnan(new_arr).sum()
    print(f"  {label}: nan_match={nan_match} (old_nan={old_nan} new_nan={new_nan})  "
          f"val_match={val_match}  [{status}]")
    if not nan_match:
        diff_pos = np.where(np.isnan(old_arr) != np.isnan(new_arr))[0]
        print(f"    NaN mismatch at indices: {diff_pos[:5]}")
    if nan_match and not val_match:
        bad = np.where(valid & (np.abs(old_arr - new_arr) > 1e-9))[0]
        print(f"    value mismatch at indices: {bad[:5]}")
        print(f"    old={old_arr[bad[:3]]}  new={new_arr[bad[:3]]}")
    return status == "MATCH"

VERSIONS = [("old", old_mod), ("new", new_mod), ("pure", pure_mod)]

# ── test 1: single param ──────────────────────────────────────────────────────
print("\n=== Single param [168] ===")
singles = {label: mod.signal_multi_params(df.copy(deep=True), [168]) for label, mod in VERSIONS}

all_pass = True
for a, b in [("old", "new"), ("old", "pure"), ("new", "pure")]:
    val_a = np.asarray(singles[a]["168"], dtype=np.float64)
    val_b = np.asarray(singles[b]["168"], dtype=np.float64)
    all_pass &= compare(f"{a} vs {b} param=168", val_a, val_b)

# ── test 2: multi param ───────────────────────────────────────────────────────
print("\n=== Multi param [168, 336] ===")
multis = {label: mod.signal_multi_params(df.copy(deep=True), [168, 336]) for label, mod in VERSIONS}

for p in [168, 336]:
    for a, b in [("old", "new"), ("old", "pure"), ("new", "pure")]:
        val_a = np.asarray(multis[a][str(p)], dtype=np.float64)
        val_b = np.asarray(multis[b][str(p)], dtype=np.float64)
        all_pass &= compare(f"{a} vs {b} param={p}", val_a, val_b)

# ── test 3: single-vs-multi consistency within each version ──────────────────
print("\n=== Internal consistency: single vs multi within each version ===")
for label, mod in VERSIONS:
    s_arr = np.asarray(singles[label]["168"], dtype=np.float64)
    m_arr = np.asarray(multis[label]["168"], dtype=np.float64)
    nan_match = np.array_equal(np.isnan(s_arr), np.isnan(m_arr))
    valid = ~np.isnan(s_arr) & ~np.isnan(m_arr)
    val_match = bool(np.allclose(s_arr[valid], m_arr[valid], atol=1e-9)) if valid.sum() > 0 else False
    status = "CONSISTENT" if (nan_match and val_match) else "INCONSISTENT"
    print(f"  {label}: single[168] vs multi[168]  [{status}]")
    if not (nan_match and val_match):
        all_pass = False

print()
if all_pass:
    print("RESULT: implementations are CONSISTENT.")
else:
    print("RESULT: implementations DIFFER.")
