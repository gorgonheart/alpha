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
NROWS = 7200
TRUNC_LEN = 52560


def _load_btcusdt(nrows: int) -> pd.DataFrame:
    path = DATA_DIR / "binance_spot_1h_resample" / "40m" / "BTCUSDT.pkl"
    df = pd.read_pickle(path).sort_values("candle_begin_time").reset_index(drop=True)
    return df.tail(nrows).reset_index(drop=True)


def main() -> None:
    MODULE5_DIR.mkdir(parents=True, exist_ok=True)
    MODULE6_DIR.mkdir(parents=True, exist_ok=True)
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
