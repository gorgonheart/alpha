# -*- coding: utf-8 -*-
"""Real-data validation: confirm the new write-once embedded-factor path reproduces
the factor columns currently persisted in the production /data batch files.

This is NOT a unit test; it reads the live production pickles under
data/preprocess_1h_resample/{offset}/ and recomputes factors via the same
apply_precomputed_factors helper, comparing against what's already on disk.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config as bmac_config
from core.preprocess import _load_factor_modules_params, apply_precomputed_factors

OFFSET = "10m"
PRE_DIR = ROOT / "data" / "preprocess_1h_resample" / OFFSET

factor_modules_params = _load_factor_modules_params()
configured_cols = []
for _m, fname, params in factor_modules_params:
    for p in params:
        configured_cols.append(f"{fname}_{p}")
print(f"configured factor columns ({len(configured_cols)}): {configured_cols}")

total_checked = 0
total_symbols = 0
mismatches = []

for trade_type in ("spot", "swap"):
    batch_idx = 1
    while True:
        bpath = PRE_DIR / f"{trade_type}_dict_batch{batch_idx}.pkl"
        if not bpath.exists():
            break
        market_dict = pd.read_pickle(bpath)
        for symbol, df in market_dict.items():
            present = [c for c in configured_cols if c in df.columns]
            if not present:
                continue
            total_symbols += 1
            # strip existing factor cols, recompute via the new shared helper
            df_stripped = df.drop(columns=present)
            recomputed = apply_precomputed_factors(df_stripped, factor_modules_params)
            for col in present:
                old = df[col].to_numpy()
                new = recomputed[col].to_numpy()
                total_checked += 1
                # exact equality treating NaN==NaN
                if not np.array_equal(old, new, equal_nan=True):
                    # allow tiny float tolerance, but report
                    close = np.allclose(old, new, rtol=0, atol=0, equal_nan=True)
                    if not close:
                        diff = np.nanmax(np.abs(old - new))
                        mismatches.append((trade_type, symbol, col, diff))
        batch_idx += 1

print(f"symbols with factors checked: {total_symbols}")
print(f"(symbol,factor) comparisons:  {total_checked}")
if mismatches:
    print(f"MISMATCHES: {len(mismatches)} (showing up to 10)")
    for m in mismatches[:10]:
        print("  ", m)
    sys.exit(1)
else:
    print("ALL MATCH: recomputed factors == production /data factor columns (exact, NaN-aware)")
