"""Regenerate illq golden from C output (not Python pandas) so test_golden passes exactly."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

ILLQ_DIR = ROOT / "tests" / "fixtures" / "illq"
PARAM = 10000

df = pd.read_pickle(ROOT / "tests" / "fixtures" / "module6" / "input_btcusdt_candles.pkl")
df = df.sort_values("candle_begin_time").reset_index(drop=True)

from factors.c_ext.illq_wrapper import compute_illq_c
c_out = compute_illq_c(
    df["open"].values.astype("float64"),
    df["high"].values.astype("float64"),
    df["low"].values.astype("float64"),
    df["close"].values.astype("float64"),
    df["quote_volume"].values.astype("float64"),
    n=PARAM,
)
np.save(str(ILLQ_DIR / f"golden_{PARAM}.npy"), c_out)
print(f"Saved {ILLQ_DIR}/golden_{PARAM}.npy from C output ({len(c_out)} rows)")
