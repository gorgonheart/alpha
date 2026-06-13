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
