from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
RESAMPLE_SAMPLE_PATHS = {
    "binance_btcusdt_40m": DATA_DIR / "binance_spot_1h_resample" / "40m" / "BTCUSDT.pkl",
    "data_api_0gusdt_40m": DATA_DIR / "data_api_spot_1h_resample" / "40m" / "0GUSDT.pkl",
}


def load_resample_sample(sample_name: str) -> pd.DataFrame:
    path = RESAMPLE_SAMPLE_PATHS[sample_name]
    df = pd.read_pickle(path)
    if "candle_begin_time" not in df.columns:
        raise AssertionError(f"Sample {sample_name} is missing candle_begin_time")
    return df.sort_values("candle_begin_time").reset_index(drop=True)
