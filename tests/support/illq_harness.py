# tests/support/illq_harness.py
from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_ILLQ_FIXTURE    = _ROOT / "tests" / "fixtures" / "illq"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "ILLQStdBbi45208"
BATCH_KEY   = "spot_dict_batch1"


def load_btcusdt_candles() -> pd.DataFrame:
    return pd.read_pickle(_MODULE6_FIXTURE / "input_btcusdt_candles.pkl").reset_index(drop=True)


def load_golden(param: int) -> np.ndarray:
    return np.load(str(_ILLQ_FIXTURE / f"golden_{param}.npy"))


def signal_multi_params_py(df: pd.DataFrame, param_list: list) -> dict:
    """Pure-Python reference — never calls C, valid after factor file is updated to C."""
    zhenfu  = (df["high"] - df["low"]) / df["open"]
    ilq2    = df["quote_volume"] / zhenfu
    ilq     = df["quote_volume"] / (
        (2 * (df["high"] - df["low"]) - (df["open"] - df["close"]).abs()) / df["open"]
    )
    ilq_sum = ilq + ilq2
    ret: dict[str, np.ndarray] = {}
    for param in param_list:
        n = int(param)
        bbi = (
            df["close"].rolling(n,     min_periods=1).mean()
          + df["close"].rolling(2 * n, min_periods=1).mean()
          + df["close"].rolling(4 * n, min_periods=1).mean()
          + df["close"].rolling(6 * n, min_periods=1).mean()
        ) / 4
        bbi_bias = df["close"] / bbi
        ret[str(param)] = np.asarray(
            ilq_sum.rolling(n, min_periods=1).std() * bbi_bias, dtype=np.float64
        )
    return ret


def make_factor_handle(tmp_path: Path, df: pd.DataFrame, run_time):
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")
    mgr.save_data_file(BATCH_KEY, {"BTCUSDT": df.copy(deep=True)})
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: mgr},
            num_batch=256,
        ),
    )
    return handle, mgr
