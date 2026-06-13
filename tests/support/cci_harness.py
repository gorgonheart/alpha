# tests/support/cci_harness.py
from __future__ import annotations
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_CCI_FIXTURE     = _ROOT / "tests" / "fixtures" / "cci"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "CciV3"
BATCH_KEY   = "spot_dict_batch1"
WINDOW      = 15000


def load_btcusdt_candles() -> pd.DataFrame:
    return pd.read_pickle(_MODULE6_FIXTURE / "input_btcusdt_candles.pkl").reset_index(drop=True)


def load_golden(param: int) -> np.ndarray:
    return np.load(str(_CCI_FIXTURE / f"golden_{param}.npy"))


def _ewm_fixed_py(arr: pd.Series, window: int, span: int, min_periods: int) -> pd.Series:
    """Python ewm_fixed — exact copy of original CciV3.ewm_fixed, no C dependency."""
    alpha = 2 / (span + 1)
    y = arr.ewm(alpha=alpha, adjust=False).mean()
    residual = (1 - alpha) ** window * y + (alpha - 1) * (1 - alpha) ** (window - 1) * arr.shift(-1)
    residual = residual.shift(window).fillna(0)
    result = y - residual
    result.iloc[:min_periods - 1] = np.nan
    return result


def signal_multi_params_py(df: pd.DataFrame, param_list: list) -> dict:
    """Pure-Python reference — never calls C, valid after factor file is updated to C."""
    ret: dict[str, np.ndarray] = {}
    for param in param_list:
        n   = int(param)
        oma = _ewm_fixed_py(df["open"],  WINDOW, span=n, min_periods=2)
        hma = _ewm_fixed_py(df["high"],  WINDOW, span=n, min_periods=2)
        lma = _ewm_fixed_py(df["low"],   WINDOW, span=n, min_periods=2)
        cma = _ewm_fixed_py(df["close"], WINDOW, span=n, min_periods=2)
        tp  = (oma + hma + lma + cma) / 4
        ma  = _ewm_fixed_py(tp,               WINDOW, span=n, min_periods=2)
        md  = _ewm_fixed_py((cma - ma).abs(), WINDOW, span=n, min_periods=2)
        ret[str(param)] = np.asarray((tp - ma) / md, dtype=np.float64)
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
