# tests/support/module5_harness.py
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_MODULE5_FIXTURE = _ROOT / "tests" / "fixtures" / "module5"
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

FACTOR_NAME = "动量因子210401修正"
BATCH_KEY = "spot_dict_batch1"

_MISSING_MSG = (
    "Fixture files not found. Run from project root:\n"
    "  conda run -n alpha311 python tests/fixtures/capture.py"
)


def load_btcusdt_candles() -> pd.DataFrame:
    """Returns the 7200-row BTCUSDT snapshot (same file used by module6 tests)."""
    path = _MODULE6_FIXTURE / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return pd.read_pickle(path).reset_index(drop=True)


def load_golden_signal(param: int) -> np.ndarray:
    """Returns the golden signal_multi_params output for BTCUSDT at the given param."""
    path = _MODULE5_FIXTURE / f"golden_signal_btcusdt_{param}.npy"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return np.load(path)


def make_factor_handle(
    tmp_path: Path,
    df: pd.DataFrame,
    run_time,
    *,
    symbol: str = "BTCUSDT",
) -> tuple[SimpleNamespace, CandleFileManager]:
    """Build a minimal handle with a single-batch market_dict ready for gen_factor_data."""
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    df_with_sym = df.copy(deep=True)
    df_with_sym["symbol"] = symbol
    preprocess_mgr.save_data_file(BATCH_KEY, {symbol: df_with_sym})

    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: preprocess_mgr},
            num_batch=256,
        ),
    )
    return handle, preprocess_mgr
