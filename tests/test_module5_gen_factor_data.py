"""
Tests for gen_factor_data() in core/preprocess.py.
Uses BTCUSDT spot real data and a SimpleNamespace handle (same pattern as module3 tests).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tests.support.module5_harness import (
    BATCH_KEY as _M5_BATCH_KEY,
    FACTOR_NAME as _M5_FACTOR_NAME,
    load_btcusdt_candles as _load_m5_candles,
    load_golden_signal,
    make_factor_handle,
)

from core.candle_manager import CandleFileManager
from core.preprocess import gen_factor_data
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL, load_btcusdt_spot

FACTOR_NAME = '动量因子210401修正'
PARAMS = [168]
BATCH_KEY = 'spot_dict_batch1'


def make_handle_with_market_dict(tmp_path: Path, df_full: pd.DataFrame, run_time):
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    df = df_full.copy(deep=True)
    df['symbol'] = 'BTCUSDT'
    market_dict = {'BTCUSDT': df}
    preprocess_mgr.save_data_file(BATCH_KEY, market_dict)

    preprocess_handle = SimpleNamespace(
        preprocess_mgr_map={offset_str: preprocess_mgr},
        num_batch=256,
    )
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=preprocess_handle,
    )
    return handle, preprocess_mgr


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


def test_gen_factor_data_adds_factor_columns(tmp, monkeypatch):
    """gen_factor_data should write factor columns into each symbol's DataFrame."""
    df_full = load_btcusdt_spot()
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1]).to_pydatetime()

    handle, preprocess_mgr = make_handle_with_market_dict(tmp, df_full, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={FACTOR_NAME: PARAMS})
    monkeypatch.setattr('core.preprocess.bmac_config', fake_config)

    gen_factor_data(handle, run_ts, 'spot')

    result = preprocess_mgr.read_candle(BATCH_KEY)
    df_result = result['BTCUSDT']

    expected_col = f'{FACTOR_NAME}_{PARAMS[0]}'
    assert expected_col in df_result.columns, (
        f"Expected column '{expected_col}', got: {df_result.columns.tolist()}"
    )
    factor_vals = df_result[expected_col].values
    non_nan = factor_vals[~np.isnan(factor_vals)]
    assert len(non_nan) > 0, "All factor values are NaN"
    assert np.all(np.isfinite(non_nan)), "Factor values contain Inf"


def test_gen_factor_data_no_config_is_noop(tmp, monkeypatch):
    """Empty precompute_factor_configs must leave market_dict unchanged."""
    df_full = load_btcusdt_spot()
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1]).to_pydatetime()

    handle, preprocess_mgr = make_handle_with_market_dict(tmp, df_full, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={})
    monkeypatch.setattr('core.preprocess.bmac_config', fake_config)

    gen_factor_data(handle, run_ts, 'spot')

    result = preprocess_mgr.read_candle(BATCH_KEY)
    df_result = result['BTCUSDT']
    factor_cols = [c for c in df_result.columns if FACTOR_NAME in c]
    assert factor_cols == [], f"Unexpected factor columns: {factor_cols}"


@pytest.mark.parametrize("params", [[168], [168, 336]])
def test_gen_factor_data_golden_values(tmp, monkeypatch, params):
    """Factor column values written by gen_factor_data must match golden signal_multi_params output."""
    df = _load_m5_candles()
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, preprocess_mgr = make_factor_handle(tmp / f"golden_{'_'.join(map(str, params))}", df, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={_M5_FACTOR_NAME: params})
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    result_dict = preprocess_mgr.read_candle(_M5_BATCH_KEY)
    df_result = result_dict["BTCUSDT"]

    for param in params:
        col = f"{_M5_FACTOR_NAME}_{param}"
        assert col in df_result.columns, f"Missing column {col!r}"
        expected = load_golden_signal(param)
        np.testing.assert_array_equal(
            df_result[col].values, expected,
            err_msg=f"gen_factor_data column {col!r} differs from golden. "
                    "Re-run tests/fixtures/capture.py if algorithm changed intentionally.",
        )


def test_gen_factor_data_idempotent(tmp, monkeypatch):
    """Calling gen_factor_data twice must produce identical output with no duplicate columns."""
    df = _load_m5_candles()
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, preprocess_mgr = make_factor_handle(tmp / "idempotent", df, run_ts)

    fake_config = SimpleNamespace(precompute_factor_configs={_M5_FACTOR_NAME: [168]})
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")
    first_values = preprocess_mgr.read_candle(_M5_BATCH_KEY)["BTCUSDT"][f"{_M5_FACTOR_NAME}_168"].values.copy()

    gen_factor_data(handle, run_ts, "spot")
    df_second = preprocess_mgr.read_candle(_M5_BATCH_KEY)["BTCUSDT"]

    factor_cols = [c for c in df_second.columns if c == f"{_M5_FACTOR_NAME}_168"]
    assert len(factor_cols) == 1, f"Duplicate factor columns after second call: {factor_cols}"
    np.testing.assert_array_equal(df_second[f"{_M5_FACTOR_NAME}_168"].values, first_values)
