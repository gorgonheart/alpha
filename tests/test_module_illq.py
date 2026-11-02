# tests/test_module_illq.py
"""Regression tests for ILLQStdBbi45208 C extension at param=10000."""
from __future__ import annotations
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from factors.c_ext.illq_wrapper import compute_illq_c  # ImportError until Task 5
from tests.support.illq_harness import (
    FACTOR_NAME, BATCH_KEY,
    load_btcusdt_candles, load_golden, signal_multi_params_py, make_factor_handle,
)
from core.preprocess import gen_factor_data

PARAM = 10000


@pytest.fixture()
def tmp(tmp_path):
    return tmp_path


@pytest.fixture()
def df():
    return load_btcusdt_candles()


def _c_out(df: pd.DataFrame) -> np.ndarray:
    return compute_illq_c(
        df["open"].values.astype("float64"),
        df["high"].values.astype("float64"),
        df["low"].values.astype("float64"),
        df["close"].values.astype("float64"),
        df["quote_volume"].values.astype("float64"),
        n=PARAM,
    )


def test_output_length(df):
    assert len(_c_out(df)) == len(df)


def test_legacy_golden_remains_numerically_compatible_with_python(df):
    # Keep the old file unchanged. It captured the old C summation order, whose
    # bits are not an oracle for the compensated variance correction.
    expected=signal_multi_params_py(df,[PARAM])[str(PARAM)]
    np.testing.assert_allclose(load_golden(PARAM),expected,rtol=1e-12,atol=1e-10,equal_nan=True)


def test_c_vs_python(df):
    c_arr  = _c_out(df)
    py_arr = signal_multi_params_py(df, [PARAM])[str(PARAM)]
    assert np.array_equal(np.isnan(c_arr), np.isnan(py_arr)), \
        "NaN positions differ between C and Python"
    valid = ~np.isnan(py_arr)
    np.testing.assert_allclose(
        c_arr[valid], py_arr[valid], rtol=1e-12, atol=1e-10,
        err_msg="C and independent Python oracle differ",
    )


def test_signal_multi_params_python_oracle(tmp, df, monkeypatch):
    run_ts = pd.Timestamp(df["candle_begin_time"].iloc[-1]).to_pydatetime()
    handle, mgr = make_factor_handle(tmp, df, run_ts)
    monkeypatch.setattr(
        "core.preprocess.bmac_config",
        SimpleNamespace(precompute_factor_configs={FACTOR_NAME: [PARAM]}),
    )
    gen_factor_data(handle, run_ts, "spot")
    col = f"{FACTOR_NAME}_{PARAM}"
    df_result = mgr.read_candle(BATCH_KEY)["BTCUSDT"]
    assert col in df_result.columns, f"Column {col!r} not found"
    np.testing.assert_allclose(
        df_result[col].values, signal_multi_params_py(df,[PARAM])[str(PARAM)],
        rtol=1e-12,atol=1e-10,equal_nan=True,
        err_msg=f"gen_factor_data column {col!r} differs from Python oracle.",
    )
