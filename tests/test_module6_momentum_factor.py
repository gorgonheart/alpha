# tests/test_module6_momentum_factor.py
"""
Regression tests for the momentum C extension.

Fixture files must be generated before running:
    conda run -n alpha311 python tests/fixtures/capture.py
"""
from __future__ import annotations

import numpy as np
import pytest

import factors.动量因子210401修正 as momentum_factor
from factors.c_ext.momentum_wrapper import compute_momentum_c
from tests.support.module6_harness import (
    TRUNC_LEN,
    load_btcusdt_candles,
    load_golden,
    signal_multi_params_py,
)


def test_compute_momentum_c_output_length_matches_input():
    """Output array must have the same length as the input arrays."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    out = compute_momentum_c(close, high, low, n=168, trunc_len=TRUNC_LEN)
    assert len(out) == len(close), f"Expected length {len(close)}, got {len(out)}"


@pytest.mark.parametrize("param", [168, 336])
def test_compute_momentum_c_golden(param):
    """C extension output must exactly match the committed golden array."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    result   = compute_momentum_c(close, high, low, n=param, trunc_len=TRUNC_LEN)
    expected = load_golden(param)
    np.testing.assert_array_equal(
        result, expected,
        err_msg=f"C extension output differs from golden for param={param}. "
                "Re-run tests/fixtures/capture.py if the algorithm changed intentionally.",
    )


def test_compute_momentum_c_vs_python_reference():
    """C and Python implementations must agree within atol=1e-9 on non-NaN positions."""
    df = load_btcusdt_candles()
    close = df["close"].values.astype(np.float64)
    high  = df["high"].values.astype(np.float64)
    low   = df["low"].values.astype(np.float64)
    c_out  = compute_momentum_c(close, high, low, n=168, trunc_len=TRUNC_LEN)
    py_out = signal_multi_params_py(df, [168])["168"]
    nan_c  = np.isnan(c_out)
    nan_py = np.isnan(py_out)
    assert np.array_equal(nan_c, nan_py), (
        f"NaN positions differ: C has {nan_c.sum()} NaNs, Python has {nan_py.sum()} NaNs"
    )
    np.testing.assert_allclose(c_out[~nan_c], py_out[~nan_py], atol=1e-9)


def test_signal_multi_params_golden():
    """signal_multi_params() end-to-end output must match the committed golden array."""
    df = load_btcusdt_candles().copy()
    df["symbol"] = "BTCUSDT"
    result   = momentum_factor.signal_multi_params(df, [168])
    expected = load_golden(168)
    np.testing.assert_array_equal(
        result["168"].values, expected,
        err_msg="signal_multi_params output differs from golden for param=168.",
    )
