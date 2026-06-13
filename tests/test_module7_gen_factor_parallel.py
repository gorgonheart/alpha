"""
Tests for the parallel gen_factor_data refactor.
Verifies that the refactored (parallel) output is bit-for-bit identical
to the serial reference implementation across multi-symbol, multi-batch scenarios.
Uses real fixture data: tests/fixtures/module6/input_btcusdt_candles.pkl (7200 rows).
"""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pytest

from core.preprocess import gen_factor_data
from tests.support.module7_harness import (
    ALL_FACTOR_CONFIGS,
    SYMBOLS_4,
    SYMBOLS_8,
    load_btcusdt_candles,
    make_multi_batch_handle,
    reference_gen_factor_data,
)


@pytest.fixture(scope="module")
def base_df():
    return load_btcusdt_candles()


def test_parallel_single_batch_multi_symbol(tmp_path, base_df, monkeypatch):
    """1 batch, 8 symbols, all 3 factors: parallel output matches serial reference."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_8]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    # Compute serial reference BEFORE calling gen_factor_data
    ref = reference_gen_factor_data(preprocess_mgr, run_ts, "spot", ALL_FACTOR_CONFIGS)

    gen_factor_data(handle, run_ts, "spot")

    for batch_key, ref_market in ref.items():
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, ref_df in ref_market.items():
            result_df = result_market[symbol]
            factor_cols = [c for c in ref_df.columns if c not in base_df.columns]
            for col in factor_cols:
                assert col in result_df.columns, f"Missing column {col!r} for {symbol}"
                np.testing.assert_array_equal(
                    result_df[col].values,
                    ref_df[col].values,
                    err_msg=f"Mismatch: {symbol} / {col}",
                )


def test_parallel_multi_batch_multi_symbol(tmp_path, base_df, monkeypatch):
    """3 batches (8/8/4 symbols), all 3 factors: parallel output matches serial reference."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batch0 = [f"{s}_B0" for s in SYMBOLS_8]
    batch1 = [f"{s}_B1" for s in SYMBOLS_8]
    batch2 = [f"{s}_B2" for s in SYMBOLS_4]
    batches = [batch0, batch1, batch2]

    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    ref = reference_gen_factor_data(preprocess_mgr, run_ts, "spot", ALL_FACTOR_CONFIGS)

    gen_factor_data(handle, run_ts, "spot")

    for batch_key, ref_market in ref.items():
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, ref_df in ref_market.items():
            result_df = result_market[symbol]
            factor_cols = [c for c in ref_df.columns if c not in base_df.columns]
            for col in factor_cols:
                assert col in result_df.columns, f"Missing {col!r} for {symbol} in {batch_key}"
                np.testing.assert_array_equal(
                    result_df[col].values,
                    ref_df[col].values,
                    err_msg=f"Mismatch: {batch_key} / {symbol} / {col}",
                )


def test_parallel_column_completeness(tmp_path, base_df, monkeypatch):
    """2 batches, 4 symbols: every expected {factor}_{param} column present, no duplicates."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_4[:2], SYMBOLS_4[2:]]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    expected_cols = [
        f"{factor_name}_{param}"
        for factor_name, param_list in ALL_FACTOR_CONFIGS.items()
        for param in param_list
    ]

    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, df in result_market.items():
            for col in expected_cols:
                assert col in df.columns, f"Missing {col!r} for {symbol} in {batch_key}"
            # No duplicate columns
            seen = [c for c in df.columns if c in expected_cols]
            assert len(seen) == len(set(seen)), (
                f"Duplicate factor columns for {symbol}: {seen}"
            )


def test_parallel_idempotent(tmp_path, base_df, monkeypatch):
    """Running gen_factor_data twice produces identical results (no drift, no duplication)."""
    run_ts = base_df["candle_begin_time"].iloc[-1].to_pydatetime()
    batches = [SYMBOLS_4[:2], SYMBOLS_4[2:]]
    handle, preprocess_mgr = make_multi_batch_handle(
        tmp_path, base_df, run_ts, batches=batches
    )

    fake_config = SimpleNamespace(precompute_factor_configs=ALL_FACTOR_CONFIGS)
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)

    gen_factor_data(handle, run_ts, "spot")

    # Snapshot first run
    first_run: dict = {}
    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        first_run[batch_key] = {
            sym: {
                col: df[col].values.copy()
                for col in df.columns
                if any(col.startswith(f) for f in ALL_FACTOR_CONFIGS)
            }
            for sym, df in result_market.items()
        }

    gen_factor_data(handle, run_ts, "spot")

    for batch_idx in range(1, 3):
        batch_key = f"spot_dict_batch{batch_idx}"
        result_market = preprocess_mgr.read_candle(batch_key)
        for symbol, df in result_market.items():
            factor_cols = [c for c in df.columns if any(c.startswith(f) for f in ALL_FACTOR_CONFIGS)]
            assert len(factor_cols) == len(set(factor_cols)), (
                f"Duplicate columns after second run: {symbol}"
            )
            for col, first_vals in first_run[batch_key][symbol].items():
                np.testing.assert_array_equal(
                    df[col].values, first_vals,
                    err_msg=f"Idempotent mismatch: {batch_key} / {symbol} / {col}",
                )
