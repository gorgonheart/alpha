"""
Lever 2 (design doc §3.2): write batches once *with* factor columns, instead of
the current two-step "write batch without factors -> read back -> compute -> rewrite".

Equivalence contract: the one-step embedded path must produce output that is
*strictly equal* (dtype + value exact) to the current two-step path
(gen_batch_data -> gen_factor_data). Grounded in real BTCUSDT data from /data.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core.preprocess import gen_batch_data, gen_batch_data_incremental, gen_factor_data
from core.utils.time import get_offset_from_runtime
from tests.support.harness_common import assert_dataframes_strict_equal
from tests.support.module3_harness import (
    RESAMPLE_INTERVAL,
    SPOT_CASE,
    load_btcusdt_spot,
    make_minimal_handle,
    build_spot_case,
)

FACTOR_NAME = "动量因子210401修正"
PARAMS = [168]


def _assert_market_dicts_equal(actual: dict, expected: dict, *, context: str = "") -> None:
    assert set(actual.keys()) == set(expected.keys()), (
        f"{context}: symbol set mismatch actual={sorted(actual)} expected={sorted(expected)}"
    )
    for symbol in sorted(expected.keys()):
        assert_dataframes_strict_equal(
            actual[symbol].reset_index(drop=True),
            expected[symbol].reset_index(drop=True),
            context=f"{context}:{symbol}",
        )


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


@pytest.fixture(params=['动量因子210401修正', '动量210401_双项异常隔离V2'])
def factor_config(monkeypatch, request):
    monkeypatch.setitem(globals(), 'FACTOR_NAME', request.param)
    fake_config = SimpleNamespace(precompute_factor_configs={FACTOR_NAME: PARAMS})
    monkeypatch.setattr("core.preprocess.bmac_config", fake_config)
    return fake_config


def test_embedded_factors_match_two_step_first_run(tmp, factor_config):
    """gen_batch_data_incremental(with_factors=True) on a first run == gen_batch_data + gen_factor_data."""
    case = build_spot_case(old_rows=600, new_rows=0)  # enough rows for param 168
    df_full = case.df_full
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    run_time = run_ts.to_pydatetime()

    # Path A: current two-step (this is "current code" output)
    handle_a, _, mgr_a = make_minimal_handle(
        tmp / "two_step", case, df_resample_for_binance=df_full.copy(deep=True), run_time=run_ts
    )
    name_a, _, _ = gen_batch_data(handle_a, run_time, case.trade_type, case.df_match.copy(deep=True),
                               [case.symbol], "batch1")
    gen_factor_data(handle_a, run_time, case.trade_type)
    expected = mgr_a.read_candle(name_a)

    # Path B: new one-step embedded. Finding 3: factors live in the RETURNED dict
    # (consumed by the Arrow publish), NOT in the persisted batch pkl (which stays raw).
    handle_b, _, mgr_b = make_minimal_handle(
        tmp / "one_step", case, df_resample_for_binance=df_full.copy(deep=True), run_time=run_ts
    )
    name_b, _, actual = gen_batch_data_incremental(handle_b, run_time, case.trade_type, case.df_match.copy(deep=True),
                                           [case.symbol], "batch1", with_factors=True)

    _assert_market_dicts_equal(actual, expected, context="first-run-embedded")
    # sanity: factor column actually present and not all-NaN
    col = f"{FACTOR_NAME}_{PARAMS[0]}"
    assert col in actual[case.symbol].columns
    vals = actual[case.symbol][col].values
    assert np.isfinite(vals[~np.isnan(vals)]).all() and (~np.isnan(vals)).any()

    # Finding 3: the persisted batch pkl must be raw (no factor columns).
    disk_b = mgr_b.read_candle(name_b)
    persisted_factor_cols = [c for c in disk_b[case.symbol].columns if FACTOR_NAME in c]
    assert persisted_factor_cols == [], (
        f"batch pkl must be raw, found factor columns: {persisted_factor_cols}"
    )


def test_embedded_factors_match_two_step_second_run(tmp, factor_config):
    """Incremental second run with embedded factors == two-step on the combined data."""
    case = build_spot_case(old_rows=600, new_rows=3)
    df_full = case.df_full
    old_rows, new_rows = case.old_rows, case.new_rows

    df_old = df_full.iloc[:old_rows].reset_index(drop=True)
    combined = df_full.iloc[:old_rows + new_rows].reset_index(drop=True)
    run_ts_old = pd.Timestamp(df_old["candle_begin_time"].iloc[-1])
    run_ts_new = pd.Timestamp(combined["candle_begin_time"].iloc[-1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()

    # Path A: current two-step on full combined data
    handle_a, _, mgr_a = make_minimal_handle(
        tmp / "two_step", case, df_resample_for_binance=combined.copy(deep=True), run_time=run_ts_new
    )
    name_a, _, _ = gen_batch_data(handle_a, run_time_new, case.trade_type, case.df_match.copy(deep=True),
                               [case.symbol], "batch1")
    gen_factor_data(handle_a, run_time_new, case.trade_type)
    expected = mgr_a.read_candle(name_a)

    # Path B: incremental first (old) then second (new) with embedded factors
    handle_b, _, mgr_b = make_minimal_handle(
        tmp / "one_step", case, df_resample_for_binance=df_old.copy(deep=True), run_time=run_ts_old
    )
    gen_batch_data_incremental(handle_b, run_time_old, case.trade_type, case.df_match.copy(deep=True),
                               [case.symbol], "batch1", with_factors=True)

    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)
    binance_mgr = handle_b.get_binance_handle(case.trade_type).resample_mgr_map[offset_str]
    binance_mgr.save_data_file(case.symbol, combined.copy(deep=True))
    binance_mgr.save_ready(case.symbol, run_time_new)

    name_b, _, actual = gen_batch_data_incremental(handle_b, run_time_new, case.trade_type, case.df_match.copy(deep=True),
                                           [case.symbol], "batch1", with_factors=True)

    _assert_market_dicts_equal(actual, expected, context="second-run-embedded")

    # Finding 3: the persisted batch pkl must stay raw across the incremental second run.
    disk_b = mgr_b.read_candle(name_b)
    persisted_factor_cols = [c for c in disk_b[case.symbol].columns if FACTOR_NAME in c]
    assert persisted_factor_cols == [], (
        f"batch pkl must be raw after incremental run, found: {persisted_factor_cols}"
    )


def test_with_factors_false_is_unchanged(tmp, factor_config):
    """Default with_factors=False must NOT add factor columns (back-compat for direct callers)."""
    case = build_spot_case(old_rows=300, new_rows=0)
    df_full = case.df_full
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    run_time = run_ts.to_pydatetime()

    handle, _, mgr = make_minimal_handle(
        tmp / "no_factors", case, df_resample_for_binance=df_full.copy(deep=True), run_time=run_ts
    )
    name, _, _ = gen_batch_data_incremental(handle, run_time, case.trade_type, case.df_match.copy(deep=True),
                                         [case.symbol], "batch1")  # with_factors defaults False
    result = mgr.read_candle(name)
    factor_cols = [c for c in result[case.symbol].columns if FACTOR_NAME in c]
    assert factor_cols == [], f"Unexpected factor columns when with_factors=False: {factor_cols}"
