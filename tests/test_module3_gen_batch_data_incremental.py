from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core import preprocess as preprocess_module
from core.preprocess import gen_batch_data
from core.utils.time import get_offset_from_runtime
from tests.support.harness_common import assert_dataframes_strict_equal, benchmark_callable
from tests.support.module3_harness import (
    RESAMPLE_INTERVAL,
    SPOT_CASE,
    SWAP_CASE,
    Module3Case,
    make_minimal_handle,
)

INCREMENTAL_CASES = [SPOT_CASE, SWAP_CASE]


def get_incremental_candidate():
    candidate = getattr(preprocess_module, "gen_batch_data_incremental", None)
    assert candidate is not None, (
        "Future seam missing: add core.preprocess.gen_batch_data_incremental "
        "before enabling module-3 incremental behavior"
    )
    return candidate


def assert_market_dicts_equal(actual: dict, expected: dict, *, context: str = "") -> None:
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


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda c: c.name)
def test_gen_batch_data_incremental_first_run_matches_full_run(case, tmp):
    """First call with no cached market_dict must match gen_batch_data exactly."""
    df_full = case.df_full
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    run_time = run_ts.to_pydatetime()

    handle_full, _, preprocess_mgr_full = make_minimal_handle(
        tmp / "full",
        case,
        df_resample_for_binance=df_full.copy(deep=True),
        run_time=run_ts,
    )
    full_result = gen_batch_data(
        handle_full, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    full_batch_name, _, _ = full_result
    expected_market_dict = preprocess_mgr_full.read_candle(full_batch_name)

    candidate = get_incremental_candidate()
    handle_inc, _, preprocess_mgr_inc = make_minimal_handle(
        tmp / "incremental",
        case,
        df_resample_for_binance=df_full.copy(deep=True),
        run_time=run_ts,
    )
    inc_result = candidate(
        handle_inc, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    inc_batch_name, _, _ = inc_result

    actual_market_dict = preprocess_mgr_inc.read_candle(inc_batch_name)
    assert_market_dicts_equal(actual_market_dict, expected_market_dict, context=f"first-run-{case.name}")


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda c: c.name)
def test_gen_batch_data_incremental_second_run_matches_full_run(case, tmp):
    """Second call with cached market_dict for old rows must produce same result as full run on combined data."""
    df_full = case.df_full
    old_rows = case.old_rows
    new_rows = case.new_rows
    df_old_batch = df_full.iloc[:old_rows].reset_index(drop=True)
    df_new_batch = df_full.iloc[old_rows:old_rows + new_rows].reset_index(drop=True)

    run_ts_old = pd.Timestamp(df_old_batch["candle_begin_time"].iloc[-1])
    run_ts_new = pd.Timestamp(df_new_batch["candle_begin_time"].iloc[-1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()

    handle_full, _, preprocess_mgr_full = make_minimal_handle(
        tmp / "full",
        case,
        df_resample_for_binance=df_full.iloc[:old_rows + new_rows].reset_index(drop=True).copy(deep=True),
        run_time=run_ts_new,
    )
    full_result = gen_batch_data(
        handle_full, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    full_batch_name, _, _ = full_result
    expected_market_dict = preprocess_mgr_full.read_candle(full_batch_name)

    candidate = get_incremental_candidate()

    handle_first, _, preprocess_mgr_inc = make_minimal_handle(
        tmp / "incremental",
        case,
        df_resample_for_binance=df_old_batch.copy(deep=True),
        run_time=run_ts_old,
    )
    candidate(
        handle_first, run_time_old, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )

    from core.utils.time import get_offset_from_runtime
    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)
    binance_mgr_new = handle_first.get_binance_handle(case.trade_type).resample_mgr_map[offset_str]
    binance_mgr_new.save_data_file(
        case.symbol,
        df_full.iloc[:old_rows + new_rows].reset_index(drop=True).copy(deep=True),
    )
    binance_mgr_new.save_ready(case.symbol, run_time_new)

    inc_result = candidate(
        handle_first, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    inc_batch_name, _, _ = inc_result
    actual_market_dict = preprocess_mgr_inc.read_candle(inc_batch_name)
    assert_market_dicts_equal(actual_market_dict, expected_market_dict, context=f"second-run-{case.name}")


def test_gen_batch_data_incremental_inherits_first_candle_time_from_old_state(tmp):
    """first_candle_time in incremental output must equal the value from the first-run (old) state."""
    case = SPOT_CASE
    df_full = case.df_full
    old_rows = case.old_rows
    new_rows = case.new_rows

    df_old_batch = df_full.iloc[:old_rows].reset_index(drop=True)
    df_new_batch = df_full.iloc[old_rows:old_rows + new_rows].reset_index(drop=True)
    run_ts_old = pd.Timestamp(df_old_batch["candle_begin_time"].iloc[-1])
    run_ts_new = pd.Timestamp(df_new_batch["candle_begin_time"].iloc[-1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()

    candidate = get_incremental_candidate()

    handle_first, _, preprocess_mgr = make_minimal_handle(
        tmp / "first-candle-time",
        case,
        df_resample_for_binance=df_old_batch.copy(deep=True),
        run_time=run_ts_old,
    )
    first_result = candidate(
        handle_first, run_time_old, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    first_batch_name, _, _ = first_result
    old_market_dict = preprocess_mgr.read_candle(first_batch_name)
    expected_first_candle_time = old_market_dict[case.symbol]["first_candle_time"].iloc[0]

    from core.utils.time import get_offset_from_runtime
    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)
    binance_mgr = handle_first.get_binance_handle(case.trade_type).resample_mgr_map[offset_str]
    binance_mgr.save_data_file(
        case.symbol,
        df_full.iloc[:old_rows + new_rows].reset_index(drop=True).copy(deep=True),
    )
    binance_mgr.save_ready(case.symbol, run_time_new)

    second_result = candidate(
        handle_first, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    second_batch_name, _, _ = second_result
    new_market_dict = preprocess_mgr.read_candle(second_batch_name)
    actual_first_candle_time = new_market_dict[case.symbol]["first_candle_time"].iloc[0]
    assert actual_first_candle_time == expected_first_candle_time, (
        f"first_candle_time must be inherited from old state: expected {expected_first_candle_time}, "
        f"got {actual_first_candle_time}"
    )


def test_gen_batch_data_incremental_last_candle_time_is_table_max(tmp):
    """last_candle_time must equal the whole-table max candle_begin_time after concat."""
    case = SPOT_CASE
    df_full = case.df_full
    old_rows = case.old_rows
    new_rows = case.new_rows

    df_old_batch = df_full.iloc[:old_rows].reset_index(drop=True)
    run_ts_old = pd.Timestamp(df_old_batch["candle_begin_time"].iloc[-1])
    run_ts_new = pd.Timestamp(df_full["candle_begin_time"].iloc[old_rows + new_rows - 1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()

    candidate = get_incremental_candidate()

    handle_first, _, preprocess_mgr = make_minimal_handle(
        tmp / "last-candle-time",
        case,
        df_resample_for_binance=df_old_batch.copy(deep=True),
        run_time=run_ts_old,
    )
    candidate(
        handle_first, run_time_old, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )

    from core.utils.time import get_offset_from_runtime
    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)
    binance_mgr = handle_first.get_binance_handle(case.trade_type).resample_mgr_map[offset_str]
    combined = df_full.iloc[:old_rows + new_rows].reset_index(drop=True)
    binance_mgr.save_data_file(case.symbol, combined.copy(deep=True))
    binance_mgr.save_ready(case.symbol, run_time_new)

    second_result = candidate(
        handle_first, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    second_batch_name, _, _ = second_result
    new_market_dict = preprocess_mgr.read_candle(second_batch_name)
    df_sym = new_market_dict[case.symbol]
    expected_last = df_sym["candle_begin_time"].max()
    assert (df_sym["last_candle_time"] == expected_last).all(), (
        f"last_candle_time must equal table max: expected {expected_last}"
    )
