from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
import pytest

from core.candle_manager import CandleFileManager
from tests.support.module2_baseline import baseline_merge_candles
from tests.support.module2_harness import (
    assert_dataframes_strict_equal,
    build_real_case,
    list_ready_file_names,
)

INCREMENTAL_CASES = [
    build_real_case("funding_0GUSDT", old_rows=256, new_rows=3, overlap_rows=0, num_candles=256),
    build_real_case("coin_cap_0GUSDT", old_rows=64, new_rows=3, overlap_rows=0, num_candles=64),
]

OVERLAP_CASES = [
    build_real_case("funding_0GUSDT", old_rows=256, new_rows=4, overlap_rows=1, num_candles=256),
    build_real_case("coin_cap_0GUSDT", old_rows=64, new_rows=4, overlap_rows=2, num_candles=64),
]


EMPTY_DF = pd.DataFrame(columns=["candle_begin_time"])


@pytest.fixture()
def manager_factory(tmp_path: Path):
    def _make(name: str) -> CandleFileManager:
        base_dir = tmp_path / name
        base_dir.mkdir(parents=True, exist_ok=True)
        return CandleFileManager(base_dir=base_dir, save_type="pickle")

    return _make


def assert_incremental_observable_contract(
    manager: CandleFileManager,
    *,
    symbol: str,
    run_time,
    stale_run_time,
    expected,
    result,
) -> None:
    assert_dataframes_strict_equal(result, expected, context=f"return-{symbol}")
    assert_dataframes_strict_equal(manager.read_candle(symbol), expected, context=f"persisted-{symbol}")
    assert manager.check_ready(symbol, run_time) is True
    assert manager.check_ready(symbol, stale_run_time) is False
    assert list_ready_file_names(manager, symbol) == [
        Path(manager.format_ready_file_path(symbol, run_time)).name
    ]


def track_update_candle_calls(manager: CandleFileManager):
    calls = []
    original_update_candle = manager.update_candle

    def tracking_update_candle(*args, **kwargs):
        calls.append((args, kwargs))
        return original_update_candle(*args, **kwargs)

    manager.update_candle = tracking_update_candle
    return calls


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_matches_baseline_observable_contract(case, manager_factory):
    manager = manager_factory("candidate")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert_incremental_observable_contract(
        manager,
        symbol=case.symbol,
        run_time=case.run_time,
        stale_run_time=stale_run_time,
        expected=expected,
        result=result,
    )


@pytest.mark.parametrize("case", OVERLAP_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_falls_back_to_baseline_contract_for_overlap(case, manager_factory):
    manager = manager_factory("candidate")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)
    calls = track_update_candle_calls(manager)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert len(calls) == 1
    assert_incremental_observable_contract(
        manager,
        symbol=case.symbol,
        run_time=case.run_time,
        stale_run_time=stale_run_time,
        expected=expected,
        result=result,
    )


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_with_none_run_time_matches_update_candle_ready_file_behavior(case, manager_factory):
    manager = manager_factory("candidate-none-runtime")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    manager.save_ready(case.symbol, case.run_time)

    result = manager.update_candle_incremental(
        case.symbol,
        None,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert_dataframes_strict_equal(result, expected, context=f"return-{case.symbol}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        expected,
        context=f"persisted-{case.symbol}",
    )
    assert list_ready_file_names(manager, case.symbol) == []


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_uses_append_only_path_for_strictly_new_rows(case, manager_factory):
    manager = manager_factory("candidate-append-only")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    def fail_if_baseline_path_used(*args, **kwargs):
        raise AssertionError("append-only incremental case should not call update_candle")

    manager.update_candle = fail_if_baseline_path_used

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert_dataframes_strict_equal(result, expected, context=f"append-only-{case.symbol}")


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_matches_baseline_when_old_data_is_unsorted(case, manager_factory):
    manager = manager_factory("candidate-unsorted-old")
    df_old_unsorted = pd.concat(
        [case.df_old.iloc[-3:], case.df_old.iloc[:-3]],
        ignore_index=True,
    )
    manager.save_data_file(case.symbol, df_old_unsorted)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        df_old_unsorted.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert_dataframes_strict_equal(result, expected, context=f"unsorted-old-{case.symbol}")


@pytest.mark.parametrize("case", INCREMENTAL_CASES, ids=lambda case: case.name)
def test_update_candle_incremental_matches_baseline_when_old_data_already_contains_duplicate_times(
    case,
    manager_factory,
):
    manager = manager_factory("candidate-duplicate-old-times")
    df_old_with_duplicate_time = pd.concat(
        [case.df_old.copy(deep=True), case.df_old.iloc[[-1]].copy(deep=True)],
        ignore_index=True,
    )
    manager.save_data_file(case.symbol, df_old_with_duplicate_time)

    def fail_if_baseline_path_used(*args, **kwargs):
        raise AssertionError("duplicate old times case should stay on incremental fast path")

    manager.update_candle = fail_if_baseline_path_used

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        df_old_with_duplicate_time.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    assert not result["candle_begin_time"].duplicated().any()
    assert_dataframes_strict_equal(result, expected, context=f"duplicate-old-times-{case.symbol}")


def test_update_candle_incremental_falls_back_to_baseline_when_symbol_does_not_exist(manager_factory):
    manager = manager_factory("candidate-no-existing-symbol")
    case = INCREMENTAL_CASES[0]
    calls = track_update_candle_calls(manager)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(None, case.df_new.copy(deep=True), case.num_candles)
    assert len(calls) == 1
    assert_dataframes_strict_equal(result, expected, context=f"no-existing-symbol-{case.symbol}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        expected,
        context=f"persisted-no-existing-symbol-{case.symbol}",
    )


def test_update_candle_incremental_falls_back_to_baseline_when_old_df_is_empty(manager_factory):
    manager = manager_factory("candidate-empty-old")
    case = INCREMENTAL_CASES[0]
    manager.save_data_file(case.symbol, EMPTY_DF.copy(deep=True))
    calls = track_update_candle_calls(manager)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(EMPTY_DF.copy(deep=True), case.df_new.copy(deep=True), case.num_candles)
    assert len(calls) == 1
    assert_dataframes_strict_equal(result, expected, context=f"empty-old-{case.symbol}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        expected,
        context=f"persisted-empty-old-{case.symbol}",
    )


def test_update_candle_incremental_falls_back_to_baseline_when_new_df_is_empty(manager_factory):
    manager = manager_factory("candidate-empty-new")
    case = INCREMENTAL_CASES[0]
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    calls = track_update_candle_calls(manager)

    result = manager.update_candle_incremental(
        case.symbol,
        case.run_time,
        EMPTY_DF.copy(deep=True),
        case.num_candles,
    )

    expected = baseline_merge_candles(
        case.df_old.copy(deep=True),
        EMPTY_DF.copy(deep=True),
        case.num_candles,
    )
    assert len(calls) == 1
    assert_dataframes_strict_equal(result, expected, context=f"empty-new-{case.symbol}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        expected,
        context=f"persisted-empty-new-{case.symbol}",
    )
