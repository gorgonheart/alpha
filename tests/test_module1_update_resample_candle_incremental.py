from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from core.candle_manager import CandleFileManager
from core import resample_candle as resample_module
from tests.support.harness_common import assert_dataframes_strict_equal, benchmark_callable
from tests.support.module1_baseline import baseline_update_resample_candle
from tests.support.module1_harness import (
    APPEND_ONLY_CASE,
    CORRECTION_OVERLAP_CASE,
    IDENTICAL_OVERLAP_CASE,
    SMALL_GAP_CASE,
    Module1Case,
    baseline_snapshot_for_case,
    list_ready_file_names,
)

FUTURE_SEAM_CASES = [
    APPEND_ONLY_CASE,
    IDENTICAL_OVERLAP_CASE,
    CORRECTION_OVERLAP_CASE,
    SMALL_GAP_CASE,
]


@pytest.fixture()
def manager_factory(tmp_path: Path):
    def _make(name: str) -> CandleFileManager:
        base_dir = tmp_path / name
        base_dir.mkdir(parents=True, exist_ok=True)
        return CandleFileManager(base_dir=base_dir, save_type="pickle")

    return _make


def get_incremental_candidate():
    candidate = getattr(resample_module, "update_resample_candle_incremental", None)
    assert candidate is not None, (
        "Future seam missing: add core.resample_candle.update_resample_candle_incremental "
        "before enabling module-1 incremental behavior"
    )
    return candidate


def assert_incremental_observable_contract(
    manager: CandleFileManager,
    *,
    case: Module1Case,
    result: tuple,
    stale_run_time,
) -> None:
    snapshot = baseline_snapshot_for_case(case)
    first_begin_time, last_begin_time = result
    assert first_begin_time == snapshot.first_begin_time
    assert last_begin_time == snapshot.last_begin_time
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


@pytest.mark.parametrize("case", FUTURE_SEAM_CASES, ids=lambda case: case.name)
def test_update_resample_candle_incremental_matches_baseline_snapshot_contract(case, manager_factory):
    manager = manager_factory("candidate")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    assert_incremental_observable_contract(
        manager,
        case=case,
        result=result,
        stale_run_time=stale_run_time,
    )


def test_update_resample_candle_incremental_with_none_run_time_matches_ready_file_behavior(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("candidate-none-runtime")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    manager.save_ready(case.symbol, case.run_time)

    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        None,
        case.resample_delta,
    )

    snapshot = baseline_snapshot_for_case(case)
    assert result == (snapshot.first_begin_time, snapshot.last_begin_time)
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-none-runtime-{case.name}",
    )
    assert list_ready_file_names(manager, case.symbol) == []


def test_update_resample_candle_incremental_without_existing_symbol_matches_baseline_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("candidate-no-existing-symbol")

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    snapshot = baseline_update_resample_candle(
        None,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.resample_delta,
    )
    assert result == (snapshot.first_begin_time, snapshot.last_begin_time)
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-missing-symbol-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


def test_update_resample_candle_incremental_with_empty_stored_dataframe_matches_baseline_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("candidate-empty-stored-dataframe")
    manager.save_data_file(case.symbol, case.df_old.iloc[0:0].copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    snapshot = baseline_update_resample_candle(
        None,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.resample_delta,
    )
    assert result == (snapshot.first_begin_time, snapshot.last_begin_time)
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-empty-stored-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


def test_update_resample_candle_incremental_with_empty_new_rows_preserves_existing_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("candidate-empty-new-rows")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    empty_new = case.df_new.iloc[0:0].copy(deep=True)
    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        empty_new,
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    snapshot = baseline_update_resample_candle(
        case.df_old.copy(deep=True),
        empty_new,
        case.num_candles,
        case.resample_delta,
    )
    assert result == (snapshot.first_begin_time, snapshot.last_begin_time)
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-empty-new-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


def test_update_resample_candle_incremental_with_empty_total_returns_none_pair(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("candidate-empty-total-output")

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    empty_new = case.df_new.iloc[0:0].copy(deep=True)
    candidate = get_incremental_candidate()
    result = candidate(
        manager,
        case.symbol,
        empty_new,
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    snapshot = baseline_update_resample_candle(
        None,
        empty_new,
        case.num_candles,
        case.resample_delta,
    )
    assert result == (snapshot.first_begin_time, snapshot.last_begin_time) == (None, None)
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        snapshot.dataframe,
        context=f"persisted-empty-total-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


@pytest.mark.parametrize("case", [APPEND_ONLY_CASE], ids=lambda case: case.name)
def test_update_resample_candle_incremental_append_only_benchmark_matches_baseline_and_is_not_slower(
    case,
    manager_factory,
):
    candidate = get_incremental_candidate()

    def run_baseline_once():
        manager = manager_factory("benchmark-baseline")
        manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
        return resample_module.update_resample_candle(
            manager,
            case.symbol,
            case.df_new.copy(deep=True),
            case.num_candles,
            case.run_time,
            case.resample_delta,
        )

    def run_incremental_once():
        manager = manager_factory("benchmark-incremental")
        manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
        return candidate(
            manager,
            case.symbol,
            case.df_new.copy(deep=True),
            case.num_candles,
            case.run_time,
            case.resample_delta,
        )

    baseline_manager = manager_factory("benchmark-baseline-verify")
    baseline_manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    baseline_result = resample_module.update_resample_candle(
        baseline_manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    incremental_manager = manager_factory("benchmark-incremental-verify")
    incremental_manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    incremental_result = candidate(
        incremental_manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    assert incremental_result == baseline_result
    assert_dataframes_strict_equal(
        incremental_manager.read_candle(case.symbol),
        baseline_manager.read_candle(case.symbol),
        context=f"benchmark-persisted-{case.name}",
    )

    baseline_benchmark = benchmark_callable(run_baseline_once, warmups=1, repeats=5)
    incremental_benchmark = benchmark_callable(run_incremental_once, warmups=1, repeats=5)
    assert incremental_benchmark.mean_seconds <= baseline_benchmark.mean_seconds, (
        f"incremental mean {incremental_benchmark.mean_seconds:.6f}s exceeded "
        f"baseline mean {baseline_benchmark.mean_seconds:.6f}s"
    )
