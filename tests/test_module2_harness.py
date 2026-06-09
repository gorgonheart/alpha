from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from core.candle_manager import CandleFileManager
from tests.support.module2_baseline import baseline_merge_candles
from tests.support.module2_harness import (
    assert_dataframes_strict_equal,
    benchmark_callable,
    build_real_case,
    list_ready_file_names,
)

BASELINE_REAL_CASES = [
    build_real_case("funding_0GUSDT", old_rows=256, new_rows=6, overlap_rows=2, num_candles=256),
    build_real_case("coin_cap_0GUSDT", old_rows=64, new_rows=5, overlap_rows=1, num_candles=64),
]


@pytest.fixture()
def manager_factory(tmp_path: Path):
    def _make(name: str) -> CandleFileManager:
        base_dir = tmp_path / name
        base_dir.mkdir(parents=True, exist_ok=True)
        return CandleFileManager(base_dir=base_dir, save_type="pickle")

    return _make


@pytest.mark.parametrize("case", BASELINE_REAL_CASES, ids=lambda case: case.name)
def test_baseline_merge_matches_current_update_candle_dataframe_result(case, manager_factory):
    current_manager = manager_factory("current")
    current_manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    current_result = current_manager.update_candle(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )
    baseline_result = baseline_merge_candles(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    assert_dataframes_strict_equal(current_result, case.expected, context=f"current-{case.name}")
    assert_dataframes_strict_equal(baseline_result, case.expected, context=f"baseline-{case.name}")


@pytest.mark.parametrize("case", BASELINE_REAL_CASES, ids=lambda case: case.name)
def test_update_candle_persists_data_and_rotates_ready_file(case, manager_factory):
    manager = manager_factory("observable")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    result = manager.update_candle(
        case.symbol,
        case.run_time,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    assert_dataframes_strict_equal(result, case.expected, context=f"result-{case.name}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        case.expected,
        context=f"persisted-{case.name}",
    )
    assert manager.check_ready(case.symbol, case.run_time) is True
    assert manager.check_ready(case.symbol, stale_run_time) is False
    assert list_ready_file_names(manager, case.symbol) == [
        Path(manager.format_ready_file_path(case.symbol, case.run_time)).name
    ]


@pytest.mark.parametrize("case", BASELINE_REAL_CASES, ids=lambda case: case.name)
def test_update_candle_with_none_run_time_clears_ready_files(case, manager_factory):
    manager = manager_factory("runtime-none")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    manager.save_ready(case.symbol, case.run_time)

    result = manager.update_candle(
        case.symbol,
        None,
        case.df_new.copy(deep=True),
        case.num_candles,
    )

    assert_dataframes_strict_equal(result, case.expected, context=f"result-{case.name}")
    assert_dataframes_strict_equal(
        manager.read_candle(case.symbol),
        case.expected,
        context=f"persisted-{case.name}",
    )
    assert list_ready_file_names(manager, case.symbol) == []


def test_benchmark_helper_collects_repeat_samples_and_uses_nearest_rank_p95():
    call_counter = {"count": 0}

    def record_call() -> int:
        call_counter["count"] += 1
        return call_counter["count"]

    perf_samples = iter([0.0, 1.0, 10.0, 12.0, 20.0, 23.0, 30.0, 34.0, 40.0, 45.0])

    from tests.support import module2_harness as harness_module

    original_perf_counter = harness_module.perf_counter
    harness_module.perf_counter = lambda: next(perf_samples)
    try:
        result = benchmark_callable(record_call, warmups=2, repeats=5)
    finally:
        harness_module.perf_counter = original_perf_counter

    assert call_counter["count"] == 7
    assert result.samples == (1.0, 2.0, 3.0, 4.0, 5.0)
    assert result.mean_seconds == pytest.approx(3.0)
    assert result.p95_seconds == 5.0
