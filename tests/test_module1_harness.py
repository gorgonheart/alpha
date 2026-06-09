from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from core.candle_manager import CandleFileManager
from core.resample_candle import update_resample_candle
from tests.support.harness_common import assert_dataframes_strict_equal, benchmark_callable
from tests.support.module1_baseline import Module1BaselineSnapshot, baseline_update_resample_candle
from tests.support.module1_harness import (
    APPEND_ONLY_CASE,
    CORRECTION_OVERLAP_CASE,
    IDENTICAL_OVERLAP_CASE,
    SMALL_GAP_CASE,
    Module1Case,
    baseline_snapshot_for_case,
    list_ready_file_names,
)

BASELINE_REAL_CASES = [
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


def assert_update_resample_candle_observable_contract(
    manager: CandleFileManager,
    *,
    case: Module1Case,
    snapshot: Module1BaselineSnapshot,
    result: tuple,
    stale_run_time,
) -> None:
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


@pytest.mark.parametrize("case", BASELINE_REAL_CASES, ids=lambda case: case.name)
def test_update_resample_candle_matches_baseline_snapshot_observable_contract(case, manager_factory):
    manager = manager_factory("current")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    result = update_resample_candle(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    snapshot = baseline_snapshot_for_case(case)
    assert_update_resample_candle_observable_contract(
        manager,
        case=case,
        snapshot=snapshot,
        result=result,
        stale_run_time=stale_run_time,
    )


def test_update_resample_candle_with_none_run_time_clears_ready_files(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("runtime-none")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))
    manager.save_ready(case.symbol, case.run_time)

    result = update_resample_candle(
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


def test_update_resample_candle_without_existing_symbol_matches_baseline_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("no-existing-symbol")

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    result = update_resample_candle(
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
    assert_update_resample_candle_observable_contract(
        manager,
        case=case,
        snapshot=snapshot,
        result=result,
        stale_run_time=stale_run_time,
    )


def test_update_resample_candle_with_empty_stored_dataframe_matches_baseline_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("empty-stored-dataframe")
    manager.save_data_file(case.symbol, case.df_old.iloc[0:0].copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    result = update_resample_candle(
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
    assert_update_resample_candle_observable_contract(
        manager,
        case=case,
        snapshot=snapshot,
        result=result,
        stale_run_time=stale_run_time,
    )


def test_update_resample_candle_with_empty_new_rows_preserves_existing_snapshot(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("empty-new-rows")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    empty_new = case.df_new.iloc[0:0].copy(deep=True)
    result = update_resample_candle(
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
    assert_update_resample_candle_observable_contract(
        manager,
        case=case,
        snapshot=snapshot,
        result=result,
        stale_run_time=stale_run_time,
    )


def test_update_resample_candle_with_empty_total_returns_none_pair(manager_factory):
    case = APPEND_ONLY_CASE
    manager = manager_factory("empty-total-output")

    stale_run_time = case.run_time - timedelta(hours=1)
    manager.save_ready(case.symbol, stale_run_time)

    empty_new = case.df_new.iloc[0:0].copy(deep=True)
    result = update_resample_candle(
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


def test_small_gap_case_fills_missing_row_with_previous_close_and_zero_volume(manager_factory):
    case = SMALL_GAP_CASE
    manager = manager_factory("small-gap")
    manager.save_data_file(case.symbol, case.df_old.copy(deep=True))

    update_resample_candle(
        manager,
        case.symbol,
        case.df_new.copy(deep=True),
        case.num_candles,
        case.run_time,
        case.resample_delta,
    )

    persisted = manager.read_candle(case.symbol)
    filled_row = persisted.loc[persisted["candle_begin_time"] == case.gap_fill_time].iloc[0]
    previous_close = persisted.loc[persisted["candle_begin_time"] < case.gap_fill_time, "close"].iloc[-1]

    assert filled_row["open"] == previous_close
    assert filled_row["high"] == previous_close
    assert filled_row["low"] == previous_close
    assert filled_row["close"] == previous_close
    assert filled_row["volume"] == 0
    assert filled_row["quote_volume"] == 0
    assert filled_row["trade_num"] == 0
    assert filled_row["taker_buy_base_asset_volume"] == 0
    assert filled_row["taker_buy_quote_asset_volume"] == 0


def test_benchmark_helper_collects_repeat_samples_and_uses_nearest_rank_p95(monkeypatch):
    call_counter = {"count": 0}

    def record_call() -> int:
        call_counter["count"] += 1
        return call_counter["count"]

    perf_samples = iter([0.0, 1.0, 10.0, 12.0, 20.0, 23.0, 30.0, 34.0, 40.0, 45.0])

    from tests.support import harness_common as common_module

    monkeypatch.setattr(common_module, "perf_counter", lambda: next(perf_samples))
    result = benchmark_callable(record_call, warmups=2, repeats=5)

    assert call_counter["count"] == 7
    assert result.samples == (1.0, 2.0, 3.0, 4.0, 5.0)
    assert result.mean_seconds == pytest.approx(3.0)
    assert result.p95_seconds == 5.0
