from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd

from tests.support.module1_baseline import (
    Module1BaselineSnapshot,
    baseline_update_resample_candle,
)
from tests.support.real_sample_harness import load_resample_sample


@dataclass(frozen=True)
class Module1Case:
    name: str
    symbol: str
    df_old: pd.DataFrame
    df_new: pd.DataFrame
    num_candles: int
    run_time: pd.Timestamp
    resample_delta: timedelta
    gap_fill_time: pd.Timestamp | None = None


APPEND_ONLY_CASE = None
IDENTICAL_OVERLAP_CASE = None
CORRECTION_OVERLAP_CASE = None
SMALL_GAP_CASE = None


def load_real_sample(sample_name: str) -> pd.DataFrame:
    return load_resample_sample(sample_name)


def build_real_case(
    sample_name: str,
    *,
    symbol: str,
    old_start: int,
    old_rows: int,
    new_start: int,
    new_rows: int,
    num_candles: int,
) -> Module1Case:
    df_full = load_real_sample(sample_name)
    df_old = df_full.iloc[old_start:old_start + old_rows].copy(deep=True).reset_index(drop=True)
    df_new = df_full.iloc[new_start:new_start + new_rows].copy(deep=True).reset_index(drop=True)
    run_time = pd.Timestamp(df_new["candle_begin_time"].iloc[-1])
    return Module1Case(
        name=(
            f"{sample_name}-old{old_start}:{old_start + old_rows}-"
            f"new{new_start}:{new_start + new_rows}-tail{num_candles}"
        ),
        symbol=symbol,
        df_old=df_old,
        df_new=df_new,
        num_candles=num_candles,
        run_time=run_time,
        resample_delta=infer_resample_delta(df_full),
    )


def build_correction_case() -> Module1Case:
    base_case = build_real_case(
        "binance_btcusdt_40m",
        symbol="BTCUSDT",
        old_start=250,
        old_rows=8,
        new_start=255,
        new_rows=4,
        num_candles=8,
    )
    corrected = base_case.df_new.copy(deep=True)
    corrected.loc[0, "high"] = corrected.loc[0, "high"] + 12.34
    return Module1Case(
        name="binance_btcusdt-corrected-overlap-high",
        symbol=base_case.symbol,
        df_old=base_case.df_old,
        df_new=corrected,
        num_candles=base_case.num_candles,
        run_time=base_case.run_time,
        resample_delta=base_case.resample_delta,
    )


def build_small_gap_case() -> Module1Case:
    base_case = build_real_case(
        "data_api_0gusdt_40m",
        symbol="0GUSDT",
        old_start=80,
        old_rows=6,
        new_start=86,
        new_rows=2,
        num_candles=8,
    )
    derived_new = base_case.df_new.iloc[[1]].copy(deep=True).reset_index(drop=True)
    gap_fill_time = pd.Timestamp(base_case.df_new.iloc[0]["candle_begin_time"])
    return Module1Case(
        name="data_api_0gusdt-small-gap-derived-from-real-sample",
        symbol=base_case.symbol,
        df_old=base_case.df_old,
        df_new=derived_new,
        num_candles=base_case.num_candles,
        run_time=pd.Timestamp(derived_new["candle_begin_time"].iloc[-1]),
        resample_delta=base_case.resample_delta,
        gap_fill_time=gap_fill_time,
    )


def infer_resample_delta(df: pd.DataFrame) -> timedelta:
    diffs = df["candle_begin_time"].sort_values().diff().dropna()
    if diffs.empty:
        raise AssertionError("Cannot infer resample delta from fewer than two rows")
    return diffs.mode().iloc[0].to_pytimedelta()


def baseline_snapshot_for_case(case: Module1Case) -> Module1BaselineSnapshot:
    return baseline_update_resample_candle(
        case.df_old.copy(deep=True),
        case.df_new.copy(deep=True),
        case.num_candles,
        case.resample_delta,
    )


def list_ready_file_names(manager, symbol: str) -> list[str]:
    return sorted(path.name for path in Path(manager.base_dir).glob(f"{symbol}_*.ready"))


def assert_case_invariants(case: Module1Case) -> None:
    assert not case.df_new.empty, f"{case.name} must include incremental rows"
    assert list(case.df_old.columns) == list(case.df_new.columns), f"{case.name} column mismatch"
    old_times = case.df_old["candle_begin_time"]
    new_times = case.df_new["candle_begin_time"]
    overlap_times = sorted(set(old_times).intersection(set(new_times)))

    if case is APPEND_ONLY_CASE:
        assert old_times.max() < new_times.min(), f"{case.name} must be append-only"
        assert not overlap_times, f"{case.name} must not overlap"
        return

    if case is IDENTICAL_OVERLAP_CASE:
        assert overlap_times, f"{case.name} must include overlap rows"
        old_overlap = case.df_old[case.df_old["candle_begin_time"].isin(overlap_times)].reset_index(drop=True)
        new_overlap = case.df_new[case.df_new["candle_begin_time"].isin(overlap_times)].reset_index(drop=True)
        pd.testing.assert_frame_equal(old_overlap, new_overlap, check_dtype=True, check_exact=True)
        return

    if case is CORRECTION_OVERLAP_CASE:
        assert overlap_times, f"{case.name} must include overlap rows"
        old_overlap = case.df_old[case.df_old["candle_begin_time"].isin(overlap_times)].reset_index(drop=True)
        new_overlap = case.df_new[case.df_new["candle_begin_time"].isin(overlap_times)].reset_index(drop=True)
        diff_mask = old_overlap.ne(new_overlap)
        changed_columns = {column for column in diff_mask.columns if bool(diff_mask[column].any())}
        assert changed_columns == {"high"}, f"{case.name} must only change high, got {changed_columns}"
        first_overlap_time = overlap_times[0]
        old_value = old_overlap.loc[old_overlap["candle_begin_time"] == first_overlap_time, "high"].iloc[0]
        new_value = new_overlap.loc[new_overlap["candle_begin_time"] == first_overlap_time, "high"].iloc[0]
        assert new_value > old_value, f"{case.name} corrected high must be greater than original"
        return

    if case is SMALL_GAP_CASE:
        assert case.gap_fill_time is not None, f"{case.name} must define gap_fill_time"
        assert old_times.max() < case.gap_fill_time < new_times.min(), f"{case.name} gap must be bounded"
        assert new_times.min() - old_times.max() == case.resample_delta * 2, (
            f"{case.name} must leave exactly one missing candle"
        )
        assert case.gap_fill_time == old_times.max() + case.resample_delta, (
            f"{case.name} gap_fill_time must be the single missing candle"
        )
        return

    raise AssertionError(f"Unhandled case invariants for {case.name}")


APPEND_ONLY_CASE = build_real_case(
    "binance_btcusdt_40m",
    symbol="BTCUSDT",
    old_start=250,
    old_rows=6,
    new_start=256,
    new_rows=3,
    num_candles=8,
)
IDENTICAL_OVERLAP_CASE = build_real_case(
    "data_api_0gusdt_40m",
    symbol="0GUSDT",
    old_start=80,
    old_rows=6,
    new_start=84,
    new_rows=3,
    num_candles=8,
)
CORRECTION_OVERLAP_CASE = build_correction_case()
SMALL_GAP_CASE = build_small_gap_case()

for _case in [APPEND_ONLY_CASE, IDENTICAL_OVERLAP_CASE, CORRECTION_OVERLAP_CASE, SMALL_GAP_CASE]:
    assert_case_invariants(_case)
