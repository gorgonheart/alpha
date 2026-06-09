from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import ceil
from pathlib import Path
from statistics import mean
from time import perf_counter
from typing import Callable

import pandas as pd
from pandas.testing import assert_frame_equal

from tests.support.module2_baseline import baseline_merge_candles

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
SAMPLE_PATHS = {
    "funding_0GUSDT": DATA_DIR / "funding" / "0GUSDT.pkl",
    "coin_cap_0GUSDT": DATA_DIR / "coin_cap" / "0GUSDT.pkl",
}


@dataclass(frozen=True)
class Module2Case:
    name: str
    symbol: str
    df_old: pd.DataFrame
    df_new: pd.DataFrame
    num_candles: int | None
    expected: pd.DataFrame
    run_time: datetime


@dataclass(frozen=True)
class BenchmarkResult:
    samples: tuple[float, ...]
    mean_seconds: float
    p95_seconds: float


def load_real_sample(sample_name: str) -> pd.DataFrame:
    path = SAMPLE_PATHS[sample_name]
    df = pd.read_pickle(path)
    if "candle_begin_time" not in df.columns:
        raise AssertionError(f"Sample {sample_name} is missing candle_begin_time")
    return df.sort_values("candle_begin_time").reset_index(drop=True)


def build_real_case(
    sample_name: str,
    *,
    old_rows: int,
    new_rows: int,
    overlap_rows: int = 0,
    num_candles: int | None = None,
) -> Module2Case:
    df_full = load_real_sample(sample_name)
    if old_rows <= 0:
        raise AssertionError("old_rows must be positive")
    if new_rows <= 0:
        raise AssertionError("new_rows must be positive")
    if overlap_rows < 0:
        raise AssertionError("overlap_rows cannot be negative")
    if overlap_rows >= old_rows:
        raise AssertionError("overlap_rows must be smaller than old_rows")

    new_start = old_rows - overlap_rows
    new_end = new_start + new_rows
    if new_end > len(df_full):
        raise AssertionError(
            f"Sample {sample_name} needs at least {new_end} rows, only has {len(df_full)}"
        )

    df_old = df_full.iloc[:old_rows].copy()
    df_new = df_full.iloc[new_start:new_end].copy()
    run_time = df_new["candle_begin_time"].iloc[-1].to_pydatetime()
    expected = baseline_merge_candles(df_old, df_new, num_candles).reset_index(drop=True)

    return Module2Case(
        name=f"{sample_name}-old{old_rows}-new{new_rows}-overlap{overlap_rows}-tail{num_candles}",
        symbol=load_symbol(df_full, sample_name),
        df_old=df_old.reset_index(drop=True),
        df_new=df_new.reset_index(drop=True),
        num_candles=num_candles,
        expected=expected,
        run_time=run_time,
    )


def load_symbol(df: pd.DataFrame, sample_name: str) -> str:
    if "symbol" in df.columns and not df["symbol"].empty:
        return str(df["symbol"].iloc[0])
    return sample_name.split("_", 1)[1]


def list_ready_file_names(manager, symbol: str) -> list[str]:
    return sorted(path.name for path in Path(manager.base_dir).glob(f"{symbol}_*.ready"))


def assert_dataframes_strict_equal(left: pd.DataFrame, right: pd.DataFrame, *, context: str = "") -> None:
    left = left.reset_index(drop=True)
    right = right.reset_index(drop=True)

    try:
        assert_frame_equal(
            left,
            right,
            check_dtype=True,
            check_exact=True,
            check_like=False,
        )
    except AssertionError as exc:
        prefix = f"{context}: " if context else ""
        diff = first_dataframe_difference(left, right)
        raise AssertionError(f"{prefix}{diff}\n{exc}") from exc


def first_dataframe_difference(left: pd.DataFrame, right: pd.DataFrame) -> str:
    if list(left.columns) != list(right.columns):
        return f"column mismatch left={list(left.columns)} right={list(right.columns)}"
    if list(left.dtypes.astype(str)) != list(right.dtypes.astype(str)):
        return (
            "dtype mismatch "
            f"left={list(left.dtypes.astype(str))} right={list(right.dtypes.astype(str))}"
        )
    if len(left) != len(right):
        return f"row count mismatch left={len(left)} right={len(right)}"

    for row_idx in range(len(left)):
        for column in left.columns:
            left_value = left.iloc[row_idx][column]
            right_value = right.iloc[row_idx][column]
            if pd.isna(left_value) and pd.isna(right_value):
                continue
            if left_value != right_value:
                return (
                    f"first diff at row={row_idx}, column={column}, "
                    f"left={left_value!r}, right={right_value!r}"
                )

    return "dataframes differ but no first diff could be isolated"


def benchmark_callable(
    func: Callable[[], object], *, warmups: int = 1, repeats: int = 10
) -> BenchmarkResult:
    if warmups < 0:
        raise AssertionError("warmups cannot be negative")
    if repeats <= 0:
        raise AssertionError("repeats must be positive")

    for _ in range(warmups):
        func()

    samples: list[float] = []
    for _ in range(repeats):
        start = perf_counter()
        func()
        samples.append(perf_counter() - start)

    ordered = sorted(samples)
    p95_index = ceil(len(ordered) * 0.95) - 1
    return BenchmarkResult(
        samples=tuple(samples),
        mean_seconds=mean(samples),
        p95_seconds=ordered[p95_index],
    )
