from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from statistics import mean
from time import perf_counter
from typing import Callable

import pandas as pd
from pandas.testing import assert_frame_equal


@dataclass(frozen=True)
class BenchmarkResult:
    samples: tuple[float, ...]
    mean_seconds: float
    p95_seconds: float


def assert_dataframes_strict_equal(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    context: str = "",
    reset_index: bool = False,
) -> None:
    if reset_index:
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
    if not left.index.equals(right.index):
        return f"index mismatch left={list(left.index)} right={list(right.index)}"
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
