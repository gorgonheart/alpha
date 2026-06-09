from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core import preprocess as preprocess_module
from core.preprocess import gen_batch_data, gen_pivot, make_market_pivot, PIVOT_COLUMNS
from core.utils.time import get_offset_from_runtime
from tests.support.harness_common import assert_dataframes_strict_equal, benchmark_callable
from tests.support.module3_harness import (
    RESAMPLE_INTERVAL,
    SPOT_CASE,
    Module3Case,
    make_minimal_handle,
)


def get_pivot_candidate():
    candidate = getattr(preprocess_module, "gen_pivot_incremental", None)
    assert candidate is not None, (
        "Future seam missing: add core.preprocess.gen_pivot_incremental "
        "before enabling module-4 incremental behavior"
    )
    return candidate


def run_full_batch_and_pivot(case: Module3Case, tmp_root: Path, rows: int, run_ts: pd.Timestamp):
    df_slice = case.df_full.iloc[:rows].reset_index(drop=True)
    run_time = run_ts.to_pydatetime()
    handle, binance_mgr, preprocess_mgr = make_minimal_handle(
        tmp_root, case, df_resample_for_binance=df_slice, run_time=run_ts
    )
    full_batch_name, year_pivot_name = gen_batch_data(
        handle, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names = {}
    for year, name in year_pivot_name.items():
        year_pivot_names[year] = [name]
    gen_pivot(handle, run_time, case.trade_type, year_pivot_names)
    return handle, preprocess_mgr, full_batch_name, year_pivot_names


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


def test_gen_pivot_produces_expected_pivot_keys(tmp):
    case = SPOT_CASE
    df_full = case.df_full
    rows = case.old_rows + case.new_rows
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[rows - 1])
    handle, preprocess_mgr, full_batch_name, _ = run_full_batch_and_pivot(case, tmp, rows, run_ts)
    offset_str = get_offset_from_runtime(run_ts.to_pydatetime(), RESAMPLE_INTERVAL)
    mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]
    pivot_name = f"market_pivot_{case.trade_type}_2025"
    assert mgr.has_symbol(pivot_name) or mgr.has_symbol(f"market_pivot_{case.trade_type}_2026"), (
        "gen_pivot should produce at least one pivot file"
    )


def test_gen_pivot_incremental_first_run_matches_full_run(tmp):
    """First call with no old pivot must produce identical pivot to gen_pivot."""
    case = SPOT_CASE
    df_full = case.df_full
    rows = case.old_rows + case.new_rows
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[rows - 1])
    run_time = run_ts.to_pydatetime()
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)

    handle_full, binance_mgr_full, preprocess_mgr_full = make_minimal_handle(
        tmp / "full", case, df_resample_for_binance=df_full.iloc[:rows].reset_index(drop=True), run_time=run_ts
    )
    full_batch_name, year_pivot_name = gen_batch_data(
        handle_full, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names = {y: [n] for y, n in year_pivot_name.items()}
    gen_pivot(handle_full, run_time, case.trade_type, year_pivot_names)

    candidate = get_pivot_candidate()
    handle_inc, binance_mgr_inc, preprocess_mgr_inc = make_minimal_handle(
        tmp / "incremental", case, df_resample_for_binance=df_full.iloc[:rows].reset_index(drop=True), run_time=run_ts
    )
    inc_batch_name, year_pivot_name_inc = gen_batch_data(
        handle_inc, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names_inc = {y: [n] for y, n in year_pivot_name_inc.items()}
    candidate(handle_inc, run_time, case.trade_type, year_pivot_names_inc)

    for key in ("market_pivot_spot_2025", "market_pivot_spot_2026"):
        if preprocess_mgr_full.has_symbol(key):
            full_pivot = preprocess_mgr_full.read_candle(key)
            assert preprocess_mgr_inc.has_symbol(key), f"incremental missing {key}"
            inc_pivot = preprocess_mgr_inc.read_candle(key)
            assert set(full_pivot.keys()) == set(inc_pivot.keys()), f"{key} key mismatch"
            for pivot_key in full_pivot:
                assert_dataframes_strict_equal(
                    inc_pivot[pivot_key].reset_index(),
                    full_pivot[pivot_key].reset_index(),
                    context=f"first-run-{key}-{pivot_key}",
                )


def test_gen_pivot_incremental_second_run_appends_new_row_without_full_rebuild(tmp):
    """Second call with old pivot + 1 new candle must produce same pivot as full rebuild."""
    case = SPOT_CASE
    df_full = case.df_full
    old_rows = case.old_rows
    new_rows = case.new_rows
    total_rows = old_rows + new_rows

    run_ts_old = pd.Timestamp(df_full["candle_begin_time"].iloc[old_rows - 1])
    run_ts_new = pd.Timestamp(df_full["candle_begin_time"].iloc[total_rows - 1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()
    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)

    handle_full, _, preprocess_mgr_full = make_minimal_handle(
        tmp / "full", case,
        df_resample_for_binance=df_full.iloc[:total_rows].reset_index(drop=True),
        run_time=run_ts_new,
    )
    full_batch_name, year_pivot_name = gen_batch_data(
        handle_full, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names = {y: [n] for y, n in year_pivot_name.items()}
    gen_pivot(handle_full, run_time_new, case.trade_type, year_pivot_names)

    candidate = get_pivot_candidate()

    handle_inc, _, preprocess_mgr_inc = make_minimal_handle(
        tmp / "incremental", case,
        df_resample_for_binance=df_full.iloc[:old_rows].reset_index(drop=True),
        run_time=run_ts_old,
    )
    inc_batch_name_old, year_pivot_name_old = gen_batch_data(
        handle_inc, run_time_old, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names_old = {y: [n] for y, n in year_pivot_name_old.items()}
    candidate(handle_inc, run_time_old, case.trade_type, year_pivot_names_old)

    binance_mgr_inc = handle_inc.get_binance_handle(case.trade_type).resample_mgr_map[offset_str]
    binance_mgr_inc.save_data_file(case.symbol, df_full.iloc[:total_rows].reset_index(drop=True).copy(deep=True))
    binance_mgr_inc.save_ready(case.symbol, run_time_new)

    from core.preprocess import gen_batch_data_incremental
    inc_batch_name_new, year_pivot_name_new = gen_batch_data_incremental(
        handle_inc, run_time_new, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1"
    )
    year_pivot_names_new = {y: [n] for y, n in year_pivot_name_new.items()}
    candidate(handle_inc, run_time_new, case.trade_type, year_pivot_names_new)


def test_gen_pivot_incremental_second_run_benchmark_matches_full_run_and_is_not_slower(tmp):
    """Performance contract for gen_pivot_incremental.

    Benchmarks the algorithmic core (make_market_pivot + pd.concat) rather than
    the full pipeline, so I/O overhead does not obscure the speedup signal.

    Full path  : make_market_pivot(N rows) for each year
    Incremental: make_market_pivot(n_new rows) + pd.concat per key, for each year
    with N = 5 000 old + 3 new (single year) to give a clear ~1 000x computational
    advantage to the incremental path.
    """
    from tests.support.module3_harness import build_spot_case
    from core.preprocess import gen_batch_data_incremental

    bench_case = build_spot_case(old_rows=5000, new_rows=3)
    candidate = get_pivot_candidate()

    df_full = bench_case.df_full
    old_rows = bench_case.old_rows
    new_rows = bench_case.new_rows
    total_rows = old_rows + new_rows

    run_ts_old = pd.Timestamp(df_full["candle_begin_time"].iloc[old_rows - 1])
    run_ts_new = pd.Timestamp(df_full["candle_begin_time"].iloc[total_rows - 1])
    run_time_old = run_ts_old.to_pydatetime()
    run_time_new = run_ts_new.to_pydatetime()
    offset_str = get_offset_from_runtime(run_time_new, RESAMPLE_INTERVAL)

    # ── Build full market dicts per year (all 5003 rows) ──────────────────────
    handle_full, _, pmgr_full = make_minimal_handle(
        tmp / "bm_full", bench_case,
        df_resample_for_binance=df_full.iloc[:total_rows].reset_index(drop=True),
        run_time=run_ts_new,
    )
    _, ypn_full = gen_batch_data(
        handle_full, run_time_new, bench_case.trade_type,
        bench_case.df_match.copy(deep=True), [bench_case.symbol], "bm",
    )
    full_market_dicts: dict[int, dict] = {}
    for year, bn in ypn_full.items():
        full_market_dicts[year] = pmgr_full.read_candle(bn)
        pmgr_full.remove_symbol(bn)

    # ── Build old pivots + incremental delta market dicts ─────────────────────
    handle_inc, _, pmgr_inc = make_minimal_handle(
        tmp / "bm_inc", bench_case,
        df_resample_for_binance=df_full.iloc[:old_rows].reset_index(drop=True),
        run_time=run_ts_old,
    )
    _, ypn_old = gen_batch_data(
        handle_inc, run_time_old, bench_case.trade_type,
        bench_case.df_match.copy(deep=True), [bench_case.symbol], "bm",
    )
    # Build 5000-row pivot state using the candidate (same code path)
    candidate(handle_inc, run_time_old, bench_case.trade_type, {y: [n] for y, n in ypn_old.items()})
    old_pivots: dict[int, dict] = {}
    for year in ypn_old:
        pk = f'market_pivot_{bench_case.trade_type}_{year}'
        if pmgr_inc.has_symbol(pk):
            old_pivots[year] = pmgr_inc.read_candle(pk)

    # Build incremental batch (3-row delta) and extract new-only rows per year
    bm_binance_mgr = handle_inc.get_binance_handle(bench_case.trade_type).resample_mgr_map[offset_str]
    bm_binance_mgr.save_data_file(
        bench_case.symbol,
        df_full.iloc[:total_rows].reset_index(drop=True).copy(deep=True),
    )
    bm_binance_mgr.save_ready(bench_case.symbol, run_time_new)
    _, ypn_new = gen_batch_data_incremental(
        handle_inc, run_time_new, bench_case.trade_type,
        bench_case.df_match.copy(deep=True), [bench_case.symbol], "bm",
    )
    new_rows_dicts: dict[int, dict] = {}
    for year, bn in ypn_new.items():
        full_year_md = pmgr_inc.read_candle(bn)
        pmgr_inc.remove_symbol(bn)
        if year in old_pivots:
            last_ts = old_pivots[year]['open'].index.max()
            new_rows_dicts[year] = {
                sym: df.loc[df['candle_begin_time'] > last_ts]
                for sym, df in full_year_md.items()
                if not df.loc[df['candle_begin_time'] > last_ts].empty
            }
        else:
            new_rows_dicts[year] = full_year_md

    # ── Pure computation benchmark (no I/O inside the measured loop) ──────────
    def run_full_once():
        for year, md in full_market_dicts.items():
            make_market_pivot(md, bench_case.trade_type)

    def run_inc_once():
        for year, nr_dict in new_rows_dicts.items():
            if not nr_dict:
                continue
            mini = make_market_pivot(nr_dict, bench_case.trade_type)
            if year in old_pivots:
                old_piv = old_pivots[year]
                for pk in old_piv:
                    if pk in mini:
                        pd.concat([old_piv[pk], mini[pk]])

    baseline_benchmark = benchmark_callable(run_full_once, warmups=2, repeats=10)
    incremental_benchmark = benchmark_callable(run_inc_once, warmups=2, repeats=10)
    assert incremental_benchmark.mean_seconds <= baseline_benchmark.mean_seconds, (
        f"incremental mean {incremental_benchmark.mean_seconds:.6f}s exceeded "
        f"baseline mean {baseline_benchmark.mean_seconds:.6f}s"
    )
