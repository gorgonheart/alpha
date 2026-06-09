from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from core.preprocess import gen_batch_data
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import (
    RESAMPLE_INTERVAL,
    SPOT_CASE,
    SWAP_CASE,
    Module3Case,
    make_minimal_handle,
)

EXPECTED_ADDED_COLS = {
    "symbol",
    "funding_fee",
    "avg_price_1m",
    "avg_price_5m",
    "是否交易",
    "first_candle_time",
    "last_candle_time",
    "is_spot",
    "symbol_spot",
    "symbol_swap",
}


def run_gen_batch_data_full(case: Module3Case, tmp_path: Path):
    df_full = case.df_full
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    run_time = run_ts.to_pydatetime()
    handle, binance_mgr, preprocess_mgr = make_minimal_handle(
        tmp_path,
        case,
        df_resample_for_binance=df_full.copy(deep=True),
        run_time=run_ts,
    )
    result = gen_batch_data(handle, run_time, case.trade_type, case.df_match.copy(deep=True), [case.symbol], "batch1")
    return result, preprocess_mgr, run_ts


def assert_market_dict_columns(market_dict: dict, trade_type: str):
    for symbol, df in market_dict.items():
        missing = EXPECTED_ADDED_COLS - set(df.columns)
        assert not missing, f"{symbol} missing columns {missing}"
        assert "candle_begin_time" in df.columns, f"{symbol} missing candle_begin_time"
        assert not df["first_candle_time"].isna().any(), f"{symbol} first_candle_time has nulls"
        assert not df["last_candle_time"].isna().any(), f"{symbol} last_candle_time has nulls"
        assert (df["first_candle_time"] == df["candle_begin_time"].min()).all(), (
            f"{symbol} first_candle_time mismatch"
        )
        assert (df["last_candle_time"] == df["candle_begin_time"].max()).all(), (
            f"{symbol} last_candle_time mismatch"
        )
        if trade_type == "spot":
            assert (df["is_spot"] == 1).all(), f"{symbol} is_spot should be 1 for spot"
        else:
            assert (df["is_spot"] == 0).all(), f"{symbol} is_spot should be 0 for swap"


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


def test_gen_batch_data_spot_returns_expected_structure(tmp):
    case = SPOT_CASE
    result, preprocess_mgr, run_ts = run_gen_batch_data_full(case, tmp / "full")
    full_batch_name, year_pivot_name = result
    assert full_batch_name.startswith(case.trade_type)
    market_dict = preprocess_mgr.read_candle(full_batch_name)
    assert case.symbol in market_dict
    assert_market_dict_columns(market_dict, case.trade_type)
    df_sym = market_dict[case.symbol]
    assert (df_sym["funding_fee"] == 0).all(), "spot funding_fee must be zero"


def test_gen_batch_data_swap_populates_funding_fee(tmp):
    case = SWAP_CASE
    result, preprocess_mgr, run_ts = run_gen_batch_data_full(case, tmp / "full_swap")
    full_batch_name, year_pivot_name = result
    market_dict = preprocess_mgr.read_candle(full_batch_name)
    assert case.symbol in market_dict
    assert_market_dict_columns(market_dict, case.trade_type)


def test_gen_batch_data_spot_first_last_candle_time_invariants(tmp):
    case = SPOT_CASE
    result, preprocess_mgr, run_ts = run_gen_batch_data_full(case, tmp / "full_invariants")
    full_batch_name, _ = result
    market_dict = preprocess_mgr.read_candle(full_batch_name)
    df_sym = market_dict[case.symbol]
    assert df_sym["first_candle_time"].nunique() == 1
    assert df_sym["last_candle_time"].nunique() == 1
    assert df_sym["first_candle_time"].iloc[0] == df_sym["candle_begin_time"].min()
    assert df_sym["last_candle_time"].iloc[0] == df_sym["candle_begin_time"].max()


def test_gen_batch_data_spot_year_pivot_contains_pivot_columns(tmp):
    case = SPOT_CASE
    from core.preprocess import PIVOT_COLUMNS
    result, preprocess_mgr, run_ts = run_gen_batch_data_full(case, tmp / "full_pivot")
    _, year_pivot_name = result
    for year, pivot_batch_name in year_pivot_name.items():
        pivot_dict = preprocess_mgr.read_candle(pivot_batch_name)
        for sym, df_piv in pivot_dict.items():
            assert list(df_piv.columns) == PIVOT_COLUMNS, f"year {year} symbol {sym} pivot columns mismatch"
