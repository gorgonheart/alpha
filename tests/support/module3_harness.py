from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"

BTCUSDT_SPOT_PATH = DATA_DIR / "binance_spot_1h_resample" / "40m" / "BTCUSDT.pkl"
BTCUSDT_SWAP_PATH = DATA_DIR / "binance_swap_1h_resample" / "40m" / "BTCUSDT.pkl"
BTCUSDT_FUNDING_PATH = DATA_DIR / "funding" / "BTCUSDT.pkl"

RESAMPLE_INTERVAL = "1h"


def load_btcusdt_spot() -> pd.DataFrame:
    return pd.read_pickle(BTCUSDT_SPOT_PATH).sort_values("candle_begin_time").reset_index(drop=True)


def load_btcusdt_swap() -> pd.DataFrame:
    return pd.read_pickle(BTCUSDT_SWAP_PATH).sort_values("candle_begin_time").reset_index(drop=True)


def load_btcusdt_funding() -> pd.DataFrame:
    return pd.read_pickle(BTCUSDT_FUNDING_PATH).sort_values("candle_begin_time").reset_index(drop=True)


@dataclass(frozen=True)
class Module3Case:
    name: str
    trade_type: str
    symbol: str
    df_full: pd.DataFrame
    df_funding: Optional[pd.DataFrame]
    old_rows: int
    new_rows: int
    df_match: pd.DataFrame


def build_match_row(symbol: str, df_resample: pd.DataFrame) -> dict:
    first = df_resample["candle_begin_time"].min()
    last = df_resample["candle_begin_time"].max()
    return {"spot": symbol, "swap": symbol, "first_begin_time": first, "last_begin_time": last}


def make_minimal_handle(
    tmp_path: Path,
    case: Module3Case,
    *,
    df_resample_for_binance: pd.DataFrame,
    run_time: pd.Timestamp,
) -> tuple[SimpleNamespace, CandleFileManager, CandleFileManager]:
    """
    Build a minimal handle-like namespace that provides what gen_batch_data needs.
    Returns (handle, binance_mgr, preprocess_mgr).
    """
    offset_str = get_offset_from_runtime(run_time.to_pydatetime(), RESAMPLE_INTERVAL)

    binance_dir = tmp_path / "binance" / offset_str
    binance_dir.mkdir(parents=True, exist_ok=True)
    binance_mgr = CandleFileManager(base_dir=binance_dir, save_type="pickle")
    binance_mgr.save_data_file(case.symbol, df_resample_for_binance.copy(deep=True))
    binance_mgr.save_ready(case.symbol, run_time.to_pydatetime())

    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    funding_mgr = None
    if case.df_funding is not None:
        funding_dir = tmp_path / "funding"
        funding_dir.mkdir(parents=True, exist_ok=True)
        funding_mgr = CandleFileManager(base_dir=funding_dir, save_type="pickle")
        funding_mgr.save_data_file(case.symbol, case.df_funding.copy(deep=True))

    binance_handle = SimpleNamespace(resample_mgr_map={offset_str: binance_mgr})
    preprocess_handle = SimpleNamespace(
        preprocess_mgr_map={offset_str: preprocess_mgr},
        num_batch=256,
    )

    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=preprocess_handle,
        data_api=None,
        fetch_funding_rate=(case.df_funding is not None),
        funding_mgr=funding_mgr,
        _binance_spot=binance_handle,
        _binance_swap=binance_handle,
    )

    def get_binance_handle(trade_type):
        return handle._binance_spot

    handle.get_binance_handle = get_binance_handle

    return handle, binance_mgr, preprocess_mgr


def build_spot_case(old_rows: int, new_rows: int) -> Module3Case:
    df_full = load_btcusdt_spot()
    assert old_rows + new_rows <= len(df_full), "Not enough rows in sample"
    start = len(df_full) - old_rows - new_rows
    match_row = build_match_row("BTCUSDT", df_full)
    df_match = pd.DataFrame([match_row])
    return Module3Case(
        name=f"btcusdt_spot_old{old_rows}_new{new_rows}",
        trade_type="spot",
        symbol="BTCUSDT",
        df_full=df_full.iloc[start:].reset_index(drop=True),
        df_funding=None,
        old_rows=old_rows,
        new_rows=new_rows,
        df_match=df_match,
    )


def build_swap_case(old_rows: int, new_rows: int) -> Module3Case:
    df_full = load_btcusdt_swap()
    df_funding = load_btcusdt_funding()
    assert old_rows + new_rows <= len(df_full), "Not enough rows in sample"
    start = len(df_full) - old_rows - new_rows
    df_slice = df_full.iloc[start:].reset_index(drop=True)
    match_row = build_match_row("BTCUSDT", df_slice)
    df_match = pd.DataFrame([match_row])
    return Module3Case(
        name=f"btcusdt_swap_old{old_rows}_new{new_rows}",
        trade_type="swap",
        symbol="BTCUSDT",
        df_full=df_slice,
        df_funding=df_funding,
        old_rows=old_rows,
        new_rows=new_rows,
        df_match=df_match,
    )


SPOT_CASE = build_spot_case(old_rows=8, new_rows=3)
SWAP_CASE = build_swap_case(old_rows=8, new_rows=3)
