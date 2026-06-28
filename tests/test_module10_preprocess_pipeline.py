"""
Integration tests for preprocess_type_data (design doc §3.2 race fix + §3.4 publish).

Two contracts:
1. RACE FIX: the ``{type}_dict`` ready flag (the consumer's gate) must be written
   only AFTER factor columns are persisted to disk. We spy on save_ready and, at
   the moment the gate flag is written, read the batch back from disk and assert
   factor columns are already present.
2. PUBLISH: after a successful run, an Arrow snapshot + its .ready flag exist, and
   it round-trips to an equivalent {symbol: DataFrame} carrying factor columns.

Grounded in real BTCUSDT data from /data.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from core import preprocess as preprocess_module
from core.candle_manager import CandleFileManager
from core.preprocess import preprocess_type_data
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL, load_btcusdt_spot

FACTOR_NAME = "动量因子210401修正"
PARAMS = [168]
FACTOR_COL = f"{FACTOR_NAME}_{PARAMS[0]}"


def _make_full_handle(tmp_path: Path, df_full: pd.DataFrame, run_ts: pd.Timestamp, symbol="BTCUSDT"):
    offset_str = get_offset_from_runtime(run_ts.to_pydatetime(), RESAMPLE_INTERVAL)

    binance_dir = tmp_path / "binance" / offset_str
    binance_dir.mkdir(parents=True, exist_ok=True)
    binance_mgr = CandleFileManager(base_dir=binance_dir, save_type="pickle")
    binance_mgr.save_data_file(symbol, df_full.copy(deep=True))
    binance_mgr.save_ready(symbol, run_ts.to_pydatetime())

    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir(parents=True, exist_ok=True)
    exg_mgr = CandleFileManager(base_dir=exg_dir, save_type="pickle")
    exg_mgr.save_data_file(f"exginfo_spot", pd.DataFrame({"symbol": [symbol]}))
    exg_mgr.save_ready(f"exginfo_spot", run_ts.to_pydatetime())
    exg_mgr.save_data_file(f"exginfo_swap", pd.DataFrame({"symbol": [symbol]}))
    exg_mgr.save_ready(f"exginfo_swap", run_ts.to_pydatetime())

    binance_handle = SimpleNamespace(resample_mgr_map={offset_str: binance_mgr})
    preprocess_handle = SimpleNamespace(
        preprocess_mgr_map={offset_str: preprocess_mgr},
        num_batch=180,
    )
    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=preprocess_handle,
        exg_mgr=exg_mgr,
        data_api=None,
        fetch_funding_rate=False,
        funding_mgr=None,
        _binance=binance_handle,
    )
    handle.get_binance_handle = lambda trade_type: handle._binance
    return handle, preprocess_mgr, offset_str


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


@pytest.fixture()
def factor_config(monkeypatch):
    monkeypatch.setattr(
        "core.preprocess.bmac_config",
        SimpleNamespace(precompute_factor_configs={FACTOR_NAME: PARAMS}, publish_kline_num=5000),
    )


def test_ready_flag_written_only_after_factors_on_disk(tmp, factor_config, monkeypatch):
    """The {type}_dict gate flag must not be observable before factor cols are on disk."""
    df_full = load_btcusdt_spot().tail(600).reset_index(drop=True)
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    handle, preprocess_mgr, _ = _make_full_handle(tmp, df_full, run_ts)

    df_match = pd.DataFrame([{
        "spot": "BTCUSDT", "swap": "BTCUSDT",
        "first_begin_time": df_full["candle_begin_time"].min(),
        "last_begin_time": df_full["candle_begin_time"].max(),
    }])

    observed = {}
    orig_save_ready = CandleFileManager.save_ready

    def spy_save_ready(self, symbol, run_time):
        if symbol == "spot_dict":
            # At the instant the consumer gate is written, the batch on disk MUST already
            # carry factor columns. Read from a fresh reader to check what's persisted.
            reader = CandleFileManager(base_dir=self.base_dir, save_type=self.save_type)
            disk = reader.read_candle("spot_dict_batch1")
            observed["factor_present_at_gate"] = FACTOR_COL in disk["BTCUSDT"].columns
        return orig_save_ready(self, symbol, run_time)

    monkeypatch.setattr(CandleFileManager, "save_ready", spy_save_ready)

    preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    assert observed.get("factor_present_at_gate") is True, (
        "RACE: spot_dict ready flag was written before factor columns were on disk"
    )


def test_arrow_snapshot_published_with_factors(tmp, factor_config):
    """After a run, an Arrow snapshot + ready flag exist and round-trip with factor cols."""
    from core.arrow_publish import read_arrow_snapshot

    df_full = load_btcusdt_spot().tail(600).reset_index(drop=True)
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    handle, preprocess_mgr, offset_str = _make_full_handle(tmp, df_full, run_ts)

    df_match = pd.DataFrame([{
        "spot": "BTCUSDT", "swap": "BTCUSDT",
        "first_begin_time": df_full["candle_begin_time"].min(),
        "last_begin_time": df_full["candle_begin_time"].max(),
    }])

    preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    pre_dir = Path(preprocess_mgr.base_dir)
    snapshots = list(pre_dir.glob("spot_dict_*.arrow"))
    assert len(snapshots) == 1, f"expected exactly one arrow snapshot, found {snapshots}"
    assert snapshots[0].with_suffix(".ready").exists(), "arrow snapshot ready flag missing"

    restored = read_arrow_snapshot(snapshots[0])
    assert "BTCUSDT" in restored
    assert FACTOR_COL in restored["BTCUSDT"].columns
    vals = restored["BTCUSDT"][FACTOR_COL].values
    assert (~np.isnan(vals)).any(), "published factor column is all-NaN"


def test_batch_on_disk_has_factor_columns(tmp, factor_config):
    """End-to-end: the persisted batch carries factor columns (write-once path)."""
    df_full = load_btcusdt_spot().tail(600).reset_index(drop=True)
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    handle, preprocess_mgr, _ = _make_full_handle(tmp, df_full, run_ts)

    df_match = pd.DataFrame([{
        "spot": "BTCUSDT", "swap": "BTCUSDT",
        "first_begin_time": df_full["candle_begin_time"].min(),
        "last_begin_time": df_full["candle_begin_time"].max(),
    }])

    preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    disk = preprocess_mgr.read_candle("spot_dict_batch1")
    assert FACTOR_COL in disk["BTCUSDT"].columns
