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


def test_ready_flag_written_only_after_factors_in_snapshot(tmp, factor_config, monkeypatch):
    """Race fix, Finding-3 edition: at the instant the consumer gate (``spot_dict.ready``)
    is written, factors must already be durable — but in the Arrow snapshot, NOT the
    batch pkl. The batch pkl carries only raw OHLCV+meta (Finding 3); the factor-carrying
    durable artifact the consumer fast-path reads is the snapshot.
    """
    from core.arrow_publish import read_arrow_snapshot

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
            reader = CandleFileManager(base_dir=self.base_dir, save_type=self.save_type)
            disk = reader.read_candle("spot_dict_batch1")
            observed["factor_absent_from_batch_pkl_at_gate"] = (
                FACTOR_COL not in disk["BTCUSDT"].columns
            )
            snaps = list(Path(self.base_dir).glob("spot_dict_*.arrow"))
            if snaps:
                restored = read_arrow_snapshot(snaps[0])
                observed["factor_present_in_snapshot_at_gate"] = (
                    FACTOR_COL in restored["BTCUSDT"].columns
                )
            else:
                observed["factor_present_in_snapshot_at_gate"] = False
        return orig_save_ready(self, symbol, run_time)

    monkeypatch.setattr(CandleFileManager, "save_ready", spy_save_ready)

    preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    assert observed.get("factor_absent_from_batch_pkl_at_gate") is True, (
        "Finding 3: batch pkl must NOT carry factor columns (they live in the snapshot)"
    )
    assert observed.get("factor_present_in_snapshot_at_gate") is True, (
        "RACE: spot_dict gate was written before the factor-carrying Arrow snapshot was durable"
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


def test_batch_on_disk_is_raw_without_factor_columns(tmp, factor_config):
    """Finding 3: the persisted batch pkl must NOT carry factor columns. Factors are
    recomputed from OHLCV every cycle and never read back as computational input, so
    persisting them is dead write-I/O. They live only in the Arrow snapshot.
    """
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
    factor_cols = [c for c in disk["BTCUSDT"].columns if FACTOR_NAME in c]
    assert factor_cols == [], (
        f"batch pkl must be raw (no factor columns), found: {factor_cols}"
    )


def test_publish_does_not_reread_batch_from_disk(tmp, factor_config, monkeypatch):
    """Finding 1: the Arrow publish must reuse the in-memory market_dict produced by
    batch generation, NOT re-read the batch pickle back from disk.

    On a first run, batch generation writes the batch but never reads it back; the
    only legacy reason to read ``{type}_dict_batch*`` was the publish read-back we are
    removing. So after the fix, ``read_candle`` is never called with a batch name.
    """
    df_full = load_btcusdt_spot().tail(600).reset_index(drop=True)
    run_ts = pd.Timestamp(df_full["candle_begin_time"].iloc[-1])
    handle, preprocess_mgr, _ = _make_full_handle(tmp, df_full, run_ts)

    df_match = pd.DataFrame([{
        "spot": "BTCUSDT", "swap": "BTCUSDT",
        "first_begin_time": df_full["candle_begin_time"].min(),
        "last_begin_time": df_full["candle_begin_time"].max(),
    }])

    batch_reads = []
    orig_read_candle = CandleFileManager.read_candle

    def spy_read_candle(self, symbol):
        if symbol.startswith("spot_dict_batch"):
            batch_reads.append(symbol)
        return orig_read_candle(self, symbol)

    monkeypatch.setattr(CandleFileManager, "read_candle", spy_read_candle)

    preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    assert batch_reads == [], (
        f"publish re-read batch pickle(s) from disk instead of using in-memory dict: {batch_reads}"
    )

    # publish still happened with factors (the in-memory dict carried them)
    from core.arrow_publish import read_arrow_snapshot
    snap = list(Path(preprocess_mgr.base_dir).glob("spot_dict_*.arrow"))
    assert len(snap) == 1
    restored = read_arrow_snapshot(snap[0])
    assert FACTOR_COL in restored["BTCUSDT"].columns


def test_old_arrow_snapshots_cleaned_up_across_runs(tmp, factor_config):
    """Finding 2: across runs, only the newest few {type}_dict_{epoch}.arrow survive."""
    df_full = load_btcusdt_spot().tail(600).reset_index(drop=True)

    # three sequential runs at distinct run_times -> three distinct epochs
    base = df_full.iloc[:597].reset_index(drop=True)
    handle = None
    preprocess_mgr = None
    for i in range(3):
        sub = df_full.iloc[:597 + i].reset_index(drop=True)
        run_ts = pd.Timestamp(sub["candle_begin_time"].iloc[-1])
        if handle is None:
            handle, preprocess_mgr, _ = _make_full_handle(tmp, sub, run_ts)
        else:
            # feed the next run's resampled candles to the same binance mgr
            offset_str = get_offset_from_runtime(run_ts.to_pydatetime(), RESAMPLE_INTERVAL)
            binance_mgr = handle.get_binance_handle("spot").resample_mgr_map[offset_str]
            binance_mgr.save_data_file("BTCUSDT", sub.copy(deep=True))
            binance_mgr.save_ready("BTCUSDT", run_ts.to_pydatetime())
        df_match = pd.DataFrame([{
            "spot": "BTCUSDT", "swap": "BTCUSDT",
            "first_begin_time": sub["candle_begin_time"].min(),
            "last_begin_time": sub["candle_begin_time"].max(),
        }])
        preprocess_type_data(handle, run_ts.to_pydatetime(), "spot", df_match)

    snaps = sorted(Path(preprocess_mgr.base_dir).glob("spot_dict_*.arrow"))
    assert len(snaps) <= 2, f"old arrow snapshots not cleaned up: {[s.name for s in snaps]}"
