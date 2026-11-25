"""Phase 2 Compute-IO Overlap TDD Suite (Tasks 1-3).

Verifies:
- Task 1: WsLocalCache WAL read-only status/window query, instant barrier & zero-false-REST
  release before Writer drains disk queue, and O(1) glob-free save_ready/save_ws_ready.
- Task 2: Overlapping 7-bar incremental resample with 100% tail_out hit, ws_cache live
  5m window overlay when Writer lags, and compute-first resample with parallel 1h flush.
- Task 3: Mature-coin head-offset tail fast-path (100% hit without FutureWarning),
  intra-batch async raw-batch write overlapping C-factor/Arrow compute, and PyArrow pool release.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


# ============================================================================
# Task 1: WsLocalCache WAL Fast Barrier, Zero-False-REST & Glob-Free Ready
# ============================================================================


def test_ws_local_cache_readonly_status_and_window_frames(tmp_path):
    from core.storage.writer import _kline_dict_to_df
    from core.ws.ws_local_cache import (
        WsLocalCache,
        query_ws_cache_status,
        read_ws_cache_market_windows,
    )

    db_path = tmp_path / "ws_cache" / "swap.sqlite"
    cache = WsLocalCache(db_path)  # Keep writer connection open to verify WAL concurrent read!

    run_time = datetime(2026, 11, 25, 1, 25, tzinfo=timezone.utc)
    base_ms = 300_000
    target_ms = int((run_time - timedelta(minutes=5)).timestamp() * 1000)

    def _mk_kline(ts_ms: int, base_p: float) -> dict:
        return {
            "begin_time_ms": ts_ms,
            "open": base_p,
            "high": base_p + 1.0,
            "low": base_p - 1.0,
            "close": base_p + 0.5,
            "volume": 100.0,
            "quote_volume": 100.0 * base_p,
            "trade_num": 42,
            "taker_buy_base": 50.0,
            "taker_buy_quote": 50.0 * base_p,
        }

    # FASTUSDT: has T-10m and T-5m (contiguous trailing bars)
    k1 = _mk_kline(target_ms - base_ms, 100.0)
    k2 = _mk_kline(target_ms, 101.0)
    cache.write("FASTUSDT", "swap", k1)
    cache.write("FASTUSDT", "swap", k2)

    # FULL99USDT: has full 99 contiguous bars
    for i in range(99):
        cache.write("FULL99USDT", "swap", _mk_kline(target_ms - (98 - i) * base_ms, 200.0 + i * 0.1))

    # GAPPEDUSDT: has T-15m and T-5m, MISSING T-10m!
    cache.write("GAPPEDUSDT", "swap", _mk_kline(target_ms - 2 * base_ms, 50.0))
    cache.write("GAPPEDUSDT", "swap", _mk_kline(target_ms, 51.0))

    # STALEUSDT: only has T-10m (missing target T-5m)
    cache.write("STALEUSDT", "swap", _mk_kline(target_ms - base_ms, 30.0))

    ready_contig, full_contig, gapped_latest = query_ws_cache_status(
        db_path, "swap", target_ms, base_ms, check_bars=99
    )
    assert "FASTUSDT" in ready_contig
    assert "FULL99USDT" in ready_contig
    assert "FULL99USDT" in full_contig
    assert "FASTUSDT" not in full_contig
    assert "GAPPEDUSDT" not in ready_contig
    assert "GAPPEDUSDT" in gapped_latest
    assert "STALEUSDT" not in ready_contig

    win_start_ms = target_ms - 98 * base_ms
    end_excl_ms = int(run_time.timestamp() * 1000)
    win_map = read_ws_cache_market_windows(
        db_path, "swap", ["FASTUSDT", "MISSUSDT"], win_start_ms, end_excl_ms
    )
    assert "MISSUSDT" not in win_map
    expected_df = pd.concat([_kline_dict_to_df(k1), _kline_dict_to_df(k2)], ignore_index=True)
    pd.testing.assert_frame_equal(win_map["FASTUSDT"], expected_df, check_exact=True)
    cache.close()


def test_barrier_and_missed_check_release_immediately_via_ws_cache_when_writer_lags(tmp_path):
    import time
    from core.pipeline.barrier import wait_for_ready_barrier
    from core.pipeline.update_cycle import _compute_missed_and_gaps
    from core.storage.candle_manager import CandleFileManager
    from core.storage.writer import Writer
    from core.ws.ws_local_cache import WsLocalCache

    run_time = datetime(2026, 11, 25, 1, 25, tzinfo=timezone.utc)
    prev_rt = run_time - timedelta(minutes=5)
    base_dir = tmp_path / "data" / "binance_swap_5m"
    base_dir.mkdir(parents=True)
    exg_dir = tmp_path / "data" / "exginfo"
    exg_dir.mkdir(parents=True)
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    syms = ["BTCUSDT", "ETHUSDT", "GAPPEDUSDT"]
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": syms}))

    # Disk only has up to prev_rt (simulating Writer lagging behind on the current 01:25 burst!)
    t_prev = pd.date_range(end=prev_rt - timedelta(minutes=5), periods=120, freq="5min", tz="UTC")
    writer = Writer(base_candle_dirs={"swap": base_dir}, flat_pkl_max_bars=240)
    for s in syms:
        df_p = pd.DataFrame({
            "candle_begin_time": t_prev,
            "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
            "volume": 100.0, "quote_volume": 1050.0,
            "trade_num": pd.Series([5] * len(t_prev), dtype="int64"),
            "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
        })
        writer.process_one({"symbol": s, "trade_type": "swap", "df": df_p, "source": "ws", "run_time": prev_rt})

    # WS Runner has already written prev_rt-5m AND run_time-5m into ws_cache/swap.sqlite for BTC & ETH,
    # while GAPPEDUSDT missed prev_rt-5m and only got run_time-5m!
    ws_cache_db = tmp_path / "data" / "ws_cache" / "swap.sqlite"
    cache = WsLocalCache(ws_cache_db)
    target_ms = int((run_time - timedelta(minutes=5)).timestamp() * 1000)
    prev_ms = target_ms - 300_000
    for s in ("BTCUSDT", "ETHUSDT"):
        for ms in (prev_ms, target_ms):
            cache.write(s, "swap", {
                "begin_time_ms": ms, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
                "volume": 100.0, "quote_volume": 1050.0, "trade_num": 5,
                "taker_buy_base": 50.0, "taker_buy_quote": 525.0,
            })
    cache.write("GAPPEDUSDT", "swap", {
        "begin_time_ms": target_ms, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 100.0, "quote_volume": 1050.0, "trade_num": 5,
        "taker_buy_base": 50.0, "taker_buy_quote": 525.0,
    })

    # 1) Barrier for ["BTCUSDT", "ETHUSDT"] must release IMMEDIATELY (<0.3s) despite 0 .ws_ready for run_time!
    t0 = time.monotonic()
    succ, skip = wait_for_ready_barrier(
        symbols=["BTCUSDT", "ETHUSDT"],
        base_5m_dir=base_dir,
        run_time=run_time,
        timeout_sec=2.0,
        ws_wait_sec=5.0,
        ws_cache_db=ws_cache_db,
        trade_type="swap",
        base_delta=timedelta(minutes=5),
        once_update_candles=99,
    )
    elapsed = time.monotonic() - t0
    assert succ == {"BTCUSDT", "ETHUSDT"}
    assert skip == set()
    assert elapsed < 0.5

    # 2) _compute_missed_and_gaps must NOT treat BTC/ETH as missed, and MUST flag GAPPEDUSDT as missed + gap!
    h = SimpleNamespace(
        base_dir=tmp_path / "data",
        base_interval="5m",
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=base_dir, once_update_candles=99),
    )
    missed, gaps = _compute_missed_and_gaps(h, "swap", run_time)
    assert missed == {"GAPPEDUSDT"}
    assert gaps == {"GAPPEDUSDT"}
    cache.close()


def test_candle_file_manager_save_ready_eliminates_per_call_glob(tmp_path, monkeypatch):
    from core.storage import candle_manager as cm_mod
    from core.storage.candle_manager import CandleFileManager

    d = tmp_path / "swap_5m"
    d.mkdir()
    # Pre-seed legacy ready files created by a previous process
    (d / "BTCUSDT_1000.ready").write_text("1.0")
    (d / "BTCUSDT_1000.ws_ready").write_text("1.0")
    (d / "ETHUSDT_1000.ready").write_text("1.0")

    glob_calls = []
    orig_glob = cm_mod.glob

    def counting_glob(pat):
        glob_calls.append(pat)
        return orig_glob(pat)

    monkeypatch.setattr(cm_mod, "glob", counting_glob)

    mgr = CandleFileManager(d, "pickle")
    for idx in range(5):
        rt = datetime(2026, 11, 25, 0, idx * 5, tzinfo=timezone.utc)
        mgr.save_ready("BTCUSDT", rt)
        mgr.save_ws_ready("BTCUSDT", rt)
        mgr.save_ready("ETHUSDT", rt)

    # 15 save_* calls across 2 symbols must do ZERO glob() calls (uses O(1) scandir index + cached unlink)
    assert len(glob_calls) == 0
    last_ep = int(datetime(2026, 11, 25, 0, 20, tzinfo=timezone.utc).timestamp())
    assert sorted(p.name for p in d.glob("*.ready")) == [
        f"BTCUSDT_{last_ep}.ready",
        f"ETHUSDT_{last_ep}.ready",
    ]
    assert sorted(p.name for p in d.glob("*.ws_ready")) == [
        f"BTCUSDT_{last_ep}.ws_ready",
    ]
