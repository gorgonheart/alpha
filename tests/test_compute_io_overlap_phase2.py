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


# ============================================================================
# Task 2: Overlapping Incremental Resample, ws_cache Overlay & Parallel Flush
# ============================================================================


def test_update_resample_candle_incremental_overlapping_7_bars_matches_oracle_and_returns_tail(tmp_path):
    from core.pipeline.resample import update_resample_candle, update_resample_candle_incremental
    from core.storage.candle_manager import CandleFileManager

    delta = timedelta(hours=1)
    t_old = pd.date_range("2026-11-15 00:25", periods=200, freq="h", tz="UTC")
    rt_old = (t_old[-1] + delta).to_pydatetime()

    def _mk_1h(times, base_p=100.0):
        n = len(times)
        return pd.DataFrame({
            "candle_begin_time": times,
            "open": [base_p + i * 0.1 for i in range(n)],
            "high": [base_p + 1.0 + i * 0.1 for i in range(n)],
            "low": [base_p - 1.0 + i * 0.1 for i in range(n)],
            "close": [base_p + 0.5 + i * 0.1 for i in range(n)],
            "volume": [1000.0 + i for i in range(n)],
            "quote_volume": [100000.0 + i * 100 for i in range(n)],
            "trade_num": pd.Series([50 + i for i in range(n)], dtype="int64"),
            "taker_buy_base_asset_volume": [500.0 for _ in range(n)],
            "taker_buy_quote_asset_volume": [50000.0 for _ in range(n)],
        })

    base_df = _mk_1h(t_old)

    # Case 1: Realistic 7-bar overlapping window (6 existing bars + 1 new bar)
    t_win1 = pd.date_range(t_old[-6], periods=7, freq="h", tz="UTC")
    df_res1 = _mk_1h(t_win1, base_p=100.0 + (len(t_old) - 6) * 0.1)
    rt1 = (t_win1[-1] + delta).to_pydatetime()

    # Case 2: Overlapping window with a 1-hour gap (missing T, arriving T+1h)
    t_win2 = pd.DatetimeIndex(list(t_old[-5:]) + [t_old[-1] + 2 * delta])
    df_res2 = _mk_1h(t_win2, base_p=120.0)
    rt2 = (t_win2[-1] + delta).to_pydatetime()

    # Case 3: Token split (>48h gap + >10% price jump) -> must match oracle AND return tail=None
    t_win3 = pd.date_range(t_old[-1] + timedelta(hours=60), periods=3, freq="h", tz="UTC")
    df_res3 = _mk_1h(t_win3, base_p=500.0)
    rt3 = (t_win3[-1] + delta).to_pydatetime()

    ora_mgr = CandleFileManager(tmp_path / "ora_1h", "pickle")
    inc_mgr = CandleFileManager(tmp_path / "inc_1h", "pickle")
    ora_mgr.base_dir.mkdir(parents=True)
    inc_mgr.base_dir.mkdir(parents=True)

    for sym, df_new, rt, expect_tail in (
        ("NORMUSDT", df_res1, rt1, True),
        ("GAP1HUSDT", df_res2, rt2, True),
        ("SPLITUSDT", df_res3, rt3, False),
    ):
        ora_mgr.set_candle(sym, rt_old, base_df.copy())
        inc_mgr.set_candle(sym, rt_old, base_df.copy())

        b_ora = update_resample_candle(ora_mgr, sym, df_new.copy(), None, rt, delta)
        f_inc, l_inc, tail_inc = update_resample_candle_incremental(
            inc_mgr, sym, df_new.copy(), None, rt, delta, return_tail_bars=12
        )
        assert (f_inc, l_inc) == b_ora
        pd.testing.assert_frame_equal(inc_mgr.read_candle(sym), ora_mgr.read_candle(sym), check_exact=True)
        if expect_tail:
            assert tail_inc is not None and len(tail_inc) == 12
            pd.testing.assert_frame_equal(
                tail_inc,
                ora_mgr.read_candle(sym).tail(12).reset_index(drop=True),
                check_exact=True,
            )
        else:
            assert tail_inc is None


def test_resample_overlays_ws_cache_when_writer_lags_behind_disk(tmp_path):
    from core.pipeline.resample import update_resample_from_base
    from core.storage.candle_manager import CandleFileManager
    from core.storage.writer import Writer
    from core.ws.ws_local_cache import WsLocalCache

    run_time = datetime(2026, 11, 25, 2, 25, tzinfo=timezone.utc)
    prev_rt = run_time - timedelta(minutes=5)
    t_all = pd.date_range(end=run_time - timedelta(minutes=5), periods=150, freq="5min", tz="UTC")
    df_all = pd.DataFrame({
        "candle_begin_time": t_all,
        "open": [50.0 + i * 0.05 for i in range(150)],
        "high": [51.0 + i * 0.05 for i in range(150)],
        "low": [49.0 + i * 0.05 for i in range(150)],
        "close": [50.2 + i * 0.05 for i in range(150)],
        "volume": 20.0, "quote_volume": 1004.0,
        "trade_num": pd.Series([7] * 150, dtype="int64"),
        "taker_buy_base_asset_volume": 10.0, "taker_buy_quote_asset_volume": 502.0,
    })

    exg_mgr = CandleFileManager(tmp_path / "exg", "pickle")
    exg_mgr.base_dir.mkdir()
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["SOLUSDT"]}))

    # Oracle env: Writer already wrote all 150 bars (up to run_time - 5m) to disk
    ora_root = tmp_path / "ora_data"
    ora_5m = ora_root / "binance_swap_5m"
    ora_1h = CandleFileManager(ora_root / "swap_1h" / "25m", "pickle")
    ora_5m.mkdir(parents=True)
    ora_1h.base_dir.mkdir(parents=True)
    Writer(base_candle_dirs={"swap": ora_5m}, flat_pkl_max_bars=240).process_one({
        "symbol": "SOLUSDT", "trade_type": "swap", "df": df_all.copy(), "source": "ws", "run_time": run_time,
    })

    # Lagging env: Writer on disk only has 149 bars (missing latest T-5m bar!), while ws_cache has T-10m & T-5m!
    lag_root = tmp_path / "lag_data"
    lag_5m = lag_root / "binance_swap_5m"
    lag_1h = CandleFileManager(lag_root / "swap_1h" / "25m", "pickle")
    lag_5m.mkdir(parents=True)
    lag_1h.base_dir.mkdir(parents=True)
    Writer(base_candle_dirs={"swap": lag_5m}, flat_pkl_max_bars=240).process_one({
        "symbol": "SOLUSDT", "trade_type": "swap", "df": df_all.iloc[:-1].copy(), "source": "ws", "run_time": prev_rt,
    })
    cache = WsLocalCache(lag_root / "ws_cache" / "swap.sqlite")
    for row in df_all.iloc[-2:].itertuples(index=False):
        cache.write("SOLUSDT", "swap", {
            "begin_time_ms": int(row.candle_begin_time.timestamp() * 1000),
            "open": float(row.open), "high": float(row.high), "low": float(row.low), "close": float(row.close),
            "volume": float(row.volume), "quote_volume": float(row.quote_volume),
            "trade_num": int(row.trade_num),
            "taker_buy_base": float(row.taker_buy_base_asset_volume),
            "taker_buy_quote": float(row.taker_buy_quote_asset_volume),
        })

    def _mk_h(d_root, b_dir, r_mgr):
        return SimpleNamespace(
            base_dir=d_root,
            base_interval="5m",
            resample_interval="1h",
            enabled_hour_offsets=("25m",),
            history_trim_ts=lambda: None,
            effective_kline_count_1h=lambda tt, rt: 15000,
            exg_mgr=exg_mgr,
            get_market_handle=lambda _: SimpleNamespace(
                base_candle_dir=b_dir, resample_mgr_map={"25m": r_mgr}, once_update_candles=99,
            ),
        )

    ora_begins = update_resample_from_base(_mk_h(ora_root, ora_5m, ora_1h), "swap", run_time)
    lag_begins, lag_tails = update_resample_from_base(
        _mk_h(lag_root, lag_5m, lag_1h), "swap", run_time, return_tail_bars=12
    )
    assert lag_begins == ora_begins
    pd.testing.assert_frame_equal(lag_1h.read_candle("SOLUSDT"), ora_1h.read_candle("SOLUSDT"), check_exact=True)
    assert "SOLUSDT" in lag_tails and len(lag_tails["SOLUSDT"]) > 0
    cache.close()


def test_compute_first_resample_defers_1h_disk_write_and_flushes_exact(tmp_path):
    from core.pipeline.resample import update_resample_from_base
    from core.storage.candle_manager import CandleFileManager
    from core.storage.writer import Writer

    rt1 = datetime(2026, 11, 25, 2, 25, tzinfo=timezone.utc)
    rt2 = datetime(2026, 11, 25, 3, 25, tzinfo=timezone.utc)
    t_all = pd.date_range(end=rt2 - timedelta(minutes=5), periods=240, freq="5min", tz="UTC")
    syms = ["AAAUSDT", "BBBUSDT"]

    exg_mgr = CandleFileManager(tmp_path / "exg", "pickle")
    exg_mgr.base_dir.mkdir()
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": syms}))

    def _build_env(sub: str):
        root = tmp_path / sub
        b5 = root / "binance_spot_5m"
        r1 = CandleFileManager(root / "spot_1h" / "25m", "pickle")
        b5.mkdir(parents=True)
        r1.base_dir.mkdir(parents=True)
        h = SimpleNamespace(
            base_dir=root,
            base_interval="5m",
            resample_interval="1h",
            enabled_hour_offsets=("25m",),
            history_trim_ts=lambda: None,
            effective_kline_count_1h=lambda tt, rt: 15000,
            exg_mgr=exg_mgr,
            get_market_handle=lambda _: SimpleNamespace(
                base_candle_dir=b5, resample_mgr_map={"25m": r1}, once_update_candles=99,
            ),
        )
        return h, b5, r1

    h_ora, b5_ora, r1_ora = _build_env("ora")
    h_def, b5_def, r1_def = _build_env("def")

    for i, s in enumerate(syms):
        df = pd.DataFrame({
            "candle_begin_time": t_all,
            "open": 10.0 + i + np.arange(240) * 0.01,
            "high": 11.0 + i + np.arange(240) * 0.01,
            "low": 9.0 + i + np.arange(240) * 0.01,
            "close": 10.5 + i + np.arange(240) * 0.01,
            "volume": 50.0, "quote_volume": 525.0,
            "trade_num": pd.Series([3] * 240, dtype="int64"),
            "taker_buy_base_asset_volume": 25.0, "taker_buy_quote_asset_volume": 262.5,
        })
        df_r1 = df[df["candle_begin_time"] < rt1].copy()
        for bdir in (b5_ora, b5_def):
            Writer(base_candle_dirs={"spot": bdir}, flat_pkl_max_bars=240).process_one(
                {"symbol": s, "trade_type": "spot", "df": df_r1, "source": "ws", "run_time": rt1}
            )

    # Round 1: warm bounds cache
    update_resample_from_base(h_ora, "spot", rt1)
    b1, t1, f1 = update_resample_from_base(h_def, "spot", rt1, return_tail_bars=12, defer_disk_write=True)
    f1()

    # Round 2: write new 5m bars up to rt2
    for i, s in enumerate(syms):
        df = pd.DataFrame({
            "candle_begin_time": t_all,
            "open": 10.0 + i + np.arange(240) * 0.01,
            "high": 11.0 + i + np.arange(240) * 0.01,
            "low": 9.0 + i + np.arange(240) * 0.01,
            "close": 10.5 + i + np.arange(240) * 0.01,
            "volume": 50.0, "quote_volume": 525.0,
            "trade_num": pd.Series([3] * 240, dtype="int64"),
            "taker_buy_base_asset_volume": 25.0, "taker_buy_quote_asset_volume": 262.5,
        })
        for bdir in (b5_ora, b5_def):
            Writer(base_candle_dirs={"spot": bdir}, flat_pkl_max_bars=240).process_one(
                {"symbol": s, "trade_type": "spot", "df": df, "source": "ws", "run_time": rt2}
            )

    ora_begins = update_resample_from_base(h_ora, "spot", rt2)
    def_begins, def_tails, flush_fn = update_resample_from_base(
        h_def, "spot", rt2, return_tail_bars=12, defer_disk_write=True
    )

    # BEFORE flush_fn() runs: begin_ts and in-memory tails ALREADY match Round 2, while disk 1h is NOT yet touched!
    assert def_begins == ora_begins
    for s in syms:
        assert not r1_def.check_ready(s, rt2)
        assert s in def_tails and def_tails[s]["candle_begin_time"].iloc[-1] == ora_begins[s][1]

    # Now execute background flush and verify disk 1h files become 100% bit-for-bit identical to oracle!
    flush_fn()
    for s in syms:
        assert r1_def.check_ready(s, rt2)
        pd.testing.assert_frame_equal(r1_def.read_candle(s), r1_ora.read_candle(s), check_exact=True)


# ============================================================================
# Task 3: Mature-Coin Head Offset Fast-Path, Async Batch Save & PyArrow Release
# ============================================================================


def test_mature_coin_with_earlier_batch_head_hits_both_tail_fast_paths_without_warnings(tmp_path, monkeypatch):
    import warnings
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp
    from core.strategy.funding_publication import FundingPublication

    # Exact alphatest topology: batch.pkl starts 11h earlier (06:25) than 1h_resample (17:25)
    times_full = pd.date_range("2025-04-16 06:25", periods=120, freq="h", tz="UTC")
    rt1 = times_full[-1].to_pydatetime()
    rt2 = (times_full[-1] + timedelta(hours=1)).to_pydatetime()
    times_r2_all = pd.date_range("2025-04-16 06:25", periods=121, freq="h", tz="UTC")
    times_r2_trimmed = times_r2_all[11:]  # Starts at 17:25 (110 bars vs 120 in df_old!)

    syms = ["MATURE1USDT", "MATURE2USDT", "DEEPREV2USDT"]
    raw_all = {}
    for i, s in enumerate(syms):
        p = 50.0 + i * 10.0 + np.arange(len(times_r2_all)) * 0.2
        raw_all[s] = pd.DataFrame({
            "candle_begin_time": times_r2_all,
            "open": p, "high": p + 0.5, "low": p - 0.5, "close": p + 0.1,
            "volume": 1000.0, "quote_volume": 1000.0 * p,
            "trade_num": pd.Series([25] * len(times_r2_all), dtype="int64"),
            "taker_buy_base_asset_volume": 500.0, "taker_buy_quote_asset_volume": 500.0 * p,
        })

    def _mk_side(sub: str):
        root = tmp_path / sub
        res_mgr = CandleFileManager(root / "res" / "25m", "pickle")
        pre_mgr = CandleFileManager(root / "pre" / "25m", "pickle")
        f_mgr = CandleFileManager(root / "funding", "pickle")
        for m in (res_mgr, pre_mgr, f_mgr):
            m.base_dir.mkdir(parents=True)
        h = SimpleNamespace(
            resample_interval="1h",
            fetch_funding_rate=True,
            data_api=None,
            history_trim_ts=lambda: None,
            funding_mgr=f_mgr,
            preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": pre_mgr}),
            get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        )
        # Round 1: seed full 120 bars (starting at 06:25) with float64 funding_fee in batch1.pkl
        for s in syms:
            res_mgr.set_candle(s, rt1, raw_all[s].iloc[:-1].reset_index(drop=True))
            f_mgr.set_candle(s, rt1, pd.DataFrame({
                "candle_begin_time": [times_r2_all[-5].floor("h")],
                "funding_rate": [-0.00012],
            }))
        m1 = pd.DataFrame([
            {"spot": s, "swap": s, "first_begin_time": times_full[0], "last_begin_time": times_full[-1]}
            for s in syms
        ])
        pp.gen_batch_data_incremental(h, rt1, "swap", m1, syms, "batch1")
        pre_mgr.save_ready("market_pivot_swap", rt1)
        # Simulate missing/empty funding on Round 2 to verify no FutureWarning on float64 funding_fee!
        for s in syms:
            f_mgr.remove_symbol(s)
        return h, res_mgr, pre_mgr

    h_fast, res_fast, pre_fast = _mk_side("fast")
    h_ora, res_ora, pre_ora = _mk_side("ora")

    # Round 2: 1h_resample only has [17:25 .. latest] (11h shorter head than batch1.pkl!)
    # - MATURE1USDT: passed via resample_tail_hints -> MUST hit Fast Path 1 (zero 1h disk read!)
    # - MATURE2USDT: no hint -> reads disk 1h_resample -> MUST hit Fast Path 2 (enrich <= 20 rows, NOT 110 rows!)
    # - DEEPREV2USDT: no hint + historical revision at bar 20 -> MUST fallback to full enrich (110 rows)
    rev_r2 = raw_all["DEEPREV2USDT"].iloc[11:].reset_index(drop=True).copy()
    rev_r2.loc[20, "close"] += 9.99

    for res_m in (res_fast, res_ora):
        res_m.set_candle("MATURE1USDT", rt2, raw_all["MATURE1USDT"].iloc[11:].reset_index(drop=True))
        res_m.set_candle("MATURE2USDT", rt2, raw_all["MATURE2USDT"].iloc[11:].reset_index(drop=True))
        res_m.set_candle("DEEPREV2USDT", rt2, rev_r2.copy())

    m2 = pd.DataFrame([
        {"spot": s, "swap": s, "first_begin_time": times_r2_trimmed[0], "last_begin_time": times_r2_trimmed[-1]}
        for s in syms
    ])

    # Run baseline oracle (force full path by disabling tail overlap bars and passing no hints)
    monkeypatch.setattr(pp, "_TAIL_FAST_OVERLAP_BARS", 999999)
    pp.gen_batch_data_incremental(h_ora, rt2, "swap", m2, syms, "batch1")
    monkeypatch.setattr(pp, "_TAIL_FAST_OVERLAP_BARS", 16)

    # Guard MATURE1USDT against disk read AND record enrich lengths on h_fast!
    orig_read = res_fast.read_candle
    monkeypatch.setattr(
        res_fast, "read_candle",
        lambda s: (_ for _ in ()).throw(AssertionError("MATURE1USDT must hit Fast Path 1 without reading disk!"))
        if s == "MATURE1USDT" else orig_read(s),
    )
    enrich_lens = {}
    orig_add_cols = pp.add_additional_cols

    def spy_add_cols(df, tt, sym, *a, **kw):
        enrich_lens[sym] = len(df)
        return orig_add_cols(df, tt, sym, *a, **kw)

    monkeypatch.setattr(pp, "add_additional_cols", spy_add_cols)

    hints = {
        "MATURE1USDT": (
            times_r2_trimmed[0],
            times_r2_trimmed[-1],
            raw_all["MATURE1USDT"].tail(12).reset_index(drop=True),
        )
    }
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        pp.gen_batch_data_incremental(h_fast, rt2, "swap", m2, syms, "batch1", resample_tail_hints=hints)

    future_warns = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert future_warns == [], f"Unexpected FutureWarnings: {future_warns}"
    assert enrich_lens["MATURE1USDT"] == 12
    assert enrich_lens["MATURE2USDT"] == 17  # 16 overlap + 1 new bar (NOT 110!)
    assert enrich_lens["DEEPREV2USDT"] == len(times_r2_trimmed)

    b_fast = pre_fast.read_candle("swap_dict_batch1")
    b_ora = pre_ora.read_candle("swap_dict_batch1")
    for s in syms:
        pd.testing.assert_frame_equal(b_fast[s], b_ora[s], check_exact=True)


def test_batch_worker_overlaps_raw_batch_save_with_factor_compute_and_propagates_io_error(tmp_path, monkeypatch):
    from threading import Barrier
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp

    times = pd.date_range("2026-11-20 00:25", periods=40, freq="h", tz="UTC")
    rt = times[-1].to_pydatetime()
    res_mgr = CandleFileManager(tmp_path / "res" / "25m", "pickle")
    pre_mgr = CandleFileManager(tmp_path / "pre" / "25m", "pickle")
    res_mgr.base_dir.mkdir(parents=True)
    pre_mgr.base_dir.mkdir(parents=True)

    df_in = pd.DataFrame({
        "candle_begin_time": times,
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 100.0, "quote_volume": 1050.0,
        "trade_num": pd.Series([5] * len(times), dtype="int64"),
        "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
    })
    res_mgr.set_candle("AAAUSDT", rt, df_in)
    df_match = pd.DataFrame([{"spot": "AAAUSDT", "swap": "AAAUSDT", "first_begin_time": times[0], "last_begin_time": times[-1]}])

    h = SimpleNamespace(
        resample_interval="1h",
        fetch_funding_rate=False,
        data_api=None,
        history_trim_ts=lambda: None,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": pre_mgr}),
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
    )

    # 1) Barrier(2) between set_candle('spot_dict_batch1') and apply_precomputed_factors proves TRUE concurrency!
    overlap_barrier = Barrier(2)
    orig_set_candle = pre_mgr.set_candle
    orig_apply_factors = pp.apply_precomputed_factors

    def synced_set_candle(sym, r_time, obj):
        if sym == "spot_dict_batch1":
            overlap_barrier.wait(timeout=3.0)
        return orig_set_candle(sym, r_time, obj)

    def synced_apply_factors(df, fmp):
        overlap_barrier.wait(timeout=3.0)
        return orig_apply_factors(df, fmp)

    monkeypatch.setattr(pre_mgr, "set_candle", synced_set_candle)
    monkeypatch.setattr(pp, "apply_precomputed_factors", synced_apply_factors)

    out = pp._run_batch_worker_task(
        h, rt, "spot", df_match, ["AAAUSDT"], "batch1", [("CciV3", [24])], 20, None
    )
    assert out[6] is True  # has_non_empty
    assert pre_mgr.has_symbol("spot_dict_batch1")

    # 2) Verify background set_candle IO failure is NOT swallowed — it must raise before worker returns!
    monkeypatch.setattr(pp, "apply_precomputed_factors", orig_apply_factors)

    def failing_set_candle(sym, r_time, obj):
        if sym == "spot_dict_batch1":
            raise OSError("simulated disk write failure")
        return orig_set_candle(sym, r_time, obj)

    monkeypatch.setattr(pre_mgr, "set_candle", failing_set_candle)
    with pytest.raises(OSError, match="simulated disk write failure"):
        pp._run_batch_worker_task(
            h, rt, "spot", df_match, ["AAAUSDT"], "batch1", [("CciV3", [24])], 20, None
        )


def test_release_memory_releases_pyarrow_unused_pool(monkeypatch):
    import pyarrow as pa
    from core.strategy import preprocess as pp

    called = []

    class FakePool:
        def release_unused(self):
            called.append(True)

    monkeypatch.setattr(pa, "default_memory_pool", lambda: FakePool())
    pp._release_memory()
    assert called == [True]


def test_writer_atomic_replace_retries_transient_permission_error(tmp_path, monkeypatch):
    import os
    from core.storage.writer import Writer

    b5 = tmp_path / "spot_5m"
    b5.mkdir()
    w = Writer(base_candle_dirs={"spot": b5}, flat_pkl_max_bars=240)
    df = pd.DataFrame({"candle_begin_time": pd.date_range("2026-11-25", periods=3, freq="5min", tz="UTC"), "close": [1.0, 2.0, 3.0]})

    orig_replace = os.replace
    attempts = []

    def flaky_replace(src, dst):
        attempts.append(1)
        if len(attempts) <= 2:
            raise PermissionError("simulated transient Windows reader lock")
        return orig_replace(src, dst)

    monkeypatch.setattr(os, "replace", flaky_replace)
    w._write_pkl(b5, "BTCUSDT", df)
    assert len(attempts) == 3
    pd.testing.assert_frame_equal(pd.read_pickle(b5 / "BTCUSDT.pkl"), df, check_exact=True)


def test_deferred_1h_on_demand_flush_when_anchor_revised(tmp_path):
    from core.pipeline.resample import _record_resample_bounds
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp

    times = pd.date_range("2026-11-20 00:25", periods=30, freq="h", tz="UTC")
    rt1 = times[-2].to_pydatetime()
    rt2 = times[-1].to_pydatetime()

    res_mgr = CandleFileManager(tmp_path / "res" / "25m", "pickle")
    pre_mgr = CandleFileManager(tmp_path / "pre" / "25m", "pickle")
    res_mgr.base_dir.mkdir(parents=True)
    pre_mgr.base_dir.mkdir(parents=True)

    df_full = pd.DataFrame({
        "candle_begin_time": times,
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 100.0, "quote_volume": 1050.0,
        "trade_num": pd.Series([5] * len(times), dtype="int64"),
        "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
    })
    res_mgr.set_candle("REVUSDT", rt1, df_full.iloc[:-1].reset_index(drop=True))
    _record_resample_bounds(res_mgr, "REVUSDT", times[0], times[-2])

    h = SimpleNamespace(
        resample_interval="1h",
        fetch_funding_rate=False,
        data_api=None,
        history_trim_ts=lambda: None,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": pre_mgr}),
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
    )
    m1 = pd.DataFrame([{"spot": "REVUSDT", "swap": "REVUSDT", "first_begin_time": times[0], "last_begin_time": times[-2]}])
    pp.gen_batch_data_incremental(h, rt1, "spot", m1, ["REVUSDT"], "batch1")

    # Round 2: simulate anchor bar revision while 1h_resample has NOT yet been flushed to disk!
    df_tail_rev = df_full.tail(6).reset_index(drop=True).copy()
    df_tail_rev.loc[0, "close"] = 99.9  # Anchor mismatch -> misses Fast Path 1 -> flushes df_tail_rev on demand!
    assert not res_mgr.check_ready("REVUSDT", rt2)

    m2 = pd.DataFrame([{"spot": "REVUSDT", "swap": "REVUSDT", "first_begin_time": times[0], "last_begin_time": times[-1]}])
    hints = {"REVUSDT": (times[0], times[-1], df_tail_rev, df_tail_rev)}
    pp.gen_batch_data_incremental(h, rt2, "spot", m2, ["REVUSDT"], "batch1", resample_tail_hints=hints)

    assert res_mgr.check_ready("REVUSDT", rt2)
    out_df = pre_mgr.read_candle("spot_dict_batch1")["REVUSDT"]
    assert len(out_df) == len(times)
    assert out_df.loc[len(times) - 6, "close"] == 99.9


# ============================================================================
# P0 Hotfixes: Cross-Process .ready Leak & ws_cache Index / Subset Seek
# ============================================================================


def test_candle_file_manager_pickle_and_cross_process_writes_never_leak_ready_files(tmp_path):
    import pickle
    from core.storage.candle_manager import CandleFileManager

    d = tmp_path / "pre_25m"
    d.mkdir()
    rt1 = datetime(2026, 11, 25, 3, 25, tzinfo=timezone.utc)
    rt2 = datetime(2026, 11, 25, 4, 25, tzinfo=timezone.utc)
    rt3 = datetime(2026, 11, 25, 5, 25, tzinfo=timezone.utc)

    # 1) Parent indexes directory BEFORE any worker writes batch1 or REVUSDT
    parent_mgr = CandleFileManager(d, "pickle")
    parent_mgr.save_ready("spot_dict", rt1)
    parent_mgr.save_ready("REVUSDT", rt1)

    # 2) Round 1 worker unpickles parent_mgr and writes batch1 (new symbol) + updates REVUSDT externally
    worker_r1 = pickle.loads(pickle.dumps(parent_mgr))
    worker_r1.save_ready("spot_dict_batch1", rt1)
    worker_r1.save_ws_ready("spot_dict_batch1", rt1)
    worker_r1.save_ready("REVUSDT", rt2)

    # 3) Round 2 worker unpickles the STILL-ALIVE parent_mgr and writes batch1 for rt2 -> must NOT leak rt1!
    worker_r2 = pickle.loads(pickle.dumps(parent_mgr))
    worker_r2.save_ready("spot_dict_batch1", rt2)
    worker_r2.save_ws_ready("spot_dict_batch1", rt2)

    # 4) Parent writes spot_dict and REVUSDT on Round 3 (after cache TTL / external modification) -> must NOT leak rt2!
    parent_mgr._ready_indexed_at = 0.0
    parent_mgr.save_ready("spot_dict", rt3)
    parent_mgr.save_ready("REVUSDT", rt3)

    ep2 = int(rt2.timestamp())
    ep3 = int(rt3.timestamp())
    assert sorted(p.name for p in d.glob("*.ready")) == sorted([
        f"spot_dict_{ep3}.ready",
        f"spot_dict_batch1_{ep2}.ready",
        f"REVUSDT_{ep3}.ready",
    ])
    assert sorted(p.name for p in d.glob("*.ws_ready")) == [
        f"spot_dict_batch1_{ep2}.ws_ready",
    ]


def test_ws_cache_index_and_symbol_subset_query_avoid_full_table_scan(tmp_path):
    import sqlite3
    from core.ws.ws_local_cache import WsLocalCache, read_ws_cache_market_windows

    # 1) Simulate an ALREADY-EXISTING v1 database created BEFORE this upgrade (no idx_kline_tt_time yet)
    db_path = tmp_path / "swap.sqlite"
    raw_conn = sqlite3.connect(str(db_path))
    raw_conn.execute("PRAGMA journal_mode=WAL")
    raw_conn.execute(
        """
        CREATE TABLE kline (
            symbol TEXT NOT NULL, trade_type TEXT NOT NULL, begin_time_ms INTEGER NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume REAL, quote_volume REAL, trade_num INTEGER,
            taker_buy_base REAL, taker_buy_quote REAL,
            PRIMARY KEY (symbol, trade_type, begin_time_ms)
        )
        """
    )
    raw_conn.execute("PRAGMA user_version = 1")
    base_ms = 1795580000000
    rows = [
        (f"SYM{i:03d}USDT", "swap", base_ms + b * 300000, 10.0 + b, 11.0, 9.0, 10.5, 100.0, 1050.0, 7, 50.0, 525.0)
        for i in range(60)
        for b in range(30)
    ]
    with raw_conn:
        raw_conn.executemany("INSERT INTO kline VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    raw_conn.close()

    # Even BEFORE WsLocalCache reopens db_path, subset read MUST use PK index (NOT full SCAN kline)
    # via WHERE symbol IN (...)!
    subset = ["SYM003USDT", "SYM017USDT"]
    win_map = read_ws_cache_market_windows(db_path, "swap", subset, base_ms + 5 * 300000, base_ms + 25 * 300000)
    assert sorted(win_map.keys()) == subset
    assert len(win_map["SYM003USDT"]) == 20
    assert str(win_map["SYM003USDT"]["trade_num"].dtype) == "int64"

    # 2) Opening WsLocalCache on existing v1 DB MUST preserve all 1,800 rows AND create idx_kline_tt_time!
    cache = WsLocalCache(db_path)
    total_after_open = cache._conn.execute("SELECT COUNT(*) FROM kline").fetchone()[0]
    assert total_after_open == 1800  # Zero data loss!
    idx_names = {r[1] for r in cache._conn.execute("PRAGMA index_list(kline)").fetchall()}
    assert "idx_kline_tt_time" in idx_names

    # Verify EXPLAIN QUERY PLAN for status query uses covering index (zero full-table SCAN kline)
    plan_status = cache._conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT symbol, COUNT(*), SUM(CASE WHEN begin_time_ms = ? THEN 1 ELSE 0 END)
          FROM kline
         WHERE trade_type = ? AND begin_time_ms >= ? AND begin_time_ms <= ?
         GROUP BY symbol
        """,
        (base_ms + 29 * 300000, "swap", base_ms, base_ms + 29 * 300000),
    ).fetchall()
    plan_detail = " ".join(str(tuple(r)) for r in plan_status)
    assert "idx_kline_tt_time" in plan_detail and "SCAN kline" not in plan_detail

    # Verify EXPLAIN QUERY PLAN for subset query uses primary key index seek
    plan_subset = cache._conn.execute(
        """
        EXPLAIN QUERY PLAN
        SELECT symbol, begin_time_ms, open FROM kline
         WHERE symbol IN (?, ?) AND trade_type = ? AND begin_time_ms >= ? AND begin_time_ms < ?
        """,
        ("SYM003USDT", "SYM017USDT", "swap", base_ms, base_ms + 25 * 300000),
    ).fetchall()
    subset_detail = " ".join(str(tuple(r)) for r in plan_subset)
    assert "SEARCH kline USING" in subset_detail and "SCAN kline" not in subset_detail
    cache.close()


# ============================================================================
# P1: Zero-Revision Restart (WS Authority Sync + Open-Interval Gap Fill)
# ============================================================================


def test_rolling_restart_syncs_ws_authority_makes_zero_rest_calls_and_preserves_1h_exact(tmp_path, monkeypatch):
    from core.history_data import candle as c_mod
    from core.pipeline.resample import merge_resample_bounds_cache, resample_history, _get_cached_resample_bounds
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager
    from core.ws.ws_local_cache import WsLocalCache

    root = tmp_path / "data"
    b5 = root / "binance_swap_5m"
    r1_dir = root / "binance_swap_1h_resample" / "25m"
    exg_dir = root / "exg"
    b5.mkdir(parents=True)
    r1_dir.mkdir(parents=True)
    exg_dir.mkdir(parents=True)

    res_mgr = CandleFileManager(r1_dir, "pickle")
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["BTCUSDT"]}))

    # Seed 32 days of 5m history up to 02:55 UTC (run_time = 03:00 UTC, like 10:57:53 CST restart!)
    run_time = datetime(2026, 11, 25, 3, 0, tzinfo=timezone.utc)
    t_5m = pd.date_range(end=run_time - timedelta(minutes=5), periods=31 * 288, freq="5min", tz="UTC")
    df_ws_true = pd.DataFrame({
        "candle_begin_time": t_5m,
        "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
        "volume": 10.0, "quote_volume": 1010.0,
        "trade_num": pd.Series([5] * len(t_5m), dtype="int64"),
        "taker_buy_base_asset_volume": 5.0, "taker_buy_quote_asset_volume": 505.0,
    })

    # Simulate pre-restart state:
    # 1) 1h_resample/25m was cleanly built from WS up to 02:25 (last closed 1h bar = 01:25 -> 02:25!)
    from core.pipeline.resample import calc_resample, fill_gaps
    df_1h_before = fill_gaps(
        calc_resample(df_ws_true[df_ws_true["candle_begin_time"] < datetime(2026, 11, 25, 2, 25, tzinfo=timezone.utc)], "1h", "25m", t_5m[0].to_pydatetime()),
        timedelta(hours=1),
    )
    res_mgr.set_candle("BTCUSDT", datetime(2026, 11, 25, 2, 25, tzinfo=timezone.utc), df_1h_before)
    finalized_before = res_mgr.read_candle("BTCUSDT").copy()

    # 2) TSManager had TWO real-world hazards:
    #    a) 5 hours ago, an old REST row (close=999.9) was in TSManager while flat_pkl/ws_cache had true WS (101.0)
    #    b) The very last shutdown bar (02:55) had landed in ws_cache, NOT yet written to TSManager!
    df_ts_dirty = df_ws_true.iloc[:-1].copy()
    df_ts_dirty.iloc[-60, df_ts_dirty.columns.get_loc("close")] = 999.9
    ts_mgr = TSManager(b5 / "BTCUSDT")
    ts_mgr.update(df_ts_dirty)

    # 3) ws_cache has the last 120 closed WS bars (including true 101.0 5h ago AND the 02:55 shutdown bar!)
    ws_cache = WsLocalCache(root / "ws_cache" / "swap.sqlite")
    for row in df_ws_true.tail(120).itertuples(index=False):
        ws_cache.write("BTCUSDT", "swap", {
            "begin_time_ms": int(row.candle_begin_time.timestamp() * 1000),
            "open": float(row.open), "high": float(row.high), "low": float(row.low), "close": float(row.close),
            "volume": float(row.volume), "quote_volume": float(row.quote_volume), "trade_num": int(row.trade_num),
            "taker_buy_base": float(row.taker_buy_base_asset_volume), "taker_buy_quote": float(row.taker_buy_quote_asset_volume),
        })
    ws_cache.close()

    # Spy fetcher: if Stage 2 calls REST at all, fail / poison with 777.7!
    rest_calls = []

    class PoisonFetcher:
        def batch_size(self):
            return 80
        def once_candles(self):
            return 99
        async def acquire(self, n):
            pass
        async def get_candle(self, symbol, interval, limit=99, **kw):
            rest_calls.append((symbol, kw))
            poison = df_ws_true.tail(5).copy()
            poison["close"] = 777.7
            return poison

    monkeypatch.setattr(c_mod, "create_fetcher", lambda *a, **kw: PoisonFetcher())

    h = SimpleNamespace(
        base_dir=root,
        base_interval="5m",
        resample_interval="1h",
        enabled_hour_offsets=("25m",),
        base_5m_retention_days=30,
        base_5m_retention_bars=lambda: 30 * 288,
        http_timeout_sec=5,
        history_trim_ts=lambda: None,
        effective_kline_count_1h=lambda tt, rt: 15000,
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=b5, resample_mgr_map={"25m": res_mgr}, once_update_candles=99),
    )

    bf = c_mod.init_history_candles(h, "swap", run_time, ["BTCUSDT"])
    assert bf == set()
    # Zero REST requests during rolling gap-free restart!
    assert rest_calls == []

    # TSManager must now contain the 02:55 bar AND the repaired 101.0 WS value from 5h ago!
    ts_after = ts_mgr.read_all()
    assert ts_after["candle_begin_time"].max() == pd.Timestamp("2026-11-25 02:55", tz="UTC")
    assert (ts_after.tail(100)["close"] == 101.0).all()

    bounds_out = resample_history(h, "swap", run_time, full_backfill_symbols=bf)
    if isinstance(bounds_out, dict):
        merge_resample_bounds_cache(bounds_out)

    # All finalized 1h bars (<= 01:25) in 1h_resample/25m MUST be 100% bit-for-bit identical (ZERO replay!)
    after_1h = res_mgr.read_candle("BTCUSDT")
    after_finalized = after_1h[after_1h["candle_begin_time"] <= finalized_before["candle_begin_time"].max()]
    pd.testing.assert_frame_equal(after_finalized.reset_index(drop=True), finalized_before.reset_index(drop=True), check_exact=True)
    # And bounds cache must be pre-warmed for immediate deferred resample on Cycle 1!
    assert _get_cached_resample_bounds(res_mgr, "BTCUSDT") is not None


def test_outage_restart_fetches_strictly_after_last_ws_bar_without_overwriting_pre_outage(tmp_path, monkeypatch):
    from core.history_data import candle as c_mod
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager

    root = tmp_path / "data"
    b5 = root / "binance_swap_5m"
    r1_dir = root / "binance_swap_1h_resample" / "25m"
    b5.mkdir(parents=True)
    r1_dir.mkdir(parents=True)
    res_mgr = CandleFileManager(r1_dir, "pickle")

    # Simulate 25-minute outage: last known WS bar is 01:55, restart run_time is 02:25 (missing 02:00..02:20!)
    run_time = datetime(2026, 11, 25, 2, 25, tzinfo=timezone.utc)
    t_pre = pd.date_range(end=datetime(2026, 11, 25, 1, 55, tzinfo=timezone.utc), periods=31 * 288, freq="5min", tz="UTC")
    df_pre = pd.DataFrame({
        "candle_begin_time": t_pre,
        "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
        "volume": 10.0, "quote_volume": 505.0,
        "trade_num": pd.Series([3] * len(t_pre), dtype="int64"),
        "taker_buy_base_asset_volume": 5.0, "taker_buy_quote_asset_volume": 252.5,
    })
    TSManager(b5 / "ETHUSDT").update(df_pre)
    res_mgr.save_data_file("ETHUSDT", pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2026-11-25 00:25"], utc=True),
        "open": [50.0], "high": [51.0], "low": [49.0], "close": [50.5],
        "volume": [120.0], "quote_volume": [6060.0], "trade_num": [36],
        "taker_buy_base_asset_volume": [60.0], "taker_buy_quote_asset_volume": [3030.0],
    }))

    seen_start_ms = []

    class OutageFetcher:
        def batch_size(self):
            return 80
        def once_candles(self):
            return 99
        async def acquire(self, n):
            pass
        async def get_candle(self, symbol, interval, limit=99, startTime=None, **kw):
            seen_start_ms.append(startTime)
            # Even if exchange would return a different value at 01:55, startTime MUST be 02:00!
            t_gap = pd.date_range("2026-11-25 02:00", "2026-11-25 02:20", freq="5min", tz="UTC")
            return pd.DataFrame({
                "candle_begin_time": t_gap,
                "open": 88.0, "high": 89.0, "low": 87.0, "close": 88.5,
                "volume": 20.0, "quote_volume": 1770.0,
                "trade_num": pd.Series([9] * len(t_gap), dtype="int64"),
                "taker_buy_base_asset_volume": 10.0, "taker_buy_quote_asset_volume": 885.0,
            })

    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def dummy_session(*a, **kw):
        yield object()

    monkeypatch.setattr(c_mod, "create_aiohttp_session", dummy_session)
    monkeypatch.setattr(c_mod, "create_fetcher", lambda *a, **kw: OutageFetcher())
    h = SimpleNamespace(
        base_dir=root, base_interval="5m", resample_interval="1h",
        enabled_hour_offsets=("25m",), base_5m_retention_days=30,
        base_5m_retention_bars=lambda: 30 * 288, http_timeout_sec=5,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=b5, resample_mgr_map={"25m": res_mgr}, once_update_candles=99),
    )
    c_mod.init_history_candles(h, "swap", run_time, ["ETHUSDT"])

    expected_0200_ms = int(datetime(2026, 11, 25, 2, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert seen_start_ms == [expected_0200_ms]
    ts_all = TSManager(b5 / "ETHUSDT").read_all()
    assert ts_all.loc[ts_all["candle_begin_time"] == pd.Timestamp("2026-11-25 01:55", tz="UTC"), "close"].iloc[0] == 50.5
    assert ts_all.loc[ts_all["candle_begin_time"] == pd.Timestamp("2026-11-25 02:20", tz="UTC"), "close"].iloc[0] == 88.5


def test_run_startup_catchup_skips_covered_ws_symbols_and_fetches_only_missed(tmp_path, monkeypatch):
    from core.pipeline import update_cycle as uc
    from core.storage.candle_manager import CandleFileManager
    from core.ws.ws_local_cache import WsLocalCache

    root = tmp_path / "data"
    sp_dir = root / "spot_5m"
    sw_dir = root / "swap_5m"
    exg_dir = root / "exg"
    for d in (sp_dir, sw_dir, exg_dir, sp_dir / "BTCUSDT", sw_dir / "BTCUSDT"):
        d.mkdir(parents=True)

    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["BTCUSDT", "MISSUSDT"]}))

    rt = datetime(2026, 11, 25, 3, 5, tzinfo=timezone.utc)
    target_ms = int((rt - timedelta(minutes=5)).timestamp() * 1000)
    prev_ms = target_ms - 300_000

    for tt in ("spot", "swap"):
        c = WsLocalCache(root / "ws_cache" / f"{tt}.sqlite")
        for ms in (prev_ms, target_ms):
            c.write("BTCUSDT", tt, {
                "begin_time_ms": ms, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
                "volume": 1.0, "quote_volume": 1.0, "trade_num": 1, "taker_buy_base": 0.5, "taker_buy_quote": 0.5,
            })
        c.close()

    monkeypatch.setattr(uc, "next_run_time", lambda _: rt + timedelta(minutes=5))
    fetch_calls = []

    def fake_fetch(handle, r_time, resample_inline=False, symbols_subset=None, symbol_limits=None):
        fetch_calls.append((symbols_subset, symbol_limits))
        df_miss = pd.DataFrame({"candle_begin_time": pd.to_datetime(["2026-11-25 03:00"], utc=True), "close": [2.0]})
        return {}, {"MISSUSDT": df_miss}, {}, {}

    monkeypatch.setattr(uc, "fetch_update_binance_candles", fake_fetch)
    routed = []
    router = SimpleNamespace(put=lambda item: routed.append(item["symbol"]))
    h = SimpleNamespace(
        base_dir=root, base_interval="5m", exg_mgr=exg_mgr,
        get_market_handle=lambda tt: SimpleNamespace(base_candle_dir=sp_dir if tt == "spot" else sw_dir, once_update_candles=99),
    )

    assert uc.run_startup_catchup(h, router, settle_sec=0.0) is True
    assert len(fetch_calls) == 1
    subset_arg, limits_arg = fetch_calls[0]
    assert subset_arg == {"spot": set(), "swap": {"MISSUSDT"}}
    assert limits_arg["swap"]["MISSUSDT"] == 5
    assert routed == ["MISSUSDT"]





