"""TDD tests for 2026-11-26 post-comparison producer optimizations (Items 3 & 4).

Covers:
1. Item 3: preprocess_offset_data deterministically submits all spot batches to
   batch_executor BEFORE any swap batch, guaranteeing spot finishes in Wave 1 on
   the 4-worker pool and eliminating 0.005s spot_early_lead_s regressions.
2. Item 4: app.update_all wakes enabled offsets at T-22s (giving +7s safety cushion
   before run_time) while keeping disabled offsets at T-15s and skipping redundant
   767-symbol realtime forecast funding PKL writes on disabled idle 5m ticks.
"""
from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest


# ============================================================================
# Item 3: Deterministic spot-first batch submission in preprocess_offset_data
# ============================================================================


def test_preprocess_offset_data_deterministically_submits_spot_batches_before_swap(tmp_path, monkeypatch):
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp

    run_time = datetime(2026, 11, 26, 12, 25, tzinfo=timezone.utc)
    exg_mgr = CandleFileManager(tmp_path / "exg", "pickle")
    pre_mgr = CandleFileManager(tmp_path / "pre" / "25m", "pickle")
    spot_res = CandleFileManager(tmp_path / "spot_1h" / "25m", "pickle")
    swap_res = CandleFileManager(tmp_path / "swap_1h" / "25m", "pickle")
    for mgr in (exg_mgr, pre_mgr, spot_res, swap_res):
        mgr.base_dir.mkdir(parents=True, exist_ok=True)

    spot_syms = [f"S{i}USDT" for i in range(6)]
    swap_syms = [f"W{i}USDT" for i in range(10)]
    df_match = pd.DataFrame([
        {"spot": s, "swap": s, "first_begin_time": run_time - timedelta(hours=5), "last_begin_time": run_time - timedelta(hours=1)}
        for s in spot_syms + swap_syms
    ])
    exg_mgr.set_candle("spot_swap_matches", run_time, df_match)
    exg_mgr.set_candle("exginfo_spot", run_time, pd.DataFrame({"symbol": spot_syms}))
    exg_mgr.set_candle("exginfo_swap", run_time, pd.DataFrame({"symbol": swap_syms}))

    monkeypatch.setattr(pp, "preprocess_match_symbols", lambda *a, **kw: df_match)
    monkeypatch.setattr(pp, "_load_factor_modules_params", lambda: [])
    monkeypatch.setattr(pp, "publish_arrow_ipc_batches", lambda *a, **kw: None)
    monkeypatch.setattr(pp, "cleanup_old_snapshots", lambda *a, **kw: None)
    monkeypatch.setattr(pp, "gen_pivot_incremental", lambda *a, **kw: None)
    monkeypatch.setattr(pp, "remember_ready_origins", lambda *a, **kw: None)

    orig_read_candle = CandleFileManager.read_candle

    def _delayed_spot_read_candle(self, name, *a, **kw):
        # Deliberately slow down spot exginfo read by 40ms to prove swap still
        # waits for spot's batch_executor submissions instead of racing ahead!
        if name == "exginfo_spot":
            time.sleep(0.04)
        return orig_read_candle(self, name, *a, **kw)

    monkeypatch.setattr(CandleFileManager, "read_candle", _delayed_spot_read_candle)

    submitted_order = []
    lock = threading.Lock()

    class _RecordingBatchExecutor:
        def submit(self, fn, handle, rt, trade_type, match_df, b_syms, batch_name, *rest):
            with lock:
                submitted_order.append((trade_type, batch_name))
            return SimpleNamespace(
                result=lambda: ({}, {}, {}, {}, b"schema", [(b_syms[0], b"ipc")], True)
            )

    handle = SimpleNamespace(
        resample_interval="1h",
        fetch_funding_rate=False,
        data_api=None,
        publish_kline_num=5000,
        arrow_demand_enabled=False,
        precompute_factor_workers=4,
        history_trim_date=None,
        exg_mgr=exg_mgr,
        preprocess_handle=SimpleNamespace(
            num_batch=2,
            preprocess_mgr_map={"25m": pre_mgr},
        ),
        get_market_handle=lambda tt: SimpleNamespace(
            resample_mgr_map={"25m": spot_res if tt == "spot" else swap_res},
        ),
    )

    pp.preprocess_offset_data(handle, run_time, batch_executor=_RecordingBatchExecutor())

    trade_types = [tt for tt, _ in submitted_order]
    assert trade_types.count("spot") == 3
    assert trade_types.count("swap") == 5
    # Every spot batch MUST be queued before the very first swap batch
    assert trade_types == ["spot"] * 3 + ["swap"] * 5


# ============================================================================
# Item 4: T-22s enabled offset wake cushion & idle 5m realtime funding skip
# ============================================================================


def test_enabled_offset_wakes_at_t_minus_22s_and_disabled_offset_skips_realtime_funding_writes(tmp_path, monkeypatch):
    from core import app
    from core.exchange.rest.binance_fetcher import BinanceFetcher
    from core.pipeline import data_update as du
    from core.pipeline import update_cycle as uc
    from core.storage.candle_manager import CandleFileManager

    rt_enabled = datetime(2026, 11, 26, 12, 25, tzinfo=timezone.utc)
    rt_idle = datetime(2026, 11, 26, 12, 30, tzinfo=timezone.utc)

    sleeps = []
    meta_calls = []

    monkeypatch.setattr(app, "sleep_until_run_time", sleeps.append)
    monkeypatch.setattr(app.time, "sleep", lambda s: None)

    async def _spy_update_meta(h, r, *, include_realtime_funding=True):
        meta_calls.append((r, include_realtime_funding))

    monkeypatch.setattr(app, "update_meta", _spy_update_meta)
    monkeypatch.setattr(app, "update_symbol_match", lambda h, r: None)
    monkeypatch.setattr(app, "maybe_backfill", lambda h: None)
    monkeypatch.setattr(
        app,
        "run_ws_cycle",
        lambda h, r, s: uc.CycleResult(is_binance_success=True),
    )

    sup = SimpleNamespace(
        warm_compute_pool=lambda: None,
        refresh_ws_subscriptions=lambda h: None,
        health_check=lambda a: None,
        flush_pending_1h=lambda offset_str=None: 0,
    )
    h = SimpleNamespace(
        base_interval="5m",
        resample_interval="1h",
        enabled_hour_offsets=("25m",),
        data_api=None,
        coin_cap_handle=None,
    )

    # 1) Enabled offset wakes at T-22s with include_realtime_funding=True
    app.update_all(h, rt_enabled, cfg=SimpleNamespace(error_webhook_url=""), supervisor=sup)
    assert sleeps[0] == rt_enabled - timedelta(seconds=22)
    assert meta_calls[-1] == (rt_enabled, True)

    # 2) Disabled offset keeps T-15s and passes include_realtime_funding=False
    sleeps.clear()
    app.update_all(h, rt_idle, cfg=SimpleNamespace(error_webhook_url=""), supervisor=sup)
    assert sleeps[0] == rt_idle - timedelta(seconds=15)
    assert meta_calls[-1] == (rt_idle, False)

    # 3) Verify update_binance_meta(..., include_realtime_funding=False) skips realtime funding
    #    fetch & pickle rewrites when history is empty, while still saving settled history if non-empty
    funding_mgr = CandleFileManager(tmp_path / "funding", "pickle")
    funding_mgr.base_dir.mkdir(parents=True)
    h_meta = SimpleNamespace(
        exchange="binance",
        http_timeout_sec=3,
        fetch_funding_rate=True,
        proxy=None,
        binance_swap=SimpleNamespace(api_type="usdt_futures"),
        get_market_handle=lambda _: SimpleNamespace(api_type="usdt_futures"),
        get_kline_count_1h=lambda _: 100,
        funding_mgr=funding_mgr,
    )
    monkeypatch.setattr(du, "create_aiohttp_session", lambda *a, **kw: AsyncExitStack())
    monkeypatch.setattr(du, "update_binance_exginfo", AsyncMock(return_value=["BTCUSDT"]))
    monkeypatch.setattr(du.time, "time", lambda: (rt_idle - timedelta(seconds=15)).timestamp())
    rt_fetch_mock = AsyncMock(side_effect=AssertionError("Idle tick must not fetch/write realtime forecast funding"))
    monkeypatch.setattr(BinanceFetcher, "get_realtime_funding_rate", rt_fetch_mock)

    # Case A: hour_complete (empty history) -> 0 pickle writes
    async def _empty_history(*a, **kw):
        return {}, dict(requests=0, success=0, empty=0, failed=0, elapsed=0.0, reason="hour_complete",
                        round_hour=rt_idle, target=1, completed=1, remaining=0)

    monkeypatch.setattr(du, "backfill_recent_funding_rates", _empty_history)
    orig_to_pickle = pd.DataFrame.to_pickle
    monkeypatch.setattr(
        pd.DataFrame,
        "to_pickle",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("No pickle write expected on idle hour_complete")),
    )
    asyncio.run(du.update_meta(h_meta, rt_idle, include_realtime_funding=False))
    rt_fetch_mock.assert_not_called()

    # Case B: non-empty settled history on idle tick -> still persisted!
    monkeypatch.setattr(pd.DataFrame, "to_pickle", orig_to_pickle)
    settled_hour = pd.Timestamp("2026-11-26 08:00:00Z")
    settled_df = pd.DataFrame({"symbol": ["BTCUSDT"], "candle_begin_time": [settled_hour], "funding_rate": [0.00025]})

    async def _nonempty_history(*a, **kw):
        return {"BTCUSDT": settled_df}, dict(
            requests=1, success=1, empty=0, failed=0, elapsed=0.01, reason="complete",
            round_hour=settled_hour, target=1, completed=0, remaining=1,
        )

    monkeypatch.setattr(du, "backfill_recent_funding_rates", _nonempty_history)
    asyncio.run(du.update_meta(h_meta, rt_idle, include_realtime_funding=False))
    assert funding_mgr.read_candle("BTCUSDT")["funding_rate"].tolist() == pytest.approx([0.00025])
    rt_fetch_mock.assert_not_called()
