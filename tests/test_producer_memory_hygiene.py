"""TDD suite for producer memory hygiene & consumer-window isolation.

Covers:
1. Compute pool lifecycle: release immediately after .ready + T-15s prewarm.
2. POSIX_FADV_DONTNEED page-cache drop for write-only PKLs (batch*.pkl & 1h_resample/*.pkl),
   leaving market_pivot_*.pkl hot in page cache.
3. Deferred bg_1h_flush pending slot on RuntimeSupervisor, flushed on idle 5m tick,
   same-offset next cycle, or supervisor.shutdown().
4. coin_cap idle-tick cooldown + cached max date (eliminating aux_exe in run_ws_cycle when
   data_api is disabled) and maybe_backfill active-consumer yield guard.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest


# ============================================================================
# 1. Compute Pool Release + T-15s Prewarm
# ============================================================================


def test_supervisor_release_and_warm_compute_pool_lifecycle(tmp_path, monkeypatch):
    from core.pipeline import runtime_supervisor as rs

    created = []
    shutdown_calls = []

    class FakePool:
        def __init__(self, max_workers=None, mp_context=None):
            self._max_workers = max_workers
            self._broken = False
            self.submitted = []
            created.append(self)

        def submit(self, fn, *args, **kwargs):
            self.submitted.append((fn, args))
            return SimpleNamespace(result=lambda timeout=None: os.getpid())

        def shutdown(self, wait=True, cancel_futures=False):
            shutdown_calls.append((self, wait, cancel_futures))

    monkeypatch.setattr(rs, "ProcessPoolExecutor", FakePool)

    handle = SimpleNamespace(
        exchange="binance",
        base_dir=tmp_path,
        num_writers=1,
        precompute_factor_workers=2,
        spot=SimpleNamespace(base_candle_dir=tmp_path / "spot_5m", once_update_candles=99),
        swap=SimpleNamespace(base_candle_dir=tmp_path / "swap_5m", once_update_candles=99),
        base_interval="5m",
        proxy=None,
    )
    sup = rs.RuntimeSupervisor(handle=handle, alert_webhook_url="")

    # Warm creates pool and submits worker prewarm tasks once
    pool1 = sup.warm_compute_pool()
    assert pool1 is created[0]
    assert len(pool1.submitted) == 2

    # Second warm on already-warmed pool is a no-op
    assert sup.warm_compute_pool() is pool1
    assert len(pool1.submitted) == 2

    # Release shuts down pool and clears reference; second release returns False
    assert sup.release_compute_pool(wait=True) is True
    assert shutdown_calls[-1] == (pool1, True, True)
    assert sup.release_compute_pool(wait=True) is False

    # Next get_compute_pool creates a fresh pool
    pool2 = sup.get_compute_pool()
    assert pool2 is not pool1
    assert len(created) == 2


def test_ws_cycle_releases_compute_pool_and_app_warms_at_t_minus_15s(tmp_path, monkeypatch):
    from core import app
    from core.pipeline import update_cycle as uc

    events = []
    rt = datetime(2026, 11, 26, 6, 25, tzinfo=timezone.utc)

    class SpySupervisor:
        def __init__(self):
            self.router = SimpleNamespace(put=lambda *a, **kw: None)

        def warm_compute_pool(self):
            events.append("warm_pool")

        def get_compute_pool(self):
            events.append("get_pool")
            return SimpleNamespace(_max_workers=2)

        def release_compute_pool(self, *, wait=False):
            events.append("release_pool")
            return True

        def flush_pending_1h(self, offset_str=None):
            events.append(("flush_pending", offset_str))
            return 0

        def defer_1h_flush(self, offset_str, run_time, flush_fns):
            events.append(("defer_1h", offset_str, len(flush_fns)))

        def refresh_ws_subscriptions(self, handle):
            pass

        def health_check(self, alert):
            pass

    sup = SpySupervisor()

    # 1) Verify app.update_all warms compute pool right after T-15s before update_meta
    monkeypatch.setattr(app, "sleep_until_run_time", lambda dt: events.append(("sleep", dt)))
    monkeypatch.setattr(app.time, "sleep", lambda s: None)

    async def fake_update_meta(h, run_time):
        events.append("update_meta")

    monkeypatch.setattr(app, "update_meta", fake_update_meta)
    monkeypatch.setattr(app, "update_symbol_match", lambda h, r: events.append("symbol_match"))
    monkeypatch.setattr(
        app,
        "run_ws_cycle",
        lambda h, r, s: (events.append("run_ws_cycle"), uc.CycleResult(is_binance_success=True))[1],
    )

    handle = SimpleNamespace(
        base_interval="5m",
        resample_interval="1h",
        enabled_hour_offsets=("25m",),
        data_api=None,
        coin_cap_handle=None,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={}),
    )
    app.update_all(handle, rt, cfg=SimpleNamespace(error_webhook_url=""), supervisor=sup)
    assert events.index("warm_pool") < events.index("update_meta") < events.index("run_ws_cycle")

    # 2) Verify run_ws_cycle releases compute pool immediately after preprocess
    events.clear()
    monkeypatch.setattr(
        uc,
        "_start_rest_fallback",
        lambda h, r, router: (
            SimpleNamespace(join=lambda timeout=None: None, is_alive=lambda: False),
            {"cancel": SimpleNamespace(set=lambda: None), "confirmed_empty": {}},
        ),
    )
    monkeypatch.setattr(
        uc,
        "_barrier_and_resample",
        lambda h, tt, r, rs, **kw: {
            "trade_type": tt,
            "success": 1,
            "total": 1,
            "skipped": [],
            "has_gaps": False,
            "begin_ts": {"BTCUSDT": (rt, rt)},
            "tail_dfs": {},
            "flush_1h": lambda: events.append(f"flushed_{tt}"),
        },
    )
    monkeypatch.setattr(
        uc,
        "choose_publication_inputs",
        lambda *a, **kw: ({"spot": {}, "swap": {}}, {"BTCUSDT": (rt, rt)}, {"BTCUSDT": (rt, rt)}),
    )
    monkeypatch.setattr(
        uc,
        "_run_preprocess",
        lambda *a, **kw: events.append("preprocess_done"),
    )

    res = uc.run_ws_cycle(handle, rt, sup)
    assert res.is_binance_success is True
    assert "preprocess_done" in events
    assert "release_pool" in events
    assert events.index("preprocess_done") < events.index("release_pool")
    # 1h flush must be deferred (not executed inline) when published=True
    assert "flushed_spot" not in events
    assert "flushed_swap" not in events
    assert ("defer_1h", "25m", 2) in events


# ============================================================================
# 2. POSIX_FADV_DONTNEED for Write-Only PKLs Only
# ============================================================================


def test_drop_file_page_cache_invokes_posix_fadvise_and_handles_missing_api(tmp_path, monkeypatch):
    from core.storage import candle_manager as cm

    p = tmp_path / "sample.pkl"
    p.write_bytes(b"hello")

    # Case A: posix_fadvise present (Linux)
    advise_calls = []
    monkeypatch.setattr(os, "POSIX_FADV_DONTNEED", 4, raising=False)
    monkeypatch.setattr(
        os,
        "posix_fadvise",
        lambda fd, offset, length, advice: advise_calls.append((fd, offset, length, advice)),
        raising=False,
    )
    assert cm.drop_file_page_cache(p) is True
    assert len(advise_calls) == 1
    assert advise_calls[0][1:] == (0, 0, 4)

    # Case B: posix_fadvise absent (Windows)
    monkeypatch.delattr(os, "posix_fadvise", raising=False)
    assert cm.drop_file_page_cache(p) is False


def test_raw_batch_and_1h_resample_drop_page_cache_while_pivot_stays_hot(tmp_path, monkeypatch):
    from core.pipeline import resample as res_mod
    from core.storage import candle_manager as cm
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp

    dropped_names = []

    def spy_drop(path):
        dropped_names.append(Path(path).name)
        return True

    monkeypatch.setattr(cm, "drop_file_page_cache", spy_drop)
    monkeypatch.setattr(pp, "drop_file_page_cache", spy_drop, raising=False)
    monkeypatch.setattr(res_mod, "drop_file_page_cache", spy_drop, raising=False)

    times = pd.date_range("2026-11-20 00:25", periods=20, freq="h", tz="UTC")
    rt = times[-1].to_pydatetime()
    res_mgr = CandleFileManager(tmp_path / "res" / "25m", "pickle")
    pre_mgr = CandleFileManager(tmp_path / "pre" / "25m", "pickle")
    res_mgr.base_dir.mkdir(parents=True)
    pre_mgr.base_dir.mkdir(parents=True)

    df = pd.DataFrame({
        "candle_begin_time": times,
        "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 100.0, "quote_volume": 1050.0,
        "trade_num": pd.Series([5] * len(times), dtype="int64"),
        "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
    })

    # 1) 1h_resample write -> MUST drop page cache
    res_mod.update_resample_candle_incremental(
        res_mgr, "BTCUSDT", df, None, rt, timedelta(hours=1)
    )
    assert "BTCUSDT.pkl" in dropped_names

    # 2) Raw batch write -> MUST drop page cache
    h = SimpleNamespace(
        resample_interval="1h",
        fetch_funding_rate=False,
        data_api=None,
        history_trim_ts=lambda: None,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": pre_mgr}),
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
    )
    df_match = pd.DataFrame([{
        "spot": "BTCUSDT", "swap": "BTCUSDT",
        "first_begin_time": times[0], "last_begin_time": times[-1],
    }])
    year_pivot_data = {}
    pp.gen_batch_data_incremental(
        h, rt, "spot", df_match, ["BTCUSDT"], "batch1", year_pivot_out=year_pivot_data
    )
    assert "spot_dict_batch1.pkl" in dropped_names

    # 3) Market pivot write -> MUST stay hot in page cache (NOT dropped!)
    dropped_names.clear()
    pp.gen_pivot_incremental(h, rt, "spot", {}, year_pivot_data=year_pivot_data)
    assert not any("market_pivot" in name for name in dropped_names)


# ============================================================================
# 3. Deferred bg_1h_flush Pending Slot + Idle Tick + Shutdown Safety
# ============================================================================


def test_supervisor_defer_flush_pending_1h_and_shutdown_safety(tmp_path):
    from core.pipeline.runtime_supervisor import RuntimeSupervisor

    handle = SimpleNamespace(
        exchange="binance",
        base_dir=tmp_path,
        num_writers=1,
        precompute_factor_workers=1,
        spot=SimpleNamespace(base_candle_dir=tmp_path / "spot_5m", once_update_candles=99),
        swap=SimpleNamespace(base_candle_dir=tmp_path / "swap_5m", once_update_candles=99),
        base_interval="5m",
        proxy=None,
    )
    sup = RuntimeSupervisor(handle=handle, alert_webhook_url="")
    ran = []
    rt1 = datetime(2026, 11, 26, 6, 25, tzinfo=timezone.utc)
    rt2 = datetime(2026, 11, 26, 7, 25, tzinfo=timezone.utc)

    sup.defer_1h_flush("25m", rt1, [lambda: ran.append("25m_r1")])
    sup.defer_1h_flush("15m", rt1, [lambda: ran.append("15m_r1")])
    assert ran == []  # Deferred, not executed yet

    # Overwriting same offset flushes older pending entry first
    sup.defer_1h_flush("25m", rt2, [lambda: ran.append("25m_r2")])
    assert ran == ["25m_r1"]

    # Targeted flush flushes only 25m
    n = sup.flush_pending_1h("25m")
    assert n == 1
    assert ran == ["25m_r1", "25m_r2"]

    # Shutdown flushes remaining pending 15m before exiting
    sup.shutdown(timeout_sec=1.0)
    assert ran == ["25m_r1", "25m_r2", "15m_r1"]


def test_idle_tick_in_app_flushes_pending_1h_and_runs_idle_coin_cap(monkeypatch):
    from core import app

    events = []
    rt_idle = datetime(2026, 11, 26, 6, 30, tzinfo=timezone.utc)

    monkeypatch.setattr(app, "sleep_until_run_time", lambda dt: None)

    async def fake_meta(h, r):
        events.append("meta")

    monkeypatch.setattr(app, "update_meta", fake_meta)
    monkeypatch.setattr(app, "update_symbol_match", lambda h, r: None)
    monkeypatch.setattr(app, "maybe_backfill", lambda h: events.append("backfill"))
    monkeypatch.setattr(
        app,
        "maybe_update_coin_cap_idle",
        lambda h, r: events.append("coin_cap_idle"),
        raising=False,
    )

    sup = SimpleNamespace(
        refresh_ws_subscriptions=lambda h: None,
        health_check=lambda a: None,
        flush_pending_1h=lambda offset_str=None: events.append("flush_pending_1h"),
    )
    handle = SimpleNamespace(
        base_interval="5m",
        enabled_hour_offsets=("25m",),
        coin_cap_handle=object(),
    )
    app.update_all(handle, rt_idle, cfg=SimpleNamespace(error_webhook_url=""), supervisor=sup)
    assert "flush_pending_1h" in events
    assert "coin_cap_idle" in events
    assert events.index("flush_pending_1h") < events.index("backfill")


# ============================================================================
# 4. coin_cap Cached Date / No aux_exe in run_ws_cycle + maybe_backfill Yield
# ============================================================================


def test_coin_cap_caches_latest_date_and_skips_aux_exe_in_ws_cycle(tmp_path, monkeypatch):
    from core.ingest import coin_cap as cc
    from core.pipeline import update_cycle as uc
    from core.storage.candle_manager import CandleFileManager

    cc_mgr = CandleFileManager(tmp_path / "coin_cap", "pickle")
    cc_mgr.base_dir.mkdir(parents=True)
    rt = datetime(2026, 11, 26, 6, 25, tzinfo=timezone.utc)
    cc_mgr.set_candle("BTCUSDT", rt, pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2026-11-25 00:00:00"], utc=True),
        "symbol": ["BTCUSDT"],
    }))

    read_count = []
    orig_read = cc_mgr.read_candle

    def counting_read(sym):
        read_count.append(sym)
        return orig_read(sym)

    monkeypatch.setattr(cc_mgr, "read_candle", counting_read)

    class FakeApi:
        def __init__(self, *a, **kw):
            pass

        async def aioreq_product_infos(self):
            return pd.DataFrame(
                {"update_time": [pd.Timestamp("2026-11-26 00:00:00", tz="UTC")]},
                index=["coin-cap-daily"],
            )

    monkeypatch.setattr(cc, "QuantclassDataApi", FakeApi)
    monkeypatch.setattr(cc, "async_retry_getter", lambda fn, **kw: fn(**kw))

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

    monkeypatch.setattr(cc, "create_aiohttp_session", lambda *a, **kw: FakeSession())

    handle = SimpleNamespace(
        http_timeout_sec=5,
        proxy=None,
        coin_cap_use_proxy=False,
        coin_cap_handle=SimpleNamespace(
            coin_cap_mgr=cc_mgr,
            cfg=SimpleNamespace(data_api_key="k", data_api_uuid="u"),
        ),
    )

    # First idle check scans disk once and caches max date; second check within cooldown skips entirely!
    cc.reset_coin_cap_idle_state()
    assert cc.maybe_update_coin_cap_idle(handle, rt, min_interval_sec=1800.0) is True
    assert len(read_count) == 1
    assert cc.maybe_update_coin_cap_idle(handle, rt, min_interval_sec=1800.0) is None
    assert len(read_count) == 1

    # After cooldown expires, cached max date avoids re-reading all PKLs from disk!
    assert cc.maybe_update_coin_cap_idle(handle, rt, min_interval_sec=0.0) is True
    assert len(read_count) == 1

    # Verify run_ws_cycle does NOT spawn aux_exe ProcessPoolExecutor when data_api is None
    monkeypatch.setattr(
        uc,
        "ProcessPoolExecutor",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("aux_exe must not be spawned when data_api is None")),
    )
    monkeypatch.setattr(
        uc,
        "_start_rest_fallback",
        lambda h, r, router: (
            SimpleNamespace(join=lambda timeout=None: None, is_alive=lambda: False),
            {"cancel": SimpleNamespace(set=lambda: None), "confirmed_empty": {}},
        ),
    )
    monkeypatch.setattr(
        uc,
        "_barrier_and_resample",
        lambda h, tt, r, rs, **kw: {
            "trade_type": tt, "success": 1, "total": 1, "skipped": [],
            "has_gaps": False, "begin_ts": {"BTCUSDT": (rt, rt)}, "tail_dfs": {}, "flush_1h": None,
        },
    )
    monkeypatch.setattr(
        uc,
        "choose_publication_inputs",
        lambda *a, **kw: ({"spot": {}, "swap": {}}, {"BTCUSDT": (rt, rt)}, {"BTCUSDT": (rt, rt)}),
    )
    monkeypatch.setattr(uc, "_run_preprocess", lambda *a, **kw: None)

    ws_handle = SimpleNamespace(
        base_interval="5m",
        resample_interval="1h",
        enabled_hour_offsets=("25m",),
        data_api=None,
        coin_cap_handle=handle.coin_cap_handle,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={}),
    )
    sup = SimpleNamespace(
        router=SimpleNamespace(put=lambda *a, **kw: None),
        get_compute_pool=lambda: SimpleNamespace(_max_workers=2),
        release_compute_pool=lambda **kw: True,
        flush_pending_1h=lambda offset_str=None: 0,
    )
    res = uc.run_ws_cycle(ws_handle, rt, sup)
    assert res.is_binance_success is True


def test_maybe_backfill_yields_when_active_consumer_detected(tmp_path, monkeypatch):
    from core.pipeline import backfill as bf

    pre_25m = tmp_path / "pre" / "25m"
    pre_25m.mkdir(parents=True)
    # Active consumer .arrow-demand present -> maybe_backfill MUST yield without cooldown!
    (pre_25m / ".arrow-demand").write_text("full", encoding="utf-8")

    pre_45m = tmp_path / "pre" / "45m"
    pre_45m.mkdir(parents=True)

    handle = SimpleNamespace(
        enabled_hour_offsets=("25m", "45m"),
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={
                "25m": SimpleNamespace(base_dir=pre_25m),
                "45m": SimpleNamespace(base_dir=pre_45m),
            }
        ),
    )

    bf._last_attempt.clear()
    monkeypatch.setattr(bf, "seconds_to_next_offset", lambda h, now: 600.0)
    monkeypatch.setattr(
        bf,
        "ProcessPoolExecutor",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("must not launch backfill while consumer is active")),
    )

    elapsed = bf.maybe_backfill(handle)
    assert elapsed == 0.0
    assert bf._last_attempt == {}
