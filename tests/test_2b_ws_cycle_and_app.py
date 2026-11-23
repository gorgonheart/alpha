import threading
import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest


def test_barrier_exits_early_after_rest_done_for_unopened_listing_without_race(tmp_path):
    """Review Focus #1: unopened new listing exits immediately after rest_done (no 45s wait),
    while a symbol whose .ready lands right before rest_done.set() is NEVER falsely skipped."""
    from core.pipeline.barrier import wait_for_ready_barrier
    from core.storage.candle_manager import CandleFileManager

    mgr = CandleFileManager(tmp_path, "pickle")
    run_time = datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc)
    mgr.save_ws_ready("BTCUSDT", run_time)
    rest_done = threading.Event()
    submitted = set()
    confirmed_empty = set()

    def rest_worker():
        time.sleep(0.08)
        # Simulate REST recovering LATEUSDT and confirming NEWLISTING empty right before setting rest_done
        mgr.save_ready("LATEUSDT", run_time)
        confirmed_empty.add("NEWLISTING")
        rest_done.set()

    threading.Thread(target=rest_worker, daemon=True).start()
    t0 = time.monotonic()
    success, skipped = wait_for_ready_barrier(
        symbols=["BTCUSDT", "LATEUSDT", "NEWLISTING"],
        base_5m_dir=tmp_path,
        run_time=run_time,
        timeout_sec=10.0,
        poll_interval_sec=0.02,
        ws_wait_sec=0.05,
        rest_done=rest_done,
        rest_submitted=submitted,
        rest_confirmed_empty=confirmed_empty,
    )
    elapsed = time.monotonic() - t0
    assert elapsed < 1.0  # Did NOT wait 10s soft timeout
    assert success == {"BTCUSDT", "LATEUSDT"}
    assert skipped == {"NEWLISTING"}


def test_barrier_does_not_skip_ready_written_mid_scan_before_rest_done(monkeypatch, tmp_path):
    """Review Focus #1: if rest_done is NOT set at top of loop, and a symbol's .ready
    appears AFTER that symbol was scanned in the same iteration right before rest_done.set(),
    the barrier MUST perform one more scan after rest_done is set before breaking."""
    from core.pipeline import barrier
    from core.storage.candle_manager import CandleFileManager

    mgr = CandleFileManager(tmp_path, "pickle")
    run_time = datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc)
    rest_done = threading.Event()
    triggered = False

    orig_check_ready = barrier.CandleFileReader.check_ready

    def raced_check_ready(self, sym, rt):
        nonlocal triggered
        res = orig_check_ready(self, sym, rt)
        if not res and sym == "LATEUSDT" and not triggered:
            triggered = True
            # After LATEUSDT was just checked as False, writer saves .ready and REST finishes
            mgr.save_ready("LATEUSDT", run_time)
            rest_done.set()
        return res

    monkeypatch.setattr(barrier.CandleFileReader, "check_ready", raced_check_ready)

    success, skipped = barrier.wait_for_ready_barrier(
        symbols=["LATEUSDT", "NEWLISTING"],
        base_5m_dir=tmp_path,
        run_time=run_time,
        timeout_sec=10.0,
        poll_interval_sec=0.01,
        ws_wait_sec=0.0,
        rest_done=rest_done,
        rest_submitted=set(),
        rest_confirmed_empty={"NEWLISTING"},
    )
    assert success == {"LATEUSDT"}
    assert skipped == {"NEWLISTING"}


def test_publication_sources_confirmed_empty_allows_unopened_listing_and_rejects_rest_failure():
    from core.pipeline.publication_sources import choose_publication_inputs

    stamp = pd.Timestamp("2026-11-23 01:25", tz="UTC")
    bounds = (stamp - pd.Timedelta(hours=3), stamp - pd.Timedelta(hours=1))

    # Case 1: NEW is confirmed empty and has no local history -> allowed to skip
    mgr_new_absent = SimpleNamespace(has_symbol=lambda s: s != "NEW")
    h1 = SimpleNamespace(
        resample_interval="1h",
        exg_mgr=SimpleNamespace(read_candle=lambda _: pd.DataFrame({"symbol": ["OLD", "NEW"]})),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": mgr_new_absent}),
    )
    ws_bn = ({}, {}, {"OLD": bounds}, {"OLD": bounds})
    sources, spot, swap = choose_publication_inputs(
        h1,
        stamp,
        ws_bn,
        None,
        binance_done=True,
        data_api_done=False,
        confirmed_empty={"spot": {"NEW"}, "swap": {"NEW"}},
    )
    assert sources == {"spot": {"OLD": "binance"}, "swap": {"OLD": "binance"}}
    assert spot == {"OLD": bounds} and swap == {"OLD": bounds}

    # Case 2: NEW is NOT in confirmed_empty (e.g. REST failed) -> must NOT skip
    with pytest.raises(ValueError, match="Incomplete"):
        choose_publication_inputs(
            h1,
            stamp,
            ws_bn,
            None,
            binance_done=True,
            data_api_done=True,
            confirmed_empty={"spot": set(), "swap": set()},
        )

    # Case 3: NEW is in confirmed_empty BUT has mature non-empty local history -> must NOT skip
    mgr_mature = SimpleNamespace(
        has_symbol=lambda s: True,
        read_candle=lambda s: pd.DataFrame({"close": [1.0]}),
    )
    h3 = SimpleNamespace(
        resample_interval="1h",
        exg_mgr=SimpleNamespace(read_candle=lambda _: pd.DataFrame({"symbol": ["OLD", "NEW"]})),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": mgr_mature}),
    )
    with pytest.raises(ValueError, match="Incomplete"):
        choose_publication_inputs(
            h3,
            stamp,
            ws_bn,
            None,
            binance_done=True,
            data_api_done=True,
            confirmed_empty={"spot": {"NEW"}, "swap": {"NEW"}},
        )


def test_ws_cycle_launches_preprocess_without_blocking_on_slow_data_api(monkeypatch, tmp_path):
    from core.pipeline import update_cycle as uc

    events = []
    api_gate = threading.Event()
    stamp = pd.Timestamp("2026-11-23 01:25", tz="UTC")
    bounds = (stamp - pd.Timedelta(hours=2), stamp - pd.Timedelta(hours=1))

    class FakeAuxPool:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def submit(self, fn, *args):
            class F:
                def result(self):
                    if fn is uc.update_data_api_candles:
                        events.append("api_result_waited")
                        api_gate.wait(timeout=2)
                        return ({"BTCUSDT": bounds}, {"BTCUSDT": bounds})
                    events.append("coin_cap_waited")
                    return True

            return F()

    monkeypatch.setattr(uc, "ProcessPoolExecutor", FakeAuxPool)
    monkeypatch.setattr(
        uc,
        "_start_rest_fallback",
        lambda *a: (
            SimpleNamespace(join=lambda **kw: None, is_alive=lambda: False),
            {
                "ok": True,
                "done": threading.Event(),
                "submitted": {"spot": set(), "swap": set()},
                "confirmed_empty": {"spot": set(), "swap": set()},
                "failed": {"spot": set(), "swap": set()},
            },
        ),
    )
    monkeypatch.setattr(
        uc,
        "_barrier_and_resample",
        lambda h, tt, rt, rs: {
            "trade_type": tt,
            "success": 1,
            "total": 1,
            "skipped": [],
            "begin_ts": {"BTCUSDT": bounds},
        },
    )

    def fake_preprocess(h, rt, sp, sw):
        events.append("preprocess_started")
        api_gate.set()

    monkeypatch.setattr(uc, "_run_preprocess", fake_preprocess)

    exg_mgr = SimpleNamespace(read_candle=lambda _: pd.DataFrame({"symbol": ["BTCUSDT"]}))
    res_mgr = SimpleNamespace(has_symbol=lambda s: True)
    h = SimpleNamespace(
        resample_interval="1h",
        data_api=object(),
        coin_cap_handle=object(),
        preprocess_handle=object(),
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
    )
    res = uc.run_ws_cycle(h, stamp.to_pydatetime(), SimpleNamespace(router=None))
    assert res.is_binance_success is True
    assert events.index("preprocess_started") < events.index("api_result_waited")


def test_app_non_enabled_offset_runs_meta_supervisor_and_backfill_without_60s_sleep(monkeypatch):
    from core import app

    calls = []

    async def fake_meta(h, rt):
        calls.append("meta")

    monkeypatch.setattr(app, "sleep_until_run_time", lambda t: calls.append(("sleep_until", t)))
    monkeypatch.setattr(app, "update_meta", fake_meta)
    monkeypatch.setattr(app, "update_symbol_match", lambda h, rt: calls.append("match"))
    monkeypatch.setattr(app, "maybe_backfill", lambda h: calls.append("backfill"))
    monkeypatch.setattr(app.time, "sleep", lambda s: (_ for _ in ()).throw(AssertionError(f"unexpected sleep {s}")))
    sup = SimpleNamespace(
        refresh_ws_subscriptions=lambda h: calls.append("refresh_ws"),
        health_check=lambda alert: calls.append("health"),
    )
    h = SimpleNamespace(enabled_hour_offsets=("25m",), base_interval="5m")
    cfg = SimpleNamespace(error_webhook_url="")
    app.update_all(h, datetime(2026, 11, 23, 1, 10, tzinfo=timezone.utc), cfg, supervisor=sup)
    assert "meta" in calls and "match" in calls and "refresh_ws" in calls and "health" in calls and "backfill" in calls


def test_app_init_history_runs_trim_and_passes_full_backfill_symbols(monkeypatch):
    from core import app

    order = []
    trim_ts = pd.Timestamp("2025-03-25 00:00:00", tz="UTC")
    run_time = datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc)
    full_backfill = {"spot": {"BTCUSDT"}, "swap": {"ETHUSDT"}}

    monkeypatch.setattr(app, "init_dirs", lambda h: order.append("init_dirs"))
    monkeypatch.setattr(
        app,
        "apply_explicit_history_trim",
        lambda h, ts: order.append(("trim", ts)),
    )
    monkeypatch.setattr(
        app,
        "run_download_all_history_with_backfill",
        lambda h: (order.append("download") or (run_time, full_backfill)),
    )

    def fake_resample(h, tt, rt, full_backfill_symbols=None):
        order.append(("resample", tt, rt, full_backfill_symbols))

    monkeypatch.setattr(app, "resample_history", fake_resample)
    monkeypatch.setattr(app, "copy_data_api_candles", lambda h: order.append("copy_data_api"))
    monkeypatch.setattr(app, "divider", lambda *a, **kw: None)

    class FakePool:
        def __init__(self, **kw):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            pass

        def submit(self, fn, *args, **kwargs):
            return SimpleNamespace(result=lambda: fn(*args, **kwargs))

    monkeypatch.setattr(app, "ProcessPoolExecutor", FakePool)

    h = SimpleNamespace(
        data_api=object(),
        history_trim_ts=lambda: trim_ts,
    )
    app.init_history(h)
    assert order[0] == "init_dirs"
    assert order[1] == ("trim", trim_ts)
    assert order[2] == "download"
    assert ("resample", "spot", run_time, {"BTCUSDT"}) in order
    assert ("resample", "swap", run_time, {"ETHUSDT"}) in order
    assert order[-1] == "copy_data_api"


def test_app_run_realtime_ws_guard_and_recovery_preserves_products(monkeypatch):
    from core import app

    class StopLoop(BaseException):
        pass

    events = []
    alerts = []
    sleeps = []

    h = SimpleNamespace(
        base_interval="5m",
        exchange="binance",
        fetch_funding_rate=True,
    )
    cfg = SimpleNamespace(
        use_websocket=True,
        error_webhook_url="https://example.invalid/hook",
    )

    class FakeSupervisor:
        def __init__(self, **kw):
            self.router = object()
            self.writer_procs = [SimpleNamespace(pid=101)]
            self.ws_procs = {"spot": [SimpleNamespace(pid=201)]}

        def start_writers(self):
            events.append("start_writers")

        def start_ws_runners(self):
            events.append("start_ws_runners")

        def resume_pending_ws_restarts(self):
            events.append("resume_ws")

        def shutdown(self):
            events.append("shutdown")

    async def fake_meta(handle, rt):
        events.append("bootstrap_meta")

    init_attempts = []

    def fake_init(handle, c=None):
        init_attempts.append(1)
        if len(init_attempts) == 1:
            raise TimeoutError("first init transient")
        events.append("init_ok")

    update_calls = []

    def fake_update(handle, rt, c, supervisor=None):
        update_calls.append(rt)
        if len(update_calls) == 1:
            raise RuntimeError("runtime transient")
        raise StopLoop()

    monkeypatch.setattr(app, "install_logkit_bridge", lambda: None)
    monkeypatch.setattr(app, "divider", lambda *a, **kw: None)
    monkeypatch.setattr(app, "debug_handle", lambda *a: None)
    monkeypatch.setattr(app, "init_history", fake_init)
    monkeypatch.setattr(app, "defer_funding_backfill", lambda: events.append("defer_funding"))
    monkeypatch.setattr(app, "update_meta", fake_meta)
    monkeypatch.setattr(app, "update_symbol_match", lambda *a: events.append("bootstrap_match"))
    monkeypatch.setattr(app, "select_protocol", lambda ex: object())
    monkeypatch.setattr(app, "RuntimeSupervisor", FakeSupervisor)
    monkeypatch.setattr(app, "run_startup_catchup", lambda handle, router: events.append("catchup"))
    monkeypatch.setattr(
        app,
        "next_run_time",
        lambda iv: datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(app, "update_all", fake_update)
    monkeypatch.setattr(app, "send_wechat_work_msg", lambda msg, url: alerts.append(msg))
    monkeypatch.setattr(app.time, "sleep", lambda s: sleeps.append(s))

    with pytest.raises(StopLoop):
        app.run_realtime(cfg, lambda c: h)

    assert len(init_attempts) == 2
    assert events.count("init_ok") == 1
    assert "defer_funding" in events
    assert "start_writers" in events and "start_ws_runners" in events and "catchup" in events
    assert "shutdown" in events
    assert sleeps == [60, 60]
    assert len(alerts) == 2
    assert "初始化" in alerts[0]
    assert "保留已有产物" in alerts[1]
