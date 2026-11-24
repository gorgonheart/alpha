from __future__ import annotations

import importlib.util
import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core.utils import path_kit


def _load_cfg(tmp_path: Path, monkeypatch, overrides: dict):
    monkeypatch.setattr(path_kit, "get_file_path", lambda *a, **kw: tmp_path / a[0])
    monkeypatch.setattr(path_kit, "get_folder_path", lambda *a, **kw: str(tmp_path / "data"))
    (tmp_path / "config.json").write_text(json.dumps(overrides), encoding="utf-8")
    spec = importlib.util.spec_from_file_location(
        "cfg_parallel_test", Path(__file__).resolve().parents[1] / "config.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ============================================================================
# Task 1: 4-Writer, Lightweight WS Payload, Early .ws_ready, Cancellable 14s Fallback
# ============================================================================


def test_config_and_handle_defaults_4_writers_and_ws_wait_14s(tmp_path, monkeypatch):
    from core.handle import create_bmac_handle_from_config
    from core.pipeline.runtime_supervisor import RuntimeSupervisor

    cfg = _load_cfg(tmp_path, monkeypatch, {"use_api": {"kline": False, "coin_cap": False}})
    assert cfg.num_writers == 4
    assert cfg.ws_wait_sec == 14.0
    assert RuntimeSupervisor.NUM_WRITERS == 4

    h = create_bmac_handle_from_config(cfg)
    assert h.num_writers == 4
    assert h.ws_wait_sec == 14.0

    for bad in ({"ws_wait_sec": -1.0}, {"ws_wait_sec": True}):
        with pytest.raises(ValueError):
            _load_cfg(tmp_path, monkeypatch, bad)


def test_ws_runner_emits_lightweight_kline_dict_and_writer_accepts_both_kline_and_df(tmp_path):
    from core.storage.ts_manager import TSManager
    from core.storage.writer import Writer
    from core.ws.ws_runner import _kline_to_writer_item

    kline = {
        "begin_time_ms": 1_763_900_100_000,
        "open": "100.5",
        "high": "102.0",
        "low": "99.5",
        "close": "101.25",
        "volume": "1234.5",
        "quote_volume": "124000.0",
        "trade_num": 456,
        "taker_buy_base": "600.0",
        "taker_buy_quote": "60500.0",
    }
    item_fast = _kline_to_writer_item("BTCUSDT", "swap", kline, timedelta(minutes=5))
    assert "kline" in item_fast
    assert "df" not in item_fast, "WS queue item must not pre-construct a DataFrame per bar"

    dir_fast = tmp_path / "fast_5m"
    dir_df = tmp_path / "df_5m"
    dir_fast.mkdir()
    dir_df.mkdir()

    begin_dt = pd.Timestamp(kline["begin_time_ms"], unit="ms", tz="UTC")
    df_equiv = pd.DataFrame([{
        "candle_begin_time": begin_dt,
        "open": float(kline["open"]),
        "high": float(kline["high"]),
        "low": float(kline["low"]),
        "close": float(kline["close"]),
        "volume": float(kline["volume"]),
        "quote_volume": float(kline["quote_volume"]),
        "trade_num": int(kline["trade_num"]),
        "taker_buy_base_asset_volume": float(kline["taker_buy_base"]),
        "taker_buy_quote_asset_volume": float(kline["taker_buy_quote"]),
    }])
    item_df = {
        "symbol": "BTCUSDT",
        "trade_type": "swap",
        "df": df_equiv,
        "source": "ws",
        "run_time": item_fast["run_time"],
    }

    Writer(base_candle_dirs={"swap": dir_fast}).process_one(item_fast)
    Writer(base_candle_dirs={"swap": dir_df}).process_one(item_df)

    pd.testing.assert_frame_equal(
        pd.read_pickle(dir_fast / "BTCUSDT.pkl"),
        pd.read_pickle(dir_df / "BTCUSDT.pkl"),
        check_exact=True,
    )
    pd.testing.assert_frame_equal(
        TSManager(dir_fast / "BTCUSDT").read_all(),
        TSManager(dir_df / "BTCUSDT").read_all(),
        check_exact=True,
    )


def test_writer_publishes_ws_ready_after_flat_pkl_and_gap_before_tsmanager(tmp_path, monkeypatch):
    from core.storage.candle_manager import CandleFileManager
    from core.storage.writer import Writer
    from core.ws.ws_runner import _kline_to_writer_item

    base_dir = tmp_path / "swap_5m"
    base_dir.mkdir()
    cfm = CandleFileManager(base_dir, "pickle")

    # Seed t0=00:00 so pushing t2=00:10 creates a 5m gap at 00:05
    t0 = pd.Timestamp("2026-11-24 00:00:00", tz="UTC")
    seed_df = pd.DataFrame([{
        "candle_begin_time": t0,
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
        "volume": 1.0, "quote_volume": 10.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 0.5, "taker_buy_quote_asset_volume": 5.0,
    }])
    writer = Writer(base_candle_dirs={"swap": base_dir})
    writer.process_one({
        "symbol": "ETHUSDT", "trade_type": "swap", "df": seed_df,
        "source": "ws", "run_time": (t0 + timedelta(minutes=5)).to_pydatetime(),
    })

    observed_during_ts = {}
    t2 = pd.Timestamp("2026-11-24 00:10:00", tz="UTC")
    rt2 = (t2 + timedelta(minutes=5)).to_pydatetime()

    orig_ts_update = writer._update_tsmanager

    def check_order_and_fail(bdir, sym, df_rows, rt):
        observed_during_ts["flat_len"] = len(pd.read_pickle(bdir / f"{sym}.pkl"))
        observed_during_ts["gap_exists"] = Path(cfm.format_gap_file_path(sym)).exists()
        observed_during_ts["ws_ready_exists"] = cfm.check_ws_ready(sym, rt)
        observed_during_ts["ready_exists"] = cfm.check_ready(sym, rt)
        orig_ts_update(bdir, sym, df_rows, rt)
        raise OSError("simulated transient partition fs lag")

    monkeypatch.setattr(writer, "_update_tsmanager", check_order_and_fail)
    item = _kline_to_writer_item("ETHUSDT", "swap", {
        "begin_time_ms": int(t2.timestamp() * 1000),
        "open": 11.0, "high": 11.0, "low": 11.0, "close": 11.0,
        "volume": 2.0, "quote_volume": 22.0, "trade_num": 2,
        "taker_buy_base": 1.0, "taker_buy_quote": 11.0,
    }, timedelta(minutes=5))

    # Even if _update_tsmanager fails, flat_pkl + .gap + .ws_ready were already durably committed
    writer.process_one(item)
    assert observed_during_ts == {
        "flat_len": 2,
        "gap_exists": True,
        "ws_ready_exists": True,
        "ready_exists": True,
    }
    assert cfm.check_ws_ready("ETHUSDT", rt2)


def test_rest_fallback_cancels_immediately_when_barriers_complete_with_zero_misses(monkeypatch, tmp_path):
    from core.pipeline import update_cycle as uc
    from core.storage.candle_manager import CandleFileManager

    spot_dir = tmp_path / "spot_5m"
    swap_dir = tmp_path / "swap_5m"
    exg_dir = tmp_path / "exg"
    for d in (spot_dir, swap_dir, exg_dir):
        d.mkdir(parents=True)

    run_time = datetime(2026, 11, 24, 1, 25, tzinfo=timezone.utc)
    CandleFileManager(spot_dir, "pickle").save_ws_ready("BTCUSDT", run_time)
    CandleFileManager(swap_dir, "pickle").save_ws_ready("BTCUSDT", run_time)
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["BTCUSDT"]}))

    rest_calls = []
    monkeypatch.setattr(
        uc, "fetch_update_binance_candles",
        lambda *a, **kw: rest_calls.append(1) or ({}, {}, {}, {}),
    )
    monkeypatch.setattr(
        uc, "update_resample_from_base",
        lambda *a, **kw: {"BTCUSDT": (run_time - timedelta(hours=2), run_time - timedelta(hours=1))},
    )

    h = SimpleNamespace(
        resample_interval="1h",
        ws_wait_sec=5.0,  # Long grace: must be cancelled immediately when barriers succeed!
        data_api=None,
        coin_cap_handle=None,
        preprocess_handle=None,
        exg_mgr=exg_mgr,
        get_market_handle=lambda tt: SimpleNamespace(
            base_candle_dir=spot_dir if tt == "spot" else swap_dir,
            resample_mgr_map={"25m": SimpleNamespace(has_symbol=lambda s: True)},
        ),
        get_binance_handle=lambda tt: SimpleNamespace(
            base_candle_dir=spot_dir if tt == "spot" else swap_dir,
            resample_mgr_map={"25m": SimpleNamespace(has_symbol=lambda s: True)},
        ),
    )

    t0 = time.monotonic()
    res = uc.run_ws_cycle(h, run_time, SimpleNamespace(router=None))
    elapsed = time.monotonic() - t0
    assert res.is_binance_success is True
    assert rest_calls == []
    assert elapsed < 1.0, f"run_ws_cycle slept on fallback grace instead of cancelling: {elapsed:.2f}s"


def test_rest_fallback_uses_small_limit_for_single_bar_miss_and_full_limit_for_gap_symbol(monkeypatch, tmp_path):
    from core.pipeline import data_update as du
    from core.pipeline import update_cycle as uc
    from core.storage.candle_manager import CandleFileManager

    spot_dir = tmp_path / "spot_5m"
    swap_dir = tmp_path / "swap_5m"
    exg_dir = tmp_path / "exg"
    for d in (spot_dir, swap_dir, exg_dir):
        d.mkdir(parents=True)

    run_time = datetime(2026, 11, 24, 1, 25, tzinfo=timezone.utc)
    spot_cfm = CandleFileManager(spot_dir, "pickle")
    swap_cfm = CandleFileManager(swap_dir, "pickle")
    # Spot all ready; Swap has OKUSDT (ws_ready), SINGLEMISS (no ws_ready, no .gap), GAPSYM (.gap marker)
    spot_cfm.save_ws_ready("BTCUSDT", run_time)
    swap_cfm.save_ws_ready("OKUSDT", run_time)
    swap_cfm.save_ws_ready("GAPSYM", run_time)
    swap_cfm.save_gap_marker("GAPSYM")

    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["OKUSDT", "SINGLEMISS", "GAPSYM"]}))

    captured_params = {}

    class FakeFetcher:
        async def batch_get_candle(self, params_list):
            for p in params_list:
                captured_params[p["symbol"]] = p["limit"]
            return [pd.DataFrame() for _ in params_list]

    class FakeAsyncCtx:
        async def __aenter__(self): return object()
        async def __aexit__(self, *a): pass

    monkeypatch.setattr(du, "create_aiohttp_session", lambda *a, **kw: FakeAsyncCtx())
    monkeypatch.setattr(du, "create_fetcher", lambda *a, **kw: FakeFetcher())

    h = SimpleNamespace(
        base_interval="5m",
        resample_interval="1h",
        exchange="binance",
        http_timeout_sec=5,
        ws_wait_sec=0.01,
        kline_count_1h=100,
        exg_mgr=exg_mgr,
        get_market_handle=lambda tt: SimpleNamespace(
            base_candle_dir=spot_dir if tt == "spot" else swap_dir,
            once_update_candles=99,
            resample_mgr_map={"25m": SimpleNamespace(has_symbol=lambda s: False)},
        ),
    )
    router = SimpleNamespace(put=lambda item: None)
    thread, state = uc._start_rest_fallback(h, run_time, router)
    thread.join(timeout=5.0)
    assert state["done"].is_set()
    assert captured_params == {"SINGLEMISS": 5, "GAPSYM": 99}


# ============================================================================
# Task 2: flat_pkl-First Resample, Persistent Compute Pool, In-Memory Tail Bars
# ============================================================================


def test_resample_prefers_flat_pkl_without_reading_tsmanager_when_window_sufficient(tmp_path, monkeypatch):
    from core.pipeline import resample as res_mod
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager
    from core.storage.writer import Writer

    run_time = datetime(2026, 11, 24, 2, 25, tzinfo=timezone.utc)
    times = pd.date_range(end=run_time - timedelta(minutes=5), periods=300, freq="5min", tz="UTC")
    df_5m = pd.DataFrame({
        "candle_begin_time": times,
        "open": [100.0 + i * 0.1 for i in range(300)],
        "high": [101.0 + i * 0.1 for i in range(300)],
        "low": [99.0 + i * 0.1 for i in range(300)],
        "close": [100.5 + i * 0.1 for i in range(300)],
        "volume": [10.0 + i for i in range(300)],
        "quote_volume": [1000.0 + i * 10 for i in range(300)],
        "trade_num": pd.Series([5 + i for i in range(300)], dtype="int64"),
        "taker_buy_base_asset_volume": [5.0 + i * 0.5 for i in range(300)],
        "taker_buy_quote_asset_volume": [500.0 + i * 5 for i in range(300)],
    })

    # Oracle dir: TSManager only (no flat_pkl)
    oracle_5m = tmp_path / "oracle_5m"
    oracle_1h = tmp_path / "oracle_1h" / "25m"
    oracle_1h.mkdir(parents=True)
    TSManager(oracle_5m / "BTCUSDT").update(df_5m.copy())
    oracle_mgr = CandleFileManager(oracle_1h, "pickle")

    # Fast dir: populated by Writer (flat_pkl=240 bars + TSManager=300 bars)
    fast_5m = tmp_path / "fast_5m"
    fast_1h = tmp_path / "fast_1h" / "25m"
    fast_5m.mkdir(parents=True)
    fast_1h.mkdir(parents=True)
    Writer(base_candle_dirs={"spot": fast_5m}, flat_pkl_max_bars=240).process_one({
        "symbol": "BTCUSDT", "trade_type": "spot", "df": df_5m.copy(),
        "source": "ws", "run_time": run_time,
    })
    fast_mgr = CandleFileManager(fast_1h, "pickle")

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))

    def make_h(bdir, rmgr):
        return SimpleNamespace(
            base_interval="5m", resample_interval="1h", enabled_hour_offsets=("25m",),
            history_trim_ts=lambda: None, effective_kline_count_1h=lambda tt, rt: 15000,
            exg_mgr=exg_mgr,
            get_market_handle=lambda _: SimpleNamespace(
                base_candle_dir=bdir, resample_mgr_map={"25m": rmgr}, once_update_candles=99,
            ),
        )

    # Run oracle first (uses TSManager)
    res_mod.update_resample_from_base(make_h(oracle_5m, oracle_mgr), "spot", run_time)
    expected_1h = oracle_mgr.read_candle("BTCUSDT")

    # Now forbid TSManager.read_partition during fast path (240 >= 99 closed bars in flat_pkl!)
    monkeypatch.setattr(
        TSManager, "read_partition",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("TSManager partition read should be skipped!")),
    )
    res_mod.update_resample_from_base(make_h(fast_5m, fast_mgr), "spot", run_time)
    actual_1h = fast_mgr.read_candle("BTCUSDT")

    pd.testing.assert_frame_equal(actual_1h, expected_1h, check_exact=True)


def test_resample_falls_back_to_tsmanager_when_flat_pkl_shorter_than_window(tmp_path):
    from core.pipeline import resample as res_mod
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager

    run_time = datetime(2026, 11, 24, 3, 25, tzinfo=timezone.utc)
    times_150 = pd.date_range(end=run_time - timedelta(minutes=5), periods=150, freq="5min", tz="UTC")
    df_150 = pd.DataFrame({
        "candle_begin_time": times_150,
        "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.5,
        "volume": 10.0, "quote_volume": 505.0,
        "trade_num": pd.Series([3] * 150, dtype="int64"),
        "taker_buy_base_asset_volume": 5.0, "taker_buy_quote_asset_volume": 252.5,
    })

    base_dir = tmp_path / "swap_5m"
    res_dir = tmp_path / "swap_1h" / "25m"
    base_dir.mkdir(parents=True)
    res_dir.mkdir(parents=True)

    # TSManager has 150 bars (covers full 99-bar window), while flat_pkl only has last 24 bars (< 99)
    TSManager(base_dir / "SOLUSDT").update(df_150.copy())
    df_150.tail(24).reset_index(drop=True).to_pickle(base_dir / "SOLUSDT.pkl")

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": ["SOLUSDT"]}))
    res_mgr = CandleFileManager(res_dir, "pickle")

    h = SimpleNamespace(
        base_interval="5m", resample_interval="1h", enabled_hour_offsets=("25m",),
        history_trim_ts=lambda: None, effective_kline_count_1h=lambda tt, rt: 15000,
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(
            base_candle_dir=base_dir, resample_mgr_map={"25m": res_mgr}, once_update_candles=99,
        ),
    )
    res_mod.update_resample_from_base(h, "swap", run_time)
    out = res_mgr.read_candle("SOLUSDT")
    # With 99 5m bars (8h15m) dropping truncated head, must produce >= 7 full 1h bars (not just 1 bar from 24-bar flat_pkl!)
    assert len(out) >= 7


def test_supervisor_persistent_compute_pool_reused_and_resample_returns_tail_bars(tmp_path):
    from core.handle import create_bmac_handle_from_config
    from core.pipeline.resample import update_resample_from_base
    from core.pipeline.runtime_supervisor import RuntimeSupervisor
    from core.storage.writer import Writer
    from core.utils.file_system import init_dirs

    cfg = SimpleNamespace(
        storage_path=str(tmp_path / "data"),
        base_interval="5m",
        resample_interval="1h",
        kline_count_1h_spot=500,
        kline_count_1h_swap=500,
        use_api={"kline": False, "coin_cap": False},
        data_api_uuid="",
        data_api_key="",
        proxy=None,
        coin_cap_use_proxy=False,
        enabled_hour_offsets=("25m",),
        funding_rate=False,
        preprocess=True,
        preprocess_num_batch=10,
        preprocess_num_recent=3,
        precompute_factor_configs={},
        precompute_factor_workers=2,
        arrow_demand_enabled=False,
        arrow_demand_min_rows=None,
        use_websocket=True,
        rest_fetch_delay_seconds=3.0,
        num_writers=2,
        ws_wait_sec=14.0,
        ws_flat_pkl_bars=240,
        base_5m_retention_days=30,
        min_history_start_date="2025-03-25",
        history_trim_date=None,
    )
    h = create_bmac_handle_from_config(cfg)
    init_dirs(h)
    run_time = datetime(2026, 11, 24, 4, 25, tzinfo=timezone.utc)
    syms = [f"COIN{i}USDT" for i in range(6)]
    h.exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": syms}))

    times = pd.date_range(end=run_time - timedelta(minutes=5), periods=180, freq="5min", tz="UTC")
    writer = Writer(base_candle_dirs={"swap": h.swap.base_candle_dir}, flat_pkl_max_bars=240)
    for i, sym in enumerate(syms):
        df = pd.DataFrame({
            "candle_begin_time": times,
            "open": 10.0 + i, "high": 11.0 + i, "low": 9.0 + i, "close": 10.5 + i,
            "volume": 100.0, "quote_volume": 1050.0,
            "trade_num": pd.Series([10] * 180, dtype="int64"),
            "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
        })
        writer.process_one({"symbol": sym, "trade_type": "swap", "df": df, "source": "ws", "run_time": run_time})

    sup = RuntimeSupervisor(handle=h, alert_webhook_url="", compute_workers=2)
    try:
        pool1 = sup.get_compute_pool()
        pool2 = sup.get_compute_pool()
        assert pool1 is pool2

        begin_ts, tail_dfs = update_resample_from_base(
            h, "swap", run_time, executor=pool1, return_tail_bars=5
        )
        assert set(begin_ts) == set(syms)
        assert set(tail_dfs) == set(syms)
        res_mgr = h.swap.resample_mgr_map["25m"]
        for sym in syms:
            disk_df = res_mgr.read_candle(sym).tail(5).reset_index(drop=True)
            pd.testing.assert_frame_equal(tail_dfs[sym].reset_index(drop=True), disk_df, check_exact=True)
    finally:
        sup.shutdown()
    assert sup._compute_pool is None
