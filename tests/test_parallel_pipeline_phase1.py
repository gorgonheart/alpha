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


# ============================================================================
# Task 3: NumPy 2D Vectorized update_pivot_rows vs Independent .loc Oracle
# ============================================================================


def _oracle_update_pivot_rows(pivot: dict, market_dict: dict, trade_type: str) -> bool:
    """Independent reference oracle preserving the original per-symbol .loc semantics."""
    frames = {s: f.set_index("candle_begin_time") for s, f in market_dict.items() if not f.empty}
    if not frames:
        return False
    index = pivot["open"].index
    for frame in frames.values():
        index = index.union(frame.index)
    index = index.sort_values()
    symbols = set(pivot["open"].columns) | set(frames)
    columns = pivot["open"].columns
    if set(columns) != symbols:
        columns = pd.CategoricalIndex(sorted(symbols), name="symbol")
    fields = {"open": "open", "close": "close", "vwap1m": "avg_price_1m"}
    if trade_type == "swap":
        fields["funding_rate"] = "funding_fee"
    changed = False
    for key, column in fields.items():
        table = pivot[key]
        if not table.index.equals(index) or not table.columns.equals(columns):
            table = table.reindex(index=index, columns=columns)
            pivot[key] = table
            changed = True
        for symbol, frame in frames.items():
            values = frame[column]
            current = table.loc[frame.index, symbol]
            different = ~(current.eq(values) | (current.isna() & values.isna()))
            if different.any():
                table.loc[values.index[different], symbol] = values[different].to_numpy()
                changed = True
        if key == "funding_rate":
            table.fillna(0, inplace=True)
    return changed


def test_update_pivot_rows_matches_independent_loc_oracle_across_edge_cases():
    import numpy as np
    from core.strategy.preprocess import make_market_pivot, update_pivot_rows

    base_idx = pd.date_range("2026-06-01 00:25", periods=20, freq="h", tz="UTC")

    def _mk_df(sym: str, ts_idx, offset: float, with_nan: bool = False):
        n = len(ts_idx)
        op = np.linspace(10.0 + offset, 20.0 + offset, n)
        cl = op + 0.5
        if with_nan and n >= 3:
            op[1] = np.nan
        return pd.DataFrame({
            "candle_begin_time": ts_idx,
            "symbol": sym,
            "open": op,
            "close": cl,
            "avg_price_1m": op + 0.1,
            "funding_fee": np.linspace(-0.0005, 0.0005, n),
        })

    # Seed initial pivot with BTCUSDT, DELISTEDUSDT, GAPUSDT
    seed_dict = {
        "BTCUSDT": _mk_df("BTCUSDT", base_idx[:15], 1.0),
        "DELISTEDUSDT": _mk_df("DELISTEDUSDT", base_idx[:15], 2.0),
        "GAPUSDT": _mk_df("GAPUSDT", base_idx[:15], 3.0),
    }
    piv_oracle = make_market_pivot(seed_dict, "swap")
    piv_fast = {k: v.copy(deep=True) for k, v in piv_oracle.items()}

    # Incoming update:
    # - BTCUSDT contiguous 0..20 with revised row 10 and NaN at row 1
    # - GAPUSDT non-contiguous subset [2, 5, 14, 18, 19]
    # - NEWLISTUSDT newly listed at rows 16..20 (triggers CategoricalIndex expansion)
    # - DELISTEDUSDT omitted (must retain columns & old values, NaN on new price rows, 0.0 on funding_rate)
    # - EMPTYUSDT empty DataFrame (must be ignored)
    upd_dict = {
        "BTCUSDT": _mk_df("BTCUSDT", base_idx, 1.0, with_nan=True),
        "GAPUSDT": _mk_df("GAPUSDT", base_idx[[2, 5, 14, 18, 19]], 3.5),
        "NEWLISTUSDT": _mk_df("NEWLISTUSDT", base_idx[16:], 9.0),
        "EMPTYUSDT": _mk_df("EMPTYUSDT", base_idx[:0], 0.0),
    }
    upd_dict["BTCUSDT"].loc[10, "close"] = 999.25

    chg_oracle = _oracle_update_pivot_rows(piv_oracle, upd_dict, "swap")
    chg_fast = update_pivot_rows(piv_fast, upd_dict, "swap")
    assert chg_fast is chg_oracle is True

    for key in piv_oracle:
        pd.testing.assert_frame_equal(piv_fast[key], piv_oracle[key], check_exact=True)
        assert piv_fast[key].columns.equals(piv_oracle[key].columns)
        assert piv_fast[key].index.equals(piv_oracle[key].index)

    # Idempotent second call with identical data -> changed must be False!
    chg_oracle_2 = _oracle_update_pivot_rows(piv_oracle, upd_dict, "swap")
    chg_fast_2 = update_pivot_rows(piv_fast, upd_dict, "swap")
    assert chg_fast_2 is chg_oracle_2 is False
    for key in piv_oracle:
        pd.testing.assert_frame_equal(piv_fast[key], piv_oracle[key], check_exact=True)


def test_update_pivot_rows_vectorized_is_at_least_5x_faster_than_loc_oracle():
    import numpy as np
    from core.strategy.preprocess import make_market_pivot, update_pivot_rows

    n_syms = 250
    n_old = 1500
    n_total = 1502
    idx_all = pd.date_range("2026-01-01 00:25", periods=n_total, freq="h", tz="UTC")
    rng = np.random.default_rng(42)

    seed = {}
    incoming = {}
    for i in range(n_syms):
        sym = f"SYM{i:03d}USDT"
        base_p = 10.0 + i * 0.1 + rng.standard_normal(n_total) * 0.01
        df_all = pd.DataFrame({
            "candle_begin_time": idx_all,
            "symbol": sym,
            "open": base_p,
            "close": base_p + 0.05,
            "avg_price_1m": base_p + 0.02,
            "funding_fee": rng.standard_normal(n_total) * 1e-4,
        })
        seed[sym] = df_all.iloc[:n_old].reset_index(drop=True)
        incoming[sym] = df_all

    piv_base = make_market_pivot(seed, "swap")
    piv_o = {k: v.copy(deep=True) for k, v in piv_base.items()}
    piv_f = {k: v.copy(deep=True) for k, v in piv_base.items()}

    t0 = time.perf_counter()
    chg_o = _oracle_update_pivot_rows(piv_o, incoming, "swap")
    t_oracle = time.perf_counter() - t0

    t0 = time.perf_counter()
    chg_f = update_pivot_rows(piv_f, incoming, "swap")
    t_fast = time.perf_counter() - t0

    assert chg_f == chg_o
    for key in piv_o:
        pd.testing.assert_frame_equal(piv_f[key], piv_o[key], check_exact=True)
    assert t_fast * 5.0 < t_oracle, (
        f"Expected >=5x speedup over .loc oracle, got fast={t_fast:.4f}s vs oracle={t_oracle:.4f}s"
    )


# ============================================================================
# Task 4: Prefix-Verified Tail Enrich, 8-Batch Multi-Process Pool & Direct Arrow IPC
# ============================================================================


def test_gen_batch_data_incremental_tail_fast_path_skips_disk_read_and_matches_oracle(tmp_path, monkeypatch):
    import numpy as np
    from core.storage.candle_manager import CandleFileManager
    from core.strategy.preprocess import gen_batch_data, gen_batch_data_incremental

    times = pd.date_range("2026-10-01 00:25", periods=80, freq="h", tz="UTC")
    t_old = times[:-1]
    rt_old = t_old[-1].to_pydatetime()
    rt_new = times[-1].to_pydatetime()

    syms = ["NORMUSDT", "DEEPREVUSDT", "MATCHCHGUSDT"]
    rng = np.random.default_rng(7)
    raw_frames = {}
    for i, sym in enumerate(syms):
        p = 20.0 + i * 5.0 + np.cumsum(rng.standard_normal(len(times)) * 0.1)
        raw_frames[sym] = pd.DataFrame({
            "candle_begin_time": times,
            "open": p, "high": p + 0.2, "low": p - 0.2, "close": p + 0.05,
            "volume": 100.0 + i, "quote_volume": (100.0 + i) * p,
            "trade_num": pd.Series([15] * len(times), dtype="int64"),
            "taker_buy_base_asset_volume": 50.0,
            "taker_buy_quote_asset_volume": 50.0 * p,
        })

    def _setup_env(root: Path):
        res_dir = root / "swap_1h" / "25m"
        pre_dir = root / "pre" / "25m"
        exg_dir = root / "exg"
        fun_dir = root / "funding"
        for d in (res_dir, pre_dir, exg_dir, fun_dir):
            d.mkdir(parents=True)
        res_mgr = CandleFileManager(res_dir, "pickle")
        pre_mgr = CandleFileManager(pre_dir, "pickle")
        exg_mgr = CandleFileManager(exg_dir, "pickle")
        fun_mgr = CandleFileManager(fun_dir, "pickle")
        exg_mgr.set_candle("exginfo_swap", rt_new, pd.DataFrame({"symbol": syms}))
        for sym in syms:
            fdf = pd.DataFrame({
                "candle_begin_time": times.floor("h"),
                "funding_rate": np.linspace(-0.0002, 0.0003, len(times)),
            }).drop_duplicates("candle_begin_time")
            fun_mgr.set_candle(sym, rt_new, fdf)
        h = SimpleNamespace(
            resample_interval="1h",
            history_trim_ts=lambda: None,
            fetch_funding_rate=True,
            funding_mgr=fun_mgr,
            data_api=None,
            exg_mgr=exg_mgr,
            get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
            get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
            preprocess_handle=SimpleNamespace(num_batch=10, preprocess_mgr_map={"25m": pre_mgr}),
        )
        return h, res_mgr, pre_mgr

    # Round 1 on incremental env: MATCHCHGUSDT initially unmatched, others matched
    h_inc, res_inc, pre_inc = _setup_env(tmp_path / "inc")
    for sym in syms:
        res_inc.set_candle(sym, rt_old, raw_frames[sym].iloc[:-1].reset_index(drop=True))
    match_r1 = pd.DataFrame([
        {"spot": "NORMUSDT", "swap": "NORMUSDT", "first_begin_time": t_old[0], "last_begin_time": t_old[-1]},
        {"spot": "DEEPREVUSDT", "swap": "DEEPREVUSDT", "first_begin_time": t_old[0], "last_begin_time": t_old[-1]},
        {"spot": None, "swap": "MATCHCHGUSDT", "first_begin_time": t_old[0], "last_begin_time": t_old[-1]},
    ])
    gen_batch_data_incremental(h_inc, rt_old, "swap", match_r1, syms, "batch1")
    pre_inc.save_ready("market_pivot_swap", rt_old)

    # Round 2:
    # - NORMUSDT: pure 1-bar append, passed via resample_tail_hints (must NOT call res_inc.read_candle!)
    # - DEEPREVUSDT: deep historical revision at bar 5 (no tail hint -> reads disk & falls back to full frame)
    # - MATCHCHGUSDT: now matched with spot across entire history!
    rev_df = raw_frames["DEEPREVUSDT"].copy()
    rev_df.loc[5, "close"] += 12.34
    res_inc.set_candle("NORMUSDT", rt_new, raw_frames["NORMUSDT"].copy())
    res_inc.set_candle("DEEPREVUSDT", rt_new, rev_df.copy())
    res_inc.set_candle("MATCHCHGUSDT", rt_new, raw_frames["MATCHCHGUSDT"].copy())

    match_r2 = pd.DataFrame([
        {"spot": sym, "swap": sym, "first_begin_time": times[0], "last_begin_time": times[-1]}
        for sym in syms
    ])

    orig_read = res_inc.read_candle

    def guarded_read(sym):
        if sym == "NORMUSDT":
            raise AssertionError("NORMUSDT should use in-memory resample_tail_hints without reading 1h pkl!")
        return orig_read(sym)

    monkeypatch.setattr(res_inc, "read_candle", guarded_read)
    tail_hints = {
        "NORMUSDT": (times[0], times[-1], raw_frames["NORMUSDT"].tail(12).reset_index(drop=True)),
        "MATCHCHGUSDT": (times[0], times[-1], raw_frames["MATCHCHGUSDT"].tail(12).reset_index(drop=True)),
    }
    yp_inc = {}
    gen_batch_data_incremental(
        h_inc, rt_new, "swap", match_r2, syms, "batch1",
        year_pivot_out=yp_inc, resample_tail_hints=tail_hints,
    )

    # Oracle env: run R1 -> R2 via baseline path (no resample_tail_hints, full validation)
    h_ora, res_ora, pre_ora = _setup_env(tmp_path / "ora")
    for sym in syms:
        res_ora.set_candle(sym, rt_old, raw_frames[sym].iloc[:-1].reset_index(drop=True))
    gen_batch_data(h_ora, rt_old, "swap", match_r1, syms, "batch1")
    pre_ora.save_ready("market_pivot_swap", rt_old)
    res_ora.set_candle("NORMUSDT", rt_new, raw_frames["NORMUSDT"].copy())
    res_ora.set_candle("DEEPREVUSDT", rt_new, rev_df.copy())
    res_ora.set_candle("MATCHCHGUSDT", rt_new, raw_frames["MATCHCHGUSDT"].copy())
    # Force full path by disabling tail hints and comparing against gen_batch_data + FundingPublication
    from core.strategy.funding_publication import FundingPublication
    fpub_ora = FundingPublication(pre_ora.base_dir, rt_new)
    gen_batch_data(h_ora, rt_new, "swap", match_r2, syms, "batch1", funding_publication=fpub_ora)

    actual_batch = pre_inc.read_candle("swap_dict_batch1")
    expected_batch = pre_ora.read_candle("swap_dict_batch1")
    for sym in syms:
        pd.testing.assert_frame_equal(actual_batch[sym], expected_batch[sym], check_exact=True)


def test_multiprocess_8_batch_preprocess_matches_serial_oracle_exact(tmp_path, monkeypatch):
    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor
    import numpy as np
    from core.handle import create_bmac_handle_from_config
    from core.strategy import preprocess as pp
    from core.strategy.arrow_publish import read_arrow_snapshot
    from core.strategy.candle_origins import load_origins
    from core.utils.file_system import init_dirs

    factor_cfgs = {
        "动量210401_双项异常隔离V2": [200],
        "ILLQStdBbi45208": [24, 48],
        "CciV3": [24],
    }

    def _build_side(sub: str, workers: int):
        root = tmp_path / sub
        cfg = SimpleNamespace(
            storage_path=str(root / "data"),
            base_interval="5m",
            resample_interval="1h",
            kline_count_1h_spot=200,
            kline_count_1h_swap=200,
            use_api={"kline": False, "coin_cap": False},
            data_api_uuid="",
            data_api_key="",
            proxy=None,
            coin_cap_use_proxy=False,
            enabled_hour_offsets=("25m",),
            funding_rate=True,
            preprocess=True,
            preprocess_num_batch=2,  # 6 symbols -> 3 batches per market = 6 batches total
            preprocess_num_recent=3,
            precompute_factor_configs=factor_cfgs,
            precompute_factor_workers=workers,
            publish_kline_num=40,
            arrow_snapshot_keep=1,
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
        return h

    times = pd.date_range("2026-11-10 00:25", periods=220, freq="h", tz="UTC")
    rt1 = times[-2].to_pydatetime()
    rt2 = times[-1].to_pydatetime()
    syms = [f"SYM{i}USDT" for i in range(6)]
    rng = np.random.default_rng(123)
    frames_spot = {}
    frames_swap = {}
    for i, s in enumerate(syms):
        for store, bias in ((frames_spot, 10.0), (frames_swap, 10.2)):
            p = bias + i * 3.0 + np.cumsum(rng.standard_normal(len(times)) * 0.08)
            p = np.maximum(p, 1.0)
            store[s] = pd.DataFrame({
                "candle_begin_time": times,
                "open": p, "high": p * 1.01, "low": p * 0.99, "close": p * 1.002,
                "volume": 500.0 + i * 10, "quote_volume": (500.0 + i * 10) * p,
                "trade_num": pd.Series([20 + i] * len(times), dtype="int64"),
                "taker_buy_base_asset_volume": 250.0,
                "taker_buy_quote_asset_volume": 250.0 * p,
            })

    def _seed_round(h, upto_idx: int, rt):
        begins_spot, begins_swap = {}, {}
        tails_spot, tails_swap = {}, {}
        for s in syms:
            sp_df = frames_spot[s].iloc[:upto_idx].reset_index(drop=True)
            sw_df = frames_swap[s].iloc[:upto_idx].reset_index(drop=True)
            h.spot.resample_mgr_map["25m"].set_candle(s, rt, sp_df)
            h.swap.resample_mgr_map["25m"].set_candle(s, rt, sw_df)
            begins_spot[s] = (sp_df["candle_begin_time"].iloc[0], sp_df["candle_begin_time"].iloc[-1])
            begins_swap[s] = (sw_df["candle_begin_time"].iloc[0], sw_df["candle_begin_time"].iloc[-1])
            tails_spot[s] = (begins_spot[s][0], begins_spot[s][1], sp_df.tail(12).reset_index(drop=True))
            tails_swap[s] = (begins_swap[s][0], begins_swap[s][1], sw_df.tail(12).reset_index(drop=True))
            fdf = pd.DataFrame({
                "candle_begin_time": sw_df["candle_begin_time"].dt.floor("h"),
                "funding_rate": np.linspace(-0.0001, 0.0002, len(sw_df)),
            }).drop_duplicates("candle_begin_time")
            h.funding_mgr.set_candle(s, rt, fdf)
        for tt in ("spot", "swap"):
            h.exg_mgr.set_candle(f"exginfo_{tt}", rt, pd.DataFrame({"symbol": syms}))
        h.exg_mgr.set_candle("spot_swap_matches", rt, pd.DataFrame({"spot": syms, "swap": syms}))
        return begins_spot, begins_swap, {"spot": tails_spot, "swap": tails_swap}

    monkeypatch.setattr(pp.bmac_config, "precompute_factor_configs", factor_cfgs)
    monkeypatch.setattr(pp.bmac_config, "publish_kline_num", 40)

    # 1) Serial in-process Oracle (R1 cold -> R2 incremental)
    h_ora = _build_side("ora", workers=0)
    monkeypatch.setattr(pp.bmac_config, "precompute_factor_workers", 0)
    bs1, bw1, _ = _seed_round(h_ora, len(times) - 1, rt1)
    pp.preprocess_offset_data(h_ora, rt1, bs1, bw1)
    bs2, bw2, _ = _seed_round(h_ora, len(times), rt2)
    pp.preprocess_offset_data(h_ora, rt2, bs2, bw2)

    # 2) Multi-process 6-batch parallel pipeline with resample_tails (R1 cold -> R2 incremental)
    h_par = _build_side("par", workers=2)
    monkeypatch.setattr(pp.bmac_config, "precompute_factor_workers", 2)
    with ProcessPoolExecutor(max_workers=2, mp_context=mp.get_context("spawn")) as pool:
        ps1, pw1, tails1 = _seed_round(h_par, len(times) - 1, rt1)
        pp.preprocess_offset_data(h_par, rt1, ps1, pw1, batch_executor=pool, resample_tails=tails1)
        ps2, pw2, tails2 = _seed_round(h_par, len(times), rt2)
        pp.preprocess_offset_data(h_par, rt2, ps2, pw2, batch_executor=pool, resample_tails=tails2)

    epoch = int(rt2.timestamp())
    ora_mgr = h_ora.preprocess_handle.preprocess_mgr_map["25m"]
    par_mgr = h_par.preprocess_handle.preprocess_mgr_map["25m"]

    for tt in ("spot", "swap"):
        assert par_mgr.check_ready(f"{tt}_dict", rt2)
        assert par_mgr.check_ready(f"market_pivot_{tt}", rt2)

        # Contract #3: raw batch PKLs contain NO factor columns and match oracle bit-for-bit
        for b_idx in (1, 2, 3):
            b_par = par_mgr.read_candle(f"{tt}_dict_batch{b_idx}")
            b_ora = ora_mgr.read_candle(f"{tt}_dict_batch{b_idx}")
            for sym in b_ora:
                assert not any(col.startswith(("ILLQ", "CciV3", "动量210401")) for col in b_par[sym].columns)
                pd.testing.assert_frame_equal(b_par[sym], b_ora[sym], check_exact=True)

        # Contract #2 & #4: Arrow 40-row snapshots match oracle bit-for-bit (including C factors)
        arr_par = read_arrow_snapshot(par_mgr.base_dir / f"{tt}_dict_{epoch}.arrow")
        arr_ora = read_arrow_snapshot(ora_mgr.base_dir / f"{tt}_dict_{epoch}.arrow")
        assert list(arr_par.keys()) == list(arr_ora.keys()) == syms
        for sym in syms:
            pd.testing.assert_frame_equal(arr_par[sym], arr_ora[sym], check_exact=True)

        # Contract #1: Yearly Pivots match oracle bit-for-bit
        piv_par = par_mgr.read_candle(f"market_pivot_{tt}_2026")
        piv_ora = ora_mgr.read_candle(f"market_pivot_{tt}_2026")
        for pk in piv_ora:
            pd.testing.assert_frame_equal(piv_par[pk], piv_ora[pk], check_exact=True)

        # Contract #5: Candle origins match oracle
        assert load_origins(par_mgr.base_dir, tt) == load_origins(ora_mgr.base_dir, tt)


def test_multiprocess_batch_failure_withholds_ready_and_compute_pool_recovers_next_round(tmp_path, monkeypatch):
    from core.handle import create_bmac_handle_from_config
    from core.pipeline import update_cycle as uc
    from core.pipeline.runtime_supervisor import RuntimeSupervisor
    from core.storage.writer import Writer
    from core.strategy import preprocess as pp
    from core.strategy.arrow_publish import read_arrow_snapshot
    from core.utils.file_system import init_dirs

    cfg = SimpleNamespace(
        storage_path=str(tmp_path / "data"),
        base_interval="5m",
        resample_interval="1h",
        kline_count_1h_spot=200,
        kline_count_1h_swap=200,
        use_api={"kline": False, "coin_cap": False},
        data_api_uuid="",
        data_api_key="",
        proxy=None,
        coin_cap_use_proxy=False,
        enabled_hour_offsets=("25m",),
        funding_rate=False,
        preprocess=True,
        preprocess_num_batch=1,  # 2 symbols -> 2 batches per market
        preprocess_num_recent=3,
        precompute_factor_configs={"CciV3": [24]},
        precompute_factor_workers=2,
        publish_kline_num=30,
        arrow_snapshot_keep=1,
        arrow_demand_enabled=False,
        arrow_demand_min_rows=None,
        use_websocket=True,
        rest_fetch_delay_seconds=3.0,
        num_writers=2,
        ws_wait_sec=0.2,
        ws_flat_pkl_bars=240,
        base_5m_retention_days=30,
        min_history_start_date="2025-03-25",
        history_trim_date=None,
    )
    h = create_bmac_handle_from_config(cfg)
    init_dirs(h)
    monkeypatch.setattr(pp.bmac_config, "precompute_factor_configs", {"CciV3": [24]})
    monkeypatch.setattr(pp.bmac_config, "publish_kline_num", 30)

    syms = ["AAAUSDT", "BBBUSDT"]
    rt1 = datetime(2026, 11, 24, 5, 25, tzinfo=timezone.utc)
    rt2 = datetime(2026, 11, 24, 6, 25, tzinfo=timezone.utc)

    def _write_5m_round(rt):
        t5 = pd.date_range(end=rt - timedelta(minutes=5), periods=180, freq="5min", tz="UTC")
        writer = Writer(
            base_candle_dirs={"spot": h.spot.base_candle_dir, "swap": h.swap.base_candle_dir},
            flat_pkl_max_bars=240,
        )
        for tt in ("spot", "swap"):
            h.exg_mgr.set_candle(f"exginfo_{tt}", rt, pd.DataFrame({"symbol": syms}))
            for i, s in enumerate(syms):
                df = pd.DataFrame({
                    "candle_begin_time": t5,
                    "open": 20.0 + i, "high": 21.0 + i, "low": 19.0 + i, "close": 20.5 + i,
                    "volume": 50.0, "quote_volume": 1025.0,
                    "trade_num": pd.Series([8] * len(t5), dtype="int64"),
                    "taker_buy_base_asset_volume": 25.0, "taker_buy_quote_asset_volume": 512.5,
                })
                writer.process_one({"symbol": s, "trade_type": tt, "df": df, "source": "ws", "run_time": rt})
        h.exg_mgr.set_candle("spot_swap_matches", rt, pd.DataFrame({"spot": syms, "swap": syms}))

    sup = RuntimeSupervisor(h, alert_webhook_url="", num_writers=1, compute_workers=2)
    pre_mgr = h.preprocess_handle.preprocess_mgr_map["25m"]
    try:
        # Round 1: inject an invalid factor name so batch worker raises ModuleNotFoundError in swap/spot
        _write_5m_round(rt1)
        monkeypatch.setattr(pp.bmac_config, "precompute_factor_configs", {"NonExistentFactorXYZ": [10]})
        with pytest.raises(ModuleNotFoundError):
            uc.run_ws_cycle(h, rt1, sup)

        # Failed round MUST withhold .ready gates
        assert not pre_mgr.check_ready("spot_dict", rt1)
        assert not pre_mgr.check_ready("swap_dict", rt1)

        # Simulate an abrupt worker crash (_broken=True) and verify supervisor recovers on Round 2
        pool1 = sup.get_compute_pool()
        pool1._broken = True

        _write_5m_round(rt2)
        monkeypatch.setattr(pp.bmac_config, "precompute_factor_configs", {"CciV3": [24]})
        res2 = uc.run_ws_cycle(h, rt2, sup)
        assert res2.is_binance_success is True
        assert sup.get_compute_pool() is not pool1

        epoch2 = int(rt2.timestamp())
        for tt in ("spot", "swap"):
            assert pre_mgr.check_ready(f"{tt}_dict", rt2)
            assert pre_mgr.check_ready(f"market_pivot_{tt}", rt2)
            snap = read_arrow_snapshot(pre_mgr.base_dir / f"{tt}_dict_{epoch2}.arrow")
            assert list(snap.keys()) == syms
            assert "CciV3_24" in snap["AAAUSDT"].columns
    finally:
        sup.shutdown(timeout_sec=5.0)
