from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest


def test_arrow_publish_writes_and_omits_history_trim_date_metadata(tmp_path):
    from core.strategy.arrow_publish import publish_arrow_snapshot

    df = pd.DataFrame({"candle_begin_time": pd.to_datetime(["2026-11-23 00:25"], utc=True), "close": [10.0]})
    p_none = tmp_path / "spot_dict_1.arrow"
    publish_arrow_snapshot({"BTCUSDT": df}, p_none, publish_kline_num=5000, history_trim_date=None)
    with pa.memory_map(str(p_none), "r") as src:
        meta = pa.ipc.open_file(src).schema.metadata or {}
        assert b"history_trim_date" not in meta

    p_set = tmp_path / "spot_dict_2.arrow"
    publish_arrow_snapshot({"BTCUSDT": df}, p_set, publish_kline_num=5000, history_trim_date="2025-06-01")
    with pa.memory_map(str(p_set), "r") as src:
        meta = pa.ipc.open_file(src).schema.metadata or {}
        assert meta.get(b"history_trim_date") == b"2025-06-01"


def test_preprocess_offset_data_schedules_swap_and_spot_concurrently(monkeypatch, tmp_path):
    from threading import Barrier, Lock
    from core.strategy import preprocess as pp

    barrier = Barrier(2)
    lock = Lock()
    started = []

    def fake_type_data(handle, run_time, trade_type, *a, **kw):
        with lock:
            started.append(trade_type)
        # Proves both markets execute concurrently in mkt_pool without serial blocking
        barrier.wait(timeout=5.0)

    monkeypatch.setattr(pp, "preprocess_type_data", fake_type_data)
    monkeypatch.setattr(pp, "load_factor_modules_params", lambda *a, **kw: [])
    exg_mgr = SimpleNamespace(read_candle=lambda _: pd.DataFrame({"spot": [], "swap": []}))
    h = SimpleNamespace(
        exg_mgr=exg_mgr, precompute_factor_configs={}, precompute_factor_workers=1,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": SimpleNamespace(base_dir=tmp_path)}),
        publish_kline_num=5000, arrow_snapshot_keep=1,
        arrow_demand_enabled=False, arrow_demand_min_rows=5000,
    )
    pp.preprocess_offset_data(h, pd.Timestamp("2026-11-23 01:25", tz="UTC"))
    assert set(started) == {"swap", "spot"}


def test_gen_pivot_fallback_preserves_delisted_columns_on_existing_year_file(tmp_path):
    from core.storage.candle_manager import CandleFileManager
    from core.strategy.preprocess import gen_pivot

    mgr = CandleFileManager(tmp_path / "25m", "pickle")
    mgr.base_dir.mkdir()
    t1 = pd.Timestamp("2026-11-22 23:25", tz="UTC")
    t2 = pd.Timestamp("2026-11-23 00:25", tz="UTC")
    old_pivot = {
        "open": pd.DataFrame({"BTCUSDT": [100.0], "ANTHROPICUSDT": [50.0]}, index=[t1]),
        "close": pd.DataFrame({"BTCUSDT": [101.0], "ANTHROPICUSDT": [51.0]}, index=[t1]),
        "vwap1m": pd.DataFrame({"BTCUSDT": [100.5], "ANTHROPICUSDT": [50.5]}, index=[t1]),
    }
    mgr.save_data_file("market_pivot_spot_2026", old_pivot)

    new_market = {
        "BTCUSDT": pd.DataFrame({
            "candle_begin_time": [t1, t2], "open": [100.0, 102.0], "close": [101.0, 103.0],
            "volume": [10.0, 10.0], "quote_volume": [1005.0, 1025.0],
        })
    }
    gen_pivot(new_market, "spot", t2 + pd.Timedelta(hours=1), mgr)
    out = mgr.read_candle("market_pivot_spot_2026")
    assert "ANTHROPICUSDT" in out["close"].columns
    assert out["close"].loc[t1, "ANTHROPICUSDT"] == 51.0
    assert pd.isna(out["close"].loc[t2, "ANTHROPICUSDT"])


def test_gen_batch_data_incremental_keeps_full_history_when_trim_ts_none(tmp_path):
    """When history_trim_ts() is None, batch PKL must NOT slide-trim older rows even beyond kline_count_1h."""
    from core.storage.candle_manager import CandleFileManager
    from core.strategy.preprocess import gen_batch_data_incremental

    times = pd.date_range("2025-03-08 00:25", periods=20, freq="h", tz="UTC")
    df_1h = pd.DataFrame({
        "candle_begin_time": times, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "volume": 1.0, "quote_volume": 1.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 0.5, "taker_buy_quote_asset_volume": 0.5,
    })
    res_dir = tmp_path / "spot_1h" / "25m"
    pre_dir = tmp_path / "pre" / "25m"
    exg_dir = tmp_path / "exg"
    for d in (res_dir, pre_dir, exg_dir):
        d.mkdir(parents=True)
    res_mgr = CandleFileManager(res_dir, "pickle")
    res_mgr.set_candle("BTCUSDT", times[-1] + pd.Timedelta(hours=1), df_1h)
    pre_mgr = CandleFileManager(pre_dir, "pickle")
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))
    match = pd.DataFrame([{"spot": "BTCUSDT", "swap": "BTCUSDT", "first_begin_time": times[0], "last_begin_time": times[-1]}])
    h = SimpleNamespace(
        kline_count_1h=5, kline_count_1h_spot=5, kline_count_1h_swap=5,
        history_trim_ts=lambda: None, fetch_funding_rate=False, funding_mgr=None,
        data_api=None, exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        preprocess_handle=SimpleNamespace(num_batch=10, preprocess_mgr_map={"25m": pre_mgr}),
    )
    market_dict, *_ = gen_batch_data_incremental(h, "spot", times[-1] + pd.Timedelta(hours=1), match, {}, {})
    # Must keep all 20 rows (NOT truncated to kline_count_1h=5)
    assert len(market_dict["BTCUSDT"]) == 20
    assert market_dict["BTCUSDT"]["candle_begin_time"].iloc[0] == times[0]


def test_gen_batch_data_incremental_applies_fixed_history_trim_ts(tmp_path):
    """When history_trim_ts() returns a UTC Timestamp, batch PKL trims rows strictly older than min_ts."""
    from core.storage.candle_manager import CandleFileManager
    from core.strategy.preprocess import gen_batch_data_incremental

    times = pd.date_range("2025-03-08 00:25", periods=20, freq="h", tz="UTC")
    df_1h = pd.DataFrame({
        "candle_begin_time": times, "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "volume": 1.0, "quote_volume": 1.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 0.5, "taker_buy_quote_asset_volume": 0.5,
    })
    res_dir = tmp_path / "spot_1h" / "25m"
    pre_dir = tmp_path / "pre" / "25m"
    exg_dir = tmp_path / "exg"
    for d in (res_dir, pre_dir, exg_dir):
        d.mkdir(parents=True)
    res_mgr = CandleFileManager(res_dir, "pickle")
    res_mgr.set_candle("BTCUSDT", times[-1] + pd.Timedelta(hours=1), df_1h)
    pre_mgr = CandleFileManager(pre_dir, "pickle")
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))
    match = pd.DataFrame([{"spot": "BTCUSDT", "swap": "BTCUSDT", "first_begin_time": times[0], "last_begin_time": times[-1]}])
    trim_ts = times[5]
    h = SimpleNamespace(
        kline_count_1h=5, kline_count_1h_spot=5, kline_count_1h_swap=5,
        history_trim_ts=lambda: trim_ts, fetch_funding_rate=False, funding_mgr=None,
        data_api=None, exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        preprocess_handle=SimpleNamespace(num_batch=10, preprocess_mgr_map={"25m": pre_mgr}),
    )
    market_dict, *_ = gen_batch_data_incremental(h, "spot", times[-1] + pd.Timedelta(hours=1), match, {}, {})
    assert len(market_dict["BTCUSDT"]) == 15
    assert market_dict["BTCUSDT"]["candle_begin_time"].iloc[0] == trim_ts
    assert market_dict["BTCUSDT"]["first_candle_time"].iloc[0] == times[0]


def test_preprocess_type_data_frees_last_batch_frames_before_pivot(tmp_path, monkeypatch):
    """All market_dict DataFrames (including the final batch) must be GC'd before gen_pivot_incremental runs."""
    import weakref
    from core.storage.candle_manager import CandleFileManager
    from core.strategy import preprocess as pp

    times = pd.date_range("2026-11-23 00:25", periods=5, freq="h", tz="UTC")
    base_df = pd.DataFrame({
        "candle_begin_time": times, "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5,
        "volume": 100.0, "quote_volume": 1050.0, "trade_num": 10,
        "taker_buy_base_asset_volume": 50.0, "taker_buy_quote_asset_volume": 525.0,
    })
    res_dir = tmp_path / "spot_1h" / "25m"
    pre_dir = tmp_path / "pre" / "25m"
    exg_dir = tmp_path / "exg"
    for d in (res_dir, pre_dir, exg_dir):
        d.mkdir(parents=True)
    run_time = times[-1]
    res_mgr = CandleFileManager(res_dir, "pickle")
    for sym in ("AAAUSDT", "ZZZUSDT"):
        res_mgr.set_candle(sym, run_time, base_df.copy())
    pre_mgr = CandleFileManager(pre_dir, "pickle")
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.set_candle("exginfo_spot", run_time, pd.DataFrame({"symbol": ["AAAUSDT", "ZZZUSDT"]}))
    match = pd.DataFrame([
        {"spot": sym, "swap": sym, "first_begin_time": times[0], "last_begin_time": times[-1]}
        for sym in ("AAAUSDT", "ZZZUSDT")
    ])
    handle = SimpleNamespace(
        resample_interval="1h",
        history_trim_date=None,
        history_trim_ts=lambda: None,
        fetch_funding_rate=False,
        funding_mgr=None,
        data_api=None,
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        get_binance_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": res_mgr}),
        preprocess_handle=SimpleNamespace(num_batch=1, preprocess_mgr_map={"25m": pre_mgr}),
    )

    frame_refs: dict[str, weakref.ReferenceType] = {}
    alive_at_pivot: dict[str, bool] = {}

    def fake_publish(h, rt, tt, market_dict, publish_kline_num=None, *, history_trim_date=None):
        for sym, df in market_dict.items():
            frame_refs[sym] = weakref.ref(df)

    def fake_pivot(h, rt, tt, year_pivot_names, *, funding_publication=None, year_pivot_data=None):
        for sym, ref in frame_refs.items():
            alive_at_pivot[sym] = ref() is not None

    monkeypatch.setattr(pp, "publish_arrow_snapshot_for_type", fake_publish)
    monkeypatch.setattr(pp, "gen_pivot_incremental", fake_pivot)

    factor_spec = [(SimpleNamespace(signal_multi_params=lambda df, params: {"1": df["close"] * 2.0}), "Dummy", [1])]
    pp.preprocess_type_data(handle, run_time, "spot", match, factor_modules_params=factor_spec, publish_kline_num=10)

    assert set(frame_refs) == {"AAAUSDT", "ZZZUSDT"}
    assert alive_at_pivot == {"AAAUSDT": False, "ZZZUSDT": False}
