from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import pandas as pd
import pytest


def test_fill_gaps_restores_integer_trade_num_dtype():
    from core.pipeline.resample import fill_gaps
    times = pd.to_datetime(["2026-11-23 00:00", "2026-11-23 00:10"], utc=True)
    df = pd.DataFrame({
        "candle_begin_time": times, "open": [1.0, 2.0], "high": [1.0, 2.0],
        "low": [1.0, 2.0], "close": [1.0, 2.0], "volume": [10.0, 20.0],
        "quote_volume": [10.0, 40.0], "trade_num": pd.Series([5, 8], dtype="int64"),
        "taker_buy_base_asset_volume": [3.0, 4.0], "taker_buy_quote_asset_volume": [3.0, 8.0],
    })
    out = fill_gaps(df, timedelta(minutes=5))
    assert len(out) == 3
    assert pd.api.types.is_integer_dtype(out["trade_num"].dtype)
    assert out["trade_num"].iloc[1] == 0


def test_detect_full_backfill_only_flags_missing_or_stale_never_existing_shorter_history(tmp_path):
    """alphatest (~2025-04-16) must take fast path with zero implicit head backfill."""
    from core.history_data.candle import detect_full_backfill_symbols
    from core.storage.candle_manager import CandleFileManager

    res_dir = tmp_path / "25m"
    res_dir.mkdir()
    mgr = CandleFileManager(res_dir, "pickle")
    run_time = datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc)
    # Existing alphatest-style coin: starts 2025-04-16, fresh up to run_time - 1h -> NOT flagged
    fresh_df = pd.DataFrame({"candle_begin_time": pd.to_datetime(["2025-04-16 00:25", "2026-11-23 00:25"], utc=True), "close": [1.0, 2.0]})
    mgr.save_data_file("BTCUSDT", fresh_df)
    # Stale > 28d -> flagged (Condition 2)
    stale_df = pd.DataFrame({"candle_begin_time": pd.to_datetime(["2025-04-16 00:25", "2026-10-20 00:25"], utc=True), "close": [1.0, 2.0]})
    mgr.save_data_file("STALEUSDT", stale_df)
    # Missing NEWOFFSET -> flagged (Condition 1)
    h = SimpleNamespace(
        enabled_hour_offsets=("25m",), base_5m_retention_days=30,
        get_market_handle=lambda _: SimpleNamespace(resample_mgr_map={"25m": mgr}),
    )
    flagged = detect_full_backfill_symbols(h, "spot", ["BTCUSDT", "STALEUSDT", "MISSINGUSDT"], run_time)
    assert flagged == {"STALEUSDT", "MISSINGUSDT"}


def test_resample_history_fast_path_preserves_old_head_and_drops_incomplete_first_bar(tmp_path):
    from core.pipeline.resample import resample_history
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager

    run_time = datetime(2026, 11, 23, 2, 25, tzinfo=timezone.utc)
    # Existing 1h history back to 2025-03-08
    res_dir = tmp_path / "spot_1h" / "25m"
    res_dir.mkdir(parents=True)
    res_mgr = CandleFileManager(res_dir, "pickle")
    old_1h_times = pd.to_datetime(["2025-03-08 00:25", "2026-11-22 23:25", "2026-11-23 00:25"], utc=True)
    old_1h = pd.DataFrame({
        "candle_begin_time": old_1h_times, "open": [100.0, 200.0, 300.0], "high": [100.0, 200.0, 300.0],
        "low": [100.0, 200.0, 300.0], "close": [100.0, 200.0, 300.0], "volume": [1.0, 1.0, 1.0],
        "quote_volume": [1.0, 1.0, 1.0], "trade_num": [1, 1, 1],
        "taker_buy_base_asset_volume": [1.0, 1.0, 1.0], "taker_buy_quote_asset_volume": [1.0, 1.0, 1.0],
    })
    res_mgr.save_data_file("BTCUSDT", old_1h)

    # Base 5m starts mid-hour at 2026-11-23 00:40 (incomplete 00:25 bar!) through 02:20
    base_dir = tmp_path / "spot_5m"
    ts_mgr = TSManager(base_dir / "BTCUSDT")
    base_times = pd.date_range("2026-11-23 00:40", "2026-11-23 02:20", freq="5min", tz="UTC")
    ts_mgr.update(pd.DataFrame({
        "candle_begin_time": base_times, "open": 999.0, "high": 999.0, "low": 999.0, "close": 999.0,
        "volume": 2.0, "quote_volume": 2.0, "trade_num": 2,
        "taker_buy_base_asset_volume": 1.0, "taker_buy_quote_asset_volume": 1.0,
    }))

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))

    h = SimpleNamespace(
        base_interval="5m", resample_interval="1h", enabled_hour_offsets=("25m",),
        base_5m_retention_days=30, history_trim_ts=lambda: None,
        effective_kline_count_1h=lambda tt, rt: 15000,
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=base_dir, resample_mgr_map={"25m": res_mgr}),
    )
    resample_history(h, "spot", run_time, full_backfill_symbols=set())
    out = res_mgr.read_candle("BTCUSDT")
    # 2025-03-08 head preserved; 00:25 untouched (still close=300.0, NOT overwritten by partial 999.0); 01:25 appended
    assert out["candle_begin_time"].iloc[0] == pd.Timestamp("2025-03-08 00:25", tz="UTC")
    assert out.loc[out["candle_begin_time"] == pd.Timestamp("2026-11-23 00:25", tz="UTC"), "close"].iloc[0] == 300.0
    assert pd.Timestamp("2026-11-23 01:25", tz="UTC") in set(out["candle_begin_time"])


def test_update_resample_from_base_uses_thread_pool_not_process_pool(monkeypatch, tmp_path):
    from core.pipeline import resample as res_mod
    from core.storage.candle_manager import CandleFileManager

    def forbid_process_pool(*a, **kw):
        raise AssertionError("ProcessPoolExecutor forbidden in update_resample_from_base (Mine 3)")
    monkeypatch.setattr(res_mod, "ProcessPoolExecutor", forbid_process_pool, raising=False)

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": []}))
    h = SimpleNamespace(
        base_interval="5m", resample_interval="1h", enabled_hour_offsets=("25m",),
        history_trim_ts=lambda: None, exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=tmp_path / "s5m", resample_mgr_map={"25m": CandleFileManager(tmp_path / "s1h", "pickle")}, once_update_candles=99),
    )
    assert res_mod.update_resample_from_base(h, "spot", datetime(2026, 11, 23, 1, 25, tzinfo=timezone.utc)) == {}


def test_resample_history_self_heal_builds_missing_offset_and_trims_base_5m_to_30d(tmp_path):
    from core.pipeline.resample import resample_history
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager

    run_time = datetime(2026, 11, 23, 2, 25, tzinfo=timezone.utc)
    base_dir = tmp_path / "spot_5m"
    ts_mgr = TSManager(base_dir / "BTCUSDT")
    # Temporary deep 5m partitions (202504 + 202611) fetched for self-heal
    t_old = pd.date_range("2025-04-01 00:00", periods=24, freq="5min", tz="UTC")
    t_new = pd.date_range("2026-11-22 23:00", "2026-11-23 02:20", freq="5min", tz="UTC")
    all_t = t_old.append(t_new)
    ts_mgr.update(pd.DataFrame({
        "candle_begin_time": all_t, "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
        "volume": 1.0, "quote_volume": 10.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 0.5, "taker_buy_quote_asset_volume": 5.0,
    }))
    res_dir = tmp_path / "spot_1h" / "25m"
    res_dir.mkdir(parents=True)
    res_mgr = CandleFileManager(res_dir, "pickle")
    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": ["BTCUSDT"]}))

    h = SimpleNamespace(
        base_interval="5m", resample_interval="1h", enabled_hour_offsets=("25m",),
        base_5m_retention_days=30, base_5m_retention_bars=lambda: 30 * 24 * 12,
        history_trim_ts=lambda: None, effective_kline_count_1h=lambda tt, rt: 15000,
        exg_mgr=exg_mgr,
        get_market_handle=lambda _: SimpleNamespace(base_candle_dir=base_dir, resample_mgr_map={"25m": res_mgr}),
    )
    resample_history(h, "spot", run_time, full_backfill_symbols={"BTCUSDT"})
    out_1h = res_mgr.read_candle("BTCUSDT")
    assert out_1h is not None and out_1h["candle_begin_time"].min() <= pd.Timestamp("2025-04-01 01:25", tz="UTC")
    # Temporary old 5m partition must be trimmed back to 30d after resample completes
    assert "202504" not in ts_mgr.list_partitions()
    assert "202611" in ts_mgr.list_partitions()


def test_update_resample_candle_min_ts_trim_and_none_no_trim(tmp_path):
    from core.pipeline.resample import update_resample_candle
    from core.storage.candle_manager import CandleFileManager

    res_dir = tmp_path / "25m"
    res_dir.mkdir()
    mgr = CandleFileManager(res_dir, "pickle")
    run_time = datetime(2026, 11, 23, 2, 25, tzinfo=timezone.utc)

    times = pd.to_datetime(["2025-03-08 00:25", "2025-03-08 01:25", "2025-03-08 02:25"], utc=True)
    df = pd.DataFrame({
        "candle_begin_time": times, "open": [1.0, 1.0, 1.0], "high": [1.0, 1.0, 1.0],
        "low": [1.0, 1.0, 1.0], "close": [1.0, 1.0, 1.0], "volume": [1.0, 1.0, 1.0],
        "quote_volume": [1.0, 1.0, 1.0], "trade_num": [1, 1, 1],
        "taker_buy_base_asset_volume": [1.0, 1.0, 1.0], "taker_buy_quote_asset_volume": [1.0, 1.0, 1.0],
    })
    # num_candles=None, min_ts=None -> no head trim
    first_t, last_t = update_resample_candle(mgr, "BTCUSDT", df, None, run_time, timedelta(hours=1), min_ts=None)
    assert first_t == pd.Timestamp("2025-03-08 00:25", tz="UTC")
    assert len(mgr.read_candle("BTCUSDT")) == 3

    # min_ts specified -> trims strictly before min_ts
    min_ts = pd.Timestamp("2025-03-08 01:25", tz="UTC")
    first_t2, _ = update_resample_candle(mgr, "BTCUSDT", df.iloc[0:0], None, run_time, timedelta(hours=1), min_ts=min_ts)
    assert first_t2 == min_ts
    assert len(mgr.read_candle("BTCUSDT")) == 2


def test_run_download_all_history_compat_and_with_backfill(monkeypatch):
    from core.history_data import history as hist_mod

    run_time = datetime(2026, 11, 23, 2, 25, tzinfo=timezone.utc)

    async def fake_get_symbols(handle, trade_type):
        return ["BTCUSDT"]

    def fake_init_history_candles(handle, trade_type, rt, symbols, full_backfill_symbols=None):
        return {"BTCUSDT"} if trade_type == "spot" else set()

    monkeypatch.setattr(hist_mod, "get_type_symbols", fake_get_symbols)
    monkeypatch.setattr(hist_mod, "init_history_candles", fake_init_history_candles)
    monkeypatch.setattr(hist_mod, "next_run_time", lambda _: run_time + timedelta(minutes=5))
    monkeypatch.setattr(hist_mod, "now_time", lambda: run_time + timedelta(minutes=1))

    h = SimpleNamespace(
        base_interval="5m", resample_interval="1h",
        fetch_funding_rate=False, coin_cap_handle=None,
    )
    rt_with, bf = hist_mod.run_download_all_history_with_backfill(h)
    assert rt_with == run_time
    assert bf == {"spot": {"BTCUSDT"}, "swap": set()}

    rt_compat = hist_mod.run_download_all_history(h)
    assert rt_compat == run_time
