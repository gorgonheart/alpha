from datetime import datetime, timezone
from types import SimpleNamespace
import pandas as pd
import pytest


def test_save_funding_hours_preserves_full_history_when_min_ts_none_and_trims_when_set(tmp_path):
    from core.storage.candle_manager import CandleFileManager
    from core.strategy.funding_hours import save_funding_hours

    mgr = CandleFileManager(tmp_path / "funding", "pickle")
    mgr.base_dir.mkdir()
    old = pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2025-03-08 00:00", "2026-11-22 00:00"], utc=True),
        "funding_rate": [0.001, 0.002],
    })
    mgr.save_data_file("BTCUSDT", old)
    new = pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2026-11-23 00:00"], utc=True),
        "funding_rate": [0.003],
    })
    # min_ts=None -> no head trim
    save_funding_hours(mgr, "BTCUSDT", new, min_ts=None)
    assert len(mgr.read_candle("BTCUSDT")) == 3
    # Explicit min_ts -> fixed timestamp cut
    save_funding_hours(mgr, "BTCUSDT", new, min_ts=pd.Timestamp("2026-11-22 00:00", tz="UTC"))
    out = mgr.read_candle("BTCUSDT")
    assert len(out) == 2 and out["candle_begin_time"].iloc[0] == pd.Timestamp("2026-11-22 00:00", tz="UTC")


def test_update_binance_base_candles_trims_all_partitions_beyond_30d(tmp_path):
    from core.pipeline.data_update import update_binance_base_candles
    from core.storage.ts_manager import TSManager

    base_dir = tmp_path / "spot_5m"
    ts_mgr = TSManager(base_dir / "BTCUSDT")
    ts_mgr.update(pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2025-04-01 00:00", "2025-05-01 00:00", "2026-11-20 00:00"], utc=True),
        "open": 1.0, "close": 1.0,
    }))
    h = SimpleNamespace(
        base_interval="5m", base_5m_retention_bars=lambda: 30 * 24 * 12,
        get_market_handle=lambda tt: SimpleNamespace(base_candle_dir=base_dir if tt == "spot" else tmp_path / "swap_5m"),
    )
    run_time = datetime(2026, 11, 23, 0, 25, tzinfo=timezone.utc)
    spot_dfs = {"BTCUSDT": pd.DataFrame({"candle_begin_time": pd.to_datetime(["2026-11-23 00:20"], utc=True), "open": 2.0, "close": 2.0})}
    update_binance_base_candles(h, run_time, spot_dfs, {})
    parts = ts_mgr.list_partitions()
    assert "202504" not in parts and "202505" not in parts and "202611" in parts


def test_history_funding_uses_effective_kline_count_1h_for_full_download(tmp_path, monkeypatch):
    import asyncio
    from contextlib import AsyncExitStack
    from core.history_data import funding as hist_funding
    from core.storage.candle_manager import CandleFileManager

    calls = []

    async def fake_hist(**params):
        calls.append(params)
        return []

    monkeypatch.setattr(hist_funding, "create_aiohttp_session", lambda *a: AsyncExitStack())
    monkeypatch.setattr(
        "core.exchange.rest.binance_fetcher.create_binance_market_api",
        lambda *a: SimpleNamespace(aioreq_funding_rate=fake_hist),
    )

    mgr = CandleFileManager(tmp_path / "funding", "pickle")
    mgr.base_dir.mkdir()
    run_time = datetime(2026, 11, 23, 8, 0, tzinfo=timezone.utc)
    seen_args = []

    def eff(tt, rt):
        seen_args.append((tt, rt))
        return 48

    h = SimpleNamespace(
        exchange="binance",
        proxy=None,
        funding_mgr=mgr,
        get_market_handle=lambda tt: SimpleNamespace(api_type="usdt_futures"),
        effective_kline_count_1h=eff,
        history_trim_ts=lambda: None,
    )
    asyncio.run(hist_funding.async_download_funding_rates(h, run_time, ["BTCUSDT"]))
    assert seen_args == [("swap", run_time)]
    expected_start_ms = int((run_time - pd.Timedelta(hours=48)).timestamp() * 1000)
    assert calls[0]["startTime"] == expected_start_ms


def test_data_api_merge_respects_history_trim_ts(tmp_path, monkeypatch):
    import asyncio
    from contextlib import AsyncExitStack
    from core.ingest import data_api_update
    from core.storage.candle_manager import CandleFileManager

    mgr = CandleFileManager(tmp_path / "data_api_swap", "pickle")
    mgr.base_dir.mkdir()
    old_times = pd.date_range("2025-04-01 00:00", periods=3, freq="h", tz="UTC")
    old_df = pd.DataFrame({
        "candle_begin_time": old_times,
        "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0,
        "volume": 10.0, "quote_volume": 10.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 5.0, "taker_buy_quote_asset_volume": 5.0,
    })
    old_df["candle_end_time"] = old_df["candle_begin_time"] + pd.Timedelta(hours=1)
    old_df = old_df.set_index("candle_end_time")
    mgr.save_data_file("BTCUSDT", old_df)

    new_time = pd.Timestamp("2025-04-01 03:00", tz="UTC")
    incoming = pd.DataFrame({
        "symbol": ["BTCUSDT"],
        "candle_begin_time": [new_time],
        "candle_end_time": [new_time + pd.Timedelta(hours=1)],
        "open": [2.0], "high": [2.0], "low": [2.0], "close": [2.0],
        "volume": [20.0], "quote_volume": [20.0], "trade_num": [2],
        "taker_buy_base_asset_volume": [10.0], "taker_buy_quote_asset_volume": [10.0],
    })

    class FakeApi:
        def __init__(self, *a, **kw):
            pass

        async def aioreq_candle_df(self, url):
            return incoming

    monkeypatch.setattr(data_api_update, "create_aiohttp_session", lambda *a: AsyncExitStack())
    monkeypatch.setattr(data_api_update, "QuantclassDataApi", FakeApi)

    run_time = datetime(2025, 4, 1, 4, 0, tzinfo=timezone.utc)
    data_api_h = SimpleNamespace(
        cfg=SimpleNamespace(data_api_key="k", data_api_uuid="u"),
        get_mgr_map=lambda tt: {"0m": mgr},
    )
    h_no_trim = SimpleNamespace(
        http_timeout_sec=5,
        data_api=data_api_h,
        kline_count_1h=2,
        history_trim_ts=lambda: None,
    )
    asyncio.run(data_api_update.async_merge_data_api_candles(h_no_trim, run_time, "swap", "https://example.invalid"))
    assert len(mgr.read_candle("BTCUSDT")) == 4

    h_trim = SimpleNamespace(
        http_timeout_sec=5,
        data_api=data_api_h,
        kline_count_1h=2,
        history_trim_ts=lambda: pd.Timestamp("2025-04-01 02:00", tz="UTC"),
    )
    asyncio.run(data_api_update.async_merge_data_api_candles(h_trim, run_time, "swap", "https://example.invalid"))
    out = mgr.read_candle("BTCUSDT")
    assert len(out) == 2
    assert out["candle_begin_time"].iloc[0] == pd.Timestamp("2025-04-01 02:00", tz="UTC")


def test_backfill_local_resample_drops_truncated_first_bar_and_preserves_history(tmp_path):
    from core.pipeline.backfill import local_resample_offset
    from core.storage.candle_manager import CandleFileManager
    from core.storage.ts_manager import TSManager

    base_dir = tmp_path / "spot_5m"
    resample_dir = tmp_path / "resample_1h" / "0m"
    resample_dir.mkdir(parents=True)
    resample_mgr = CandleFileManager(resample_dir, "pickle")

    # Existing 1h history has complete bars for 00:00 and 01:00
    old_times = pd.to_datetime(["2026-11-22 00:00", "2026-11-22 01:00"], utc=True)
    old_1h = pd.DataFrame({
        "candle_begin_time": old_times,
        "open": [10.0, 20.0], "high": [15.0, 25.0], "low": [9.0, 19.0], "close": [12.0, 22.0],
        "volume": [100.0, 200.0], "quote_volume": [1000.0, 2000.0], "trade_num": [10, 20],
        "taker_buy_base_asset_volume": [50.0, 100.0], "taker_buy_quote_asset_volume": [500.0, 1000.0],
    })
    old_1h["candle_end_time"] = old_1h["candle_begin_time"] + pd.Timedelta(hours=1)
    resample_mgr.save_data_file("BTCUSDT", old_1h.set_index("candle_end_time"))

    # Base 5m starts mid-hour at 01:30 (truncated 01:00 bucket!) plus full 02:00 bucket
    t_trunc = pd.date_range("2026-11-22 01:30", "2026-11-22 01:55", freq="5min", tz="UTC")
    t_full = pd.date_range("2026-11-22 02:00", "2026-11-22 02:55", freq="5min", tz="UTC")
    base_times = t_trunc.append(t_full)
    ts_mgr = TSManager(base_dir / "BTCUSDT")
    ts_mgr.update(pd.DataFrame({
        "candle_begin_time": base_times,
        "open": [999.0] * len(t_trunc) + [30.0] * len(t_full),
        "high": [999.0] * len(t_trunc) + [35.0] * len(t_full),
        "low": [999.0] * len(t_trunc) + [29.0] * len(t_full),
        "close": [999.0] * len(t_trunc) + [32.0] * len(t_full),
        "volume": 1.0, "quote_volume": 10.0, "trade_num": 1,
        "taker_buy_base_asset_volume": 0.5, "taker_buy_quote_asset_volume": 5.0,
    }))

    h = SimpleNamespace(
        resample_interval="1h",
        effective_kline_count_1h=lambda tt, rt: 48,
        history_trim_ts=lambda: None,
        get_market_handle=lambda tt: SimpleNamespace(
            base_candle_dir=base_dir,
            resample_mgr_map={"0m": resample_mgr},
        ),
    )
    run_time = datetime(2026, 11, 22, 3, 0, tzinfo=timezone.utc)
    result = local_resample_offset(h, "spot", run_time, "0m")
    assert "BTCUSDT" in result
    out = resample_mgr.read_candle("BTCUSDT").set_index("candle_begin_time")
    # 00:00 preserved, 01:00 NOT overwritten by truncated 999.0 bar, 02:00 appended!
    assert out.loc[pd.Timestamp("2026-11-22 00:00", tz="UTC"), "open"] == 10.0
    assert out.loc[pd.Timestamp("2026-11-22 01:00", tz="UTC"), "open"] == 20.0
    assert out.loc[pd.Timestamp("2026-11-22 02:00", tz="UTC"), "open"] == 30.0
