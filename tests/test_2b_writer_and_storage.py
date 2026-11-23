from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import pytest


def test_writer_caps_flat_pkl_at_240_and_trims_all_old_partitions(tmp_path):
    from core.storage.writer import Writer
    from core.storage.ts_manager import TSManager

    base_dir = tmp_path / "spot_5m"
    base_dir.mkdir()
    # Seed 3 old monthly partitions (202504, 202505, 202610) + recent 202611
    ts_mgr = TSManager(base_dir / "BTCUSDT")
    old_times = pd.to_datetime(
        ["2025-04-01 00:00", "2025-05-01 00:00", "2026-10-01 00:00", "2026-11-20 00:00"], utc=True
    )
    ts_mgr.update(pd.DataFrame({"candle_begin_time": old_times, "open": 1.0, "close": 1.0}))
    assert len(ts_mgr.list_partitions()) == 4

    writer = Writer(
        base_candle_dirs={"spot": base_dir},
        flat_pkl_max_bars=240,
        partition_retention_bars=30 * 24 * 12,
        base_delta=timedelta(minutes=5),
    )
    # Push 300 bars via WS -> flat pkl must cap at 240, partitions older than 30d ALL deleted
    run_time = datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)
    times = pd.date_range(end=run_time - timedelta(minutes=5), periods=300, freq="5min", tz="UTC")
    df_new = pd.DataFrame({"candle_begin_time": times, "open": 2.0, "close": 2.0})
    writer.process_one({"symbol": "BTCUSDT", "trade_type": "spot", "df": df_new, "source": "ws", "run_time": run_time})

    flat = pd.read_pickle(base_dir / "BTCUSDT.pkl")
    assert len(flat) == 240
    assert flat["candle_begin_time"].dtype == "datetime64[ns, UTC]"
    # All 300 pushed bars must be persisted in TSManager (not truncated by flat_pkl_max_bars)
    ts_all = ts_mgr.read_all()
    assert ts_all is not None
    assert set(times).issubset(set(ts_all["candle_begin_time"]))
    assert len(ts_all) == 301
    # 202504, 202505, 202610 must ALL be gone (upstream bug only trimmed partitions[0])
    remaining = ts_mgr.list_partitions()
    assert "202504" not in remaining and "202505" not in remaining and "202610" not in remaining
    assert "202611" in remaining


def test_writer_normalizes_mixed_naive_and_ms_timestamps_in_legacy_flat_pkl(tmp_path):
    """Review Focus #2: legacy flat pkl with tz-naive or ms precision merges cleanly with ns UTC."""
    from core.storage.writer import Writer

    base_dir = tmp_path / "swap_5m"
    base_dir.mkdir()
    legacy = pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2026-11-22 23:50:00"]).astype("datetime64[ms]"),
        "open": [10.0], "close": [10.0],
    })
    legacy.to_pickle(base_dir / "ETHUSDT.pkl")

    writer = Writer(base_candle_dirs={"swap": base_dir}, flat_pkl_max_bars=240, partition_retention_bars=8640)
    run_time = datetime(2026, 11, 23, 0, 0, tzinfo=timezone.utc)
    incoming = pd.DataFrame({
        "candle_begin_time": pd.to_datetime(["2026-11-22 23:55:00"], utc=True),
        "open": [11.0], "close": [11.0],
    })
    writer.process_one({"symbol": "ETHUSDT", "trade_type": "swap", "df": incoming, "source": "ws", "run_time": run_time})
    merged = pd.read_pickle(base_dir / "ETHUSDT.pkl")
    assert len(merged) == 2
    assert merged["candle_begin_time"].dtype == "datetime64[ns, UTC]"


def test_init_dirs_preserves_existing_pivots_batches_and_arrow_demand(tmp_path):
    from core.storage.candle_manager import CandleFileManager
    from core.utils.file_system import init_dirs

    pre_dir = tmp_path / "preprocess" / "25m"
    (pre_dir / ".arrow-demand").mkdir(parents=True)
    pivot_file = pre_dir / "market_pivot_spot_2026.pkl"
    complete_file = pre_dir / "market_pivot_spot_2026.complete"
    demand_file = pre_dir / ".arrow-demand" / "acct.json"
    batch_file = pre_dir / "spot_dict_batch0.pkl"
    for f in (pivot_file, complete_file, demand_file, batch_file):
        f.write_bytes(b"keep")
    res_dir = tmp_path / "spot_1h" / "25m"
    res_dir.mkdir(parents=True)
    res_file = res_dir / "BTCUSDT.pkl"
    res_file.write_bytes(b"keep-1h")
    exg_dir = tmp_path / "exginfo"
    exg_dir.mkdir()
    exg_file = exg_dir / "exginfo_spot.pkl"
    exg_file.write_bytes(b"keep-exg")

    h = SimpleNamespace(
        spot=SimpleNamespace(base_candle_dir=tmp_path / "spot_5m", resample_dir=tmp_path / "spot_1h",
                             resample_mgr_map={"25m": CandleFileManager(res_dir, "pickle")}),
        swap=SimpleNamespace(base_candle_dir=tmp_path / "swap_5m", resample_dir=tmp_path / "swap_1h",
                             resample_mgr_map={}),
        exg_mgr=CandleFileManager(exg_dir, "pickle"),
        funding_mgr=CandleFileManager(tmp_path / "funding", "pickle"),
        data_api=None,
        preprocess_handle=SimpleNamespace(preprocess_dir=tmp_path / "preprocess",
                                          preprocess_mgr_map={"25m": CandleFileManager(pre_dir, "pickle")}),
        coin_cap_handle=None,
    )
    init_dirs(h)
    for f in (pivot_file, complete_file, demand_file, batch_file, res_file, exg_file):
        assert f.exists() and f.read_bytes().startswith(b"keep")


def test_apply_explicit_history_trim_noop_when_none_and_trims_when_set(tmp_path):
    from core.storage.candle_manager import CandleFileManager
    from core.utils.file_system import apply_explicit_history_trim

    res_dir = tmp_path / "spot_1h" / "25m"
    pre_dir = tmp_path / "preprocess" / "25m"
    fun_dir = tmp_path / "funding"
    for d in (res_dir, pre_dir, fun_dir):
        d.mkdir(parents=True)
    times = pd.to_datetime(["2024-12-31 23:25", "2025-06-01 00:25", "2026-11-01 00:25"], utc=True)
    df = pd.DataFrame({"candle_begin_time": times, "close": [1.0, 2.0, 3.0]})
    df.to_pickle(res_dir / "BTCUSDT.pkl")
    pd.to_pickle({"BTCUSDT": df.copy()}, pre_dir / "spot_dict_batch0.pkl")
    df[["candle_begin_time"]].assign(funding_rate=0.001).to_pickle(fun_dir / "BTCUSDT.pkl")
    old_pivot = pre_dir / "market_pivot_spot_2024.pkl"
    old_complete = pre_dir / "market_pivot_spot_2024.complete"
    cur_complete = pre_dir / "market_pivot_spot_2025.complete"
    for f in (old_pivot, old_complete, cur_complete):
        f.write_bytes(b"p")

    h = SimpleNamespace(
        enabled_hour_offsets=("25m",),
        spot=SimpleNamespace(resample_mgr_map={"25m": CandleFileManager(res_dir, "pickle")}),
        swap=SimpleNamespace(resample_mgr_map={}),
        funding_mgr=CandleFileManager(fun_dir, "pickle"),
        data_api=None,
        preprocess_handle=SimpleNamespace(preprocess_mgr_map={"25m": CandleFileManager(pre_dir, "pickle")}),
    )
    assert apply_explicit_history_trim(h, None) == {"resample": 0, "batch": 0, "funding": 0, "data_api": 0, "pivots_removed": 0}
    assert len(pd.read_pickle(res_dir / "BTCUSDT.pkl")) == 3

    trim_ts = pd.Timestamp("2025-06-01 00:00:00", tz="UTC")
    stats = apply_explicit_history_trim(h, trim_ts)
    assert stats["resample"] == 1 and stats["batch"] == 1 and stats["funding"] == 1 and stats["pivots_removed"] == 1
    assert len(pd.read_pickle(res_dir / "BTCUSDT.pkl")) == 2
    assert len(pd.read_pickle(pre_dir / "spot_dict_batch0.pkl")["BTCUSDT"]) == 2
    assert len(pd.read_pickle(fun_dir / "BTCUSDT.pkl")) == 2
    assert not old_pivot.exists() and not old_complete.exists() and not cur_complete.exists()
