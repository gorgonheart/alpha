"""Offline cross-repo end-to-end contract tests (D:/alpha <-> D:/alphaonline).

Exercises the real filesystem Arrow/PKL boundary with zero runtime cross-repo
import coupling: the producer side runs D:/alpha code to write Arrow snapshots
and pivot PKLs under tmp_path, and the consumer side runs D:/alphaonline code in
an isolated subprocess with PYTHONPATH pointed only at ../alphaonline.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pandas as pd
import pytest


ALPHA_ROOT = Path(__file__).resolve().parents[1]
ALPHAONLINE_ROOT = ALPHA_ROOT.parent / "alphaonline"


def _require_alphaonline() -> Path:
    if not (ALPHAONLINE_ROOT / "tools" / "robust40").is_dir():
        pytest.skip(f"Sibling alphaonline repo not found at {ALPHAONLINE_ROOT}")
    return ALPHAONLINE_ROOT


def _run_alphaonline_script(script: str, payload: dict) -> dict:
    """Execute consumer verification in a clean subprocess rooted at ../alphaonline."""
    consumer_root = _require_alphaonline()
    env = dict(os.environ)
    env["PYTHONPATH"] = str(consumer_root)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["CROSS_REPO_PAYLOAD"] = json.dumps(payload)
    proc = subprocess.run(
        [sys.executable, "-X", "utf8", "-B", "-c", script],
        cwd=str(consumer_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, (
        f"alphaonline consumer check failed (rc={proc.returncode}):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    lines = [line for line in proc.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _make_ohlcv(times: pd.DatetimeIndex, base_price: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "candle_begin_time": times,
            "open": base_price,
            "high": base_price + 1.0,
            "low": base_price - 1.0,
            "close": base_price + 0.5,
            "volume": 100.0,
            "quote_volume": 100.0 * (base_price + 0.25),
            "trade_num": 10,
            "taker_buy_base_asset_volume": 50.0,
            "taker_buy_quote_asset_volume": 50.0 * (base_price + 0.25),
            "first_candle_time": times[0],
            "last_candle_time": times[-1],
        }
    )


def _build_producer_handle(tmp_path: Path, *, history_trim_date=None):
    from core.storage.candle_manager import CandleFileManager

    spot_5m = tmp_path / "spot_5m"
    swap_5m = tmp_path / "swap_5m"
    spot_1h = tmp_path / "spot_1h"
    swap_1h = tmp_path / "swap_1h"
    pre_root = tmp_path / "preprocess"
    exg_dir = tmp_path / "exg"
    for d in (spot_5m, swap_5m, spot_1h / "25m", swap_1h / "25m", pre_root / "25m", exg_dir):
        d.mkdir(parents=True, exist_ok=True)

    spot_res_mgr = CandleFileManager(spot_1h / "25m", "pickle")
    swap_res_mgr = CandleFileManager(swap_1h / "25m", "pickle")
    pre_mgr = CandleFileManager(pre_root / "25m", "pickle")
    exg_mgr = CandleFileManager(exg_dir, "pickle")

    spot_handle = SimpleNamespace(
        base_candle_dir=spot_5m,
        resample_dir=spot_1h,
        resample_mgr_map={"25m": spot_res_mgr},
    )
    swap_handle = SimpleNamespace(
        base_candle_dir=swap_5m,
        resample_dir=swap_1h,
        resample_mgr_map={"25m": swap_res_mgr},
    )
    preprocess_handle = SimpleNamespace(
        preprocess_dir=pre_root,
        preprocess_mgr_map={"25m": pre_mgr},
    )
    handle = SimpleNamespace(
        resample_interval="1h",
        spot=spot_handle,
        swap=swap_handle,
        binance_spot=spot_handle,
        binance_swap=swap_handle,
        preprocess_handle=preprocess_handle,
        exg_mgr=exg_mgr,
        funding_mgr=None,
        fetch_funding_rate=False,
        data_api=None,
        coin_cap_handle=None,
        publish_kline_num=5000,
        arrow_snapshot_keep=2,
        history_trim_date=history_trim_date,
    )
    return handle, pre_root, pre_mgr, spot_res_mgr, swap_res_mgr


def test_restart_zero_rebuild_cross_repo_contract(tmp_path):
    """End-to-end Path 1: Round 1 (BTC+ANTHROPIC) -> init_dirs() restart -> Round 2 (BTC only, trim=None)
    => consumer check_upstream_history_trim_date is False and handoff.first_revision is None."""
    _require_alphaonline()

    from core.strategy.preprocess import gen_pivot, publish_arrow_snapshot_for_type
    from core.utils.file_system import init_dirs

    handle, pre_root, pre_mgr, _, _ = _build_producer_handle(tmp_path, history_trim_date=None)

    # Round 1: BTCUSDT + ANTHROPICUSDT
    t_r1 = pd.date_range("2026-11-22 19:25", periods=5, freq="h", tz="UTC")
    run_time_1 = pd.Timestamp("2026-11-23 00:25", tz="UTC")
    r1_market = {
        "BTCUSDT": _make_ohlcv(t_r1, 100.0),
        "ANTHROPICUSDT": _make_ohlcv(t_r1, 50.0),
    }
    for trade_type in ("spot", "swap"):
        publish_arrow_snapshot_for_type(
            handle, run_time_1, trade_type, r1_market, publish_kline_num=5000, history_trim_date=None
        )
        gen_pivot(r1_market, trade_type, run_time_1, pre_mgr)
        pre_mgr.save_ready(f"{trade_type}_dict", run_time_1)

    # Snapshot Round 1 pivot PKLs before restart
    r1_spot_pivot = tmp_path / "r1_market_pivot_spot_2026.pkl"
    r1_swap_pivot = tmp_path / "r1_market_pivot_swap_2026.pkl"
    pd.to_pickle(pre_mgr.read_candle("market_pivot_spot_2026"), r1_spot_pivot)
    pd.to_pickle(pre_mgr.read_candle("market_pivot_swap_2026"), r1_swap_pivot)

    # Simulate process restart: init_dirs() must preserve existing preprocess/pivot artifacts
    init_dirs(handle)
    assert (pre_root / "25m" / "market_pivot_spot_2026.pkl").exists()
    assert (pre_root / "25m" / "market_pivot_swap_2026.pkl").exists()

    # Round 2: ANTHROPICUSDT delisted, only BTCUSDT published, history_trim_date=None
    t_r2 = pd.date_range("2026-11-22 19:25", periods=6, freq="h", tz="UTC")
    run_time_2 = pd.Timestamp("2026-11-23 01:25", tz="UTC")
    r2_market = {"BTCUSDT": _make_ohlcv(t_r2, 100.0)}
    for trade_type in ("spot", "swap"):
        publish_arrow_snapshot_for_type(
            handle, run_time_2, trade_type, r2_market, publish_kline_num=5000, history_trim_date=None
        )
        gen_pivot(r2_market, trade_type, run_time_2, pre_mgr)
        pre_mgr.save_ready(f"{trade_type}_dict", run_time_2)

    r2_spot_pivot = pre_root / "25m" / "market_pivot_spot_2026.pkl"
    r2_swap_pivot = pre_root / "25m" / "market_pivot_swap_2026.pkl"

    # Producer preserved delisted column in merged pivot
    r2_spot_obj = pd.read_pickle(r2_spot_pivot)
    assert "ANTHROPICUSDT" in r2_spot_obj["close"].columns

    # Consumer verification in isolated D:/alphaonline subprocess
    consumer_script = """
import json, os
from pathlib import Path
import pandas as pd
from tools.robust40 import handoff
from tools.robust40.cli import server_robust40 as cli

p = json.loads(os.environ["CROSS_REPO_PAYLOAD"])
trim_res = cli.check_upstream_history_trim_date(Path(p["pre_root"]), 25, p["stamp_r2"], saved_trim=None)

def load_pivots(spot_pkl, swap_pkl):
    out = {}
    for market, path in (("spot", spot_pkl), ("swap", swap_pkl)):
        data = pd.read_pickle(path)
        for field, df in data.items():
            norm = df.copy()
            if getattr(norm.index, "tz", None) is not None:
                norm.index = norm.index.tz_convert("UTC").tz_localize(None)
            out[f"pivot/{market}/{field}"] = norm
    return out

old_piv = load_pivots(p["r1_spot"], p["r1_swap"])
new_piv = load_pivots(p["r2_spot"], p["r2_swap"])
held = {("spot", "BTCUSDT"), ("swap", "BTCUSDT")}
rev_preserved = handoff.first_revision(old_piv, new_piv, held_symbols=held)

# Defense-in-depth: even if unheld ANTHROPICUSDT were dropped from new pivot, first_revision stays None
dropped_piv = {k: v.drop(columns=["ANTHROPICUSDT"], errors="ignore") for k, v in new_piv.items()}
rev_unheld_dropped = handoff.first_revision(old_piv, dropped_piv, held_symbols=held)

print(json.dumps({
    "trim_need": trim_res[0],
    "trim_old": trim_res[1],
    "trim_new": trim_res[2],
    "rev_preserved": None if rev_preserved is None else str(rev_preserved),
    "rev_unheld_dropped": None if rev_unheld_dropped is None else str(rev_unheld_dropped),
}))
"""
    result = _run_alphaonline_script(
        consumer_script,
        {
            "pre_root": str(pre_root),
            "stamp_r2": int(run_time_2.timestamp()),
            "r1_spot": str(r1_spot_pivot),
            "r1_swap": str(r1_swap_pivot),
            "r2_spot": str(r2_spot_pivot),
            "r2_swap": str(r2_swap_pivot),
        },
    )
    assert result["trim_need"] is False
    assert result["trim_old"] is None
    assert result["trim_new"] is None
    assert result["rev_preserved"] is None
    assert result["rev_unheld_dropped"] is None


def test_planned_history_trim_cross_repo_contract(tmp_path):
    """End-to-end Path 2: apply_explicit_history_trim('2025-06-01') + Arrow publish
    => consumer check_upstream_history_trim_date returns (True, None, '2025-06-01'),
    and scan_arrow_earliest_bar + resolve_initialization_s3_origin advances S3 origin
    to 2025-07-21 00:25:00 (min_kline_num=480, n=720, >=365d remaining)."""
    _require_alphaonline()

    from core.strategy.preprocess import publish_arrow_snapshot_for_type
    from core.utils.file_system import apply_explicit_history_trim

    handle, pre_root, pre_mgr, spot_res_mgr, swap_res_mgr = _build_producer_handle(
        tmp_path, history_trim_date="2025-06-01"
    )

    # Populate pre-trim history spanning 2025-05-25 00:25 UTC -> 2025-06-15 00:25 UTC (505 hourly bars)
    times = pd.date_range("2025-05-25 00:25", periods=505, freq="h", tz="UTC")
    btc_df = _make_ohlcv(times, 100.0)
    spot_res_mgr.save_data_file("BTCUSDT", btc_df.copy())
    swap_res_mgr.save_data_file("BTCUSDT", btc_df.copy())
    pre_mgr.save_data_file("spot_dict_batch0", {"BTCUSDT": btc_df.copy()})
    pre_mgr.save_data_file("swap_dict_batch0", {"BTCUSDT": btc_df.copy()})
    pre_mgr.save_data_file("market_pivot_spot_2024", {"close": pd.DataFrame()})

    trim_ts = pd.Timestamp("2025-06-01", tz="UTC")
    stats = apply_explicit_history_trim(handle, trim_ts)
    assert stats["resample"] == 2
    assert stats["batch"] == 2
    assert stats["pivots_removed"] == 1
    assert not (pre_root / "25m" / "market_pivot_spot_2024.pkl").exists()

    spot_trimmed = spot_res_mgr.read_candle("BTCUSDT")
    swap_trimmed = swap_res_mgr.read_candle("BTCUSDT")
    assert spot_trimmed["candle_begin_time"].iloc[0] == pd.Timestamp("2025-06-01 00:25:00", tz="UTC")
    assert swap_trimmed["candle_begin_time"].iloc[0] == pd.Timestamp("2025-06-01 00:25:00", tz="UTC")

    run_time = pd.Timestamp("2027-01-01 01:25:00", tz="UTC")
    stamp = int(run_time.timestamp())
    publish_arrow_snapshot_for_type(
        handle, run_time, "spot", {"BTCUSDT": spot_trimmed}, publish_kline_num=5000, history_trim_date="2025-06-01"
    )
    publish_arrow_snapshot_for_type(
        handle, run_time, "swap", {"BTCUSDT": swap_trimmed}, publish_kline_num=5000, history_trim_date="2025-06-01"
    )
    swap_arrow = pre_root / "25m" / f"swap_dict_{stamp}.arrow"
    assert swap_arrow.exists()

    consumer_script = """
import json, os
from pathlib import Path
import pandas as pd
from tools.robust40 import server
from tools.robust40.cli import server_robust40 as cli

p = json.loads(os.environ["CROSS_REPO_PAYLOAD"])
need, old_t, new_t = cli.check_upstream_history_trim_date(Path(p["pre_root"]), 25, p["stamp"], saved_trim=None)
earliest = server.scan_arrow_earliest_bar(Path(p["swap_arrow"]), minimum_history_bars=100)
resolved = server.resolve_initialization_s3_origin(
    pd.Timestamp("2025-05-20 00:25:00"),
    earliest,
    720,
    480,
    pd.Timestamp("2027-01-01 00:25:00"),
)
print(json.dumps({
    "need": need,
    "old_t": old_t,
    "new_t": new_t,
    "earliest": str(earliest),
    "resolved": str(resolved),
}))
"""
    result = _run_alphaonline_script(
        consumer_script,
        {
            "pre_root": str(pre_root),
            "stamp": stamp,
            "swap_arrow": str(swap_arrow),
        },
    )
    assert (result["need"], result["old_t"], result["new_t"]) == (True, None, "2025-06-01")
    assert result["earliest"] == "2025-06-01 00:25:00"
    assert result["resolved"] == "2025-07-21 00:25:00"
