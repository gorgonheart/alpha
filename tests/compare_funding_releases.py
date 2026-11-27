"""Offline comparison of two alpha producer sources; never imports a live configuration.

alpha311 tests/compare_funding_releases.py <formal-source> <testing-source>
Everything except the supplied source is synthetic and confined to a temporary directory.
"""
import asyncio
import json
import logging
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd


def probe(root, testing):
    from core import preprocess as pp
    from core import binance_update
    from core.arrow_publish import read_arrow_snapshot
    from core.candle_manager import CandleFileManager
    from core.funding_backfill import backfill_recent_funding_rates, save_funding_hours
    import pyarrow as pa
    pp.logger.setLevel(logging.WARNING)

    def mgr(name):
        folder = root / name
        folder.mkdir(parents=True)
        return CandleFileManager(folder, "pickle")

    symbols = ["AAAUSDT", "BBBUSDT"]
    markets = ("spot", "swap")
    offsets = ("5m", "25m")
    pre = {off: mgr("pre/" + off) for off in offsets}
    src = {kind: SimpleNamespace(resample_mgr_map={
        off: mgr(f"candles/{kind}/{off}") for off in offsets}) for kind in markets}
    funding, exg = mgr("funding"), mgr("exg")
    handle = SimpleNamespace(resample_interval="1h", preprocess_handle=SimpleNamespace(
        preprocess_mgr_map=pre, num_batch=1), exg_mgr=exg, fetch_funding_rate=True,
        funding_mgr=funding, get_binance_handle=src.__getitem__, data_api=None, proxy=None,
        binance_swap=SimpleNamespace(api_type="usdt_futures"), get_kline_count_1h=lambda _: 6000)

    end = pd.Timestamp("2026-01-01 00:00:00", tz="UTC")
    settlement = end - pd.Timedelta(hours=1)

    async def exchange_history(symbol, startTime, endTime, limit):
        # One repeated raw observation and a second distinct, same-hour settlement.
        j = symbols.index(symbol) + 1
        events = [int(settlement.timestamp()) * 1000 + d for d in (1, 1, 600_001)]
        return [dict(symbol=symbol, fundingTime=t, fundingRate=str(-.001*j))
                for t in events if startTime <= t <= endTime][:limit]

    config = SimpleNamespace(publish_kline_num=0, arrow_demand_enabled=testing,
                             arrow_demand_min_rows=5000, precompute_factor_workers=2)
    if testing:
        owner = pre["25m"].base_dir / ".arrow-demand" / "dry-run"
        owner.mkdir(parents=True)
        (owner / "25m.json").write_text(json.dumps({"version": 1, "rows": 5000}))
    factor = SimpleNamespace(signal_multi_params=lambda df, ns: {
        str(n): df.funding_fee.rolling(n, min_periods=1).sum() for n in ns})
    observed = {}
    with patch.object(pp, "bmac_config", config), patch.object(pp, "_load_factor_modules_params",
            return_value=[(factor, "FundingSum", [24])]):
        for hour in range(3):
            if testing and hour == 2:
                # A pending reconstruction takes precedence over the normal short request.
                (owner / "25m.json").write_text(json.dumps({"version": 1, "rows": 0}))
            # A correction arrives after BOTH offsets published their first round.
            # Write the shared source only; no consumer asks for a replacement.
            rate = [0.001, -0.002, -0.002][hour]
            tick = end + pd.Timedelta(hours=hour)
            if hour == 0:
                for j, symbol in enumerate(symbols, 1):
                    save_funding_hours(funding, symbol, tick, pd.DataFrame({
                        "symbol": [symbol], "candle_begin_time": [settlement], "funding_rate": [rate*j]}), 6000)
            else:
                with patch("core.binance_fetcher_async.create_binance_market_api",
                           return_value=SimpleNamespace(aioreq_funding_rate=exchange_history)):
                    history, stats = asyncio.run(backfill_recent_funding_rates(
                        handle, None, tick, symbols, deadline=time.monotonic()+5))
                binance_update.save_funding_updates(handle, tick, pd.DataFrame(), history, stats, time.monotonic()+5)
                assert stats["remaining"] == 0
                for j, symbol in enumerate(symbols, 1):
                    source = funding.read_candle(symbol)
                    assert list(zip(source.candle_begin_time, source.funding_rate)) == [(settlement, rate*j)]
            for off in offsets:
                minute = int(off[:-1])
                tick = end + pd.Timedelta(hours=hour, minutes=minute)
                rows = 5010 + hour
                times = pd.date_range(end=tick - pd.Timedelta(hours=1), periods=rows, freq="h")
                counterpart = pre["25m" if off == "5m" else "5m"].base_dir

                def other_files():
                    return {p.name: (p.stat().st_size, p.stat().st_mtime_ns)
                            for p in counterpart.iterdir() if p.is_file()}
                before = other_files()
                begins = {s: (times[0], times[-1]) for s in symbols}
                for kind in markets:
                    exg.set_candle("exginfo_" + kind, tick, pd.DataFrame({"symbol": symbols}))
                    for j, s in enumerate(symbols, 1):
                        price = np.arange(rows, dtype=float) + 10*j + (kind == "swap")
                        src[kind].resample_mgr_map[off].set_candle(s, tick, pd.DataFrame({
                            "candle_begin_time": times, "open": price, "close": price + 0.25}))
                exg.set_candle("spot_swap_matches", tick, pd.DataFrame({"spot": symbols, "swap": symbols}))
                pp.preprocess_offset_data(handle, tick, begins, begins)
                assert before == other_files(), "publication touched a different offset"
                manager = pre[off]
                assert all(manager.check_ready(key, tick) for kind in markets
                           for key in (kind + "_dict", "market_pivot_" + kind))
                for kind in markets:
                    arrow_path = manager.base_dir / f"{kind}_dict_{int(tick.timestamp())}.arrow"
                    arrow = read_arrow_snapshot(arrow_path)  # one observation of this ready round
                    tail = testing and off == "25m" and hour < 2
                    with pa.memory_map(str(arrow_path), "r") as arrow_file:
                        assert pa.ipc.open_file(arrow_file).schema.metadata[b"full_history"] == (b"0" if tail else b"1")
                    assert set(arrow) == set(symbols)
                    for j, symbol in enumerate(symbols, 1):
                        raw = manager.read_candle(f"{kind}_dict_batch{j}")[symbol]
                        expected = np.where(times.floor("h") == settlement, rate*j*(kind == "swap"), 0.)
                        np.testing.assert_array_equal(raw.funding_fee, expected,
                            err_msg=f"{kind}/{off}/round {hour}: batch did not incorporate settlement")
                        snap = arrow[symbol]
                        assert len(snap) == (5000 if tail else rows)
                        np.testing.assert_array_equal(snap.funding_fee, expected[-len(snap):])
                        np.testing.assert_array_equal(snap.FundingSum_24,
                            pd.Series(expected).rolling(24, min_periods=1).sum().tail(len(snap)))
                        # Normalize only the expressly different publication depth.
                        key = f"{off}/{hour}/{kind}/{symbol}"
                        depth = 5000 if off == "25m" and hour < 2 else rows
                        observed[key + "/common"] = snap.tail(depth).reset_index(drop=True)
                        observed[key + "/raw"] = raw
                    for year in times.year.unique():
                        pivot = manager.read_candle(f"market_pivot_{kind}_{year}")
                        if kind == "swap":
                            df = pivot["funding_rate"]
                            for j, symbol in enumerate(symbols, 1):
                                np.testing.assert_array_equal(df[symbol], np.where(
                                    df.index.floor("h") == settlement, rate*j, 0.))
                        for col, table in pivot.items():
                            observed[f"{off}/{hour}/{kind}/{year}/{col}"] = table
    pd.to_pickle(observed, root / "observation.pkl")


def compare(formal, testing):
    with tempfile.TemporaryDirectory(prefix="alpha-funding-ab-") as workspace:
        results = []
        for version, source in (("formal", formal), ("testing", testing)):
            root = Path(workspace) / version
            root.mkdir()
            shutil.copytree(Path(source) / "core", root / "core", ignore=shutil.ignore_patterns("__pycache__"))
            (root / "config.py").write_text("# intentional isolated offline config\n")
            shutil.copyfile(__file__, root / "probe.py")
            subprocess.run([sys.executable, "-B", "probe.py", "--probe", version], cwd=root, check=True)
            results.append(pd.read_pickle(root / "observation.pkl"))
        left, right = results
        assert left.keys() == right.keys()
        for key in left:
            pd.testing.assert_frame_equal(left[key], right[key], check_exact=True, obj=key)
        print(f"PASS: {len(left)} exact frame comparisons; 2 offsets x 3 rounds x 2 markets; "
              "test 25m Arrow 5000/5000/5012 vs formal 5010/5011/5012; funding oracle passed.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: compare_funding_releases.py FORMAL_SOURCE TESTING_SOURCE")
    if sys.argv[1] == "--probe":
        probe(Path.cwd(), sys.argv[2] == "testing")
    else:
        compare(*sys.argv[1:])
