import asyncio
import queue
import time
from datetime import timedelta
from types import SimpleNamespace
import pandas as pd
import pytest


def test_ws_local_cache_prune_before_deletes_old_rows_only(tmp_path):
    from core.ws.ws_local_cache import WsLocalCache
    cache = WsLocalCache(tmp_path / "spot.sqlite")
    old_ms = 1_700_000_000_000
    new_ms = old_ms + 72 * 3600 * 1000
    for ms in (old_ms, new_ms):
        cache.write("BTCUSDT", "spot", {
            "begin_time_ms": ms, "open": 1, "high": 2, "low": 1, "close": 2,
            "volume": 10, "quote_volume": 20, "trade_num": 5, "taker_buy_base": 3, "taker_buy_quote": 6,
        })
    deleted = cache.prune_before(new_ms - 48 * 3600 * 1000)
    assert deleted == 1
    rows = cache.replay_after("BTCUSDT", "spot", 0)
    assert [r["begin_time_ms"] for r in rows] == [new_ms]
    cache.close()


def test_ws_runner_multichunk_single_process_isolates_chunk_failure_and_empty_symbols(tmp_path):
    """Review Focus #3: >200 symbols split into multi-chunk coroutines; chunk-1 error does not crash chunk-0; empty symbols safe."""
    from core.ws.ws_runner import WsRunner
    from core.exchange.protocol import BinanceProtocol

    created_clients = []
    q = queue.Queue()

    class FakeClient:
        def __init__(self, url, chunk_idx, on_closed):
            self.url = url
            self.chunk_idx = chunk_idx
            self.on_closed = on_closed
            self.subscribed = []
            self.stopped = False
            self.run_attempts = 0
        async def connect(self): pass
        async def subscribe(self, streams): self.subscribed = list(streams)
        async def run(self):
            self.run_attempts += 1
            if self.chunk_idx == 1 and self.run_attempts == 1:
                raise ConnectionResetError("transient chunk-1 drop")
            if self.chunk_idx == 0:
                self.on_closed({"symbol": "SYM0", "kline": {
                    "begin_time_ms": 1_700_000_000_000, "open": 1, "high": 1, "low": 1, "close": 1,
                    "volume": 1, "quote_volume": 1, "trade_num": 1, "taker_buy_base": 1, "taker_buy_quote": 1,
                }})
            await asyncio.sleep(0.05)
        async def close(self): self.stopped = True

    def factory(on_closed, url=None, chunk_idx=0):
        c = FakeClient(url, chunk_idx, on_closed)
        created_clients.append(c)
        return c

    runner = WsRunner(
        url="wss://example", trade_type="spot", queue=q,
        cache_path=tmp_path / "spot.sqlite", base_delta=timedelta(minutes=5),
        ws_client_factory=factory, protocol=BinanceProtocol(), chunk_retry_base_sec=0.01,
    )
    symbols = [f"SYM{i}" for i in range(450)]  # 3 chunks: 200 + 200 + 50
    async def exercise():
        await runner.start(symbols=symbols, replay_on_start=False)
        assert len(created_clients) == 3
        assert [len(c.subscribed) for c in created_clients] == [200, 200, 50]
        task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.08)
        await runner.stop()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(exercise())
    assert not q.empty() and q.get_nowait()["symbol"] == "SYM0"
    assert created_clients[1].run_attempts >= 2
    assert all(c.stopped for c in created_clients)

    # Empty symbols must not hang in start(), run(), or stop()
    empty_runner = WsRunner(url="wss://example", trade_type="spot", queue=q,
                            cache_path=tmp_path / "empty.sqlite", ws_client_factory=factory)
    async def exercise_empty():
        await empty_runner.start(symbols=[], replay_on_start=False)
        run_task = asyncio.create_task(empty_runner.run())
        await asyncio.sleep(0.02)
        await empty_runner.stop()
        await asyncio.wait_for(run_task, timeout=1.0)
    asyncio.run(exercise_empty())


def test_ws_runner_prunes_stale_cache_on_start_and_periodically(tmp_path):
    from core.ws.ws_local_cache import WsLocalCache
    from core.ws.ws_runner import WsRunner

    cache_path = tmp_path / "spot.sqlite"
    now_ms = int(time.time() * 1000)
    old_ms = now_ms - 60 * 3600 * 1000  # 60h ago (> 48h)
    fresh_ms = now_ms - 1 * 3600 * 1000  # 1h ago (< 48h)

    pre = WsLocalCache(cache_path)
    for ms in (old_ms, fresh_ms):
        pre.write("BTCUSDT", "spot", {
            "begin_time_ms": ms, "open": 1, "high": 2, "low": 1, "close": 2,
            "volume": 10, "quote_volume": 20, "trade_num": 5, "taker_buy_base": 3, "taker_buy_quote": 6,
        })
    pre.close()

    q = queue.Queue()

    class NoopClient:
        async def connect(self): pass
        async def subscribe(self, streams): pass
        async def run(self): await asyncio.sleep(0.1)
        async def close(self): pass

    runner = WsRunner(
        url="wss://example", trade_type="spot", queue=q,
        cache_path=cache_path,
        ws_client_factory=lambda on_closed, url=None, chunk_idx=0: NoopClient(),
        prune_interval_sec=0.02,
    )

    async def exercise():
        await runner.start(symbols=["BTCUSDT"], replay_on_start=True)
        # Replay replayed both rows before start-prune, then start-prune deleted old_ms
        remaining = runner._cache.replay_after("BTCUSDT", "spot", 0)
        assert [r["begin_time_ms"] for r in remaining] == [fresh_ms]

        # Insert another stale row while running and verify periodic prune removes it
        runner._cache.write("ETHUSDT", "spot", {
            "begin_time_ms": old_ms, "open": 1, "high": 2, "low": 1, "close": 2,
            "volume": 10, "quote_volume": 20, "trade_num": 5, "taker_buy_base": 3, "taker_buy_quote": 6,
        })
        task = asyncio.create_task(runner.run())
        await asyncio.sleep(0.06)
        assert runner._cache.replay_after("ETHUSDT", "spot", 0) == []
        await runner.stop()
        await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(exercise())


def test_runtime_supervisor_spawns_one_ws_spec_per_market_and_two_writers(tmp_path):
    from core.pipeline.runtime_supervisor import RuntimeSupervisor
    from core.storage.candle_manager import CandleFileManager

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": [f"S{i}USDT" for i in range(450)]}))
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": [f"F{i}USDT" for i in range(550)]}))

    h = SimpleNamespace(
        base_dir=tmp_path, base_interval="5m", exchange="binance", proxy=None,
        num_writers=2, num_base_candles=8640,
        flat_pkl_max_bars=lambda: 240, base_5m_retention_bars=lambda: 8640,
        exg_mgr=exg_mgr,
        spot=SimpleNamespace(base_candle_dir=tmp_path / "s5m", once_update_candles=99),
        swap=SimpleNamespace(base_candle_dir=tmp_path / "f5m", once_update_candles=99),
        get_market_handle=lambda tt: h.spot if tt == "spot" else h.swap,
    )
    sup = RuntimeSupervisor(handle=h, alert_webhook_url="")
    assert sup.num_writers == 2 and len(sup.queues) == 2
    spot_specs = sup._build_ws_runner_specs("spot")
    swap_specs = sup._build_ws_runner_specs("swap")
    assert len(spot_specs) == 1 and len(spot_specs[0].symbols) == 450
    assert len(swap_specs) == 1 and len(swap_specs[0].symbols) == 550
    assert spot_specs[0].cache_path.name == "spot.sqlite"
    assert swap_specs[0].cache_path.name == "swap.sqlite"


def test_runtime_supervisor_process_names_and_writer_args(tmp_path, monkeypatch):
    from core.pipeline.runtime_supervisor import RuntimeSupervisor, run_writer_subprocess
    from core.storage.candle_manager import CandleFileManager

    exg_dir = tmp_path / "exg"
    exg_dir.mkdir()
    exg_mgr = CandleFileManager(exg_dir, "pickle")
    exg_mgr.save_data_file("exginfo_spot", pd.DataFrame({"symbol": [f"S{i}USDT" for i in range(250)]}))
    exg_mgr.save_data_file("exginfo_swap", pd.DataFrame({"symbol": [f"F{i}USDT" for i in range(250)]}))

    h = SimpleNamespace(
        base_dir=tmp_path, base_interval="5m", exchange="binance", proxy=None,
        num_writers=2,
        flat_pkl_max_bars=lambda: 240, base_5m_retention_bars=lambda: 8640,
        exg_mgr=exg_mgr,
        spot=SimpleNamespace(base_candle_dir=tmp_path / "s5m", once_update_candles=99),
        swap=SimpleNamespace(base_candle_dir=tmp_path / "f5m", once_update_candles=99),
        get_market_handle=lambda tt: h.spot if tt == "spot" else h.swap,
    )
    sup = RuntimeSupervisor(handle=h, alert_webhook_url="")

    spawned = []

    class FakeProc:
        def __init__(self, target, name, args, daemon=False):
            self.target = target
            self.name = name
            self.args = args
            self.daemon = daemon
            self.started = False
        def start(self):
            self.started = True

    monkeypatch.setattr(sup.ctx, "Process", lambda **kw: spawned.append(FakeProc(**kw)) or spawned[-1])

    sup.start_writers()
    sup.start_ws_runners()

    writer_procs = [p for p in spawned if p.target is run_writer_subprocess]
    ws_procs = [p for p in spawned if p.target is not run_writer_subprocess]

    assert [p.name for p in writer_procs] == ["bmac-binance-writer-0", "bmac-binance-writer-1"]
    assert [p.name for p in ws_procs] == ["bmac-binance-ws-spot", "bmac-binance-ws-swap"]
    # Verify flat_pkl_max_bars=240 and partition_retention_bars=8640 are forwarded to run_writer_subprocess
    for p in writer_procs:
        assert p.args[2] == 240
        assert p.args[3] == 8640
