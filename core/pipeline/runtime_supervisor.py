"""进程编排：writer + ws_runner 子进程的生命周期管理（5 进程瘦身架构）。

main 进程持有一个 RuntimeSupervisor，它拥有：
  - num_writers 个 mp.Queue（默认 2 个，由 handle.num_writers 控制）
  - num_writers 个 writer 子进程（mp.Process，spawn context）
  - 2 个 ws_runner 子进程（spot / swap 各 1 个，内部单进程多协程管理多个 WS chunk，
    各自持有一份 ws_cache/{trade_type}.sqlite）

进程总数 = 1 (main) + 2 (writer) + 2 (ws_runner) = 5。
"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing as mp
import signal
import sys
import time
import zlib
from dataclasses import dataclass, field
from datetime import timedelta
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Optional

from core.alert import Alert
from core.exchange.protocol import BinanceProtocol, ExchangeProtocol
from core.storage.writer import Writer
from core.utils.path_kit import get_file_path
from core.ws.ws_runner import WsRunner

logger = logging.getLogger(__name__)


def writer_shard(symbol: str, num_writers: int) -> int:
    """确定性分片：同一 symbol 恒定路由到同一 writer。

    必须用确定性 hash —— Python 内置 hash() 跨进程随机化（PYTHONHASHSEED），
    ws_runner 子进程 / REST 线程 / main 必须算出一致的分片，所以用 zlib.crc32。
    """
    if num_writers <= 1:
        return 0
    return zlib.crc32(symbol.encode("utf-8")) % num_writers


class WriterQueueRouter:
    """鸭子类型 put(item)：按 item['symbol'] 分片路由到对应 writer 的 queue。"""

    def __init__(self, queues: list):
        self.queues = queues
        self.num_writers = len(queues)

    def put(self, item, **kwargs):
        symbol = item.get("symbol", "")
        shard = writer_shard(symbol, self.num_writers)
        self.queues[shard].put(item, **kwargs)


def _configure_subprocess_logging(proc_name: str) -> None:
    """spawn 子进程 root logger 没有 handler；加 stdout + 独立日志文件。"""
    root = logging.getLogger()
    if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        return
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter(
        f"%(asctime)s [{proc_name}] %(levelname)s %(name)s: %(message)s"
    ))
    file_handler = TimedRotatingFileHandler(
        get_file_path("logs", f"{proc_name.lower()}.log"),
        when="midnight", interval=1, backupCount=7, encoding="utf-8",
    )
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    ))
    root.handlers = [stream, file_handler]
    root.setLevel(logging.INFO)


@dataclass
class WsRunnerSpec:
    """用于 spawn 一个 ws_runner 子进程的 pickle-safe 载荷。"""

    url: str
    trade_type: str  # 'spot' | 'swap'
    cache_path: Path
    base_candle_dir: Path
    base_delta_seconds: int
    symbols: list[str]
    queue_put_timeout_sec: float = 5.0
    protocol: ExchangeProtocol = field(default_factory=BinanceProtocol)
    proxy: Optional[str] = None


def run_writer_subprocess(
    queue: mp.Queue,
    base_candle_dirs: dict,
    flat_pkl_max_bars: Optional[int],
    partition_retention_bars: Optional[int],
    base_delta_seconds: int,
    alert_webhook_url: str,
    gap_check_bars: int = 99,
) -> None:
    """顶层 writer Process target。队列上收到 None 哨兵 → 干净退出。"""
    _configure_subprocess_logging(mp.current_process().name)
    alert = Alert(webhook_url=alert_webhook_url) if alert_webhook_url else None
    writer = Writer(
        base_candle_dirs={k: Path(v) for k, v in base_candle_dirs.items()},
        flat_pkl_max_bars=flat_pkl_max_bars,
        partition_retention_bars=partition_retention_bars,
        base_delta=timedelta(seconds=base_delta_seconds),
        alert=alert,
        gap_check_bars=gap_check_bars,
    )
    writer.consume_loop(queue)


def run_ws_runner_subprocess(
    queue: mp.Queue,
    spec: WsRunnerSpec,
    alert_webhook_url: str,
    shared_stop_event,
) -> None:
    """运行单市场多协程 WS 子进程，通过共享事件退出，避免中断队列发送。"""
    _configure_subprocess_logging(mp.current_process().name)
    alert = Alert(webhook_url=alert_webhook_url) if alert_webhook_url else None
    runner = WsRunner(
        url=spec.url,
        trade_type=spec.trade_type,
        queue=queue,
        cache_path=spec.cache_path,
        base_delta=timedelta(seconds=spec.base_delta_seconds),
        alert=alert,
        queue_put_timeout_sec=spec.queue_put_timeout_sec,
        base_candle_dir=spec.base_candle_dir,
        protocol=spec.protocol,
        proxy=spec.proxy,
    )

    async def _amain():
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        started = False
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass  # Windows 没有 add_signal_handler

        async def _run():
            nonlocal started
            if not shared_stop_event.is_set() and not stop_event.is_set():
                await runner.start(symbols=spec.symbols, replay_on_start=True)
                started = True
                await runner.run()

        async def _wait_stop():
            while not shared_stop_event.is_set() and not stop_event.is_set():
                await asyncio.sleep(0.1)

        run_task = asyncio.create_task(_run())
        stop_task = asyncio.create_task(_wait_stop())
        try:
            done, _ = await asyncio.wait(
                {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
            )
            if run_task in done:
                await run_task
        finally:
            if not started:
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
            try:
                await runner.stop()
            finally:
                for task in (run_task, stop_task):
                    task.cancel()
                await asyncio.gather(run_task, stop_task, return_exceptions=True)

    asyncio.run(_amain())


class RuntimeSupervisor:
    """持有 mp.Queue + 子进程（默认 2 writer + 2 ws_runner）。"""

    QUEUE_MAXSIZE = 10000
    GRACE_WAIT_SEC = 2
    WS_QUEUE_PUT_TIMEOUT_SEC = 5.0
    SHUTDOWN_JOIN_TIMEOUT_SEC = 30.0
    # 瘦身后的默认 writer 进程数（可通过 handle.num_writers 或 num_writers 参数覆盖）
    NUM_WRITERS = 2
    WS_RESTART_COOLDOWN_SEC = 60.0
    WS_RESTART_JOIN_TIMEOUT_SEC = 10.0

    def __init__(
        self,
        handle,
        alert_webhook_url: str,
        protocol: Optional[ExchangeProtocol] = None,
        num_writers: Optional[int] = None,
    ):
        ctx = mp.get_context("spawn")
        self.ctx = ctx
        self.handle = handle
        if num_writers is not None:
            resolved_writers = int(num_writers)
        else:
            resolved_writers = int(getattr(handle, "num_writers", self.NUM_WRITERS))
        if resolved_writers < 1:
            raise ValueError(f"num_writers must be >= 1, got {resolved_writers}")
        self.num_writers: int = resolved_writers

        self.queues: list = [
            ctx.Queue(maxsize=self.QUEUE_MAXSIZE)
            for _ in range(self.num_writers)
        ]
        self.router = WriterQueueRouter(self.queues)
        self.alert_webhook_url = alert_webhook_url
        self.protocol = protocol if protocol is not None else BinanceProtocol()
        self.ws_cache_dir: Path = Path(handle.base_dir) / "ws_cache"
        self.ws_cache_dir.mkdir(parents=True, exist_ok=True)
        self.writer_procs: list = []
        self.ws_procs: dict = {"spot": [], "swap": []}
        self._ws_stop_events: dict = {}
        self._spawned_symbols: dict = {"spot": [], "swap": []}
        self._last_restart_at: dict = {}

    def start_writers(self) -> None:
        """Spawn self.num_writers 个 writer 子进程；writer[i] 消费 queues[i]。"""
        from core.utils.time import convert_interval_to_timedelta

        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        base_candle_dirs = {
            "spot": self.handle.spot.base_candle_dir,
            "swap": self.handle.swap.base_candle_dir,
        }
        flat_pkl_max_bars = (
            self.handle.flat_pkl_max_bars()
            if callable(getattr(self.handle, "flat_pkl_max_bars", None))
            else 240
        )
        partition_retention_bars = (
            self.handle.base_5m_retention_bars()
            if callable(getattr(self.handle, "base_5m_retention_bars", None))
            else getattr(self.handle, "num_base_candles", 8640)
        )
        gap_check_bars = getattr(self.handle.spot, "once_update_candles", 99)

        for i in range(self.num_writers):
            p = self.ctx.Process(
                target=run_writer_subprocess,
                name=f"bmac-{self.handle.exchange}-writer-{i}",
                args=(
                    self.queues[i],
                    base_candle_dirs,
                    flat_pkl_max_bars,
                    partition_retention_bars,
                    int(base_delta.total_seconds()),
                    self.alert_webhook_url,
                    gap_check_bars,
                ),
                daemon=False,
            )
            p.start()
            self.writer_procs.append(p)

    def _build_ws_runner_specs(self, trade_type: str) -> list[WsRunnerSpec]:
        """每市场构造单个 WsRunnerSpec，全量 symbols 在单个 ws_runner 进程内按 chunk 协程并发订阅。"""
        from core.utils.time import convert_interval_to_timedelta

        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        bh = self.handle.get_market_handle(trade_type)
        df_exg = self.handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
        symbols = df_exg["symbol"].to_list() if df_exg is not None else []

        return [
            WsRunnerSpec(
                url=self.protocol.ws_url(trade_type, 0),
                trade_type=trade_type,
                cache_path=self.ws_cache_dir / f"{trade_type}.sqlite",
                base_candle_dir=bh.base_candle_dir,
                base_delta_seconds=int(base_delta.total_seconds()),
                symbols=symbols,
                queue_put_timeout_sec=self.WS_QUEUE_PUT_TIMEOUT_SEC,
                protocol=self.protocol,
                proxy=self.handle.proxy,
            )
        ]

    def _spawn_ws_runners_for(self, trade_type: str) -> list:
        """读取最新订阅并启动该市场的单个 ws_runner 子进程。"""
        specs = self._build_ws_runner_specs(trade_type)
        stop_event = self.ctx.Event()
        self._ws_stop_events[trade_type] = stop_event
        self.ws_procs[trade_type] = []
        symbols: list = []
        try:
            for chunk_idx, spec in enumerate(specs):
                proc_name = (
                    f"bmac-{self.handle.exchange}-ws-{trade_type}"
                    if len(specs) == 1
                    else f"bmac-{self.handle.exchange}-ws-{trade_type}-{chunk_idx}"
                )
                p = self.ctx.Process(
                    target=run_ws_runner_subprocess,
                    name=proc_name,
                    args=(self.router, spec, self.alert_webhook_url, stop_event),
                    daemon=False,
                )
                p.start()
                self.ws_procs[trade_type].append(p)
                symbols.extend(spec.symbols)
        except Exception:
            stop_event.set()
            raise
        self._spawned_symbols[trade_type] = symbols
        return symbols

    def start_ws_runners(self) -> None:
        """每市场（spot / swap）仅启动 1 个 ws_runner 子进程。"""
        for trade_type in ("spot", "swap"):
            self._spawn_ws_runners_for(trade_type)

    def restart_ws_runners(self, trade_type: str) -> None:
        """请求整组退出；超时保留旧进程，绝不强杀共享队列的生产者。"""
        stop_event = self._ws_stop_events[trade_type]
        if stop_event.is_set():
            return
        stop_event.set()
        old = self.ws_procs.get(trade_type, [])
        deadline = time.monotonic() + self.WS_RESTART_JOIN_TIMEOUT_SEC
        for p in old:
            p.join(max(0.0, deadline - time.monotonic()))
        if any(p.is_alive() for p in old):
            logger.warning(
                "WS 订阅重启延后\n  市场=%s，旧进程尚未全部退出，保留队列及进程引用",
                trade_type,
            )
        self.resume_pending_ws_restarts()

    def resume_pending_ws_restarts(self) -> None:
        """非阻塞检查已请求停止的组，全部退出后才启动替代进程。"""
        for trade_type, stop_event in list(self._ws_stop_events.items()):
            if not stop_event.is_set():
                continue
            old = self.ws_procs[trade_type]
            if any(p.is_alive() for p in old):
                continue
            for p in old:
                p.join(0)
            symbols = self._spawn_ws_runners_for(trade_type)
            self._last_restart_at[trade_type] = time.monotonic()
            logger.info(
                "WS 订阅重启完成\n  市场=%s，进程=%d，交易对=%d",
                trade_type, len(self.ws_procs[trade_type]), len(symbols),
            )

    def refresh_ws_subscriptions(self, handle) -> None:
        """每轮调用：某 trade_type 的 exginfo symbol 集相对上次 spawn 有增减则重启它。"""
        for trade_type in ("spot", "swap"):
            stop_event = self._ws_stop_events.get(trade_type)
            if stop_event is not None and stop_event.is_set():
                continue
            df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
            if df_exg is None or df_exg.empty:
                continue
            new_set = set(df_exg["symbol"].to_list())
            old_set = set(self._spawned_symbols.get(trade_type, []))
            if new_set == old_set:
                continue
            now = time.monotonic()
            last = self._last_restart_at.get(trade_type, 0.0)
            if now - last < self.WS_RESTART_COOLDOWN_SEC:
                logger.info(
                    "refresh_ws_subscriptions[%s]: 订阅集变化但在冷却(%.0fs)内，跳过本轮",
                    trade_type, self.WS_RESTART_COOLDOWN_SEC,
                )
                continue
            added = sorted(new_set - old_set)
            removed = sorted(old_set - new_set)
            logger.info(
                "refresh_ws_subscriptions[%s]: 订阅集变化 +%d -%d (added=%s, removed=%s) → 重启",
                trade_type, len(added), len(removed), added[:5], removed[:5],
            )
            self.restart_ws_runners(trade_type)

    def health_check(self, alert: Optional[Alert]) -> None:
        """检查子进程死亡，忽略计划退出期间 WS 的正常退出。"""
        ws_all = [
            p for tt, procs in self.ws_procs.items() for p in procs
            if not (self._ws_stop_events[tt].is_set() and p.exitcode == 0)
        ]
        for p in [*self.writer_procs, *ws_all]:
            if p is not None and not p.is_alive():
                if alert is not None:
                    try:
                        alert.critical(
                            f"supervisor.dead_child.{p.name}",
                            f"child process {p.name} (pid={p.pid}) exited "
                            f"with code {p.exitcode}",
                        )
                    except Exception:  # pragma: no cover - defensive
                        pass

    def shutdown(self, timeout_sec: Optional[float] = None) -> None:
        """通知 WS 退出，再按现有等待预算关闭 writer。"""
        timeout = timeout_sec if timeout_sec is not None else self.SHUTDOWN_JOIN_TIMEOUT_SEC
        ws_all = [p for procs in self.ws_procs.values() for p in procs]
        for stop_event in self._ws_stop_events.values():
            stop_event.set()
        for p in ws_all:
            try:
                p.join(timeout)
            except Exception:
                pass
        for q in self.queues:
            try:
                q.put(None, timeout=5)
            except Exception:
                pass
        for p in self.writer_procs:
            try:
                p.join(timeout)
            except Exception:
                pass
