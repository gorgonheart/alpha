"""T1 进程编排：writer + ws_runner 子进程的生命周期管理。

main 进程持有一个 RuntimeSupervisor，它拥有：
  - 一个 mp.Queue（writer 消费者 + ws_runner/REST 生产者共享）
  - 一个 writer 子进程（mp.Process，spawn context）
  - 两个 ws_runner 子进程（spot / swap，各持一份 ws_local_cache sqlite）

所有默认值集中为类常量；config.py 仅暴露 use_websocket 开关。
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
    """鸭子类型 put(item)：按 item['symbol'] 分片路由到对应 writer 的 queue。

    封装多 writer queue，让 ws_runner / REST submit 处代码无需感知分片 —— 它们照常
    调 router.put(item)，router 内部路由。可 pickle（spawn 时随 Process args 一起按
    继承机制传递内部的 mp.Queue 列表）。
    """

    def __init__(self, queues: list):
        self.queues = queues
        self.num_writers = len(queues)

    def put(self, item, **kwargs):
        symbol = item.get("symbol", "")
        shard = writer_shard(symbol, self.num_writers)
        self.queues[shard].put(item, **kwargs)


def _configure_subprocess_logging(proc_name: str) -> None:
    """spawn 子进程 root logger 没有 handler；加 stdout + 独立日志文件，否则 logger.info() 全部丢弃。

    每个子进程入口必须调用这个，否则 ws_client / ws_runner / writer 的日志都看不到。

    proc_name 必须全局唯一（用 mp.Process 的 name，形如 bmac-{exchange}-writer-{i}），
    因为它同时是日志文件名：TimedRotatingFileHandler 的午夜轮转不是多进程安全的，
    唯一文件名保证同一文件永远只有一个进程在写。不能用 log_kit —— 它在 import 时就按
    sys.argv[0]（spawn 子进程会还原父进程 argv）打开与主进程相同的日志文件。
    """
    root = logging.getLogger()
    if any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        return  # 已配置过
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
    num_base_candles: int,
    base_delta_seconds: int,
    alert_webhook_url: str,
    gap_check_bars: int = 99,
) -> None:
    """顶层 writer Process target。队列上收到 None 哨兵 → 干净退出。"""
    # mp.Process 的 name（bmac-{exchange}-writer-{i}）分片唯一，直接作为日志标签/文件名
    _configure_subprocess_logging(mp.current_process().name)
    alert = Alert(webhook_url=alert_webhook_url) if alert_webhook_url else None
    writer = Writer(
        base_candle_dirs={k: Path(v) for k, v in base_candle_dirs.items()},
        num_base_candles=num_base_candles,
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
    """运行 WS 子进程，通过共享事件退出，避免中断队列发送。

    Args:
        queue: writer 路由；spec: 订阅配置；alert_webhook_url: 告警地址。
        shared_stop_event: 同组进程共享的停止事件，父进程在启动前创建。
    Returns:
        无。停止时清理客户端，运行异常继续抛出供进程健康检查识别。
    """
    # mp.Process 的 name（bmac-{exchange}-ws-{trade_type}-{chunk_idx}）连接唯一，
    # 直接作为日志标签/文件名
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
        """同时观察启动/接收与退出请求；结束时收拢异步任务并清理客户端。"""
        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        started = False
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                pass  # Windows 没有 add_signal_handler

        async def _run():
            """把启动纳入可取消任务，停止事件早到时不再建立连接。"""
            nonlocal started
            if not shared_stop_event.is_set() and not stop_event.is_set():
                await runner.start(symbols=spec.symbols, replay_on_start=True)
                started = True
                await runner.run()

        async def _wait_stop():
            """短间隔检查进程事件，避免用阻塞 wait 卡住异步循环。"""
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
            # 启动未完成时先取消，防止清理后又创建资源；运行中先退订再关闭接收任务。
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
    """持有 mp.Queue + 子进程。由 core.app.run_realtime 使用。"""

    # ---- 默认值（不暴露到 config_*.py，调改在此） ----
    # WS URL 路由、单连接 stream 上限等交易所相关知识在 core/exchange_protocol.py。
    QUEUE_MAXSIZE = 10000
    # ---- timeouts & concurrency ----
    GRACE_WAIT_SEC = 2
    # barrier 超时/轮询在 core/barrier.py（wait_for_ready_barrier 默认值），不在此处
    WS_QUEUE_PUT_TIMEOUT_SEC = 5.0
    SHUTDOWN_JOIN_TIMEOUT_SEC = 30.0
    # writer 进程数：1000+ symbol 的 ws+rest 落盘是 barrier 的吞吐瓶颈，按 symbol 分片
    # 到 N 个 writer 并行落盘（I/O-bound，进程并行有效）。
    NUM_WRITERS = 4
    # 动态订阅：同一 trade_type 两次重启的最小间隔，防 exginfo 偶发抖动
    WS_RESTART_COOLDOWN_SEC = 60.0
    # 动态订阅重启时，整组旧进程首次退出的总等待预算（热路径，比 shutdown 短）
    WS_RESTART_JOIN_TIMEOUT_SEC = 10.0

    def __init__(self, handle, alert_webhook_url: str, protocol: Optional[ExchangeProtocol] = None):
        """建立 writer 队列及 WS 生命周期记录。

        Args:
            handle: 数据配置；alert_webhook_url: 告警地址；protocol: 交易所协议。
        Returns:
            无。停止事件同时标识待完成重启，不额外维护一套 pending 状态。
        """
        ctx = mp.get_context("spawn")
        self.ctx = ctx
        # N 个 writer queue + 一个分片 router（producers 用 router.put，按 symbol 路由）
        self.queues: list = [ctx.Queue(maxsize=self.QUEUE_MAXSIZE) for _ in range(self.NUM_WRITERS)]
        self.router = WriterQueueRouter(self.queues)
        self.handle = handle
        self.alert_webhook_url = alert_webhook_url
        # 交易所协议：决定 WS URL、stream 上限、帧格式。默认 binance。
        self.protocol = protocol if protocol is not None else BinanceProtocol()
        self.ws_cache_dir: Path = Path(handle.base_dir) / "ws_cache"
        self.ws_cache_dir.mkdir(parents=True, exist_ok=True)
        self.writer_procs: list = []
        # ws_procs 按 trade_type 分组，便于单独重启某一边（动态订阅）
        self.ws_procs: dict = {"spot": [], "swap": []}
        self._ws_stop_events: dict = {}
        # 每个 trade_type 上次 spawn 用的 symbol 列表（用于 diff 检测）
        self._spawned_symbols: dict = {"spot": [], "swap": []}
        # 每个 trade_type 上次重启的 monotonic 时间（用于冷却防抖）
        self._last_restart_at: dict = {}

    def start_writers(self) -> None:
        """Spawn NUM_WRITERS writer 子进程；writer[i] 消费 queues[i]。"""
        from core.utils.time import convert_interval_to_timedelta
        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        base_candle_dirs = {
            "spot": self.handle.spot.base_candle_dir,
            "swap": self.handle.swap.base_candle_dir,
        }
        for i in range(self.NUM_WRITERS):
            p = self.ctx.Process(
                target=run_writer_subprocess,
                name=f"bmac-{self.handle.exchange}-writer-{i}",
                args=(
                    self.queues[i],
                    base_candle_dirs,
                    self.handle.num_base_candles,
                    int(base_delta.total_seconds()),
                    self.alert_webhook_url,
                    # 断档检测窗口 = REST 单次拉取根数，只标记 REST 能补回的缺口
                    getattr(self.handle.spot, "once_update_candles", 99),
                ),
                daemon=False,
            )
            p.start()
            self.writer_procs.append(p)

    def _build_ws_runner_specs(self, trade_type: str) -> list:
        """把该 trade_type 的交易 symbol 切成每块 <=max_streams_per_conn 的若干 chunk，
        每个 chunk 构造一个 WsRunnerSpec。

        每个 spec 有自己的 ws URL（由交易所协议按 chunk 序号给出）和自己的缓存文件。
        """
        from core.utils.time import convert_interval_to_timedelta
        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        bh = self.handle.get_market_handle(trade_type)
        df_exg = self.handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
        symbols = df_exg["symbol"].to_list() if df_exg is not None else []
        chunk_size = self.protocol.max_streams_per_conn
        chunks = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)] or [[]]

        specs: list = []
        for chunk_idx, chunk_syms in enumerate(chunks):
            specs.append(WsRunnerSpec(
                url=self.protocol.ws_url(trade_type, chunk_idx),
                trade_type=trade_type,
                cache_path=self.ws_cache_dir / f"{trade_type}-{chunk_idx}.sqlite",
                base_candle_dir=bh.base_candle_dir,
                base_delta_seconds=int(base_delta.total_seconds()),
                symbols=chunk_syms,
                queue_put_timeout_sec=self.WS_QUEUE_PUT_TIMEOUT_SEC,
                protocol=self.protocol,
                proxy=self.handle.proxy,
            ))
        return specs

    def _spawn_ws_runners_for(self, trade_type: str) -> list:
        """读取最新订阅并启动一组 WS；调用前旧组必须已全部退出。

        Args:
            trade_type: spot 或 swap。
        Returns:
            成功启动的完整交易对列表。部分失败时保留已启动进程引用并请求退出，
            由后续主循环收尾重启；订阅记录仅在整组启动成功后更新。
        """
        specs = self._build_ws_runner_specs(trade_type)
        stop_event = self.ctx.Event()
        self._ws_stop_events[trade_type] = stop_event
        self.ws_procs[trade_type] = []
        symbols: list = []
        try:
            for chunk_idx, spec in enumerate(specs):
                p = self.ctx.Process(
                    target=run_ws_runner_subprocess,
                    name=f"bmac-{self.handle.exchange}-ws-{trade_type}-{chunk_idx}",
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
        """每块 <=max_streams_per_conn 个 symbol 起一个 ws_runner 子进程。

        命名：bmac-ws-{spot|swap}-{chunk_index}。每个子进程持有自己的 ws 连接、自己的
        ws_local_cache sqlite 文件，并订阅它那一片 symbol。所有子进程共享同一个 writer
        队列 router。
        """
        for trade_type in ("spot", "swap"):
            self._spawn_ws_runners_for(trade_type)

    def restart_ws_runners(self, trade_type: str) -> None:
        """请求整组退出；超时保留旧进程，绝不强杀共享队列的生产者。

        Args:
            trade_type: 待刷新订阅的市场类型。
        Returns:
            无。首次最多等待整组预算，重复请求不再等待；事件保持设置直到重启成功。
        """
        stop_event = self._ws_stop_events[trade_type]
        if stop_event.is_set():
            return
        stop_event.set()
        old = self.ws_procs.get(trade_type, [])
        deadline = time.monotonic() + self.WS_RESTART_JOIN_TIMEOUT_SEC
        for p in old:
            p.join(max(0.0, deadline - time.monotonic()))
        if any(p.is_alive() for p in old):
            logger.warning("WS 订阅重启延后\n  市场=%s，旧进程尚未全部退出，保留队列及进程引用", trade_type)
        self.resume_pending_ws_restarts()

    def resume_pending_ws_restarts(self) -> None:
        """非阻塞检查已请求停止的组，全部退出后才启动替代进程。

        Args:
            无；使用当前停止事件与进程记录。
        Returns:
            无。新进程仍使用原 router；不恢复此前已损坏的队列。
        """
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
            logger.info("WS 订阅重启完成\n  市场=%s，进程=%d，交易对=%d",
                        trade_type, len(self.ws_procs[trade_type]), len(symbols))

    def refresh_ws_subscriptions(self, handle) -> None:
        """每轮调用：某 trade_type 的 exginfo symbol 集相对上次 spawn 有增减则重启它。

        带冷却防抖；exginfo 读空时跳过（不在缺数据时误重启）。缺口由 REST 兜底 + 缓存重放补齐。

        Args:
            handle: 用于读取最新交易对元数据的配置。
        Returns:
            无。已在退出中的组交给主循环收尾，不重复发起重启。
        """
        for trade_type in ("spot", "swap"):
            stop_event = self._ws_stop_events.get(trade_type)
            if stop_event is not None and stop_event.is_set():
                continue
            df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
            if df_exg is None or df_exg.empty:
                continue  # 缺数据时不误重启
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
        """检查子进程死亡，忽略计划退出期间 WS 的正常退出。

        Args:
            alert: 可选告警发送器。
        Returns:
            无。writer 死亡和 WS 非零退出码仍告警。
        """
        ws_all = [p for tt, procs in self.ws_procs.items() for p in procs
                  if not (self._ws_stop_events[tt].is_set() and p.exitcode == 0)]
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
        """通知 WS 退出，再按现有等待预算关闭 writer。

        Args:
            timeout_sec: 可选的进程 join 等待秒数。
        Returns:
            无。共享事件不依赖子进程信号处理器是否已经安装。
        """
        timeout = timeout_sec if timeout_sec is not None else self.SHUTDOWN_JOIN_TIMEOUT_SEC
        # 1) 先通知所有 WS 退出，writer 在等待期间继续排空队列。
        ws_all = [p for procs in self.ws_procs.values() for p in procs]
        for stop_event in self._ws_stop_events.values():
            stop_event.set()
        for p in ws_all:
            try:
                p.join(timeout)
            except Exception:
                pass
        # 2) 通知每个 writer 排空剩余 item 并干净退出
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
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
