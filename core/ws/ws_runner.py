"""
ws_runner：单进程多连接协程编排器，把多个 WsKlineClient → 共享 WsLocalCache + writer 队列串起来。

职责：
  1. 打开单个市场共享的 WsLocalCache（`ws_cache/{trade_type}.sqlite`，崩溃恢复持久缓冲）。
  2. 按 `protocol.max_streams_per_conn` 将单市场全量 symbols 切片为多个 chunk，
     每个 chunk 构造一个 `WsKlineClient` 在同一事件循环并发运行。
  3. 每个 chunk 拥有独立的异常隔离与退避重连循环，单个 chunk 故障绝不崩溃或阻塞其他 chunk。
  4. 启动重放后执行一次 `prune_before(now_ms - 48h)`，并在运行期每小时定时清理过期缓存。
  5. `symbols=[]` 时安全返回，`start()` / `run()` / `stop()` 均不挂起。
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import queue as stdlib_queue
import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from core.exchange.protocol import BinanceProtocol, ExchangeProtocol
from core.ws.ws_kline_client import WsKlineClient
from core.ws.ws_local_cache import WsLocalCache


logger = logging.getLogger(__name__)

_DEFAULT_CACHE_RETENTION_HOURS = 48.0
_DEFAULT_PRUNE_INTERVAL_SEC = 3600.0
_DEFAULT_CHUNK_RETRY_BASE_SEC = 1.0
_DEFAULT_CHUNK_RETRY_MAX_SEC = 30.0


def _kline_to_writer_item(
    symbol: str,
    trade_type: str,
    kline: dict,
    base_delta: timedelta,
) -> dict:
    """把归一化的缓存结构 K 线 dict 转成 writer 队列 item。

    关键约定：writer 期望长形式的 binance 列名（taker_buy_base_asset_volume, ...），
    而缓存用短形式。本函数是唯一的转换边界。
    """
    begin_dt = pd.Timestamp(kline["begin_time_ms"], unit="ms", tz="UTC")
    df = pd.DataFrame([{
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
    run_time = (begin_dt + pd.Timedelta(base_delta)).to_pydatetime()
    return {
        "symbol": symbol,
        "trade_type": trade_type,
        "df": df,
        "source": "ws",
        "run_time": run_time,
    }


class WsRunner:
    """编排单市场多连接（多 chunk 协程）的 WS 生命周期 + 共享 SQLite 持久化。

    构造函数保持同步，以便在子进程入口里运行；`start`、`run`、`stop` 是异步的。
    """

    def __init__(
        self,
        url: str,
        trade_type: str,
        queue,  # mp.Queue 或任何鸭子类型的 put(item)
        cache_path: Path,
        base_delta: timedelta = timedelta(minutes=5),
        ws_client_factory: Optional[Callable[..., Any]] = None,
        alert: Optional[Any] = None,
        queue_put_timeout_sec: float = 5.0,
        base_candle_dir: Optional[Path] = None,
        protocol: Optional[ExchangeProtocol] = None,
        proxy: Optional[str] = None,
        chunk_retry_base_sec: float = _DEFAULT_CHUNK_RETRY_BASE_SEC,
        chunk_retry_max_sec: float = _DEFAULT_CHUNK_RETRY_MAX_SEC,
        prune_interval_sec: float = _DEFAULT_PRUNE_INTERVAL_SEC,
        cache_retention_hours: float = _DEFAULT_CACHE_RETENTION_HOURS,
    ) -> None:
        self.url = url
        self.trade_type = trade_type
        self.queue = queue
        self.cache_path = Path(cache_path)
        self.base_delta = base_delta
        self._ws_client_factory = ws_client_factory or self._default_client_factory
        self._cache: Optional[WsLocalCache] = None
        # 多连接 chunk 列表：[(chunk_idx, client, streams), ...]
        self._chunks: list[tuple[int, Any, list[str]]] = []
        self._client: Optional[Any] = None  # 向后兼容属性（指向首个 client 或 None）
        self._started: bool = False
        self._stop_event: Optional[asyncio.Event] = None
        self._alert = alert
        self.queue_put_timeout_sec = float(queue_put_timeout_sec)
        self.base_candle_dir = Path(base_candle_dir) if base_candle_dir else None
        self._closed_count = 0
        self._protocol = protocol if protocol is not None else BinanceProtocol()
        self._proxy = proxy
        self.chunk_retry_base_sec = float(chunk_retry_base_sec)
        self.chunk_retry_max_sec = float(chunk_retry_max_sec)
        self.prune_interval_sec = float(prune_interval_sec)
        self.cache_retention_hours = float(cache_retention_hours)

    # ---- 回调 / 辅助 ----

    def _default_client_factory(
        self,
        on_closed_kline: Callable[[dict], None],
        url: Optional[str] = None,
        chunk_idx: int = 0,
    ) -> WsKlineClient:
        return WsKlineClient(
            url=url or self.url,
            trade_type=self.trade_type,
            on_closed_kline=on_closed_kline,
            protocol=self._protocol,
            proxy=self._proxy,
        )

    def _create_client(self, chunk_url: str, chunk_idx: int) -> Any:
        """调用 ws_client_factory，兼容 (on_closed) 与 (on_closed, url=..., chunk_idx=...) 两种签名。"""
        factory = self._ws_client_factory
        try:
            sig = inspect.signature(factory)
            params = sig.parameters
            has_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
            kwargs: dict[str, Any] = {}
            if has_var_kw or "url" in params:
                kwargs["url"] = chunk_url
            if has_var_kw or "chunk_idx" in params:
                kwargs["chunk_idx"] = chunk_idx
            return factory(self._on_closed_kline, **kwargs)
        except (TypeError, ValueError):
            return factory(self._on_closed_kline)

    def _on_closed_kline(self, payload: dict) -> None:
        """每个 k.x=true 帧由任意 chunk 的 WsKlineClient 调到这里。

        先持久化到本地缓存（耐久性），再提交 writer 队列（可见性）。
        同一事件循环单线程执行，共享单个 WsLocalCache 无锁竞争。
        """
        symbol = payload["symbol"]
        kline = payload["kline"]
        if self._cache is not None:
            self._cache.write(symbol, self.trade_type, kline)
        item = _kline_to_writer_item(symbol, self.trade_type, kline, self.base_delta)

        self._closed_count += 1
        self._log_closed_kline_progress(symbol, kline)

        try:
            self.queue.put(item, block=True, timeout=self.queue_put_timeout_sec)
        except stdlib_queue.Full:
            self._alert_queue_full(symbol, kline)

    def _log_closed_kline_progress(self, symbol: str, kline: dict) -> None:
        """首条 + 每 100 条可见性日志，让运维看到数据面活着。"""
        if self._closed_count == 1:
            logger.info(
                "ws_runner[%s] FIRST closed kline queued for writer: %s @ %s",
                self.trade_type, symbol, kline.get("begin_time_ms"),
            )
        elif self._closed_count % 100 == 0:
            logger.info(
                "ws_runner[%s] closed klines queued cumulative=%d (last=%s)",
                self.trade_type, self._closed_count, symbol,
            )

    def _alert_queue_full(self, symbol: str, kline: dict) -> None:
        """队列饱和：缓存里仍有这根 K，所以大声告警，只丢弃队列路径那一份。"""
        if self._alert is not None:
            try:
                self._alert.critical(
                    f"ws_runner.queue_full.{self.trade_type}",
                    f"ws_runner queue.put timeout after {self.queue_put_timeout_sec}s; "
                    f"dropped {symbol} {kline.get('begin_time_ms')} from queue path "
                    f"(cache still has it)",
                )
            except Exception:  # pragma: no cover - defensive
                pass

    def _prune_cache(self) -> int:
        """清理超过 cache_retention_hours（默认 48h）的本地缓存行。"""
        if self._cache is None:
            return 0
        cutoff_ms = int((time.time() - self.cache_retention_hours * 3600.0) * 1000)
        try:
            deleted = self._cache.prune_before(cutoff_ms)
            if deleted > 0:
                logger.info(
                    "ws_runner[%s] pruned %d stale cache rows (cutoff_ms=%d)",
                    self.trade_type, deleted, cutoff_ms,
                )
            return deleted
        except Exception:
            logger.exception("ws_runner[%s] prune_before failed", self.trade_type)
            return 0

    def replay_cache(self, symbols: Iterable[str]) -> int:
        """启动时，重放缓存里比磁盘水位线更新的那些行。"""
        if self._cache is None:
            self._cache = WsLocalCache(self.cache_path)
        watermarks = self._build_watermark_map()
        count = 0
        for symbol in symbols:
            watermark_ms = watermarks.get(symbol, 0)
            rows = self._cache.replay_after(symbol, self.trade_type, watermark_ms)
            for kline in rows:
                item = _kline_to_writer_item(symbol, self.trade_type, kline, self.base_delta)
                self.queue.put(item)
                count += 1
        return count

    def _build_watermark_map(self) -> dict:
        """一次 glob ready 文件，解析出每个 symbol 的磁盘水位线 begin_time_ms。"""
        watermarks: dict = {}
        if self.base_candle_dir is None:
            return watermarks
        base_delta_sec = int(self.base_delta.total_seconds())
        for ready_path in self.base_candle_dir.glob("*.ready"):
            stem = ready_path.stem
            sep = stem.rfind("_")
            if sep <= 0:
                continue
            symbol = stem[:sep]
            try:
                run_ts = int(stem[sep + 1:])
            except ValueError:
                continue
            begin_time_ms = (run_ts - base_delta_sec) * 1000
            watermarks[symbol] = max(watermarks.get(symbol, 0), begin_time_ms)
        return watermarks

    # ---- 异步生命周期 ----

    async def start(
        self,
        symbols: list[str],
        replay_on_start: bool = True,
    ) -> None:
        """打开共享缓存、按 max_streams_per_conn 切片并连接订阅所有 chunk，随后重放并清理 48h 前缓存。"""
        if self._cache is None:
            self._cache = WsLocalCache(self.cache_path)
        self._stop_event = asyncio.Event()
        self._chunks = []
        self._client = None

        all_symbols = list(symbols)
        if not all_symbols:
            self._started = True
            self._prune_cache()
            logger.info("ws_runner[%s] started with 0 symbols (idle)", self.trade_type)
            return

        chunk_size = max(1, int(self._protocol.max_streams_per_conn))
        symbol_chunks = [
            all_symbols[i:i + chunk_size]
            for i in range(0, len(all_symbols), chunk_size)
        ]
        interval = self._interval_str()

        # 1) 先 connect + subscribe 所有 chunk，确保重放期间实时帧不漏。
        for chunk_idx, chunk_syms in enumerate(symbol_chunks):
            chunk_url = self._protocol.ws_url(self.trade_type, chunk_idx)
            client = self._create_client(chunk_url, chunk_idx)
            streams = [self._protocol.stream_name(s, interval) for s in chunk_syms]
            await client.connect()
            await client.subscribe(streams)
            self._chunks.append((chunk_idx, client, streams))

        self._client = self._chunks[0][1] if self._chunks else None
        self._started = True
        logger.info(
            "ws_runner[%s] subscribed %d streams across %d chunks",
            self.trade_type, len(all_symbols), len(self._chunks),
        )

        # 2) 再重放缓存，并在重放后清理 48h 之前的旧行。
        if replay_on_start:
            self.replay_cache(all_symbols)
        self._prune_cache()

    async def _run_chunk_loop(self, chunk_idx: int, client: Any, streams: list[str]) -> None:
        """单个 chunk 的独立接收 + 异常隔离退避重连循环。"""
        assert self._stop_event is not None
        backoff = self.chunk_retry_base_sec
        while not self._stop_event.is_set():
            try:
                await client.run()
                if self._stop_event.is_set():
                    break
                backoff = self.chunk_retry_base_sec
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "ws_runner[%s] chunk=%d run raised (%r); backoff=%.2fs",
                    self.trade_type, chunk_idx, exc, backoff,
                )

            if self._stop_event.is_set():
                break
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=backoff)
                break
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2.0, self.chunk_retry_max_sec)

            if self._stop_event.is_set():
                break
            try:
                await client.connect()
                if streams:
                    await client.subscribe(streams)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "ws_runner[%s] chunk=%d reconnect failed (%r)",
                    self.trade_type, chunk_idx, exc,
                )

    async def _prune_loop(self) -> None:
        """运行期每 prune_interval_sec（默认 1 小时）清理 48h 之前的本地缓存。"""
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.prune_interval_sec,
                )
                break
            except asyncio.TimeoutError:
                self._prune_cache()

    async def run(self) -> None:
        """并发运行全部 chunk 协程与周期缓存清理任务。须在 start() 之后调用。"""
        if not self._started or self._stop_event is None:
            raise RuntimeError("start() must be called before run()")
        if not self._chunks:
            # 空 symbol 集：安全等待 stop() 或任务取消，不挂起、不崩溃。
            await self._stop_event.wait()
            return

        tasks = [
            asyncio.create_task(self._run_chunk_loop(chunk_idx, client, streams))
            for chunk_idx, client, streams in self._chunks
        ]
        tasks.append(asyncio.create_task(self._prune_loop()))
        try:
            await asyncio.gather(*tasks)
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """优雅关停所有 chunk client 并关闭共享 SQLite 缓存。幂等。"""
        if self._stop_event is not None:
            self._stop_event.set()
        for _, client, _ in self._chunks:
            try:
                await client.close()
            except Exception:
                pass
        self._chunks = []
        self._client = None
        if self._cache is not None:
            try:
                self._cache.close()
            except Exception:
                pass
            self._cache = None

    # ---- 内部 ----

    def _interval_str(self) -> str:
        """把 base_delta 转成周期字符串格式（'5m'、'1h'）。"""
        total_seconds = int(self.base_delta.total_seconds())
        if total_seconds < 3600:
            return f"{total_seconds // 60}m"
        return f"{total_seconds // 3600}h"
