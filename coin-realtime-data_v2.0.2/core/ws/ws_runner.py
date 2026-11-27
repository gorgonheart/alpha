"""
ws_runner：把 WsKlineClient → WsLocalCache + writer 队列 串起来的子进程入口。

职责：
  1. 打开 WsLocalCache（崩溃恢复用的持久缓冲；能在 writer 或 main 进程死亡后存活）。
  2. 启动时把缓存重放到 writer 队列，使 ws_runner 在两次落盘之间累积的 K 不丢。
     writer 的优先级合并会去重。
  3. 创建 WsKlineClient，把每根闭合 K 转发给 (a) cache.write 和 (b) 以 writer 兼容的
     item 结构 queue.put。
  4. 优雅关停：关闭 client，关闭 cache。

一个 WsRunner 实例管理一个 trade_type。spot+swap 各跑一个 runner 实例（各自独立子
进程），这样缓存文件不冲突、ws 连接也保持在单连接 stream 上限以下。

ws_runner 是一个 main 子进程，承载进程崩溃恢复链路。
"""
from __future__ import annotations

import logging
import queue as stdlib_queue
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import pandas as pd

from core.exchange.protocol import BinanceProtocol, ExchangeProtocol
from core.ws.ws_kline_client import WsKlineClient
from core.ws.ws_local_cache import WsLocalCache


logger = logging.getLogger(__name__)


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
    """编排一条 ws 连接的生命周期 + 持久化。

    构造函数保持同步，以便在子进程入口里运行；`start` 和 `run` 是异步的
    （底层 client 用 aiohttp）。
    """

    def __init__(
        self,
        url: str,
        trade_type: str,
        queue,  # mp.Queue 或任何鸭子类型的 put(item)
        cache_path: Path,
        base_delta: timedelta = timedelta(minutes=5),
        ws_client_factory: Optional[Callable[[Callable[[dict], None]], WsKlineClient]] = None,
        alert: Optional[Any] = None,
        queue_put_timeout_sec: float = 5.0,
        base_candle_dir: Optional[Path] = None,
        protocol: Optional[ExchangeProtocol] = None,
        proxy: Optional[str] = None,
    ) -> None:
        self.url = url
        self.trade_type = trade_type
        self.queue = queue
        self.cache_path = Path(cache_path)
        self.base_delta = base_delta
        self._ws_client_factory = ws_client_factory or self._default_client_factory
        self._cache: Optional[WsLocalCache] = None
        self._client: Optional[WsKlineClient] = None
        # queue.put 超时事件的可选告警接收端。
        self._alert = alert
        self.queue_put_timeout_sec = float(queue_put_timeout_sec)
        # writer 的 pkl 目录（按 trade_type），用于查重放水位线。
        self.base_candle_dir = Path(base_candle_dir) if base_candle_dir else None
        # 已转发的闭合 K 计数，用于里程碑日志。
        self._closed_count = 0
        # 交易所协议（注入给 WsKlineClient，并用于 stream 命名）。
        self._protocol = protocol if protocol is not None else BinanceProtocol()
        # 代理：透传给 WsKlineClient，使 WS 与 REST 走同一条 Clash 隧道。
        self._proxy = proxy

    # ---- 回调 / 辅助 ----

    def _default_client_factory(self, on_closed_kline: Callable[[dict], None]) -> WsKlineClient:
        return WsKlineClient(
            url=self.url,
            trade_type=self.trade_type,
            on_closed_kline=on_closed_kline,
            protocol=self._protocol,
            proxy=self._proxy,
        )

    def _on_closed_kline(self, payload: dict) -> None:
        """每个 k.x=true 帧由 WsKlineClient 调到这里。

        先持久化到本地缓存（耐久性），再提交 writer 队列（可见性）。顺序很关键：
        若 queue.put 失败或 writer 死了，这根 K 仍可在下次启动重放时从缓存恢复。

        queue.put 用有限超时；队列满时优雅降级（告警 + 从队列路径丢弃 —— 缓存里还有，
        留给重放），这样 ws 接收循环绝不会被滞后的 writer 阻塞。
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
                "ws_runner[%s] ✅ FIRST closed kline queued for writer: %s @ %s",
                self.trade_type, symbol, kline.get("begin_time_ms"),
            )
        elif self._closed_count % 100 == 0:
            logger.info(
                "ws_runner[%s] closed klines queued cumulative=%d (last=%s)",
                self.trade_type, self._closed_count, symbol,
            )

    def _alert_queue_full(self, symbol: str, kline: dict) -> None:
        """队列饱和：缓存里仍有这根 K，所以大声告警，只丢弃队列路径那一份。
        之后的重放或 REST 兜底会把它补回来。"""
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

    def replay_cache(self, symbols: Iterable[str]) -> int:
        """启动时，重放缓存里比磁盘水位线更新的那些行。

        一次性扫 ready 文件构建每 symbol 的磁盘水位线，再配合
        `cache.replay_after(watermark_ms)` 只重放更新的行，避免重复重放、也突破固定
        根数上限。无 ready 文件的 symbol 水位线按 0 处理 → 全量重放（writer 去重）。

        返回提交到队列的 item 数量。
        """
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
        """一次 glob ready 文件，解析出每个 symbol 的磁盘水位线 begin_time_ms。

        writer 每次成功落盘后 save_ready 产生 `{symbol}_{int(run_time.timestamp())}.ready`。
        因 run_time = begin_time + base_delta，故 begin_time_ms = (int_ts - base_delta_sec)*1000。
        相比逐 symbol 读全量 pkl，这里只做一次目录遍历 + 文件名解析，启动快得多。
        无 ready 文件的 symbol 不在返回的 map 里（调用方按 0 处理 → 全量重放）。
        """
        watermarks: dict = {}
        if self.base_candle_dir is None:
            return watermarks
        base_delta_sec = int(self.base_delta.total_seconds())
        for ready_path in self.base_candle_dir.glob("*.ready"):
            stem = ready_path.stem  # 形如 "{symbol}_{int_ts}"
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
        """打开缓存、打开并订阅 ws client，然后再重放缓存。

        connect+subscribe 必须在重放之前发生，这样重放窗口期间不会漏掉实时帧。在缓存
        预热充分时重放可能耗时 >10s（2400+ 条 × 写延迟）；那段窗口里 ws 连接是活的，
        会和重放 item 一起把新帧喂进队列。writer 的优先级合并会去重任何重叠。
        """
        if self._cache is None:
            self._cache = WsLocalCache(self.cache_path)

        all_symbols = list(symbols)
        # 所有 symbol 原样订阅（不做非 ASCII 预过滤）：若 binance 拒绝某条 stream，
        # 只影响那一条，其余 stream 继续推帧。

        # 1) 先 connect + subscribe。
        self._client = self._ws_client_factory(self._on_closed_kline)
        await self._client.connect()
        streams = [self._protocol.stream_name(s, self._interval_str()) for s in all_symbols]
        await self._client.subscribe(streams)
        logger.info(
            "ws_runner[%s] subscribed %d streams (no filter)",
            self.trade_type, len(streams),
        )
        # 2) 再重放缓存。实时帧可能穿插进来；writer 去重。
        if replay_on_start:
            self.replay_cache(all_symbols)

    async def run(self) -> None:
        """在 ws client 上阻塞接收循环。须在 start() 之后调用。"""
        if self._client is None:
            raise RuntimeError("start() must be called before run()")
        await self._client.run()

    async def stop(self) -> None:
        """优雅关停。幂等。"""
        if self._client is not None:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None
        if self._cache is not None:
            try:
                self._cache.close()
            except Exception:
                pass
            self._cache = None

    # ---- 内部 ----

    def _interval_str(self) -> str:
        """把 base_delta 转成 binance 的周期字符串格式（'5m'、'1h'）。"""
        total_seconds = int(self.base_delta.total_seconds())
        if total_seconds < 3600:
            return f"{total_seconds // 60}m"
        return f"{total_seconds // 3600}h"
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
