"""
binance combined-stream K 线流的单连接 asyncio websocket 客户端。

一个 ``WsKlineClient`` 实例持有一条 ws 连接。为覆盖两种交易类型、或为不超过单连接
1024 stream 的上限，``ws_runner`` 编排器会实例化多个客户端：

    - spot 用 ``wss://stream.binance.com:9443/stream``（测试里用 mock url）
    - swap 用 ``wss://fstream.binance.com/stream``

职责：

    1. 通过 ``aiohttp.ClientSession.ws_connect`` 打开一条 ws 连接。
    2. 发送 SUBSCRIBE / UNSUBSCRIBE 控制帧，带 4 条/分钟 的节流（binance 允许 5 条/分钟，
       我们留出余量）。
    3. 接收帧、解析 JSON，丢弃 ``k.x == false``（未闭合 K 线）。
    4. 对 ``k.x == true``，构造归一化的 K 线 payload 并调用 ``on_closed_kline``。
    5. 心跳：若 ``heartbeat_timeout_sec`` 内无消息到达，视为半开连接并强制重连。
    6. 断连或心跳超时后，按配置的退避序列重连（最后一个值无限重复）。重连成功后，
       对此前订阅过的每条 stream 重新发送 SUBSCRIBE（服务端的订阅状态按连接维护）。
    7. 通过 ``on_health_change`` 上报健康状态切换（``'healthy'`` / ``'reconnecting'`` /
       ``'down'``）。
    8. JSON 格式错误的帧静默跳过；``parse_errors`` 记数，便于监控暴露异常上游。
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Tuple

import aiohttp

from core.exchange.protocol import BinanceProtocol, ExchangeProtocol, HEARTBEAT_NONE, HEARTBEAT_TEXT


logger = logging.getLogger(__name__)


# 默认值放模块级，便于测试覆盖，也让取值与设计文档规范保持一致。
_DEFAULT_BACKOFF: Tuple[float, ...] = (1.0, 2.0, 5.0, 10.0)
# 接收超时：连续这么久收不到任何帧（TEXT/BINARY/CLOSE）就判定连接死、强制重连。
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
# 保活模型：不主动发 client PING（futures 端点不保证对 client ping 回 pong，主动 ping
# 等 pong 会在高负载/边界 burst 时误判半开 → 规律性误重连）；服务器每 3min 发的 PING
# 由 aiohttp autoping 自动回 PONG。
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
# 取值依据「数据流」而非「服务器 ping」：每条连接只要活着就持续推未闭合 K 线帧，连最小
# 的 22-symbol 分片也有 ~2.8 帧/秒，故 60s 内零帧 = 连接已被（代理节点）静默掐死。60s
# 能让死连接快速重建、常能赶上下一个 5m 边界；活连接数据从不中断 60s，绝不会误杀。
# （旧值 240s 会让一条静默死连接横跨整个 5m 边界才被发现 —— 实测见 swap 静默 4 分钟、
#  漏掉该边界 WS 推送、只能靠 REST 兜底的案例。）
_DEFAULT_HEARTBEAT_TIMEOUT_SEC = 60.0
_DEFAULT_SUBSCRIBE_INTERVAL_SEC = 15.0  # 60s / 4 条 ≈ 4 条/分钟
# OKX 应用层心跳：空闲超过这么久就主动发一帧 text "ping"（OKX 文档建议 30s 内有交互，
# 取 20s 留余量）。仅 heartbeat_mode == "okx_text" 的协议（OkxProtocol）生效。
_OKX_PROACTIVE_PING_IDLE_SEC = 20.0


class WsKlineClient:
    """管理单条连接的 asyncio websocket 客户端。

    生命周期：
        client = WsKlineClient(url, trade_type, on_closed_kline)
        await client.connect()
        run_task = asyncio.create_task(client.run())
        await client.subscribe([...])
        ...
        await client.close()
        await run_task

    线程：所有公共方法都是协程，必须在同一事件循环上 await。本类非线程安全。
    """

    def __init__(
        self,
        url: str,
        trade_type: str,
        on_closed_kline: Callable[[dict], None],
        on_health_change: Optional[Callable[[str], None]] = None,
        heartbeat_timeout_sec: float = _DEFAULT_HEARTBEAT_TIMEOUT_SEC,
        backoff_schedule_sec: Tuple[float, ...] = _DEFAULT_BACKOFF,
        _subscribe_min_interval_sec: float = _DEFAULT_SUBSCRIBE_INTERVAL_SEC,
        protocol: Optional[ExchangeProtocol] = None,
        proxy: Optional[str] = None,
    ) -> None:
        if trade_type not in ("spot", "swap"):
            # 防御性校验 —— 错误的 trade_type 会把每条数据都贴错标签。
            raise ValueError(f"trade_type must be 'spot' or 'swap', got {trade_type!r}")
        if not backoff_schedule_sec:
            raise ValueError("backoff_schedule_sec must be non-empty")

        self.url = url
        self.trade_type = trade_type
        self._on_closed_kline = on_closed_kline
        self._on_health_change = on_health_change
        self._heartbeat_timeout_sec = heartbeat_timeout_sec
        self._backoff_schedule_sec = backoff_schedule_sec
        self._subscribe_min_interval_sec = _subscribe_min_interval_sec
        # 交易所协议：URL 路由 / 订阅帧 / 帧解析 / stream 命名都委托给它。
        self._protocol = protocol if protocol is not None else BinanceProtocol()
        # 心跳模型：OKX 用应用层 text "ping"/"pong"，Binance 走 WS 控制帧（autoping）。
        # 协议未声明 heartbeat_mode 时默认 "none"，保证 Binance 路径完全不受影响。
        self._heartbeat_mode = getattr(self._protocol, "heartbeat_mode", HEARTBEAT_NONE)
        # 代理：与 REST 路径（binance_market_async）保持一致，走同一条 Clash 隧道。
        # None 表示直连（测试用 mock url 时即为 None）。
        self._proxy = proxy

        # 连接状态。
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

        # 订阅状态 —— 跨重连保留。
        self._subscribed_streams: set[str] = set()
        self._next_request_id = 1

        # SUBSCRIBE/UNSUBSCRIBE 的节流状态。
        self._last_subscribe_send_at: float = 0.0

        # 遥测计数。
        self.parse_errors: int = 0
        self._reconnect_count: int = 0
        # 入站存活时钟：仅由真实入站帧（含服务器 ping/pong/控制帧）刷新；
        # 死连接判定 (_receive_until_disconnect) 完全以此为准。
        self._last_message_at: float = 0.0
        # 出站主动 ping 的节流时钟：与入站时钟分离，避免半开连接上 send 成功
        # 却无入站帧时持续重置入站时钟、令死判定永不触发。
        self._last_proactive_ping_at: float = 0.0
        # 用于周期性可见性日志的帧计数。
        self._frames_total: int = 0
        self._closed_klines_total: int = 0
        self._log_every_n_frames: int = 1000
        self._first_kline_logged: bool = False

        # 健康状态 —— 用于去重 on_health_change 回调。
        self._current_health: str = "down"

        # 运行循环控制。
        self._stop_event = asyncio.Event()
        self._run_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ 属性
    @property
    def reconnect_count(self) -> int:
        return self._reconnect_count

    @property
    def is_healthy(self) -> bool:
        """若最近一条消息在 ``heartbeat_timeout_sec`` 内收到则为 True。"""
        if self._ws is None or self._ws.closed:
            return False
        if self._last_message_at == 0.0:
            # 还没收到消息，但 socket 已开 —— 连接后的心跳窗口内视为健康。
            return True
        elapsed = time.monotonic() - self._last_message_at
        return elapsed < self._heartbeat_timeout_sec

    # ------------------------------------------------------------ 公共 API
    async def connect(self) -> None:
        """打开 ws 连接。已连接则幂等。"""
        if self._session is None:
            self._session = aiohttp.ClientSession()
        if self._ws is not None and not self._ws.closed:
            return
        self._ws = await self._session.ws_connect(
            self.url, heartbeat=None, proxy=self._proxy,  # 不主动发 client ping；走与 REST 同一代理
        )
        self._last_message_at = time.monotonic()
        self._set_health("healthy")
        logger.info("ws_client[%s] connected to %s", self.trade_type, self.url)

    async def subscribe(self, streams: List[str]) -> None:
        """发送 SUBSCRIBE；记入 ``_subscribed_streams`` 以便重连时重新订阅。"""
        new_streams = [s for s in streams if s not in self._subscribed_streams]
        self._subscribed_streams.update(streams)
        if not new_streams:
            return
        await self._send_subscribe_control("SUBSCRIBE", new_streams)

    async def unsubscribe(self, streams: List[str]) -> None:
        """发送 UNSUBSCRIBE；从 ``_subscribed_streams`` 移除。"""
        to_remove = [s for s in streams if s in self._subscribed_streams]
        for s in to_remove:
            self._subscribed_streams.discard(s)
        if not to_remove:
            return
        await self._send_subscribe_control("UNSUBSCRIBE", to_remove)

    async def run(self) -> None:
        """带自动重连的主接收循环。

        取消：``close()`` 设置 ``_stop_event`` 并取消 ws，使内层接收循环解除阻塞。
        本协程在干净关停时返回 —— 调用方在 ``close()`` 之后 ``await`` 它，以确保所有
        socket 资源都已释放。
        """
        try:
            while not self._stop_event.is_set():
                # 万一进入 run() 时没有存活的 socket，先重连。
                if self._ws is None or self._ws.closed:
                    await self._reconnect_loop()
                    if self._stop_event.is_set():
                        break

                disconnected = await self._receive_until_disconnect()
                if self._stop_event.is_set():
                    break
                if disconnected:
                    # 对端关闭或心跳超时 —— 进入重连循环，它也会重新订阅。
                    await self._reconnect_loop()
        finally:
            await self._close_ws_only()

    async def close(self) -> None:
        """优雅关停 —— 先尽力 UNSUBSCRIBE，再停运行循环并释放 socket。"""
        await self._unsubscribe_all_best_effort()
        self._stop_event.set()
        await self._close_ws_only()
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._set_health("down")

    async def _unsubscribe_all_best_effort(self) -> None:
        """关停前主动 UNSUBSCRIBE 当前所有订阅（绕过节流、吞掉异常）。

        交易所在连接关闭时本会回收订阅，但显式退订是良好实践，且能在快速重启场景下
        减少对交易所连接/订阅配额的占用。socket 已关或无订阅时直接跳过。
        """
        if self._ws is None or self._ws.closed or not self._subscribed_streams:
            return
        streams = sorted(self._subscribed_streams)
        payload = self._build_subscribe_payload("UNSUBSCRIBE", streams)
        try:
            await self._ws.send_json(payload)
            logger.info(
                "ws_client[%s] sent UNSUBSCRIBE on close (count=%d)",
                self.trade_type, len(streams),
            )
        except Exception:
            # 关停途中 socket 可能已坏 —— 忽略即可。
            pass
        self._subscribed_streams.clear()

    # --------------------------------------------------------------- 内部
    async def _close_ws_only(self) -> None:
        if self._ws is not None and not self._ws.closed:
            try:
                await self._ws.close()
            except Exception:
                # 强制关闭路径下 socket 可能已损坏 —— 忽略。
                pass
        self._ws = None

    def _set_health(self, new_state: str) -> None:
        if new_state == self._current_health:
            return
        prev = self._current_health
        self._current_health = new_state
        logger.info(
            "ws_client[%s] health: %s -> %s (reconnects=%d)",
            self.trade_type, prev, new_state, self._reconnect_count,
        )
        if self._on_health_change is not None:
            try:
                self._on_health_change(new_state)
            except Exception:
                logger.exception("on_health_change callback raised")

    def _build_subscribe_payload(self, method: str, streams: List[str]) -> dict:
        """构造一个 SUBSCRIBE/UNSUBSCRIBE 控制帧，并消费一个 request id。

        帧的具体格式委托给交易所协议；request-id 的递增逻辑集中在这里一处。
        """
        payload = self._protocol.build_subscribe(method, streams, self._next_request_id)
        self._next_request_id += 1
        return payload

    async def _send_subscribe_control(self, method: str, streams: List[str]) -> None:
        """发送一个 SUBSCRIBE / UNSUBSCRIBE 控制帧，遵守节流。"""
        if self._ws is None or self._ws.closed:
            # 调用方须先 connect；订阅状态已被记录，重连时会正确地重新订阅。
            return

        # 节流：每 _subscribe_min_interval_sec 最多发一个控制帧。
        if self._subscribe_min_interval_sec > 0:
            now = time.monotonic()
            gap = now - self._last_subscribe_send_at
            if gap < self._subscribe_min_interval_sec:
                await asyncio.sleep(self._subscribe_min_interval_sec - gap)

        payload = self._build_subscribe_payload(method, streams)
        request_id = payload.get("id", "n/a")  # 某些交易所的订阅帧无 id 字段
        try:
            await self._ws.send_json(payload)
            self._last_subscribe_send_at = time.monotonic()
            sample = streams[:3] if len(streams) > 3 else streams
            logger.info(
                "ws_client[%s] sent %s id=%s count=%d sample=%s",
                self.trade_type, method, request_id, len(streams), sample,
            )
        except (ConnectionResetError, aiohttp.ClientError) as exc:
            # 发送途中 socket 挂了 —— 运行循环会在下次 recv 时发现并重连
            #（重连会从 stream 集合重新订阅）。
            logger.warning(
                "ws_client[%s] %s id=%s send failed (%r); will resubscribe on reconnect",
                self.trade_type, method, request_id, exc,
            )

    async def _maybe_handle_heartbeat_text(self, data: str) -> bool:
        """OKX 应用层心跳：消费 text "ping" / "pong" 帧并刷新活动时间。

        仅在 heartbeat_mode == "okx_text" 生效；返回 True 表示已作为心跳处理
        （调用方不要再路由给 _handle_text_message）：

          - text "ping"（服务器主动探活）：回一帧 text "pong"，再刷新活动时间。
          - text "pong"（对我方主动 ping 的应答）：仅作为入站存活信号，刷新活动
            时间，绝不回帧、也绝不进 JSON 解析路径。

        其余情况返回 False，让正常的 JSON 解析路径接手。Binance（无 heartbeat_mode）
        永远返回 False。
        """
        if self._heartbeat_mode != HEARTBEAT_TEXT:
            return False
        if data not in ("ping", "pong"):
            return False
        if data == "ping" and self._ws is not None and not self._ws.closed:
            try:
                await self._ws.send_str("pong")
            except (ConnectionResetError, aiohttp.ClientError) as exc:
                # 回 pong 失败：接收循环会在下次 recv 时发现断连并重连。
                logger.warning("ws_client[%s] 回 pong 失败: %r", self.trade_type, exc)
        # 不论是收到 ping 还是 pong，入站帧本身就是连接存活的证据 → 刷新活动时间。
        self._last_message_at = time.monotonic()
        return True

    async def _maybe_send_proactive_heartbeat(self) -> bool:
        """OKX 应用层心跳：空闲超过 _OKX_PROACTIVE_PING_IDLE_SEC 时主动发 text "ping"。

        仅 heartbeat_mode == "okx_text" 生效；返回 True 表示发出了一帧 ping。Binance
        （无 heartbeat_mode）永远是 no-op，返回 False，保证其保活模型不受影响。
        """
        if self._heartbeat_mode != HEARTBEAT_TEXT:
            return False
        if self._ws is None or self._ws.closed:
            return False
        now = time.monotonic()
        # 触发条件：自最近一次「真实入站活动」起空闲超过阈值。入站时钟是死判定的
        # 唯一依据，这里只读不写。
        idle = now - self._last_message_at
        if idle < _OKX_PROACTIVE_PING_IDLE_SEC:
            return False
        # 节流：自上一次「主动 ping」起也需超过阈值，避免空闲窗口内每轮 recv 超时都重发。
        since_last_ping = now - self._last_proactive_ping_at
        if since_last_ping < _OKX_PROACTIVE_PING_IDLE_SEC:
            return False
        try:
            await self._ws.send_str("ping")
        except (ConnectionResetError, aiohttp.ClientError) as exc:
            logger.warning("ws_client[%s] 主动发 ping 失败: %r", self.trade_type, exc)
            return False
        # 仅更新出站 ping 节流时钟；绝不触碰入站存活时钟 _last_message_at——
        # 否则半开连接（send 成功但无入站帧）会因每轮重置而永远检测不到死连接。
        self._last_proactive_ping_at = time.monotonic()
        return True

    async def _receive_until_disconnect(self) -> bool:
        """单条连接的接收循环。断连返回 True，收到 stop 返回 False。"""
        assert self._ws is not None
        ws = self._ws
        # OKX 应用层心跳模式下用更短的 recv 超时，以便在 _heartbeat_timeout_sec 死判定
        # 之前还能主动发 text "ping"；Binance 走 autoping，沿用原 _heartbeat_timeout_sec。
        if self._heartbeat_mode == HEARTBEAT_TEXT:
            recv_timeout = min(_OKX_PROACTIVE_PING_IDLE_SEC, self._heartbeat_timeout_sec)
        else:
            recv_timeout = self._heartbeat_timeout_sec
        while not self._stop_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=recv_timeout)
            except asyncio.TimeoutError:
                # 接收超时窗口到。OKX 模式下先看是否到了真正的死判定阈值；没到就主动发
                # 一帧 text "ping" 并继续等（保活）。Binance 模式 recv_timeout 即死判定阈值。
                if self._heartbeat_mode == HEARTBEAT_TEXT:
                    idle = time.monotonic() - self._last_message_at
                    if idle < self._heartbeat_timeout_sec:
                        await self._maybe_send_proactive_heartbeat()
                        continue
                # 连续这么久没收到任何帧（含服务器 ping）→ 视为连接死。
                logger.warning("ws_client[%s] 接收超时 %ss 无任何帧 → 重连",
                               self.trade_type, self._heartbeat_timeout_sec)
                return True
            except (aiohttp.ClientError, ConnectionResetError) as exc:
                logger.warning("ws_client[%s] 接收异常 → 重连: %r", self.trade_type, exc)
                return True

            if msg.type == aiohttp.WSMsgType.TEXT:
                # OKX 应用层心跳的 "ping" 帧不是 JSON kline，单独消费、不进解析路径。
                if await self._maybe_handle_heartbeat_text(msg.data):
                    continue
                self._last_message_at = time.monotonic()
                self._handle_text_message(msg.data)
            elif msg.type == aiohttp.WSMsgType.BINARY:
                # binance combined-stream 是 TEXT 上的 JSON；忽略意外的 binary。
                self._last_message_at = time.monotonic()
            elif msg.type in (
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.ERROR,
            ):
                # 记录断开原因，便于排查是 binance 主动关（看 close_code）还是异常。
                logger.warning(
                    "ws_client[%s] 连接关闭 type=%s close_code=%s exc=%r → 重连",
                    self.trade_type, msg.type.name, ws.close_code, ws.exception(),
                )
                return True
            else:
                # PING / PONG / 其他控制帧：也算作活动，这样一个安静的 chunk（低交易量
                # symbol）在 binance 仍每 3 分钟发服务端 PING 时不会误判超时。
                self._last_message_at = time.monotonic()
        return False

    def _handle_text_message(self, data: str) -> None:
        """解析一个 TEXT 帧，把闭合 K 线转发给回调。

        解析交给交易所协议（parse_frame），本方法只按返回的 type 分发：ack 打日志、
        unclosed 周期性打可见性日志、parse_error 记数、closed 入回调、其余忽略。
        """
        payload = self._decode_json(data)
        if payload is None:
            return
        self._frames_total += 1

        parsed = self._protocol.parse_frame(payload, self.trade_type)
        kind = parsed["type"]

        if kind == "ack":
            logger.info(
                "ws_client[%s] SUBSCRIBE ack id=%s result=%s",
                self.trade_type, parsed.get("request_id"), parsed.get("result"),
            )
            return
        if kind == "parse_error":
            # 上游 kline schema 违例：记数让健康监控可见，但不崩循环。
            self.parse_errors += 1
            return
        if kind == "unclosed":
            # 周期性可见性：每 N 帧打一次，让用户在闭合 K 之间也能看到数据在流动。
            if self._frames_total % self._log_every_n_frames == 0:
                logger.info(
                    "ws_client[%s] frames cumulative=%d closed=%d (last unclosed=%s @ %s)",
                    self.trade_type, self._frames_total, self._closed_klines_total,
                    parsed.get("symbol"), parsed.get("begin_time_ms"),
                )
            return
        if kind != "closed":
            return  # "other"：非 kline 帧，忽略

        self._closed_klines_total += 1
        kline_payload = {
            "symbol": parsed["symbol"],
            "trade_type": self.trade_type,
            "kline": parsed["kline"],
        }
        self._log_closed_kline(kline_payload)

        try:
            self._on_closed_kline(kline_payload)
        except Exception:
            logger.exception("on_closed_kline callback raised for %s",
                             kline_payload.get("symbol"))

    def _decode_json(self, data: str) -> Optional[Any]:
        """把 TEXT 帧解析成 Python 对象；格式错误的 JSON 记数并丢弃。"""
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            self.parse_errors += 1
            return None

    def _log_closed_kline(self, kline_payload: dict) -> None:
        """闭合 K 线的首条 + 每 100 条里程碑可见性日志。"""
        if not self._first_kline_logged:
            self._first_kline_logged = True
            logger.info(
                "ws_client[%s] ✅ FIRST closed kline received: symbol=%s close=%s ts=%s",
                self.trade_type,
                kline_payload["symbol"],
                kline_payload["kline"]["close"],
                kline_payload["kline"]["begin_time_ms"],
            )
        elif self._closed_klines_total % 100 == 0:
            logger.info(
                "ws_client[%s] closed klines cumulative=%d (last=%s @ %s)",
                self.trade_type, self._closed_klines_total,
                kline_payload["symbol"],
                kline_payload["kline"]["begin_time_ms"],
            )

    async def _reconnect_loop(self) -> None:
        """带退避地重连直到成功或停止。成功后重新订阅。"""
        self._set_health("reconnecting")
        await self._close_ws_only()

        backoff = list(self._backoff_schedule_sec)
        idx = 0
        while not self._stop_event.is_set():
            self._reconnect_count += 1
            wait_sec = backoff[idx] if idx < len(backoff) else backoff[-1]
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=wait_sec
                )
                # 退避睡眠期间 stop_event 触发 —— 退出。
                return
            except asyncio.TimeoutError:
                pass  # 退避时间到，尝试重连

            try:
                assert self._session is not None
                self._ws = await self._session.ws_connect(
                    self.url, heartbeat=None, proxy=self._proxy,  # 不主动发 client ping；走与 REST 同一代理
                )
                self._last_message_at = time.monotonic()
            except (aiohttp.ClientError, ConnectionResetError, OSError) as exc:
                logger.warning("reconnect attempt %d failed: %s", self._reconnect_count, exc)
                idx += 1
                continue

            # 重新订阅此前订阅过的所有 stream。新连接上服务端从零开始，所以这一步
            # 必须在上报 'healthy' 之前完成。
            if self._subscribed_streams:
                streams = sorted(self._subscribed_streams)
                # 首次重订阅绕过节流 —— 这单个控制帧是状态恢复，不是新流量。
                payload = self._build_subscribe_payload("SUBSCRIBE", streams)
                try:
                    await self._ws.send_json(payload)
                    self._last_subscribe_send_at = time.monotonic()
                except (ConnectionResetError, aiohttp.ClientError):
                    # 新连接立刻又挂了 —— 再试。
                    idx += 1
                    continue

            self._set_health("healthy")
            return
