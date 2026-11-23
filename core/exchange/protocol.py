"""交易所协议适配层。

把"和某个交易所 WS 打交道"的全部差异收敛到一个类里：WS URL 路由、订阅帧格式、
帧解析（ack 识别 / 拆包 / kline schema / 闭合标志）、stream 命名、单连接 stream 上限。
ws_kline_client / ws_runner / runtime_supervisor 只依赖这个接口，代码里不再出现任何
交易所特定字段。

下个月接入 OKX 时，新增一个并列的 OkxProtocol 类（同名方法）即可，无需改 ws 层。

实例无状态（只有类属性 + 纯方法），可随子进程 spawn 一起 pickle。
"""
from __future__ import annotations

from typing import Any, Protocol


HEARTBEAT_NONE = "none"
HEARTBEAT_TEXT = "okx_text"


class ExchangeProtocol(Protocol):
    """交易所 WS 协议的结构化类型（typing.Protocol）。

    BinanceProtocol / OkxProtocol 结构上满足本协议；ws_kline_client / ws_runner /
    runtime_supervisor 的类型注解用它而非某个具体类，表达"承载任意交易所协议"，
    使新增交易所只需新写一个同构 protocol 类、无需改 ws 层注解。
    """

    name: str
    max_streams_per_conn: int
    heartbeat_mode: str

    def ws_url(self, trade_type: str, chunk_idx: int) -> str: ...

    def stream_name(self, symbol: str, interval: str) -> str: ...

    def build_subscribe(self, method: str, streams: list, request_id: int) -> dict: ...

    def parse_frame(self, payload: Any, trade_type: str) -> dict: ...


class BinanceProtocol:
    """Binance combined-stream WS 协议。"""

    name = "binance"
    heartbeat_mode = HEARTBEAT_NONE
    # Spot: `/ws` + JSON-RPC SUBSCRIBE（所有 chunk 同一 URL，无占位符）。
    spot_url = "wss://stream.binance.com:9443/ws"
    # Futures: 必须用 routed endpoint `/market/ws/{chunk_idx}` 才能拿到 market(kline) 帧；
    # {chunk_idx} 是 streamIndex 整数。
    swap_url = "wss://fstream.binance.com/market/ws/{chunk_idx}"
    # 单连接订阅的 stream 上限。经实测 ~524 streams 时 binance 在 5m 边界不下发
    # closed kline burst；沿用 ccxt 默认的保守值 200。
    max_streams_per_conn = 200

    def ws_url(self, trade_type: str, chunk_idx: int) -> str:
        """按 trade_type + chunk 序号给出该连接的 WS URL。"""
        template = self.spot_url if trade_type == "spot" else self.swap_url
        try:
            return template.format(chunk_idx=chunk_idx)
        except (KeyError, IndexError):
            return template

    def stream_name(self, symbol: str, interval: str) -> str:
        """binance combined-stream 的 stream 名，如 `btcusdt@kline_5m`。"""
        return f"{symbol.lower()}@kline_{interval}"

    def build_subscribe(self, method: str, streams: list, request_id: int) -> dict:
        """构造 SUBSCRIBE / UNSUBSCRIBE 控制帧（binance JSON-RPC）。"""
        return {"method": method, "params": list(streams), "id": request_id}

    def parse_frame(self, payload: Any, trade_type: str) -> dict:
        """把一个已解析的 JSON 帧分类，返回下列之一：

          {"type": "ack", "request_id": Any, "result": Any}
              SUBSCRIBE/UNSUBSCRIBE 确认帧。
          {"type": "closed", "symbol": str, "kline": {...10 字段...}}
              一根闭合 K 线，kline 子 dict 用 ws_local_cache schema。
          {"type": "unclosed", "symbol": str, "begin_time_ms": int}
              未闭合 K 线（仅供周期性可见性日志）。
          {"type": "parse_error"}
              是闭合 K 线但字段 schema 违例（调用方计 parse_errors）。
          {"type": "other"}
              其他帧（非 kline / 结构不符），忽略。
        """
        # ack: {"result": null, "id": N}
        if isinstance(payload, dict) and "result" in payload and "id" in payload:
            return {"type": "ack", "request_id": payload.get("id"), "result": payload.get("result")}

        # /market/ws/<N> 与 /ws 发的是 raw 帧；/stream 包成 {"stream":..., "data":...}。
        # 两种都支持：先试包裹，回退到 raw。
        if isinstance(payload, dict) and "data" in payload:
            frame = payload.get("data")
        else:
            frame = payload

        if not isinstance(frame, dict) or frame.get("e") != "kline":
            return {"type": "other"}
        k = frame.get("k")
        if not isinstance(k, dict):
            return {"type": "other"}

        if not k.get("x"):
            return {"type": "unclosed", "symbol": frame.get("s"), "begin_time_ms": k.get("t")}

        try:
            kline = {
                "begin_time_ms": int(k["t"]),
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "quote_volume": float(k["q"]),
                "trade_num": int(k["n"]),
                "taker_buy_base": float(k["V"]),
                "taker_buy_quote": float(k["Q"]),
            }
            symbol = frame["s"]
        except (KeyError, TypeError, ValueError):
            return {"type": "parse_error"}
        return {"type": "closed", "symbol": symbol, "kline": kline}


class OkxProtocol:
    """OKX v5 WS candle 协议（business 频道）。

    与 BinanceProtocol 同名方法、同返回契约，使 ws 层无需感知交易所差异。

    关键差异：
      - 单一 business 端点承载所有 candle 频道（spot/swap 都走同一 URL，忽略 chunk_idx）。
      - 订阅控制帧用 {"op": "subscribe"/"unsubscribe", "args": [{"channel","instId"}]}，无 id。
      - 推送帧的 candle data 是数组：[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]，
        confirm=='1' 表示该 bar 已闭合，confirm=='0' 为形成中。
      - instId（BTC-USDT / BTC-USDT-SWAP）原样作为 symbol 透传，绝不反向映射成 BTCUSDT。
      - spot 用 vol（成交张/币数）作 volume；swap 用 volCcy（成交币数）作 volume；
        两者 quote_volume 都取 volCcyQuote。trade_num / taker_buy_* OKX 不提供，补零。
    """

    name = "okx"
    # 心跳模型：OKX 用应用层 text 帧 "ping"/"pong"（而非 WS 控制帧 PING/PONG）。
    # ws_kline_client 据此：收到 text "ping" 回 text "pong"；空闲 ~20s 主动发 text "ping"。
    heartbeat_mode = HEARTBEAT_TEXT
    # OKX candle 推送在 business 频道（探针 scripts/okx_probe.py 已实测可用）。
    business_url = "wss://ws.okx.com:8443/ws/v5/business"
    # 单连接 stream 上限（保守值，留心跳/控制帧余量）。
    max_streams_per_conn = 120

    def ws_url(self, trade_type: str, chunk_idx: int) -> str:
        """所有 trade_type / chunk 都走同一 business 端点。"""
        return self.business_url

    def stream_name(self, symbol: str, interval: str) -> str:
        """OKX stream 名约定，如 `BTC-USDT@candle5m`（instId 大小写原样）。"""
        return f"{symbol}@candle{interval}"

    def build_subscribe(self, method: str, streams: list, request_id: int) -> dict:
        """构造 subscribe / unsubscribe 控制帧（OKX op/args 结构，无 id）。

        每个 stream 形如 `INSTID@candleINTERVAL`，拆成
        {"channel": "candleINTERVAL", "instId": "INSTID"}。
        """
        args = []
        for stream in streams:
            inst_id, _, candle = stream.partition("@")
            args.append({"channel": candle, "instId": inst_id})
        return {"op": method.lower(), "args": args}

    def parse_frame(self, payload: Any, trade_type: str) -> dict:
        """把一个已解析的 OKX JSON 帧分类，返回与 BinanceProtocol 相同的契约：

          {"type": "ack", "request_id": None, "result": arg}
              subscribe / unsubscribe 确认帧（OKX 不回 id，故 request_id=None）。
          {"type": "other"}
              error 事件或非 candle / 结构不符帧。
          {"type": "closed", "symbol": str, "kline": {...10 字段...}}
              confirm=='1' 的闭合 K，kline 子 dict 用 ws_local_cache schema。
          {"type": "unclosed", "symbol": str, "begin_time_ms": int}
              confirm=='0' 的形成中 K。
          {"type": "parse_error"}
              candle 数组字段转换失败。
        """
        if not isinstance(payload, dict):
            return {"type": "other"}

        event = payload.get("event")
        if event in ("subscribe", "unsubscribe"):
            return {"type": "ack", "request_id": None, "result": payload.get("arg")}
        if event == "error":
            return {"type": "other"}

        arg = payload.get("arg")
        data = payload.get("data")
        if not isinstance(arg, dict) or not isinstance(data, list):
            return {"type": "other"}
        symbol = arg.get("instId")
        if symbol is None:
            return {"type": "other"}

        # 一个 data 数组可能同时含多根 bar（如 [闭合, 形成中]，顺序不定）。
        # 策略：prefer-closed、顺序无关、单坏行不丢整帧。
        #   - 跨所有行各自累积：最新的 closed 候选 / 第一个 unclosed 候选 / 是否见过坏行。
        #   - 任何坏行（非 list/tuple、len<9、或数值转换失败）只置 saw_parse_error
        #     并 continue，绝不丢弃已解析出的合法 bar。
        #   - 返回优先级：closed > unclosed > parse_error > other。
        closed_candidate: dict | None = None
        unclosed_candidate: dict | None = None
        saw_parse_error = False
        for row in data:
            if not isinstance(row, (list, tuple)) or len(row) < 9:
                saw_parse_error = True
                continue
            confirm = row[8]
            if str(confirm) == "0":
                # 形成中：保留第一个 unclosed 候选。
                if unclosed_candidate is None:
                    try:
                        begin_time_ms = int(row[0])
                    except (TypeError, ValueError):
                        saw_parse_error = True
                        continue
                    unclosed_candidate = {
                        "type": "unclosed",
                        "symbol": symbol,
                        "begin_time_ms": begin_time_ms,
                    }
                continue
            if str(confirm) != "1":
                # 其他 confirm 值：忽略该行。
                continue
            # confirm == '1'：闭合 K。
            try:
                if trade_type == "swap":
                    volume = float(row[6])  # volCcy
                else:
                    volume = float(row[5])  # vol
                kline = {
                    "begin_time_ms": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": volume,
                    "quote_volume": float(row[7]),  # volCcyQuote
                    "trade_num": 0,
                    "taker_buy_base": 0.0,
                    "taker_buy_quote": 0.0,
                }
            except (TypeError, ValueError, IndexError):
                saw_parse_error = True
                continue
            # 保留 begin_time_ms 最大（最新）的 closed 候选，与数组顺序无关。
            if (
                closed_candidate is None
                or kline["begin_time_ms"] > closed_candidate["kline"]["begin_time_ms"]
            ):
                closed_candidate = {"type": "closed", "symbol": symbol, "kline": kline}

        if closed_candidate is not None:
            return closed_candidate
        if unclosed_candidate is not None:
            return unclosed_candidate
        if saw_parse_error:
            return {"type": "parse_error"}
        return {"type": "other"}
