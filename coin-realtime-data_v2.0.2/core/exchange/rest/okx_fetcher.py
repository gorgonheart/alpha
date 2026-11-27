"""OKX 异步数据获取器（mirror feat/ws BinanceFetcher 接口）。

本模块对齐 ``core.exchange.rest.binance_fetcher.BinanceFetcher`` 的对外签名，便于上层
统一调度 Binance / OKX 两类交易所：

  - ``_pandas_parse_okx_kline`` 把 OKX 原始 K 线（9 列）解析为与
    ``BinanceFetcher.pandas_parse_kline`` 完全一致的 10 列 DataFrame。
  - ``OkxFetcher`` 封装 :class:`core.exchange.rest.okx_market.OkxMarketApi`，
    并用两个令牌桶（recent / history）对两类 K 线接口分别限速。

说明：从 okx 分支 ``core/fetcher/okx.py`` salvage 而来，已：
  - 用 ``trade_type`` 字符串（'spot' / 'swap'）替代 ``InstType`` 枚举；
  - 丢弃 ``Exchange`` 枚举与 ``SymbolInfo`` 依赖；
  - 重接到 feat/ws 的 ``core.utils.network.async_retry_getter``。
"""

import asyncio
from decimal import Decimal

import pandas as pd

from core.exchange.rest.okx_market import OkxMarketApi
from core.exchange.rate_limit import get_okx_shared_limiter
from core.utils.log_kit import logger
from core.utils.network import async_retry_getter, is_rate_limited

# OKX K 线接口单次返回上限
OKX_CANDLES_LIMIT_CAP = 300

# 调用方未传 limit 时，OKX K 线接口默认返回 100 条
OKX_CANDLES_DEFAULT_LIMIT = 100

# OKX funding-rate-history 单次返回上限
OKX_FUNDING_HISTORY_LIMIT_CAP = 400

# “历史 vs 最近” 端点切换阈值：游标早于此时长（约 3 天）走 history-candles
HISTORY_CUTOFF_MS = 3 * 24 * 60 * 60 * 1000


def _interval_to_milliseconds(interval: str) -> int:
    """将分钟、小时或天粒度的 K 线周期换算为毫秒。

    Args:
        interval: 例如 ``5m``、``1h``、``1d`` 的标准 K 线周期。

    Returns:
        int: 一个 K 线周期对应的毫秒数。

    Raises:
        ValueError: interval 不是受支持的“正整数 + m/h/d”格式。
    """
    if (
        not isinstance(interval, str)
        or len(interval) < 2
        or not interval[:-1].isdigit()
        or int(interval[:-1]) <= 0
    ):
        raise ValueError(f"unsupported candle interval: {interval!r}")

    unit_ms = {"m": 60_000, "h": 3_600_000, "d": 86_400_000}
    unit = interval[-1].lower()
    if unit not in unit_ms:
        raise ValueError(f"unsupported candle interval: {interval!r}")
    return int(interval[:-1]) * unit_ms[unit]


def _decimal_or_default(value, default: str) -> Decimal:
    if value in (None, ""):
        return Decimal(default)
    return Decimal(str(value))


def _pandas_parse_okx_kline(trade_type: str, klines) -> pd.DataFrame:
    """将 OKX 的 K 线数据解析为与 BinanceFetcher 一致的 10 列 DataFrame。

    Args:
        trade_type: 'spot' 或 'swap'。
        klines: OKX K 线二维数组，每行
            ``[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]``。

    Returns:
        10 列 DataFrame，列顺序与 ``BinanceFetcher.pandas_parse_kline`` 输出一致：
        candle_begin_time, open, high, low, close, volume, quote_volume,
        trade_num, taker_buy_base_asset_volume, taker_buy_quote_asset_volume。
    """
    # OKX 原始字段列名
    columns = [
        "candle_begin_time",  # ts: 开始时间（ms）
        "open",  # o: 开盘价
        "high",  # h: 最高价
        "low",  # l: 最低价
        "close",  # c: 收盘价
        "vol",  # vol: 交易量（张数或交易货币数量）
        "volCcy",  # volCcy: 交易量（币为单位）
        "volCcyQuote",  # volCcyQuote: 交易量（计价货币为单位）
        "confirm",  # confirm: K 线状态
    ]

    df = pd.DataFrame(klines, columns=columns)

    # 仅保留已闭合 K（confirm=='1'），丢弃形成中 bar（confirm=='0'）。
    # OKX 在 recent-fill 路径下最新一根可能是 confirm=='0' 的形成中 bar；
    # Binance 无此标志、靠 run_time 边界过滤，OKX 既然给了 confirm 就直接用它，
    # 杜绝把未闭合 bar 当作已闭合写入 base。历史/回填行均为 confirm=='1'，不受影响。
    df = df[df["confirm"].astype(str) == "1"].copy()

    # 根据 trade_type 映射 volume / quote_volume
    if trade_type == "spot":
        # 现货：volume <- vol, quote_volume <- volCcyQuote
        rename_dict = {"vol": "volume", "volCcyQuote": "quote_volume"}
        df.drop(columns=["volCcy", "confirm"], inplace=True)
    else:
        # 合约（swap）：volume <- volCcy, quote_volume <- volCcyQuote
        rename_dict = {"volCcy": "volume", "volCcyQuote": "quote_volume"}
        df.drop(columns=["vol", "confirm"], inplace=True)

    df.rename(columns=rename_dict, inplace=True)

    dtypes = {
        "candle_begin_time": int,
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "quote_volume": float,
    }
    df = df.astype(dtypes)

    # ts(ms) -> UTC datetime64
    df["candle_begin_time"] = pd.to_datetime(df["candle_begin_time"], unit="ms", utc=True)

    # 去重 + 排序
    df.drop_duplicates("candle_begin_time", inplace=True)
    df.sort_values("candle_begin_time", inplace=True, ignore_index=True)

    # 补齐到 10 列：Binance 有而 OKX 没有的字段填 0
    df["trade_num"] = 0
    df["taker_buy_base_asset_volume"] = 0.0
    df["taker_buy_quote_asset_volume"] = 0.0

    # 与 BinanceFetcher.pandas_parse_kline 完全一致的列顺序
    df = df[[
        "candle_begin_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trade_num",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
    ]]
    return df


class OkxFetcher:
    """OKX 数据获取器，对齐 BinanceFetcher 的对外签名。"""

    # 429 快速失败：True 时 get_candle 遇到 429 不在原地退避重试，直接抛出，交给调用方处理。
    # 仅历史下载按实例开启（其下载循环会把该 symbol 排到队尾、整批 429 时整体冷却），
    # 避免一次 5–10s 的退避拖住同一轮的其他 symbol；实时 REST 兜底保持默认的退避重试。
    fail_fast_on_rate_limit: bool = False

    # interval(标准) -> OKX bar 粒度
    _INTERVAL_MAP = {
        "5m": "5m",
        "1h": "1H",
        "15m": "15m",
        "30m": "30m",
        "1d": "1D",
    }

    def __init__(self, trade_type, session, proxy=None):
        if trade_type not in ("spot", "swap"):
            raise ValueError(f"Type {trade_type} not supported")
        self.trade_type = trade_type
        self.market_api = OkxMarketApi(session, proxy)
        # 进程级共享限速器：spot / swap 两个 OkxFetcher（冷启动时跑在不同线程、各持
        # 独立 asyncio loop）对同一端点共享同一滑动窗口，杜绝同一 IP 上请求速率翻倍。
        # /market/candles 40 次/2s；/market/history-candles 20 次/2s（本地留余量，见 rate_limit）。
        # 严格滑动窗口，杜绝初始装满导致的首窗突发。
        self.recent_bucket = get_okx_shared_limiter("recent")
        self.history_bucket = get_okx_shared_limiter("history")
        # 公共端点（instruments / funding）共享的保守限速窗口（10/2s）。这些端点此前
        # 直接走 async_retry_getter、完全绕过限速，与 candle 扇出叠加易触 429。
        self.public_bucket = get_okx_shared_limiter("public")

    def batch_size(self) -> int:
        """单批并发请求的交易对数量上限。"""
        return 20

    def once_candles(self) -> int:
        """单次 K 线请求的最大返回条数。"""
        return 300

    async def acquire(self, n, *, history=False) -> None:
        """循环级限速钩子——对 OKX 为 no-op（不申请任何 token）。

        OKX 的限速已下沉到 ``get_candle`` 内部，按实际选中的端点
        （recent / history）逐请求申请 token。共享下载循环
        ``core/history_data/candle.py`` 每轮仅调用一次本方法，
        若此处再申请会与 per-request 限速重复门控（double-gate），
        故此处直接返回。Binance 的 acquire 仍在循环级按权重门控，不受影响。
        """
        return

    async def get_exchange_info(self) -> dict[str, dict]:
        """获取交易规则信息，shape 兼容 BinanceFetcher.get_exchange_info。

        Returns:
            dict，key 为 instId，value 含 ``symbol`` / ``status`` /
            ``base_asset`` / ``quote_asset`` / ``instType`` / ``ct_type`` 等字段。
            ``ct_type`` 为原始 OKX ``ctType``（swap/futures 为 'linear'/'inverse'，
            现货为 ''）。
        """
        inst_type = "SPOT" if self.trade_type == "spot" else "SWAP"
        # 公共端点限速：与 funding 共享 'public' 窗口，避免绕过限速触发 429。
        await self.public_bucket.acquire(1)
        api_data = await async_retry_getter(self.market_api.aioreq_instruments, instType=inst_type)
        results: dict[str, dict] = {}
        if not api_data:
            return results
        for inst in api_data:
            inst_id = inst["instId"]
            price_tick = _decimal_or_default(inst.get("tickSz"), "0.01")
            lot_sz = _decimal_or_default(inst.get("lotSz"), "0.001")
            if self.trade_type == "spot":
                base_asset = inst.get("baseCcy")
                quote_asset = inst.get("quoteCcy")
                lot_size = lot_sz
            else:
                base_asset = inst.get("ctValCcy")
                quote_asset = inst.get("settleCcy")
                # OKX swap lotSz 是张数步长；仓管模拟器需要“一手对应的币数量”。
                lot_size = lot_sz * _decimal_or_default(inst.get("ctVal"), "1") * _decimal_or_default(
                    inst.get("ctMult"), "1"
                )
            # ctType 仅对 swap / futures 有意义（'linear' / 'inverse'）；现货为 ''
            ct_type = "" if self.trade_type == "spot" else inst.get("ctType", "")
            results[inst_id] = {
                "symbol": inst_id,
                "status": inst.get("state"),
                "base_asset": base_asset,
                "quote_asset": quote_asset,
                "instType": inst.get("instType", inst_type),
                "ct_type": ct_type,
                "price_tick": price_tick,
                "lot_size": lot_size,
                # OKX instruments 无静态最小名义价值字段；下游当前只要求列存在。
                "min_notional_value": Decimal("0"),
            }
        return results

    @staticmethod
    def _is_history_cursor(before, after) -> bool:
        """根据查询游标判断是否应走 history-candles 端点。

        游标早于约 3 天则视为历史区间。两端都给时，由更旧的边界（更小的 Unix ms
        值 = 更靠过去）决定，即 ``min(before, after)``；只给一端则用该端；都没有
        则返回 False（走 recent）。
        """
        if before is not None and after is not None:
            cursor = min(int(before), int(after))
        elif after is not None:
            cursor = after
        elif before is not None:
            cursor = before
        else:
            return False
        # 端点切换基于墙钟毫秒（与游标的 Unix ms 时间戳同基准）；
        # asyncio loop.time() 是单调时钟、非墙钟，不能用于此判断。
        now_ms = pd.Timestamp.utcnow().value // 1_000_000
        return (now_ms - int(cursor)) > HISTORY_CUTOFF_MS

    async def get_candle(self, symbol, interval, **kwargs) -> pd.DataFrame:
        """获取指定交易对的 K 线，映射 Binance 风格参数到 OKX。

        参数映射：
          - ``startTime`` -> 一页宽度的排他 ``before/after`` 区间
          - ``endTime``   -> ``after``（请求此时间戳之前的更旧数据）
          - ``limit``     -> ``min(limit, 300)``

        端点选择：游标早于约 3 天走 history-candles，否则走 candles。

        Args:
            symbol: OKX instId，例如 ``BTC-USDT``。
            interval: Binance 风格的 K 线周期，例如 ``5m``、``1h``。
            **kwargs: 可选的 ``startTime``、``endTime``、``limit`` 及其它
                OKX K 线请求参数。

        Returns:
            pd.DataFrame: 与 Binance Fetcher 对齐的 10 列已闭合 K 线。

        Raises:
            ValueError: 带 startTime 时 interval 无法换算为页宽。
        """
        bar = self._INTERVAL_MAP.get(interval, interval)

        params = dict(kwargs)
        start_time = params.pop("startTime", None)
        end_time = params.pop("endTime", None)
        limit = params.pop("limit", None)
        page_limit = (
            min(int(limit), OKX_CANDLES_LIMIT_CAP)
            if limit is not None
            else OKX_CANDLES_DEFAULT_LIMIT
        )

        before = None
        after = None
        if start_time is not None:
            start_time = int(start_time)
            # OKX 的 before/after 都是排他游标。下界减 1 ms 可包含 startTime
            # 对应的边界 K；上界限制为一页宽度，避免单独 before 跳到最新页。
            before = start_time - 1
            page_upper = start_time + page_limit * _interval_to_milliseconds(
                interval
            )
            after = (
                min(int(end_time), page_upper)
                if end_time is not None
                else page_upper
            )
        elif end_time is not None:
            # endTime-only 是阶段 3 向过去回溯的原有路径，保持 after 映射不变。
            after = int(end_time)

        req = dict(params)
        req["instId"] = symbol
        req["bar"] = bar
        if before is not None:
            req["before"] = before
        if after is not None:
            req["after"] = after
        if limit is not None:
            req["limit"] = page_limit

        history = self._is_history_cursor(before, after)

        # per-request 限速：按实际选中的端点向对应桶申请 1 个 token。
        # （循环级 acquire 已设为 no-op，故此处是唯一限速点，不会 double-gate。）
        await (self.history_bucket if history else self.recent_bucket).acquire(1)

        if self.fail_fast_on_rate_limit:
            try:
                data = await self.market_api.aioreq_candles(history=history, **req)
            except Exception as e:
                if is_rate_limited(e):
                    raise
                # 非 429（超时 / 网络抖动等）仍按原逻辑退避重试
                data = await async_retry_getter(
                    self.market_api.aioreq_candles, history=history, **req
                )
        else:
            data = await async_retry_getter(
                self.market_api.aioreq_candles, history=history, **req
            )
        # 空结果也走 parser，返回带完整 10 列 schema 的空 DataFrame（与
        # BinanceFetcher.pandas_parse_kline 契约一致）。否则下游
        # df["candle_begin_time"] 会 KeyError——例如新上市标的在 first_begin_time
        # 之前已无更早历史，阶段3 回填会拉回空。
        return _pandas_parse_okx_kline(self.trade_type, data or [])

    async def batch_get_candle(self, params_list: list[dict]) -> list[pd.DataFrame]:
        """批量获取多个交易对的 K 线，返回顺序与 params_list 一致。

        异常隔离（BUG2 cascade 修复）：单个交易对的请求在 ``async_retry_getter``
        重试耗尽后抛出的异常（如 429 / timeout）**绝不**向外传播——否则会穿透
        调用方 ``async with create_aiohttp_session()``，导致 session 在 ~670 个兄弟
        协程仍在飞行时被关闭，引发 getaddrinfo NoneType / Session closed 级联。

        故此处 ``asyncio.gather(..., return_exceptions=True)``，逐位置对齐：失败位
        记 ``logger.warning`` 并替换为空 DataFrame，返回列表长度恒等于
        ``len(params_list)``。
        """
        tasks = [
            self.get_candle(
                p["symbol"],
                p["interval"],
                **{k: v for k, v in p.items() if k not in ("symbol", "interval")},
            )
            for p in params_list
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        out: list[pd.DataFrame] = []
        for params, result in zip(params_list, results):
            if isinstance(result, Exception):
                logger.warning(
                    f"OKX batch_get_candle 单交易对失败已隔离 "
                    f"symbol={params.get('symbol')} {result!r}"
                )
                # 占位空 df 同样保持完整 10 列 schema（与 BinanceFetcher 契约一致），
                # 喂给 fetch_update_resample 的 df["candle_begin_time"] 不会 KeyError。
                out.append(_pandas_parse_okx_kline(self.trade_type, []))
            else:
                out.append(result)
        return out

    async def get_realtime_funding_rate(self) -> pd.DataFrame:
        """获取并解析实时资金费率（mirror BinanceFetcher.get_realtime_funding_rate）。

        OKX ``/public/funding-rate`` 支持 ``instId='ANY'`` 一次返回全部永续合约的
        资金费率，无需逐个 instId 枚举。

        Returns:
            DataFrame，列为 ``symbol`` / ``funding_rate`` / ``next_funding_time``，
            其中 ``symbol`` 即 OKX 的 ``instId``（如 BTC-USDT-SWAP），绝不映射成
            BTCUSDT。``next_funding_time`` 取自 OKX ``fundingTime``（即将结算的时间），
            不是 OKX ``nextFundingTime``（再下一期）。空数据返回空 DataFrame。

        Raises:
            RuntimeError: 当 trade_type == 'spot' 时（现货无资金费）。
        """
        if self.trade_type == "spot":
            raise RuntimeError("Cannot request funding rate for spot")

        # 公共端点限速：与 instruments 共享 'public' 窗口，避免绕过限速触发 429。
        await self.public_bucket.acquire(1)
        # 获取所有永续合约的资金费率
        data = await async_retry_getter(self.market_api.aioreq_funding_rate, instId="ANY")
        if not data:
            return pd.DataFrame()

        # 1. 直接将原始数据加载到 DataFrame
        df = pd.DataFrame(data)

        # 2. 安全转换 fundingRate 与 fundingTime
        # OKX 的 fundingTime 才是 fundingRate 对应的「即将结算」时间（等价于 Binance 的
        # nextFundingTime）；OKX 的 nextFundingTime 是再下一期，用它会让费率晚一期落库。
        df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        next_funding_ms = pd.to_numeric(df["fundingTime"], errors="coerce")

        # 3. 在时间转换之前先过滤掉缺失值，再进行 astype(int)
        mask = next_funding_ms.notna()
        df = df.loc[mask].copy()
        df["next_funding_time"] = pd.to_datetime(next_funding_ms[mask].astype("int64"), unit="ms", utc=True)

        # 4. symbol 即 instId，并选择所需的列
        df["symbol"] = df["instId"]
        return df[["symbol", "funding_rate", "next_funding_time"]].copy()

    async def get_hist_funding_rate(self, symbol, **kwargs) -> pd.DataFrame:
        """获取并解析历史资金费率（mirror BinanceFetcher.get_hist_funding_rate）。

        参数映射（与 ``get_candle`` 一致，把 Binance 风格参数翻译到 OKX）：
          - ``startTime`` -> ``before``（请求此时间戳之后的更新数据）
          - ``endTime``   -> ``after``（请求此时间戳之前的更旧数据）
          - ``limit``     -> ``min(limit, 400)``

        若调用方直接传 OKX 原生 ``before`` / ``after`` / ``limit``，则原样透传
        （仅对 ``limit`` 做 400 上限裁剪）。

        Args:
            symbol: OKX instId，如 BTC-USDT-SWAP。
            **kwargs: Binance 风格分页参数（``startTime`` / ``endTime`` / ``limit``）
                或 OKX 原生参数（``before`` / ``after`` / ``limit``，limit 最大 400）。

        Returns:
            DataFrame，列为 ``symbol`` / ``funding_rate`` / ``candle_begin_time``，
            其中 ``symbol`` 即 instId，``candle_begin_time`` 由 OKX ``fundingTime``(ms)
            舍入到最近整小时（UTC），与 BinanceFetcher 对齐；按 ``candle_begin_time``
            升序排序（OKX 返回最新在前），使调用方 max(candle_begin_time)+1h 分页
            能正确前进。空数据返回空 DataFrame。

        Raises:
            RuntimeError: 当 trade_type == 'spot' 时（现货无资金费）。
        """
        if self.trade_type == "spot":
            raise RuntimeError("Cannot request funding rate for spot")

        # Binance 风格参数 -> OKX 参数（与 get_candle 同一套翻译规则）。
        params = dict(kwargs)
        before = params.pop("startTime", params.pop("before", None))
        after = params.pop("endTime", params.pop("after", None))
        limit = params.pop("limit", None)

        req = dict(params)
        req["instId"] = symbol
        if before is not None:
            req["before"] = before
        if after is not None:
            req["after"] = after
        if limit is not None:
            req["limit"] = min(int(limit), OKX_FUNDING_HISTORY_LIMIT_CAP)

        # 公共端点限速：与 instruments / 实时 funding 共享 'public' 窗口，避免触发 429。
        await self.public_bucket.acquire(1)
        data = await async_retry_getter(
            self.market_api.aioreq_funding_rate_history, **req
        )
        if not data:
            return pd.DataFrame()

        # 1. 直接将原始数据加载到 DataFrame
        df = pd.DataFrame(data)

        # 2. 安全转换 fundingRate 与 fundingTime
        df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
        funding_ms = pd.to_numeric(df["fundingTime"], errors="coerce")

        # 3. 在时间转换之前先过滤掉缺失值，再进行 astype(int)
        mask = funding_ms.notna()
        df = df.loc[mask].copy()
        funding_time = pd.to_datetime(funding_ms[mask].astype("int64"), unit="ms", utc=True)
        df["candle_begin_time"] = funding_time.dt.round("h")  # 舍入到最近整小时

        # 4. symbol 即 instId，选择列并排序、重置索引
        df["symbol"] = df["instId"]
        df = df[["symbol", "funding_rate", "candle_begin_time"]].sort_values(
            "candle_begin_time"
        ).reset_index(drop=True)
        return df
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
