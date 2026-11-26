"""交易所适配层 + 注册表（多交易所架构核心）。

把"一个交易所与系统其余部分的全部差异"收敛到一个 :class:`ExchangeAdapter`
对象，把 ``exchange 字符串 -> Adapter`` 收敛成一次查表（:func:`get_adapter`）。
每个历史散落的 if/elif dispatch 点（fetcher 构造 / meta 更新 / spot-swap 配对 /
WS 协议 / 展示名 / 限流策略）映射到 adapter 的一个方法。

加一个新交易所 = 在 ``core/adapters/`` 下实现一个 ExchangeAdapter 子类 + 在模块
尾部 ``register_exchange(...)`` 一行，无需改任何现有 dispatch 点。

adapter 无状态（只有类属性 + 纯方法 / 委派），可随子进程 spawn 一起 pickle。
"""
from __future__ import annotations

from abc import ABC, abstractmethod


class _NoopLimiter:
    """默认限流器：不限流。

    与 :mod:`core.exchange.rate_limit` 的限流器同契约（``async def acquire(n=1)``），使
    ``adapter.get_limiter(...)`` 的返回值可被调用方无差别 ``await``。Binance 的请求
    门控在 fetcher 循环级按 weight 自理，故默认 no-op。
    """

    async def acquire(self, n: int = 1) -> None:
        return None


_NOOP_LIMITER = _NoopLimiter()


class ExchangeAdapter(ABC):
    """一个交易所接入系统所需的全部差异化行为的契约。

    每个方法精确对应当前一处散落的 if/elif dispatch 点。除这些方法外，交易所之间
    无差异——核心管线（writer / supervisor / barrier / resample / preprocess /
    candle_manager / ts_manager）对所有交易所一字不改地复用。
    """

    #: 注册键，等于 ``handle.exchange``（如 'binance' / 'okx'）。
    name: str = ""
    #: 用户可见展示名（如 '币安' / 'OKX'），吸收 ``handle.exchange_display_name``。
    display_name: str = ""

    @abstractmethod
    def create_handle(self, cfg):
        """从 cfg 构造该交易所的 BmacHandle（委派 ``create_*_handle``）。"""

    @abstractmethod
    def create_fetcher(self, handle, trade_type, session):
        """构造该交易所的 Fetcher。

        负责把统一的 ``trade_type``（'spot'/'swap'）翻译成本交易所的 Fetcher 入参：
        OKX 直接用 ``trade_type``；Binance 经
        ``handle.get_market_handle(trade_type).api_type``。
        """

    @abstractmethod
    def match_spot_swap(self, spot_symbols, swap_symbols):
        """返回 spot<->swap 配对表（``pd.DataFrame``，列 'spot'/'swap'）。"""

    async def update_meta(self, handle, run_time, *, include_realtime_funding: bool = True):
        """刷新 exginfo_spot / exginfo_swap（+ 可选 funding）。

        默认实现对所有交易所通用（委派 ``core.pipeline.data_update.update_exchange_meta``，
        exginfo / 资金费均经 ``create_fetcher`` 按 ``handle.exchange`` 分派）。新增交易所
        无需实现本方法；仅当某交易所 meta 流程真正分叉时才 override。
        """
        from core.pipeline.data_update import update_exchange_meta  # 惰性导入规避循环

        if include_realtime_funding:
            await update_exchange_meta(handle, run_time)
        else:
            await update_exchange_meta(handle, run_time, include_realtime_funding=False)

    @abstractmethod
    def create_protocol(self):
        """返回 WS 协议实例（其 ``heartbeat_mode`` 驱动 ws_kline_client 心跳）。"""

    def get_limiter(self, endpoint: str):
        """返回该 endpoint 的限流器（``async def acquire(n=1)`` 契约）。

        默认 no-op（无限流）；按滑动窗口 / 令牌桶限流的交易所（如 OKX）override。
        """
        return _NOOP_LIMITER


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------
_REGISTRY: dict[str, ExchangeAdapter] = {}
_UNSET = object()
_BOOTSTRAPPED = False


def register_exchange(adapter: ExchangeAdapter) -> ExchangeAdapter:
    """注册一个 adapter 实例（无状态单例即可）。重复注册同名 raise。"""
    if adapter.name in _REGISTRY:
        raise ValueError(f"exchange already registered: {adapter.name!r}")
    _REGISTRY[adapter.name] = adapter
    return adapter


def _ensure_registered() -> None:
    """惰性导入内置 adapter 模块，触发其模块尾部的 ``register_exchange`` 副作用。

    惰性（首次查表时）而非 import 时触发，彻底规避 import-time 循环依赖：被 adapter
    模块导入的 ``core.handle`` / ``core.exchange.symbol_match`` / ``core.pipeline.data_update`` 等在
    首次 :func:`get_adapter` 被调用（运行期）时均已完成加载。``_BOOTSTRAPPED`` 守卫
    使内置注册只发生一次（子进程 spawn 后各自重建）。
    """
    global _BOOTSTRAPPED
    if _BOOTSTRAPPED:
        return
    _BOOTSTRAPPED = True
    from core.exchange.adapters import binance_adapter, okx_adapter  # noqa: F401  (register on import)


def get_adapter(exchange, default=_UNSET) -> ExchangeAdapter:
    """``exchange 字符串 -> adapter``。

    严格模式（不传 ``default``）：未知交易所 raise ``ValueError``（不再静默退化
    Binance），把配置错误变成可观察的失败。

    宽松模式（传 ``default``，如 ``default='binance'``）：未知 / ``None`` 交易所回退到
    ``default`` adapter，用于保留现有 dispatch 点 "非 okx → binance" 的历史默认语义。
    """
    _ensure_registered()
    adapter = _REGISTRY.get(exchange)
    if adapter is not None:
        return adapter
    if default is not _UNSET:
        return _REGISTRY[default]
    raise ValueError(f"unknown exchange {exchange!r}; registered: {sorted(_REGISTRY)}")


def registered_exchanges() -> list[str]:
    """返回已注册交易所名（排序）。"""
    _ensure_registered()
    return sorted(_REGISTRY)
