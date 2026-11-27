"""OKX 交易所适配器。

把 OKX 的差异化行为收敛到一个 :class:`ExchangeAdapter` 子类。每个方法委派到现有的
自由函数 / 构造现有对象，行为与迁移前逐一一致。相对 Binance 多 override 一个
:meth:`get_limiter`（OKX 走滑动窗口限流，``get_okx_shared_limiter``）。

模块尾部 ``register_exchange`` 自注册。
"""
from __future__ import annotations

from core.exchange.adapter import ExchangeAdapter, register_exchange
from core.exchange.rest.okx_fetcher import OkxFetcher
from core.exchange.protocol import OkxProtocol
from core.handle import create_okx_handle
from core.exchange.symbol_match import get_okx_spot_swap_matches
from core.exchange.rate_limit import get_okx_shared_limiter


class OkxAdapter(ExchangeAdapter):
    """OKX 适配器（spot + U 本位线性永续，business 频道 WS）。"""

    name = "okx"
    display_name = "OKX"

    def create_handle(self, cfg):
        return create_okx_handle(cfg)

    def create_fetcher(self, handle, trade_type, session):
        return OkxFetcher(trade_type, session, handle.proxy)

    def match_spot_swap(self, spot_symbols, swap_symbols):
        return get_okx_spot_swap_matches(spot_symbols, swap_symbols)

    def create_protocol(self):
        return OkxProtocol()

    def get_limiter(self, endpoint: str):
        return get_okx_shared_limiter(endpoint)


register_exchange(OkxAdapter())
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
