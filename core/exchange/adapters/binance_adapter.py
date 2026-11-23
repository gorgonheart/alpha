"""Binance 交易所适配器。

把 Binance 的差异化行为收敛到一个 :class:`ExchangeAdapter` 子类。每个方法委派到
现有的自由函数 / 构造现有对象，行为与迁移前逐一一致——本文件不引入任何新逻辑，
只是把分散在 fetcher_factory / binance_update / symbol_match / app / handle 里的
"binance 分支"归位到一处。

模块尾部 ``register_exchange`` 自注册：被 ``core.exchange.adapter._ensure_registered``
惰性导入时触发。
"""
from __future__ import annotations

from core.exchange.adapter import ExchangeAdapter, register_exchange
from core.exchange.rest.binance_fetcher import BinanceFetcher
from core.exchange.protocol import BinanceProtocol
from core.handle import create_binance_handle
from core.exchange.symbol_match import get_spot_swap_matches


class BinanceAdapter(ExchangeAdapter):
    """Binance 适配器（spot + U 本位永续）。"""

    name = "binance"
    display_name = "币安"

    def create_handle(self, cfg):
        return create_binance_handle(cfg)

    def create_fetcher(self, handle, trade_type, session):
        binance_handle = handle.get_market_handle(trade_type)
        return BinanceFetcher(binance_handle.api_type, session, handle.proxy)

    def match_spot_swap(self, spot_symbols, swap_symbols):
        return get_spot_swap_matches(spot_symbols, swap_symbols)

    def create_protocol(self):
        return BinanceProtocol()


register_exchange(BinanceAdapter())
