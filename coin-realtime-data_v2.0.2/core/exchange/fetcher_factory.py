"""Fetcher 工厂：按 ``handle.exchange`` 经交易所注册表创建对应交易所的数据获取器。

统一上层调度入口：所有需要构造 Fetcher 的调用点改走 ``create_fetcher``，分派收敛到
``core.exchange.adapter.get_adapter``——每个交易所的 Fetcher 构造逻辑（含 trade_type
词表翻译：Binance 经 ``api_type``，OKX 直接用 'spot'/'swap'）落在各自 adapter 的
``create_fetcher`` 里，调用点不再出现任何 if/else。
"""

from core.exchange.adapter import get_adapter


def create_fetcher(handle, trade_type, session):
    """根据 ``handle.exchange`` 创建对应交易所的 Fetcher。

    Args:
        handle: BmacHandle 实例，需含 ``exchange`` / ``proxy`` /
            ``get_market_handle`` 属性。
        trade_type: 'spot' 或 'swap'。
        session: aiohttp ClientSession（或测试时的 None）。

    Returns:
        BinanceFetcher | OkxFetcher。

    Raises:
        ValueError: ``handle.exchange`` 未注册（未知交易所不再静默退化 Binance）。
    """
    return get_adapter(handle.exchange).create_fetcher(handle, trade_type, session)
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
