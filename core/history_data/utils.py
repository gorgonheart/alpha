from core.binance_fetcher_async import BinanceFetcher
from core.handle import BmacHandle
from core.utils.network import create_aiohttp_session


async def get_type_symbols(handle: BmacHandle, trade_type: str):
    """获取指定交易类型的交易对列表。
    
    通过 Binance API 获取指定交易类型的所有可用交易对，经过过滤后按字母顺序排序返回。
    
    Args:
        handle (BmacHandle): Binance 市场数据处理器实例。
        trade_type (str): 交易类型标识符。
        
    Returns:
        list[str]: 按字母顺序排序的交易对符号列表。
        
    Raises:
        Exception: 当网络请求失败或 API 响应异常时抛出。
    """
    binance_handle = handle.get_binance_handle(trade_type)
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        fetcher = BinanceFetcher(binance_handle.api_type, session, handle.proxy)

        # 异步获取交易所信息
        exginfo = await fetcher.get_exchange_info()

        # 过滤交易对
        symbols_trading = binance_handle.symbol_filter(exginfo)

    # 返回按字母排序的交易对列表
    return sorted(symbols_trading)
