import asyncio
from decimal import Decimal

import pandas as pd

from core.exchange.rest.binance_market import create_binance_market_api
from core.utils.network import async_retry_getter
from core.utils.time import async_sleep_until_run_time, convert_interval_to_timedelta, next_run_time


def _get_from_filters(filters, filter_type, field_name):
    """
    从交易对的过滤器列表中提取指定类型的字段值。

    :param filters: 交易对的过滤器列表，通常包含价格、数量等限制条件。
    :param filter_type: 过滤器的类型，例如 'PRICE_FILTER'、'LOT_SIZE' 等。
    :param field_name: 需要提取的字段名称，例如 'tickSize'、'stepSize' 等。
    :return: 返回指定过滤器类型中对应字段的值。如果未找到，则返回 None。
    """
    for f in filters:  # 遍历过滤器列表
        if f['filterType'] == filter_type:  # 检查过滤器类型是否匹配
            return f[field_name]  # 返回指定字段的值
    return None  # 如果未找到匹配的过滤器类型，返回 None


def _check_from_permission_sets(permission_sets, permission_name):
    """
    检查交易对的权限集中是否包含指定的权限。

    :param permission_sets: 交易对的权限集列表，通常包含交易对的权限信息。
    :param permission_name: 需要检查的权限名称，例如 'PRE_MARKET'。
    :return: 如果权限集中包含指定权限，则返回 True；否则返回 False。
    """
    for permission_set in permission_sets:  # 遍历权限集列表
        if permission_name in permission_set:  # 检查权限名称是否存在于当前权限集中
            return True  # 如果存在，返回 True
    return False  # 如果遍历完所有权限集仍未找到，返回 False


def _parse_usdt_futures_syminfo(info):
    """
    解析 USDT 永续合约交易对的交易规则信息。
    
    :param info: 包含交易对信息的字典，通常从币安 API 获取。
    :return: 返回一个包含解析后交易规则的字典。
    """
    filters = info['filters']  # 获取交易对的过滤器信息

    # 安全获取过滤器值，为 None 值提供默认值
    price_tick = _get_from_filters(filters, 'PRICE_FILTER', 'tickSize') or '0.01'
    lot_size = _get_from_filters(filters, 'LOT_SIZE', 'stepSize') or '0.001'
    min_notional = _get_from_filters(filters, 'MIN_NOTIONAL', 'notional') or '5'

    return {
        'symbol': info['symbol'],  # 交易对名称
        'contract_type': info['contractType'],  # 合约类型
        'status': info['status'],  # 交易对状态
        'base_asset': info['baseAsset'],  # 基础资产
        'quote_asset': info['quoteAsset'],  # 计价资产
        'margin_asset': info['marginAsset'],  # 保证金资产
        'price_tick': Decimal(price_tick),  # 价格最小变动单位
        'lot_size': Decimal(lot_size),  # 最小交易量
        'min_notional_value': Decimal(min_notional),  # 最小名义价值
    }


def _parse_coin_futures_syminfo(info):
    """
    解析币本位合约交易对的交易规则信息。
    
    :param info: 包含交易对信息的字典，通常从币安 API 获取。
    :return: 返回一个包含解析后交易规则的字典。
    """
    filters = info['filters']  # 获取交易对的过滤器信息

    # 安全获取过滤器值，为 None 值提供默认值
    price_tick = _get_from_filters(filters, 'PRICE_FILTER', 'tickSize') or '0.01'

    return {
        'symbol': info['symbol'],  # 交易对名称
        'contract_type': info['contractType'],  # 合约类型
        'status': info['contractStatus'],  # 合约状态
        'base_asset': info['baseAsset'],  # 基础资产
        'quote_asset': info['quoteAsset'],  # 计价资产
        'margin_asset': info['marginAsset'],  # 保证金资产
        'price_tick': Decimal(price_tick),  # 价格最小变动单位
        'lot_size': Decimal(info['contractSize']),  # 一手合约价值(美元)
    }


def _parse_spot_syminfo(info):
    """
    解析现货交易对的交易规则信息。
    
    :param info: 包含交易对信息的字典，通常从币安 API 获取。
    :return: 返回一个包含解析后交易规则的字典。
    """
    filters = info['filters']  # 获取交易对的过滤器信息
    permission_sets = info.get('permissionSets', [])  # 获取交易对的权限集

    # 安全获取过滤器值，为 None 值提供默认值
    price_tick = _get_from_filters(filters, 'PRICE_FILTER', 'tickSize') or '0.01'
    lot_size = _get_from_filters(filters, 'LOT_SIZE', 'stepSize') or '0.001'
    min_notional = _get_from_filters(filters, 'NOTIONAL', 'minNotional') or '10'

    return {
        'symbol': info['symbol'],  # 交易对名称
        'status': info['status'],  # 交易对状态
        'base_asset': info['baseAsset'],  # 基础资产
        'quote_asset': info['quoteAsset'],  # 计价资产
        'price_tick': Decimal(price_tick),  # 价格最小变动单位
        'lot_size': Decimal(lot_size),  # 最小交易量
        'min_notional_value': Decimal(min_notional),  # 最小名义价值
        'pre_market': _check_from_permission_sets(permission_sets, 'PRE_MARKET'),  # 是否是盘前交易状态
    }


def pandas_parse_kline(klines):
    """
    将币安的 K 线数据（二维数组）转化为 Pandas DataFrame，并进行数据清洗和格式化。

    :param klines: 币安 K 线数据，通常是一个二维数组，每行代表一根 K 线。
    :return: 返回一个处理后的 Pandas DataFrame，包含格式化后的 K 线数据。
    """
    # 定义 DataFrame 的列名，与币安 K 线数据的字段一一对应
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ]

    # 将 K 线数据转换为 DataFrame，并指定列名
    df = pd.DataFrame(klines, columns=columns)

    # 删除不需要的列（'ignore' 和 'close_time'）
    df.drop(columns=['ignore', 'close_time'], inplace=True)

    # 定义每列的数据类型
    dtypes = {
        'candle_begin_time': int,  # K 线开始时间（时间戳，毫秒）
        'open': float,  # 开盘价
        'high': float,  # 最高价
        'low': float,  # 最低价
        'close': float,  # 收盘价
        'volume': float,  # 成交量
        'quote_volume': float,  # 成交额
        'trade_num': int,  # 成交笔数
        'taker_buy_base_asset_volume': float,  # 主动买入的成交量
        'taker_buy_quote_asset_volume': float  # 主动买入的成交额
    }

    # 将 DataFrame 的列转换为指定的数据类型
    df = df.astype(dtypes)

    # 将 'candle_begin_time' 列的时间戳（毫秒）转换为 UTC 时区的 datetime 类型
    df['candle_begin_time'] = pd.to_datetime(df['candle_begin_time'], unit='ms', utc=True)

    # 基于 'candle_begin_time' 列去除重复的 K 线
    df.drop_duplicates('candle_begin_time', inplace=True)

    # 按 'candle_begin_time' 列对数据进行排序，并重置索引
    df.sort_values('candle_begin_time', inplace=True, ignore_index=True)

    # 返回处理后的 DataFrame
    return df


class BinanceFetcher:

    TYPE_MAP = {
        'usdt_futures': _parse_usdt_futures_syminfo,
        'coin_futures': _parse_coin_futures_syminfo,
        'spot': _parse_spot_syminfo,
    }

    def __init__(self, trade_type, session, proxy=None):
        self.trade_type = trade_type
        self.market_api = create_binance_market_api(trade_type, session, proxy)

        if trade_type in self.TYPE_MAP:
            self.syminfo_parse_func = self.TYPE_MAP[trade_type]
        else:
            raise ValueError(f'Type {trade_type} not supported')

    def get_api_limits(self) -> tuple[int, int]:
        return self.market_api.MAX_MINUTE_WEIGHT, self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    def batch_size(self) -> int:
        """单批并发请求的交易对数量上限。"""
        return 80

    def once_candles(self) -> int:
        """单次 K 线请求的最大返回条数。"""
        return self.market_api.WEIGHT_EFFICIENT_ONCE_CANDLES

    async def acquire(self, n, *, history=False) -> None:
        """限速门控：基于每分钟权重。

        若当前已用权重超过 ``MAX_MINUTE_WEIGHT * 0.9``，则 sleep 直至下一分钟整点
        后重新查询，直到权重低于阈值再放行（参数 ``n`` / ``history`` 用于与其它
        交易所 Fetcher 接口对齐，币安路径不使用）。
        """
        max_minute_weight = self.market_api.MAX_MINUTE_WEIGHT
        while True:
            _server_time, weight = await self.get_time_and_weight()
            if weight > max_minute_weight * 0.9:
                await async_sleep_until_run_time(next_run_time("1m"))
                continue
            return

    async def get_time_and_weight(self) -> tuple[pd.Timestamp, int]:
        server_timestamp, weight = await async_retry_getter(self.market_api.aioreq_time_and_weight)
        server_timestamp = pd.to_datetime(server_timestamp, unit='ms', utc=True)
        return server_timestamp, weight

    async def get_exchange_info(self) -> dict[str, dict]:
        """
        Parse trading rules from return values of /exchangeinfo API
        """
        exg_info = await async_retry_getter(self.market_api.aioreq_exchange_info)
        results = dict()
        for info in exg_info['symbols']:
            results[info['symbol']] = self.syminfo_parse_func(info)
        return results

    async def get_candle(self, symbol, interval, **kwargs) -> pd.DataFrame:
        """
        异步获取指定交易对的 K 线数据，并将其转换为 Pandas DataFrame。

        :param symbol: 交易对符号，例如 'BTCUSDT'。
        :param interval: K 线的时间间隔，例如 '5m'、'1h'等。
        :param kwargs: 其他可选参数，用于传递给币安 API 的 K 线接口。例如：
                    - startTime: 获取 K 线的起始时间（时间戳，毫秒）。
                    - endTime: 获取 K 线的结束时间（时间戳，毫秒）。
                    - limit: 获取 K 线的数量限制。
        :return: 返回一个包含 K 线数据的 Pandas DataFrame
        """
        # 异步调用币安 API 获取 K 线数据
        data = await async_retry_getter(self.market_api.aioreq_klines, symbol=symbol, interval=interval, **kwargs)

        # 将获取的 K 线数据转换为 Pandas DataFrame
        return pandas_parse_kline(data)

    async def batch_get_candle(self, params_list: list[dict]) -> list[pd.DataFrame]:
        """
        异步批量获取多个交易对的 K 线数据，并将其转换为 Pandas DataFrame 列表。

        :param params_list: 包含多个请求参数的字典列表。每个字典的键值对如下：
                            - symbol: 交易对符号，例如 'BTCUSDT'。
                            - interval: K 线的时间间隔，例如 '5m'、'1h'等。
                            - limit: 获取 K 线的数量限制（可选）。
                            - startTime: 获取 K 线的起始时间（可选，时间戳，毫秒）。
                            - endTime: 获取 K 线的结束时间（可选，时间戳，毫秒）。
                            - 其他可选参数，用于传递给币安 API 的 K 线接口。
        :return: 返回一个列表，每个元素是对应交易对的 K 线数据 DataFrame。列表顺序与 params_list 一致。
        """
        # 创建异步任务列表，每个任务异步调用币安 API 获取 K 线数据
        tasks = [async_retry_getter(self.market_api.aioreq_klines, **params) for params in params_list]

        # 使用 asyncio.gather 并发执行所有任务，获取原始 K 线数据
        raw_data_list = await asyncio.gather(*tasks)

        # 将原始 K 线数据解析为 DataFrame，并存储在列表中
        result = [pandas_parse_kline(raw_data) for raw_data in raw_data_list]
        return result

    async def get_realtime_funding_rate(self) -> pd.DataFrame:
        '''
        Parse realtime funding rates from /premiumIndex
        '''
        if self.trade_type == 'spot':
            raise RuntimeError('Cannot request funding rate for spot')
        data = await async_retry_getter(self.market_api.aioreq_premium_index)
        frame = pd.DataFrame.from_records(data, columns=['symbol', 'lastFundingRate', 'nextFundingTime'])
        rates = pd.to_numeric(frame['lastFundingRate'], errors='coerce')
        times = pd.to_datetime(pd.to_numeric(frame['nextFundingTime'], errors='coerce'),
                               unit='ms', utc=True, errors='coerce')
        result = pd.DataFrame(dict(symbol=frame['symbol'], funding_rate=rates, next_funding_time=times))
        return result.loc[result.symbol.notna() & times.notna() & rates.abs().lt(float('inf'))]

    async def get_hist_funding_rate(self, symbol, *, _reserve_request=None, **kwargs):
        """Return raw settlement timestamps; pagination precedes UTC-hour aggregation."""
        if self.trade_type == 'spot':
            raise RuntimeError('Cannot request funding rate for spot')

        async def request(**params):
            if _reserve_request is not None:
                await _reserve_request()
            return await self.market_api.aioreq_funding_rate(**params)

        data = await async_retry_getter(request, symbol=symbol, _sleep_seconds=75, **kwargs)
        if not isinstance(data, list):
            raise ValueError('Invalid historical funding response')
        frame = pd.DataFrame.from_records(data, columns=['symbol', 'fundingTime', 'fundingRate'])
        if len(frame) > kwargs.get('limit', 1000):
            raise ValueError('Oversized historical funding response')
        millis = pd.to_numeric(frame['fundingTime'], errors='coerce')
        times = pd.to_datetime(millis, unit='ms', utc=True, errors='coerce')
        rates = pd.to_numeric(frame['fundingRate'], errors='coerce')
        valid = (frame.symbol.eq(symbol) & times.notna() & millis.mod(1).eq(0) &
                 millis.between(kwargs.get('startTime', 0), kwargs.get('endTime', float('inf'))) &
                 rates.abs().lt(float('inf')))
        if not valid.all():
            raise ValueError(f'Invalid historical funding row for {symbol}')
        return pd.DataFrame(dict(symbol=frame.symbol, candle_begin_time=times, funding_rate=rates))
