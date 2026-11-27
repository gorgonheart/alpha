import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime, timedelta

import pandas as pd

from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.exchange.fetcher_factory import create_fetcher
from core.exchange.adapter import get_adapter
from core.handle import MarketHandle, BmacHandle, exchange_display_name
from core.pipeline.resample import (calc_resample, update_resample_candle)
from core.utils.network import async_retry_getter, create_aiohttp_session
from core.utils.time import (async_sleep_until_run_time, convert_interval_to_timedelta, format_time,
                             get_offset_from_runtime, now_time_str)


async def update_exginfo(handle: BmacHandle, session, trade_type, run_time):
    """更新指定 trade_type 的 Exchange Info（交易所无关，按 handle.exchange 分派 Fetcher）。"""
    binance_handle: MarketHandle = handle.get_market_handle(trade_type)
    fetcher = create_fetcher(handle, trade_type, session)
    exginfo = await fetcher.get_exchange_info()
    symbols_trading = binance_handle.symbol_filter(exginfo)
    infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    df_exginfo = pd.DataFrame.from_records(infos_trading)
    handle.exg_mgr.set_candle(f'exginfo_{trade_type}', run_time, df_exginfo)


async def update_funding_rates(handle: BmacHandle, session, run_time):
    """实时资金费更新（交易所无关，按 handle.exchange 分派 Fetcher）。

    OKX 走 OkxFetcher（symbol 即 instId，如 BTC-USDT-SWAP），Binance 走
    BinanceFetcher；两者 get_realtime_funding_rate 输出列一致
    （symbol / funding_rate / next_funding_time），下游存储逻辑统一。
    """
    funding_mgr = handle.funding_mgr
    fetcher = create_fetcher(handle, 'swap', session)
    df_funding = await fetcher.get_realtime_funding_rate()

    if df_funding.empty:
        return

    df_funding = df_funding.rename(columns={'next_funding_time': 'candle_begin_time'})
    # 交割合约 / 已下架币的 nextFundingTime=0，会落成 1970-01-01 占位行，丢弃
    df_funding = df_funding[df_funding['candle_begin_time'] >= pd.Timestamp('2000-01-01', tz='UTC')]
    for symbol, g in df_funding.groupby('symbol'):
        funding_mgr.update_candle(symbol, run_time, g, handle.kline_count_1h)


async def update_exchange_meta(handle: BmacHandle, run_time):
    """交易所无关的 meta 更新：exginfo_spot + exginfo_swap (+ 可选资金费)。

    exginfo 与资金费均经 ``create_fetcher`` 按 ``handle.exchange`` 分派到对应交易所
    （Binance / OKX 走各自 Fetcher，输出列一致），故本流程对所有交易所通用——此前
    update_binance_meta / update_okx_meta 两个函数体逐字节相同，现合并为一份实现。
    当 ``handle.fetch_funding_rate`` 为真（两交易所均由 config.funding_rate 驱动）时
    额外抓取实时资金费。
    """
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        exginfo_spot = update_exginfo(handle, session, 'spot', run_time)
        exginfo_swap = update_exginfo(handle, session, 'swap', run_time)

        tasks = [exginfo_spot, exginfo_swap]

        if handle.fetch_funding_rate:
            funding = update_funding_rates(handle, session, run_time)
            tasks.append(funding)

        await asyncio.gather(*tasks)


# 向后兼容别名：历史调用点 / 测试仍按交易所命名引用 meta 更新；实现已统一为
# update_exchange_meta（消除此前两个字节相同的函数体），名字保留不破坏既有 import。
update_binance_meta = update_exchange_meta
update_okx_meta = update_exchange_meta


async def update_meta(handle: BmacHandle, run_time):
    """按 handle.exchange 经交易所注册表分派 meta 更新（exginfo_spot/swap + 可选 funding）。

    未知 exchange 直接 raise ValueError（不再静默退化 Binance）。
    """
    await get_adapter(handle.exchange).update_meta(handle, run_time)


async def fetch_update_resample(handle: BmacHandle, binance_handle: MarketHandle, fetcher, trade_type,
                                run_time, resample_inline: bool = True, symbols_subset=None):
    """Fetch base 5m K via REST. Optionally resample inline.

    resample_inline=False 时只抓 base 5m（供 writer 写 base 存储兜底），
    不做 resample —— realtime resample 改为 barrier 之后从 ws 优先级合并的 base 存储读
    （update_resample_from_base），保证 resample 用闭合 K 而非 REST 可能未结算的最近一根。
    resample_inline=True 保留旧行为（纯 REST 模式 / use_websocket=False 时用）。

    symbols_subset（可选 set[str]）：仅抓取该子集（与 exginfo 交易对全集求交集）。
    用于 WS 模式 REST 兜底只补 WS 漏掉的 symbol。None 时抓全集（向后兼容，Binance 不受影响）。
    """
    base_interval = handle.base_interval
    resample_interval = handle.resample_interval
    exg_mgr = handle.exg_mgr
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    once_update_candles = binance_handle.once_update_candles
    # resample_mgr_map 只含 enabled_hour_offsets；仅内联 resample 时才需要按 offset 取。
    # WS 模式（resample_inline=False）只抓 base，run_time 可以落在未启用的 offset 上
    # （如启动补齐用的是"当前已闭合"时刻），此时不能查表，否则 KeyError。
    resample_mgr = binance_handle.resample_mgr_map[offset_str] if resample_inline else None

    # 读取正在交易的交易对 Exchange Info
    df_exginfo = exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list()

    # WS 模式 REST 兜底：仅抓子集（与交易对全集求交集，保持原顺序，过滤不在全集内的）
    if symbols_subset is not None:
        symbols_trading = [s for s in symbols_trading if s in symbols_subset]

    num_symbols = len(symbols_trading)

    name = exchange_display_name(handle.exchange)
    start_time = time.perf_counter()
    logger.info(f'开始更新{name} {trade_type} K 线, 交易对数量={num_symbols}, 当前时间={now_time_str()}')

    # 生成所有交易对的 K 线请求参数
    params_list = [{'symbol': s, 'interval': base_interval, 'limit': once_update_candles} for s in symbols_trading]

    # 批量请求 K 线
    base_klines_list = await fetcher.batch_get_candle(params_list)

    symbol_base_df = dict()

    # 记录每个 symbol Resample 周期(1h) K线的 candle_begin_time 最大值和最小值
    symbol_begin_time = dict()

    for symbol, df_base in zip(symbols_trading, base_klines_list):
        # 必须在过滤进行中 K 线之前判断：原始返回满 limit 根才说明取数窗口被截断
        window_truncated = len(df_base) >= once_update_candles
        df_base = df_base[df_base['candle_begin_time'] < run_time]
        symbol_base_df[symbol] = df_base

        if not resample_inline:
            # ws 模式：仅收集 base 5m，resample 留到 barrier 后从 base 存储做
            continue

        # 将基础周期(5m) K 线 DF, Resample 成带偏移的 Resample 周期(1h) K 线 DF
        theoretical_start = run_time - handle.kline_count_1h * resample_delta
        df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)

        # 取数窗口被 limit 截断时，首根 resample K 线缺少更早的基础 K 线、不完整，
        # 丢弃以免覆盖已有的完整 K 线；窗口未截断（返回不足 limit 根，如新币）
        # 说明拿到完整历史，首根完整，必须保留以便用闭合后的数据修正先前写入的未闭合值
        if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
            df_resample = df_resample.iloc[1:]

        # 与已有 Resample 周期(1h) K 线 DF 合并，过滤/填充 K 线缺失，最后写入硬盘
        first_begin_time, last_begin_time = update_resample_candle(resample_mgr, symbol, df_resample,
                                                                   handle.kline_count_1h, run_time, resample_delta)
        if first_begin_time is not None and last_begin_time is not None:
            symbol_begin_time[symbol] = first_begin_time, last_begin_time

    time_elapsed = time.perf_counter() - start_time
    if resample_inline:
        logger.ok(f'{name} {trade_type} API, 获取 {base_interval} 成功, Resample 并更新 {resample_interval} 成功, '
                  f'耗时={time_elapsed:.1f}秒, 当前时间={now_time_str()}')
    else:
        logger.ok(f'{name} {trade_type} API, 获取 {base_interval} 成功 (resample 延后到 base 存储), '
                  f'耗时={time_elapsed:.1f}秒, 当前时间={now_time_str()}')

    return symbol_base_df, symbol_begin_time


async def async_fetch_update_resample_candles(handle: BmacHandle, run_time: datetime, trade_type,
                                              resample_inline: bool = True, symbols_subset=None):
    binance_handle: MarketHandle = handle.get_market_handle(trade_type)

    try:
        async with create_aiohttp_session(handle.http_timeout_sec) as session:
            fetcher = create_fetcher(handle, trade_type, session)
            symbol_base_df, symbol_begin_time = await fetch_update_resample(handle, binance_handle, fetcher,
                                                                            trade_type, run_time, resample_inline,
                                                                            symbols_subset=symbols_subset)
    except:
        logger.exception(f'{exchange_display_name(handle.exchange)} API 更新数据失败')
        return None

    return symbol_base_df, symbol_begin_time


def fetch_update_binance_candles(handle: BmacHandle, run_time: datetime, resample_inline: bool = True,
                                 symbols_subset=None):
    """Fetch base K (+ optional inline resample). Returns 4-tuple of dfs/timestamps.

    The writer-queue submission is done by the CALLER in
    the main process. This function runs inside a ProcessPoolExecutor worker
    and mp.Queue objects are not pickle-safe (they can only cross process
    boundaries via inheritance). Returning the dfs and letting the caller
    submit avoids that constraint.

    resample_inline=False (ws 模式) 时只抓 base 5m，不 resample；
    realtime resample 改由 update_resample_from_base 在 barrier 后从 base 存储做。
    此时返回的 begin_time 字典为空（caller 不使用）。

    symbols_subset（可选 dict[str, set[str]]，键 'spot'/'swap'）：WS 模式 REST 兜底
    只补各 trade_type 漏掉的 symbol。None 时抓全集（向后兼容，Binance 入口不受影响）。

    Returns:
      (symbol_base_df_spot, symbol_base_df_swap,
       symbol_begin_time_spot, symbol_begin_time_swap)
    """
    spot_subset = symbols_subset.get('spot') if symbols_subset is not None else None
    swap_subset = symbols_subset.get('swap') if symbols_subset is not None else None
    with ThreadPoolExecutor(max_workers=4) as exe:
        bn_spot = exe.submit(asyncio.run, async_fetch_update_resample_candles(
            handle, run_time, 'spot', resample_inline, symbols_subset=spot_subset))
        bn_swap = exe.submit(asyncio.run, async_fetch_update_resample_candles(
            handle, run_time, 'swap', resample_inline, symbols_subset=swap_subset))
        spot_result = bn_spot.result()
        swap_result = bn_swap.result()

    if spot_result is None or swap_result is None:
        return None

    symbol_base_df_spot, symbol_begin_time_spot = spot_result
    symbol_base_df_swap, symbol_begin_time_swap = swap_result

    return symbol_base_df_spot, symbol_base_df_swap, symbol_begin_time_spot, symbol_begin_time_swap


def update_base_candles(handle: BmacHandle, run_time, trade_type, symbol_dfs):
    binance_handle: MarketHandle = handle.get_market_handle(trade_type)
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    asyncio.run(async_sleep_until_run_time(run_time + timedelta(minutes=3)))

    name = exchange_display_name(handle.exchange)
    logger.info(f'开始合并{name} {trade_type} {handle.base_interval} K 线, 当前时间={now_time_str()}')

    for symbol, df_new in symbol_dfs.items():
        symbol_dir = binance_handle.base_candle_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        ts_mgr.update(df_new)
        required_first_begin = run_time - handle.num_base_candles * base_delta
        partitions = ts_mgr.list_partitions()
        ts_mgr.trim_partition(partitions[0], required_first_begin, run_time)
    logger.ok(f'合并{name} {trade_type} {handle.base_interval} K 线成功, 当前时间={now_time_str()}')


def update_binance_base_candles(handle: BmacHandle, run_time: datetime, spot_dfs, swap_dfs):
    with ProcessPoolExecutor(max_workers=2) as exe:

        bn_spot_base = exe.submit(update_base_candles, handle, run_time, 'spot', spot_dfs)
        bn_swap_base = exe.submit(update_base_candles, handle, run_time, 'swap', swap_dfs)

        bn_spot_base.result()
        bn_swap_base.result()
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
