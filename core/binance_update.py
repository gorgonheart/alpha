import asyncio
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime, timedelta

import pandas as pd

from core.ts_manager import TSManager
from core.utils.log_kit import logger
from core.binance_fetcher_async import BinanceFetcher
from core.funding_backfill import (NETWORK_SECONDS, backfill_recent_funding_rates,
                                   mark_funding_backfill_completed, save_funding_hours)
from core.handle import BinanceHandle, BmacHandle
from core.resample_candle import (calc_resample, update_resample_candle)
from core.utils.network import async_retry_getter, create_aiohttp_session
from core.utils.time import (async_sleep_until_run_time, convert_interval_to_timedelta, format_time,
                             get_offset_from_runtime, now_time_str)


async def update_binance_exginfo(handle: BmacHandle, session, trade_type, run_time):
    binance_handle: BinanceHandle = handle.get_binance_handle(trade_type)
    fetcher = BinanceFetcher(binance_handle.api_type, session, handle.proxy)
    exginfo = await fetcher.get_exchange_info()
    symbols_trading = binance_handle.symbol_filter(exginfo)
    infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    df_exginfo = pd.DataFrame.from_records(infos_trading)
    handle.exg_mgr.set_candle(f'exginfo_{trade_type}', run_time, df_exginfo)
    return symbols_trading


async def update_binance_funding_rates(handle: BmacHandle, session, run_time):
    binance_handle: BinanceHandle = handle.get_binance_handle('swap')
    fetcher = BinanceFetcher(binance_handle.api_type, session, handle.proxy)
    df_funding = await fetcher.get_realtime_funding_rate()

    if df_funding.empty:
        return df_funding

    df_funding = df_funding.rename(columns={'next_funding_time': 'candle_begin_time'})
    # Once a UTC hour has begun, a later forecast for that same bucket must
    # not replace its settled events. History supplies the authoritative total.
    df_funding['candle_begin_time'] = df_funding['candle_begin_time'].dt.floor('h')
    return df_funding.loc[df_funding['candle_begin_time'] >= run_time]


async def update_binance_meta(handle: BmacHandle, run_time):
    meta_started = time.monotonic()
    write_deadline = meta_started + run_time.timestamp() - time.time() - 2
    deadline = min(meta_started + NETWORK_SECONDS, write_deadline)
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        exginfo_spot = asyncio.create_task(update_binance_exginfo(handle, session, 'spot', run_time))
        exginfo_swap = asyncio.create_task(update_binance_exginfo(handle, session, 'swap', run_time))
        tasks = [exginfo_spot, exginfo_swap]
        funding_task = history_task = None

        if handle.fetch_funding_rate:
            funding_task = asyncio.create_task(update_binance_funding_rates(handle, session, run_time))

            async def history_branch():
                stats = dict(requests=0, success=0, failed=0, elapsed=0., reason='deadline')
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {}, stats
                try:
                    async with asyncio.timeout(remaining):
                        # 可选历史超时不能取消共享市场信息任务。
                        symbols = await asyncio.shield(exginfo_swap)
                    # The collector drains/cancels its own workers at this same
                    # deadline and returns the requests already finished.
                    return await backfill_recent_funding_rates(handle, session, run_time, symbols, deadline=deadline)
                except TimeoutError:
                    stats['reason'] = 'deadline'
                except Exception:
                    stats['reason'] = 'history_failed'
                stats['elapsed'] = time.monotonic() - meta_started
                return {}, stats

            history_task = asyncio.create_task(history_branch())
            tasks.append(funding_task)

        try:
            await asyncio.gather(*tasks)
            if funding_task is not None:
                realtime = funding_task.result()
                saved_realtime = []
                started = time.monotonic()
                if not realtime.empty and not history_task.done():
                    # Let optional requests progress during mandatory disk I/O. Keep
                    # one writer; as soon as history arrives, combine remaining saves.
                    depth = handle.get_kline_count_1h('swap')
                    for symbol, current in realtime.groupby('symbol'):
                        if history_task.done():
                            break
                        await asyncio.to_thread(save_funding_hours, handle.funding_mgr, symbol,
                                                run_time, current, depth)
                        saved_realtime.append(symbol)
                if saved_realtime:
                    realtime = realtime.loc[~realtime.symbol.isin(saved_realtime)]
                    logger.info('等待历史期间已落盘实时资金费: 币种=%d 耗时=%.3fs',
                                len(saved_realtime), time.monotonic() - started)
                history, stats = await history_task
                save_funding_updates(handle, run_time, realtime, history, stats, write_deadline)
        finally:
            if history_task is not None:
                tasks.append(history_task)
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def save_funding_updates(handle, run_time, realtime, history, stats, write_deadline):
    """Merge once per symbol; acknowledge nonempty history only after persistence."""
    started = time.monotonic()
    realtime = dict(tuple(realtime.groupby('symbol'))) if not realtime.empty else {}
    depth = handle.get_kline_count_1h('swap')
    saved = 0
    # History-only symbols consume only time left after all required writes.
    for symbol in [*realtime, *sorted(history.keys() - realtime.keys())]:
        current = realtime.get(symbol)
        settled = history.get(symbol)
        if settled is not None and current is not None:
            incoming = pd.concat([current, settled], ignore_index=True)
        else:
            incoming = current if current is not None else settled
        if current is not None:
            save_funding_hours(handle.funding_mgr, symbol, run_time, incoming, depth)
        elif time.monotonic() < write_deadline:
            try:
                save_funding_hours(handle.funding_mgr, symbol, run_time, incoming, depth)
            except Exception as exc:
                logger.warning('历史资金费保存失败 %s: %s', symbol, exc)
                continue
        else:
            continue
        if settled is not None and mark_funding_backfill_completed(stats.get('round_hour'), symbol):
            saved += 1
            stats['completed'] += 1
            stats['remaining'] -= 1
    logger.info('历史资金费回补: 小时=%s 目标=%s 完成=%s 剩余=%s 请求=%s 落盘=%s 空=%s '
                '失败/取消=%s 原因=%s 网络=%.3fs 保存=%.3fs',
                stats.get('round_hour'), stats.get('target', 0), stats.get('completed', 0),
                stats.get('remaining', 0), stats['requests'], saved, stats.get('empty', 0),
                stats['failed'], stats['reason'], stats['elapsed'], time.monotonic() - started)


async def fetch_update_resample(handle: BmacHandle, binance_handle: BinanceHandle, fetcher: BinanceFetcher, trade_type,
                                run_time):

    base_interval = handle.base_interval
    resample_interval = handle.resample_interval
    exg_mgr = handle.exg_mgr
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    once_update_candles = binance_handle.once_update_candles
    resample_mgr = binance_handle.resample_mgr_map[offset_str]

    # 读取正在交易的交易对 Exchange Info
    df_exginfo = exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list()
    num_symbols = len(symbols_trading)

    start_time = time.perf_counter()
    logger.info(f'开始更新币安 {trade_type} K 线, 交易对数量={num_symbols}, 当前时间={now_time_str()}')

    # 生成所有交易对的 K 线请求参数
    params_list = [{'symbol': s, 'interval': base_interval, 'limit': once_update_candles} for s in symbols_trading]

    # 批量请求 K 线
    base_klines_list = await fetcher.batch_get_candle(params_list)

    symbol_base_df = dict()

    # 记录每个 symbol Resample 周期(1h) K线的 candle_begin_time 最大值和最小值
    symbol_begin_time = dict()

    for symbol, df_base in zip(symbols_trading, base_klines_list):
        window_truncated = len(df_base) >= once_update_candles
        df_base = df_base[df_base['candle_begin_time'] < run_time]
        symbol_base_df[symbol] = df_base

        # 将基础周期(5m) K 线 DF, Resample 成带偏移的 Resample 周期(1h) K 线 DF
        theoretical_start = run_time - handle.get_kline_count_1h(trade_type) * resample_delta
        df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)

        # 增量更新时，第一根 resample K 线可能因 API 数据截断而不完整
        # 如果该 symbol 已有旧数据，丢弃首根避免覆盖已有的完整 K 线
        # 新币未取满请求窗口时，即使已有部分首根，也须用闭合值覆盖
        if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
            df_resample = df_resample.iloc[1:]

        # 与已有 Resample 周期(1h) K 线 DF 合并，过滤/填充 K 线缺失，最后写入硬盘
        first_begin_time, last_begin_time = update_resample_candle(resample_mgr, symbol, df_resample,
                                                                   handle.get_kline_count_1h(trade_type), run_time, resample_delta)
        if first_begin_time is not None and last_begin_time is not None:
            symbol_begin_time[symbol] = first_begin_time, last_begin_time

    time_elapsed = time.perf_counter() - start_time
    logger.ok(f'Binance {trade_type} API, 获取 {base_interval} 成功, Resample 并更新 {resample_interval} 成功, '
              f'耗时={time_elapsed:.1f}秒, 当前时间={now_time_str()}')

    return symbol_base_df, symbol_begin_time


async def async_fetch_update_resample_candles(handle: BmacHandle, run_time: datetime, trade_type):
    binance_handle: BinanceHandle = handle.get_binance_handle(trade_type)

    try:
        async with create_aiohttp_session(handle.http_timeout_sec) as session:
            fetcher = BinanceFetcher(binance_handle.api_type, session, handle.proxy)
            symbol_base_df, symbol_begin_time = await fetch_update_resample(handle, binance_handle, fetcher,
                                                                            trade_type, run_time)
    except:
        logger.exception('Binance API 更新数据失败')
        return None

    return symbol_base_df, symbol_begin_time


def fetch_update_binance_candles(handle: BmacHandle, run_time: datetime):
    with ThreadPoolExecutor(max_workers=4) as exe:
        bn_spot = exe.submit(asyncio.run, async_fetch_update_resample_candles(handle, run_time, 'spot'))
        bn_swap = exe.submit(asyncio.run, async_fetch_update_resample_candles(handle, run_time, 'swap'))
        spot_result = bn_spot.result()
        swap_result = bn_swap.result()

    if spot_result is None or swap_result is None:
        return None

    symbol_base_df_spot, symbol_begin_time_spot = spot_result
    symbol_base_df_swap, symbol_begin_time_swap = swap_result

    return symbol_base_df_spot, symbol_base_df_swap, symbol_begin_time_spot, symbol_begin_time_swap


def update_base_candles(handle: BmacHandle, run_time, trade_type, symbol_dfs):
    binance_handle: BinanceHandle = handle.get_binance_handle(trade_type)
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    asyncio.run(async_sleep_until_run_time(run_time + timedelta(minutes=2)))

    logger.info(f'开始合并币安 {trade_type} {handle.base_interval} K 线, 当前时间={now_time_str()}')

    for symbol, df_new in symbol_dfs.items():
        symbol_dir = binance_handle.base_candle_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        ts_mgr.update(df_new)
        required_first_begin = run_time - handle.get_num_base_candles(trade_type) * base_delta
        partitions = ts_mgr.list_partitions()
        if partitions:
            ts_mgr.trim_partition(partitions[0], required_first_begin, run_time)
    logger.ok(f'合并币安 {trade_type} {handle.base_interval} K 线成功, 当前时间={now_time_str()}')


def update_binance_base_candles(handle: BmacHandle, run_time: datetime, spot_dfs, swap_dfs):
    with ProcessPoolExecutor(max_workers=2) as exe:

        bn_spot_base = exe.submit(update_base_candles, handle, run_time, 'spot', spot_dfs)
        bn_swap_base = exe.submit(update_base_candles, handle, run_time, 'swap', swap_dfs)

        bn_spot_base.result()
        bn_swap_base.result()
