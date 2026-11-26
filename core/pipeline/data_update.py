import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from core.exchange.adapter import get_adapter
from core.exchange.fetcher_factory import create_fetcher
from core.exchange.rest.binance_fetcher import BinanceFetcher
from core.handle import BmacHandle, MarketHandle, exchange_display_name
from core.pipeline.resample import calc_resample, update_resample_candle
from core.storage.ts_manager import TSManager
from core.strategy.funding_backfill import (
    NETWORK_SECONDS,
    backfill_recent_funding_rates,
    mark_funding_backfill_completed,
)
from core.strategy.funding_hours import save_funding_hours
from core.utils.log_kit import logger
from core.utils.network import async_retry_getter, create_aiohttp_session
from core.utils.time import (
    async_sleep_until_run_time,
    convert_interval_to_timedelta,
    format_time,
    get_offset_from_runtime,
    now_time_str,
)


def _get_market_handle(handle: BmacHandle, trade_type: str) -> MarketHandle:
    if hasattr(handle, 'get_market_handle'):
        return handle.get_market_handle(trade_type)
    return handle.get_binance_handle(trade_type)


def _resolve_kline_count_1h(handle: BmacHandle, trade_type: str, run_time: datetime) -> int:
    if hasattr(handle, 'effective_kline_count_1h'):
        return int(handle.effective_kline_count_1h(trade_type, run_time))
    if hasattr(handle, 'get_kline_count_1h'):
        return int(handle.get_kline_count_1h(trade_type))
    return int(getattr(handle, 'kline_count_1h', 15000))


def _resolve_history_trim_ts(handle: BmacHandle):
    if hasattr(handle, 'history_trim_ts'):
        return handle.history_trim_ts()
    return None


def _resolve_funding_depth(handle: BmacHandle):
    if hasattr(handle, 'history_trim_ts'):
        return None
    if hasattr(handle, 'get_kline_count_1h'):
        return handle.get_kline_count_1h('swap')
    return getattr(handle, 'kline_count_1h', None)


def _resolve_base_5m_retention_bars(handle: BmacHandle, trade_type: str) -> int:
    if hasattr(handle, 'base_5m_retention_bars'):
        return int(handle.base_5m_retention_bars())
    if hasattr(handle, 'get_num_base_candles'):
        return int(handle.get_num_base_candles(trade_type))
    return int(getattr(handle, 'num_base_candles', 30 * 24 * 12))


def _save_funding_with_trim(funding_mgr, symbol, run_time, incoming, depth, min_ts):
    if min_ts is not None:
        save_funding_hours(funding_mgr, symbol, run_time, incoming, depth, min_ts=min_ts)
    else:
        save_funding_hours(funding_mgr, symbol, run_time, incoming, depth)


async def update_binance_exginfo(handle: BmacHandle, session, trade_type, run_time):
    market_handle: MarketHandle = _get_market_handle(handle, trade_type)
    fetcher = BinanceFetcher(market_handle.api_type, session, handle.proxy)
    exginfo = await fetcher.get_exchange_info()
    symbols_trading = market_handle.symbol_filter(exginfo)
    infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    df_exginfo = pd.DataFrame.from_records(infos_trading)
    handle.exg_mgr.set_candle(f'exginfo_{trade_type}', run_time, df_exginfo)
    return symbols_trading


async def update_exginfo(handle: BmacHandle, session, trade_type, run_time):
    """更新指定 trade_type 的 Exchange Info（交易所无关，按 handle.exchange 分派 Fetcher）。"""
    market_handle: MarketHandle = _get_market_handle(handle, trade_type)
    fetcher = create_fetcher(handle, trade_type, session)
    exginfo = await fetcher.get_exchange_info()
    symbols_trading = market_handle.symbol_filter(exginfo)
    infos_trading = [info for sym, info in exginfo.items() if sym in symbols_trading]
    df_exginfo = pd.DataFrame.from_records(infos_trading)
    handle.exg_mgr.set_candle(f'exginfo_{trade_type}', run_time, df_exginfo)
    return symbols_trading


async def update_binance_funding_rates(handle: BmacHandle, session, run_time):
    market_handle: MarketHandle = _get_market_handle(handle, 'swap')
    fetcher = BinanceFetcher(market_handle.api_type, session, handle.proxy)
    df_funding = await fetcher.get_realtime_funding_rate()

    if df_funding.empty:
        return df_funding

    df_funding = df_funding.rename(columns={'next_funding_time': 'candle_begin_time'})
    # Once a UTC hour has begun, a later forecast for that same bucket must
    # not replace its settled events. History supplies the authoritative total.
    df_funding['candle_begin_time'] = df_funding['candle_begin_time'].dt.floor('h')
    return df_funding.loc[df_funding['candle_begin_time'] >= run_time]


async def update_funding_rates(handle: BmacHandle, session, run_time):
    """实时资金费更新（OKX / 通用交易所路径，沿用 v2.0.2 逻辑）。"""
    funding_mgr = handle.funding_mgr
    fetcher = create_fetcher(handle, 'swap', session)
    df_funding = await fetcher.get_realtime_funding_rate()

    if df_funding.empty:
        return

    df_funding = df_funding.rename(columns={'next_funding_time': 'candle_begin_time'})
    # 交割合约 / 已下架币的 nextFundingTime=0，会落成 1970-01-01 占位行，丢弃
    df_funding = df_funding[df_funding['candle_begin_time'] >= pd.Timestamp('2000-01-01', tz='UTC')]
    kline_count = _resolve_kline_count_1h(handle, 'swap', run_time)
    for symbol, g in df_funding.groupby('symbol'):
        funding_mgr.update_candle(symbol, run_time, g, kline_count)


async def update_binance_meta(handle: BmacHandle, run_time, *, include_realtime_funding: bool = True):
    meta_started = time.monotonic()
    write_deadline = meta_started + run_time.timestamp() - time.time() - 2
    deadline = min(meta_started + NETWORK_SECONDS, write_deadline)
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        exginfo_spot = asyncio.create_task(update_binance_exginfo(handle, session, 'spot', run_time))
        exginfo_swap = asyncio.create_task(update_binance_exginfo(handle, session, 'swap', run_time))
        tasks = [exginfo_spot, exginfo_swap]
        funding_task = history_task = None

        if handle.fetch_funding_rate:
            if include_realtime_funding:
                funding_task = asyncio.create_task(update_binance_funding_rates(handle, session, run_time))
                tasks.append(funding_task)

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

        try:
            await asyncio.gather(*tasks)
            if history_task is not None:
                if funding_task is not None:
                    realtime = funding_task.result()
                    saved_realtime = []
                    started = time.monotonic()
                    if not realtime.empty and not history_task.done():
                        # Let optional requests progress during mandatory disk I/O. Keep
                        # one writer; as soon as history arrives, combine remaining saves.
                        depth = _resolve_funding_depth(handle)
                        min_ts = _resolve_history_trim_ts(handle)
                        for symbol, current in realtime.groupby('symbol'):
                            if history_task.done():
                                break
                            await asyncio.to_thread(
                                _save_funding_with_trim,
                                handle.funding_mgr,
                                symbol,
                                run_time,
                                current,
                                depth,
                                min_ts,
                            )
                            saved_realtime.append(symbol)
                    if saved_realtime:
                        realtime = realtime.loc[~realtime.symbol.isin(saved_realtime)]
                        logger.info('等待历史期间已落盘实时资金费: 币种=%d 耗时=%.3fs',
                                    len(saved_realtime), time.monotonic() - started)
                else:
                    realtime = pd.DataFrame()
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
    depth = _resolve_funding_depth(handle)
    min_ts = _resolve_history_trim_ts(handle)
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
            _save_funding_with_trim(handle.funding_mgr, symbol, run_time, incoming, depth, min_ts)
        elif time.monotonic() < write_deadline:
            try:
                _save_funding_with_trim(handle.funding_mgr, symbol, run_time, incoming, depth, min_ts)
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


async def update_exchange_meta(handle: BmacHandle, run_time, *, include_realtime_funding: bool = True):
    """按交易所分派 meta 更新：Binance 走并发 funding_task + 8s 回补；OKX 保留 v2.0.2 逻辑。"""
    if getattr(handle, 'exchange', 'binance') == 'binance':
        if include_realtime_funding:
            await update_binance_meta(handle, run_time)
        else:
            await update_binance_meta(handle, run_time, include_realtime_funding=False)
        return

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        exginfo_spot = update_exginfo(handle, session, 'spot', run_time)
        exginfo_swap = update_exginfo(handle, session, 'swap', run_time)
        tasks = [exginfo_spot, exginfo_swap]
        if handle.fetch_funding_rate and include_realtime_funding:
            tasks.append(update_funding_rates(handle, session, run_time))
        await asyncio.gather(*tasks)


update_okx_meta = update_exchange_meta


async def update_meta(handle: BmacHandle, run_time, *, include_realtime_funding: bool = True):
    """按 handle.exchange 经交易所注册表分派 meta 更新（exginfo_spot/swap + 可选 funding）。"""
    adapter = get_adapter(handle.exchange)
    if include_realtime_funding:
        await adapter.update_meta(handle, run_time)
    else:
        await adapter.update_meta(handle, run_time, include_realtime_funding=False)


async def fetch_update_resample(
    handle: BmacHandle,
    binance_handle: MarketHandle,
    fetcher,
    trade_type,
    run_time,
    resample_inline: bool = True,
    symbols_subset=None,
    symbol_limits: Optional[dict[str, int]] = None,
):
    base_interval = handle.base_interval
    resample_interval = handle.resample_interval
    exg_mgr = handle.exg_mgr
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    resample_delta = convert_interval_to_timedelta(resample_interval)

    once_update_candles = binance_handle.once_update_candles
    resample_mgr = binance_handle.resample_mgr_map[offset_str] if resample_inline else None

    df_exginfo = exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list()

    if symbols_subset is not None:
        symbols_trading = [s for s in symbols_trading if s in symbols_subset]

    num_symbols = len(symbols_trading)
    if num_symbols == 0:
        return dict(), dict()

    name = exchange_display_name(getattr(handle, 'exchange', 'binance'))
    start_time = time.perf_counter()
    logger.info(f'开始更新{name} {trade_type} K 线, 交易对数量={num_symbols}, 当前时间={now_time_str()}')

    limits_map = symbol_limits or {}
    params_list = [
        {'symbol': s, 'interval': base_interval, 'limit': int(limits_map.get(s, once_update_candles))}
        for s in symbols_trading
    ]
    base_klines_list = await fetcher.batch_get_candle(params_list)

    symbol_base_df = dict()
    symbol_begin_time = dict()

    kline_count_1h = _resolve_kline_count_1h(handle, trade_type, run_time)
    min_ts = _resolve_history_trim_ts(handle)
    bound_num_candles = None if hasattr(handle, 'history_trim_ts') else kline_count_1h

    for symbol, df_base in zip(symbols_trading, base_klines_list):
        req_limit = int(limits_map.get(symbol, once_update_candles))
        window_truncated = len(df_base) >= req_limit
        if not df_base.empty and 'candle_begin_time' in df_base.columns:
            df_base = df_base[df_base['candle_begin_time'] < run_time]
        symbol_base_df[symbol] = df_base

        if not resample_inline:
            continue

        theoretical_start = run_time - kline_count_1h * resample_delta
        df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)

        if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
            df_resample = df_resample.iloc[1:]

        if min_ts is not None:
            first_begin_time, last_begin_time = update_resample_candle(
                resample_mgr,
                symbol,
                df_resample,
                bound_num_candles,
                run_time,
                resample_delta,
                min_ts=min_ts,
            )
        else:
            first_begin_time, last_begin_time = update_resample_candle(
                resample_mgr,
                symbol,
                df_resample,
                bound_num_candles,
                run_time,
                resample_delta,
            )
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


async def async_fetch_update_resample_candles(
    handle: BmacHandle,
    run_time: datetime,
    trade_type,
    resample_inline: bool = True,
    symbols_subset=None,
    symbol_limits: Optional[dict[str, int]] = None,
):
    if symbols_subset is not None and len(symbols_subset) == 0:
        return dict(), dict()

    market_handle: MarketHandle = _get_market_handle(handle, trade_type)

    try:
        async with create_aiohttp_session(handle.http_timeout_sec) as session:
            fetcher = create_fetcher(handle, trade_type, session)
            symbol_base_df, symbol_begin_time = await fetch_update_resample(
                handle,
                market_handle,
                fetcher,
                trade_type,
                run_time,
                resample_inline,
                symbols_subset=symbols_subset,
                symbol_limits=symbol_limits,
            )
    except Exception:
        logger.exception(f'{exchange_display_name(getattr(handle, "exchange", "binance"))} API 更新数据失败')
        return None

    return symbol_base_df, symbol_begin_time


def fetch_update_binance_candles(
    handle: BmacHandle,
    run_time: datetime,
    resample_inline: bool = True,
    symbols_subset=None,
    symbol_limits: Optional[dict[str, dict[str, int]]] = None,
):
    spot_subset = symbols_subset.get('spot') if symbols_subset is not None else None
    swap_subset = symbols_subset.get('swap') if symbols_subset is not None else None
    spot_limits = symbol_limits.get('spot') if symbol_limits is not None else None
    swap_limits = symbol_limits.get('swap') if symbol_limits is not None else None
    with ThreadPoolExecutor(max_workers=4) as exe:
        bn_spot = exe.submit(
            asyncio.run,
            async_fetch_update_resample_candles(
                handle, run_time, 'spot', resample_inline,
                symbols_subset=spot_subset, symbol_limits=spot_limits,
            ),
        )
        bn_swap = exe.submit(
            asyncio.run,
            async_fetch_update_resample_candles(
                handle, run_time, 'swap', resample_inline,
                symbols_subset=swap_subset, symbol_limits=swap_limits,
            ),
        )
        spot_result = bn_spot.result()
        swap_result = bn_swap.result()

    if spot_result is None or swap_result is None:
        return None

    symbol_base_df_spot, symbol_begin_time_spot = spot_result
    symbol_base_df_swap, symbol_begin_time_swap = swap_result

    return symbol_base_df_spot, symbol_base_df_swap, symbol_begin_time_spot, symbol_begin_time_swap


def update_base_candles(handle: BmacHandle, run_time, trade_type, symbol_dfs):
    if not symbol_dfs:
        return
    market_handle: MarketHandle = _get_market_handle(handle, trade_type)
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    asyncio.run(async_sleep_until_run_time(run_time + timedelta(minutes=2)))

    name = exchange_display_name(getattr(handle, 'exchange', 'binance'))
    logger.info(f'开始合并{name} {trade_type} {handle.base_interval} K 线, 当前时间={now_time_str()}')

    retention_bars = _resolve_base_5m_retention_bars(handle, trade_type)
    required_first_begin = run_time - retention_bars * base_delta

    for symbol, df_new in symbol_dfs.items():
        symbol_dir = market_handle.base_candle_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        ts_mgr.update(df_new)
        if hasattr(ts_mgr, 'trim'):
            ts_mgr.trim(required_first_begin, run_time)
        else:
            partitions = ts_mgr.list_partitions()
            if partitions:
                ts_mgr.trim_partition(partitions[0], required_first_begin, run_time)
    logger.ok(f'合并{name} {trade_type} {handle.base_interval} K 线成功, 当前时间={now_time_str()}')


def update_binance_base_candles(handle: BmacHandle, run_time: datetime, spot_dfs, swap_dfs):
    with ThreadPoolExecutor(max_workers=2) as exe:
        futures = []
        if spot_dfs:
            futures.append(exe.submit(update_base_candles, handle, run_time, 'spot', spot_dfs))
        if swap_dfs:
            futures.append(exe.submit(update_base_candles, handle, run_time, 'swap', swap_dfs))
        for fut in futures:
            fut.result()
