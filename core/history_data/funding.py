import asyncio
from collections import deque
import random
import time
from datetime import datetime, timedelta

import pandas as pd

from core.exchange.fetcher_factory import create_fetcher
from core.exchange.rest.binance_fetcher import BinanceFetcher
from core.exchange.rest.okx_fetcher import OKX_FUNDING_HISTORY_LIMIT_CAP
from core.handle import BmacHandle
from core.strategy.funding_backfill import (
    REQUEST_LIMIT,
    WINDOW_SECONDS,
    recent_funding_window,
    settled_funding_hours,
)
from core.strategy.funding_hours import save_funding_hours
from core.utils.log_kit import logger
from core.utils.network import create_aiohttp_session
from core.utils.time import now_time_str


def _is_rate_limited(exc: BaseException) -> bool:
    status = getattr(exc, 'status_code', getattr(exc, 'status', None))
    return status == 429 or '429' in str(exc)

# 常量定义
FUNDING_RATE_BATCH_LIMIT = 1000
FUNDING_INCREMENTAL_BACKFILL_HOURS = 8
# fundingRate 与 fundingInfo 共享 500/5min/IP，一次性把上千个 symbol 全打出去会被 WAF 拦成 403。
# 实测并发 4 与 8 的吞吐都在 5 次/秒左右（瓶颈在代理），这里取 4 压住瞬时并发；
# 若仍出现 429/403，把该值降到 2
FUNDING_MAX_CONCURRENCY = 4
# OKX 已由 fetcher 内的 'public' 限速窗口控频，批次间只需短暂停顿
_OKX_BATCH_SLEEP_SEC = 2
# 非 429 异常：同一 symbol 连续失败此次数则放弃（成功一次即清零）；429 不计次
_MAX_SYMBOL_FAILURES = 3


def _resolve_swap_kline_count_1h(handle: BmacHandle, run_time: datetime) -> int:
    if hasattr(handle, 'effective_kline_count_1h'):
        return int(handle.effective_kline_count_1h('swap', run_time))
    if hasattr(handle, 'get_kline_count_1h'):
        return int(handle.get_kline_count_1h('swap'))
    return int(getattr(handle, 'kline_count_1h', 15000))


def _resolve_swap_api_type(handle: BmacHandle) -> str:
    if hasattr(handle, 'get_market_handle'):
        return handle.get_market_handle('swap').api_type
    return handle.binance_swap.api_type


def _calc_symbol_funding_start_time(funding_mgr, symbol, global_start_time, recent_start, observed_until):
    """计算单个交易对的资金费历史下载起始时间。

    返回:
        tuple[start_time, mode]
        - start_time: 实际下载起始时间
        - mode: "incremental" 或 "full"
    """
    if not funding_mgr.has_symbol(symbol):
        return global_start_time, "full"

    try:
        df_existing = funding_mgr.read_candle(symbol)
    except Exception:
        return global_start_time, "full"

    if df_existing.empty or "candle_begin_time" not in df_existing.columns:
        return global_start_time, "full"

    time_series = pd.to_datetime(df_existing["candle_begin_time"], utc=True, errors="coerce").dropna()
    # The nextFundingTime forecast may lie after this snapshot's observed history.
    if observed_until is not None:
        time_series = time_series.loc[time_series <= observed_until]
    if time_series.empty:
        return global_start_time, "full"

    latest_local_time = time_series.max().to_pydatetime()
    incremental_start = latest_local_time - timedelta(hours=FUNDING_INCREMENTAL_BACKFILL_HOURS)
    incremental_start = incremental_start.replace(minute=0, second=0, microsecond=0)
    if recent_start is not None:
        start_time = max(global_start_time, min(incremental_start, recent_start))
    else:
        start_time = max(global_start_time, incremental_start)
    return start_time, "incremental"


async def _download_okx_funding_rates(handle: BmacHandle, run_time: datetime, symbols: list[str]):
    funding_mgr = handle.funding_mgr
    t_start = time.perf_counter()
    kline_count_1h = _resolve_swap_kline_count_1h(handle, run_time)
    global_start_time = (run_time - timedelta(hours=kline_count_1h)).replace(minute=0, second=0, microsecond=0)

    symbol_start_times = {}
    incremental_count = 0
    full_count = 0
    for symbol in symbols:
        start_time, mode = _calc_symbol_funding_start_time(
            funding_mgr, symbol, global_start_time, None, None
        )
        symbol_start_times[symbol] = start_time
        if mode == "incremental":
            incremental_count += 1
        else:
            full_count += 1

    logger.info(
        f"获取历史资金费数据(OKX)，"
        f"当前时间={now_time_str()}，"
        f"1h K线数量(swap): {kline_count_1h}，"
        f"全局起始时间: {global_start_time}，"
        f"增量symbol数量: {incremental_count}，"
        f"全量symbol数量: {full_count}，"
        f"增量冗余小时数: {FUNDING_INCREMENTAL_BACKFILL_HOURS}"
    )

    completed_symbols = set()
    batch_count = 0
    failures: dict[str, int] = {}
    skipped: set[str] = set()
    batch_limit = OKX_FUNDING_HISTORY_LIMIT_CAP

    async with create_aiohttp_session(30) as session:
        fetcher = create_fetcher(handle, 'swap', session)
        while len(completed_symbols) < len(symbols):
            batch_count += 1
            remaining_symbols = [s for s in symbols if s not in completed_symbols]
            tasks = [
                fetcher.get_hist_funding_rate(
                    symbol, limit=batch_limit, startTime=int(symbol_start_times[symbol].timestamp()) * 1000
                )
                for symbol in remaining_symbols
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            batch_completed_symbols = []
            next_batch_start_times = {}
            for symbol, df_new in zip(remaining_symbols, results):
                if isinstance(df_new, BaseException):
                    if not isinstance(df_new, Exception):
                        raise df_new
                    if not _is_rate_limited(df_new):
                        failures[symbol] = failures.get(symbol, 0) + 1
                        if failures[symbol] >= _MAX_SYMBOL_FAILURES:
                            skipped.add(symbol)
                            batch_completed_symbols.append(symbol)
                            logger.warning(f'资金费 {symbol} 连续失败 {failures[symbol]} 次，跳过: {df_new!r}')
                    continue
                failures.pop(symbol, None)

                if df_new.empty:
                    batch_completed_symbols.append(symbol)
                    continue

                if funding_mgr.has_symbol(symbol):
                    df_existing = funding_mgr.read_candle(symbol)
                    df_merged = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_merged = df_new

                df_merged.drop_duplicates("candle_begin_time", keep="last", ignore_index=True, inplace=True)
                df_merged.sort_values("candle_begin_time", inplace=True, ignore_index=True)
                funding_mgr.set_candle(symbol, run_time, df_merged)

                if len(df_new) < batch_limit:
                    batch_completed_symbols.append(symbol)
                else:
                    next_batch_start_times[symbol] = df_new["candle_begin_time"].max()

            completed_symbols.update(batch_completed_symbols)
            symbol_start_times.update(next_batch_start_times)

            if len(completed_symbols) < len(symbols):
                await asyncio.sleep(_OKX_BATCH_SLEEP_SEC)

    if skipped:
        logger.warning(f"资金费: {len(skipped)} 个 symbol 连续失败已跳过，下次重启时会继续补: {sorted(skipped)}")
    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"获取历史资金费率成功, 耗时={time_elapsed:.2f}分钟, 当前时间={now_time_str()}")


async def async_download_funding_rates(handle: BmacHandle, run_time: datetime, symbols: list[str]):
    """异步下载历史资金费率数据。

    批量获取指定交易对的历史资金费率数据，支持断点续传和增量更新。
    Binance 路径保留并发 4 + 滚动 450/300s 限流 + settled_funding_hours + 原子 save_funding_hours；
    OKX 路径沿用 v2.0.2 批次下载与按 symbol 容错。
    """
    if getattr(handle, 'exchange', 'binance') == 'okx':
        await _download_okx_funding_rates(handle, run_time, symbols)
        return

    funding_mgr = handle.funding_mgr
    t_start = time.perf_counter()

    # 1. 计算全局 start time = now - effective_kline_count_1h('swap', run_time) 小时，然后 floor 取整
    kline_count_1h = _resolve_swap_kline_count_1h(handle, run_time)
    global_start_time = run_time - timedelta(hours=kline_count_1h)
    global_start_time = global_start_time.replace(minute=0, second=0, microsecond=0)
    recent_ms, end_ms = recent_funding_window(run_time)
    recent_start = pd.Timestamp(recent_ms, unit='ms', tz='UTC')
    observed_until = pd.Timestamp(end_ms, unit='ms', tz='UTC')
    min_ts = handle.history_trim_ts() if hasattr(handle, 'history_trim_ts') else None

    symbol_start_times = {}
    incremental_count = 0
    full_count = 0
    for symbol in symbols:
        start_time, mode = _calc_symbol_funding_start_time(
            funding_mgr, symbol, global_start_time, recent_start, observed_until
        )
        symbol_start_times[symbol] = start_time
        if mode == "incremental":
            incremental_count += 1
        else:
            full_count += 1

    logger.info(
        f"获取历史资金费数据，"
        f"当前时间={now_time_str()}，"
        f"1h K线数量(swap): {kline_count_1h}，"
        f"全局起始时间: {global_start_time}，"
        f"增量symbol数量: {incremental_count}，"
        f"全量symbol数量: {full_count}，"
        f"增量冗余小时数: {FUNDING_INCREMENTAL_BACKFILL_HOURS}"
    )

    # Bound peak memory by keeping at most four symbols' pages in flight. The
    # full hour may straddle pages, so combine once, after reaching endTime.
    pending = iter(symbol_start_times.items())
    request_times = deque()
    completed = 0
    last_progress = t_start

    async with create_aiohttp_session(30) as session:
        fetcher = BinanceFetcher(_resolve_swap_api_type(handle), session, handle.proxy)

        async def reserve_request():
            # Called for every HTTP attempt, including retries. No await between
            # the capacity check and reservation on the shared event loop.
            while True:
                now = time.monotonic()
                while request_times and request_times[0] <= now - WINDOW_SECONDS:
                    request_times.popleft()
                if len(request_times) < REQUEST_LIMIT:
                    request_times.append(now)
                    return
                await asyncio.sleep(request_times[0] + WINDOW_SECONDS - now)

        async def worker():
            nonlocal completed, last_progress
            for symbol, start in pending:
                cursor = int(start.timestamp() * 1000)
                pages = []
                while cursor <= end_ms:
                    page = await fetcher.get_hist_funding_rate(
                        symbol, limit=FUNDING_RATE_BATCH_LIMIT, startTime=cursor, endTime=end_ms,
                        _reserve_request=reserve_request)
                    if page.empty:
                        break
                    pages.append(page)
                    if len(page) < FUNDING_RATE_BATCH_LIMIT:
                        break
                    cursor = page['candle_begin_time'].max().value // 1_000_000 + 1

                if pages:
                    settled = settled_funding_hours(pd.concat(pages, ignore_index=True))
                    save_funding_hours(funding_mgr, symbol, run_time, settled, None, min_ts=min_ts)
                completed += 1
                if time.perf_counter() - last_progress >= 30:
                    logger.info('下载资金费数据，进度=%d/%d', completed, len(symbol_start_times))
                    last_progress = time.perf_counter()

        async with asyncio.TaskGroup() as group:
            for _ in range(min(len(symbol_start_times), FUNDING_MAX_CONCURRENCY)):
                group.create_task(worker())

    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"获取历史资金费率成功, 耗时={time_elapsed:.2f}分钟, 当前时间={now_time_str()}")
