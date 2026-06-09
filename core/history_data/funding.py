import asyncio
import random
import time
from datetime import datetime, timedelta

import pandas as pd

from core.binance_fetcher_async import BinanceFetcher
from core.handle import BmacHandle
from core.utils.log_kit import logger
from core.utils.network import create_aiohttp_session
from core.utils.time import now_time_str

# 常量定义
FUNDING_RATE_BATCH_LIMIT = 1000
FUNDING_INCREMENTAL_BACKFILL_HOURS = 8


def _calc_symbol_funding_start_time(funding_mgr, symbol: str, global_start_time: datetime, backfill_hours: int):
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
    if time_series.empty:
        return global_start_time, "full"

    latest_local_time = time_series.max().to_pydatetime()
    incremental_start = latest_local_time - timedelta(hours=backfill_hours)
    incremental_start = incremental_start.replace(minute=0, second=0, microsecond=0)
    start_time = max(global_start_time, incremental_start)
    return start_time, "incremental"


async def async_download_funding_rates(handle: BmacHandle, run_time: datetime, symbols: list[str]):
    """异步下载历史资金费率数据。

    批量获取指定交易对的历史资金费率数据，支持断点续传和增量更新。
    采用分批获取策略，避免API限制，自动处理数据合并和去重。

    Args:
        handle: 包含API配置和数据管理器的句柄对象
        run_time: 当前运行时间，用于计算数据获取范围
        symbols: 需要获取资金费率数据的交易对列表
    """
    # 初始化API句柄和数据管理器
    binance_handle = handle.binance_swap
    funding_mgr = handle.funding_mgr

    t_start = time.perf_counter()

    # 1. 计算全局 start time = now - swap kline_count_1h 小时，然后 floor 取整
    global_start_time = run_time - timedelta(hours=handle.get_kline_count_1h('swap'))
    # Floor 取整到小时
    global_start_time = global_start_time.replace(minute=0, second=0, microsecond=0)

    # 为每个 symbol 初始化起始时间
    symbol_start_times = {}
    incremental_count = 0
    full_count = 0
    for symbol in symbols:
        start_time, mode = _calc_symbol_funding_start_time(
            funding_mgr, symbol, global_start_time, FUNDING_INCREMENTAL_BACKFILL_HOURS
        )
        symbol_start_times[symbol] = start_time
        if mode == "incremental":
            incremental_count += 1
        else:
            full_count += 1

    logger.info(
        f"获取历史资金费数据，"
        f"当前时间={now_time_str()}，"
        f"1h K线数量(swap): {handle.get_kline_count_1h('swap')}，"
        f"全局起始时间: {global_start_time}，"
        f"增量symbol数量: {incremental_count}，"
        f"全量symbol数量: {full_count}，"
        f"增量冗余小时数: {FUNDING_INCREMENTAL_BACKFILL_HOURS}"
    )

    completed_symbols = set()
    batch_count = 0

    async with create_aiohttp_session(30) as session:
        fetcher = BinanceFetcher(binance_handle.api_type, session, handle.proxy)

        # 2. 循环执行数据获取逻辑
        while len(completed_symbols) < len(symbols):
            batch_count += 1
            remaining_symbols = [s for s in symbols if s not in completed_symbols]

            # 2.1 对于所有剩余 symbols，每次 batch 获取从各自的 starttime 开始的 1000 条资金费数据
            tasks = []
            for symbol in remaining_symbols:
                start_ts = int(symbol_start_times[symbol].timestamp()) * 1000
                tasks.append(fetcher.get_hist_funding_rate(symbol, limit=FUNDING_RATE_BATCH_LIMIT, startTime=start_ts))

            results: list[pd.DataFrame] = await asyncio.gather(*tasks)

            # 2.2 遍历所有获得的资金费 DataFrame，使用 funding_mgr，对于每个 symbol 和已有的资金费数据合并
            batch_completed_symbols = []
            next_batch_start_times = {}

            for symbol, df_new in zip(remaining_symbols, results):
                if df_new.empty:
                    # 如果获取的数据为空，标记为完成
                    batch_completed_symbols.append(symbol)
                    continue

                # 获取现有数据并合并
                if funding_mgr.has_symbol(symbol):
                    df_existing = funding_mgr.read_candle(symbol)
                    df_merged = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_merged = df_new

                # 按照 candle_begin_time 排序去重
                df_merged.drop_duplicates("candle_begin_time", ignore_index=True, inplace=True)
                df_merged.sort_values("candle_begin_time", inplace=True, ignore_index=True)

                # 保存合并后的数据
                funding_mgr.set_candle(symbol, run_time, df_merged)

                # 2.3 如果某 symbol 这一batch获取的数量不足 FUNDING_RATE_BATCH_LIMIT，则标记为该symbol已经完成
                if len(df_new) < FUNDING_RATE_BATCH_LIMIT:
                    batch_completed_symbols.append(symbol)
                else:
                    # 2.4 对于这一batch 还没获取完成的 symbols 用 max(candle_begin_time) + 1h 作为下一batch的 start time
                    max_time = df_new["candle_begin_time"].max()
                    next_start_time = max_time + timedelta(hours=1)
                    next_batch_start_times[symbol] = next_start_time

            # 更新完成的交易对和下一批次的起始时间
            completed_symbols.update(batch_completed_symbols)
            symbol_start_times.update(next_batch_start_times)

            # 只在第一轮和有新的symbol更新完成时才打印进度信息
            if batch_count == 1 or len(batch_completed_symbols) > 0:
                progress = (len(completed_symbols) / len(symbols)) * 100
                time_elapsed = (time.perf_counter() - t_start) / 60
                logger.debug(
                    f"下载资金费数据，"
                    f"进度={progress:.1f}%({len(completed_symbols)}/{len(symbols)}), "
                    f"已耗时={time_elapsed:.1f}分钟, "
                    f"当前时间={now_time_str()}"
                )

            # 2.5 直到所有的 symbol 都更新完毕，结束循环；否则 sleep（5分钟 + 随机数）开始下一 batch 获取
            if len(completed_symbols) < len(symbols):
                sleep_time = 300 + random.uniform(30, 60)
                await asyncio.sleep(sleep_time)

    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"获取历史资金费率成功, 耗时={time_elapsed:.2f}分钟, 当前时间={now_time_str()}")
