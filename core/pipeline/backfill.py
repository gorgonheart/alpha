# -*- coding: utf-8 -*-
"""
首次启动的“空档补算”。

`update_all()` 只有 run_time.minute 命中 enabled_hour_offsets 时才更新数据并写 ready。
`init_history()` 结束后，除 init 那个分钟归属的 offset 外，其余 offset 既没有 resample 的
run_time ready，也没有 preprocess 产物，消费者读不到任何东西，只能干等下一个 offset 分钟。

本模块在未命中正常offset的基础周期调度点被调用：挑出最近即将到来的一个缺ready的offset，
用磁盘5m数据重采样和预处理，复用最新完整的共享exchangeInfo，缺失时补取一次。
"""
import asyncio
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from copy import copy
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from core.exchange.symbol_match import update_symbol_match
from core.handle import BmacHandle
from core.pipeline.data_update import update_binance_meta, update_meta
from core.pipeline.resample import calc_resample, update_resample_candle, update_resample_candle_incremental
from core.preprocess import preprocess_offset_data
from core.storage.candle_manager import CandleFileManager
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.time import (
    convert_interval_to_timedelta,
    get_offset_from_runtime,
    now_time,
    now_time_str,
)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 每个空档只补最近即将到来的一个缺失offset。
MAX_OFFSETS_PER_WINDOW = 1
# 距离下一个 offset 分钟不足这么多秒时，不再开始新的补算（preprocess 本身要几分钟）
MIN_REMAIN_SECONDS = 240
# 同一个 offset 失败后多久才重试，避免每分钟都重跑一遍重活
RETRY_COOLDOWN_SECONDS = 1800
# 本地重采样的并发线程数
RESAMPLE_THREADS = 8
# 探测磁盘 5m 数据新鲜度用的探针交易对（只读最后一个分区）
FRESHNESS_PROBE_SYMBOLS = ('BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BTCUSDC')
# 探针交易对都不存在时，退化为取前几个有数据的目录
FRESHNESS_FALLBACK_PROBES = 3

# offset -> 上次尝试时间（只在主进程里有意义）
_last_attempt: dict[str, datetime] = {}


def _get_market_handle(handle: BmacHandle, trade_type: str):
    if hasattr(handle, 'get_market_handle'):
        return handle.get_market_handle(trade_type)
    return handle.get_binance_handle(trade_type)


def _resolve_kline_count_1h(handle: BmacHandle, trade_type: str, run_time: datetime) -> int:
    if hasattr(handle, 'effective_kline_count_1h'):
        return int(handle.effective_kline_count_1h(trade_type, run_time))
    if hasattr(handle, 'get_kline_count_1h'):
        return int(handle.get_kline_count_1h(trade_type))
    return int(getattr(handle, 'kline_count_1h', 15000))


def pending_offsets(handle: BmacHandle) -> list[str]:
    """返回还没有 preprocess ready 的 offset 列表（主进程调用，只做 glob，很轻）。"""
    if handle.preprocess_handle is None or not handle.enabled_hour_offsets:
        return []

    missing = [offset for offset in handle.enabled_hour_offsets if not offset_ready(handle, offset)]
    return missing


def offset_ready(handle: BmacHandle, offset: str) -> bool:
    """该 offset 是否已经产出过 spot_dict / swap_dict 的 ready，即预处理已完成。"""
    mgr = handle.preprocess_handle.preprocess_mgr_map.get(offset)
    if mgr is None:
        return True
    base_dir = mgr.base_dir
    spot_ready = next(base_dir.glob('spot_dict_*.ready'), None)
    swap_ready = next(base_dir.glob('swap_dict_*.ready'), None)
    return spot_ready is not None and swap_ready is not None


def latest_offset_run_time(offset: str, base_interval: str, now: datetime):
    """该 offset 最近一个已经到达的时间边界；不是基础周期整数倍时返回 None。"""
    try:
        minute = int(offset.rstrip('m'))
    except ValueError:
        return None

    base_minutes = int(convert_interval_to_timedelta(base_interval).total_seconds() // 60)
    if base_minutes <= 0 or minute % base_minutes != 0:
        return None

    current = now.replace(second=0, microsecond=0)
    return current - timedelta(minutes=(current.minute - minute) % 60)


def seconds_to_next_offset(handle: BmacHandle, now: datetime) -> float:
    """距离下一个 offset 分钟还有多少秒，用于给补算卡时间预算。"""
    current = now.replace(second=0, microsecond=0)
    for i in range(1, 61):
        candidate = current + timedelta(minutes=i)
        if f'{candidate.minute}m' in handle.enabled_hour_offsets:
            return (candidate - now).total_seconds()
    return float('inf')


def newest_base_time(market_handle) -> datetime | None:
    """磁盘上基础周期数据最新一根的 candle_begin_time。

    只读最后一个分区，开销与交易对数量无关：先试主流币探针，都不存在时退化为任意几个有数据的目录。
    """
    base_dir = market_handle.base_candle_dir
    if not base_dir.exists():
        return None

    probes = [base_dir / symbol for symbol in FRESHNESS_PROBE_SYMBOLS]
    probes = [path for path in probes if path.is_dir()]
    if not probes:
        probes = sorted((item for item in base_dir.iterdir() if item.is_dir()),
                        key=lambda item: item.name)[:FRESHNESS_FALLBACK_PROBES]

    newest = None
    for symbol_dir in probes:
        ts_mgr = TSManager(symbol_dir)
        partitions = ts_mgr.list_partitions()
        if not partitions:
            continue
        df = ts_mgr.read_partition(partitions[-1])
        if df is None or df.empty:
            continue
        candidate = df['candle_begin_time'].max()
        if newest is None or candidate > newest:
            newest = candidate
    return newest


def is_base_data_fresh(handle: BmacHandle, trade_type: str, run_time: datetime) -> tuple[bool, datetime | None]:
    """磁盘基础数据是否已覆盖到 run_time 需要的最后一根。"""
    newest = newest_base_time(_get_market_handle(handle, trade_type))
    if newest is None:
        return False, None
    return newest + convert_interval_to_timedelta(handle.base_interval) >= run_time, newest


def maybe_backfill(handle: BmacHandle) -> float:
    """在空闲等待里补算缺失的 offset，返回本次耗时（秒）。

    主进程只做轻量检查并维护重试冷却，真正的重活丢给子进程，避免主进程内存被撑爆。
    """
    missing = pending_offsets(handle)
    targets = [offset for offset in missing if not _in_cooldown(offset)]
    if not targets:
        return 0.0

    remain = seconds_to_next_offset(handle, now_time())
    if remain < MIN_REMAIN_SECONDS:
        logger.info(f'距离下一个 offset 只剩 {remain:.0f}s，本次不补算，当前时间={now_time_str()}')
        return 0.0

    now = now_time()
    targets.sort(key=lambda offset: (int(offset.rstrip('m')) - now.minute) % 60 or 60)
    targets = targets[:MAX_OFFSETS_PER_WINDOW]
    logger.info(f'检测到 Offset 缺少预处理 ready: {missing}，'
                f'本次尝试补算: {targets}，当前时间={now_time_str()}')

    started = time.perf_counter()
    for offset in targets:
        _last_attempt[offset] = now_time()

    with ProcessPoolExecutor(max_workers=1) as exe:
        status = exe.submit(run_backfill, handle, targets).result()

    # stale / skip 下个5分钟调度点再检查；只有真出错才冷却半小时。
    for offset, state in (status or {}).items():
        if state in ('stale', 'skip'):
            _last_attempt.pop(offset, None)

    return time.perf_counter() - started


def run_backfill(handle: BmacHandle, targets: list[str]) -> dict[str, str]:
    """在子进程里逐个 offset 补算（本地重采样 + 预处理），返回 offset -> 状态。"""
    status = dict()
    for offset in targets:
        run_time = latest_offset_run_time(offset, handle.base_interval, now_time())
        if run_time is None:
            logger.warning(f'Offset={offset} 不是基础周期 {handle.base_interval} 的整数倍分钟，跳过')
            status[offset] = 'skip'
            continue

        expected_offset = get_offset_from_runtime(run_time, handle.resample_interval)
        if expected_offset != offset:
            logger.warning(f'Offset={offset} 与 run_time={run_time} 反推的 {expected_offset} 不一致，跳过补算，'
                           f'请检查 enabled_hour_offsets 与 resample_interval 的配置')
            status[offset] = 'skip'
            continue

        remain = seconds_to_next_offset(handle, now_time())
        if remain < MIN_REMAIN_SECONDS:
            logger.info(f'距离下一个 offset 只剩 {remain:.0f}s，本次跳过 Offset={offset} 补算')
            status[offset] = 'skip'
            continue

        try:
            status[offset] = backfill_one_offset(handle, offset, run_time)
        except Exception as exc:
            logger.error(f'Offset={offset} run_time={run_time} 补算失败: {exc!r}')
            logger.debug(traceback.format_exc())
            status[offset] = 'error'
    return status


def backfill_one_offset(handle: BmacHandle, offset: str, run_time: datetime) -> str:
    """用当前磁盘数据补齐一个 offset 的 resample ready 与 preprocess 产物。"""
    logger.info(f'开始补算 Offset={offset}, run_time={run_time}（用磁盘现有数据，不重新拉 K 线）')

    for trade_type in ('spot', 'swap'):
        fresh, newest = is_base_data_fresh(handle, trade_type, run_time)
        if not fresh:
            logger.info(f'Offset={offset} run_time={run_time} 暂不补算：{trade_type} 磁盘最新 {handle.base_interval} '
                        f'K 线为 {newest}，还没覆盖到该 run_time，等下个空档重试')
            return 'stale'

    # Reuse the latest complete shared pair, independent of the historical candle time.
    rounds = [{p.stem.rsplit('_', 1)[-1]
               for p in handle.exg_mgr.base_dir.glob(f'exginfo_{kind}_*.ready')}
              for kind in ('spot', 'swap')]
    if not (rounds[0] & rounds[1]) or not all(
            handle.exg_mgr.has_symbol(f'exginfo_{kind}') for kind in ('spot', 'swap')):
        logger.info(f'Offset={offset} 共享 exchangeInfo 缺失或不完整，主动更新一次现货/合约信息')
        metadata_handle = copy(handle)
        metadata_handle.fetch_funding_rate = False
        if getattr(handle, 'exchange', 'binance') == 'binance':
            asyncio.run(update_binance_meta(metadata_handle, now_time()))
        else:
            asyncio.run(update_meta(metadata_handle, now_time()))
        remain = seconds_to_next_offset(handle, now_time())
        if remain < MIN_REMAIN_SECONDS:
            logger.info(f'共享 exchangeInfo 已更新，距离下个offset只剩{remain:.0f}s，暂不开始补算')
            return 'stale'

    with TemporaryDirectory(prefix='alpha-backfill-exg-') as folder:
        local = copy(handle)
        if not hasattr(local, 'exchange'):
            local.exchange = 'binance'
        local.exg_mgr = CandleFileManager(Path(folder), save_type='pickle')
        for kind in ('spot', 'swap'):
            local.exg_mgr.set_candle(f'exginfo_{kind}', run_time,
                                     handle.exg_mgr.read_candle(f'exginfo_{kind}'))
        logger.info(f'Offset={offset} 复用最新共享 exchangeInfo（当前交易池）')
        update_symbol_match(local, run_time)
        begin_time_map = {}
        for trade_type in ('spot', 'swap'):
            begin_time_map[trade_type] = local_resample_offset(local, trade_type, run_time, offset)
        preprocess_offset_data(local, run_time, begin_time_map['spot'], begin_time_map['swap'])
    logger.ok(f'补算 Offset={offset} 完成, run_time={run_time}, 当前时间={now_time_str()}')
    return 'ok'


def _truncate_resample_before(resample_mgr, symbol: str, run_time: datetime) -> bool:
    """把该 offset 目录里已有的、覆盖到 run_time 之后的数据截断掉，返回是否发生了截断。"""
    if not resample_mgr.has_symbol(symbol):
        return False

    df_old = resample_mgr.read_candle(symbol)
    if df_old is None or df_old.empty or 'candle_begin_time' not in df_old.columns:
        return False
    if df_old['candle_begin_time'].max() < run_time:
        return False

    resample_mgr.save_data_file(symbol, df_old[df_old['candle_begin_time'] < run_time])
    return True


def local_resample_offset(handle: BmacHandle, trade_type: str, run_time: datetime, offset: str):
    """用基础周期数据本地重采样出该 offset 的 resample 数据，返回 symbol -> (首根, 末根) 时间。"""
    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr = market_handle.resample_mgr_map[offset]
    resample_delta = convert_interval_to_timedelta(handle.resample_interval)
    num_candles = _resolve_kline_count_1h(handle, trade_type, run_time)
    min_ts = handle.history_trim_ts() if hasattr(handle, 'history_trim_ts') else None
    bound_num_candles = None if hasattr(handle, 'history_trim_ts') else num_candles
    theoretical_start = run_time - num_candles * resample_delta
    base_dir = market_handle.base_candle_dir

    symbols = sorted(item.name for item in base_dir.iterdir() if item.is_dir())
    started = time.perf_counter()
    truncated = []

    def _one(symbol):
        df = TSManager(base_dir / symbol).read_all()
        if df is None or df.empty:
            return None

        # 与线上一致：只用 run_time 之前的数据，避免把“未来”K 线算进这个 run_time
        df = df[df['candle_begin_time'] < run_time]
        if df.empty:
            return None

        df_resample = calc_resample(df, handle.resample_interval, offset, theoretical_start)
        if df_resample.empty:
            return None

        # 5m 存储只保留近 30d，若已有 1h 历史且首根重采样桶被 5m 窗口截断（首根 5m 晚于桶起点），
        # 丢弃首根避免用不完整 K 线覆盖已有完整历史。
        if (resample_mgr.has_symbol(symbol) and len(df_resample) > 1
                and df['candle_begin_time'].min() > df_resample['candle_begin_time'].iloc[0]):
            df_resample = df_resample.iloc[1:]

        # 目录里若残留更晚 run_time 的数据，先截断，否则合并后会把未来 K 线带进产物
        if _truncate_resample_before(resample_mgr, symbol, run_time):
            truncated.append(symbol)

        first_begin_time, last_begin_time = update_resample_candle_incremental(
            resample_mgr,
            symbol,
            df_resample,
            bound_num_candles,
            run_time,
            resample_delta,
            min_ts=min_ts,
        )
        if first_begin_time is None or last_begin_time is None:
            return None

        return symbol, first_begin_time, last_begin_time

    begin_time_map = {}
    with ThreadPoolExecutor(max_workers=RESAMPLE_THREADS) as pool:
        for item in pool.map(_one, symbols):
            if item is not None:
                begin_time_map[item[0]] = (item[1], item[2])

    if truncated:
        logger.info(f'{trade_type} Offset={offset} 有 {len(truncated)} 个交易对的旧 resample 数据晚于 '
                    f'run_time={run_time}，已截断后再合并')

    logger.ok(f'本地重采样 {trade_type} Offset={offset} 完成, 有效交易对={len(begin_time_map)}/{len(symbols)}, '
              f'覆盖到={max((item[1] for item in begin_time_map.values()), default=None)}, '
              f'耗时={time.perf_counter() - started:.1f}秒, 当前时间={now_time_str()}')
    return begin_time_map


def _in_cooldown(offset: str) -> bool:
    last = _last_attempt.get(offset)
    if last is None:
        return False
    return (now_time() - last).total_seconds() < RETRY_COOLDOWN_SECONDS
