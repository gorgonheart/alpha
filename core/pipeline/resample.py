import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

from core.handle import BmacHandle
from core.storage.candle_manager import CandleFileManager
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.time import (convert_interval_to_timedelta,
                             get_offset_from_runtime, now_time_str)
from core.ws.ws_local_cache import read_ws_cache_market_windows

_RESAMPLE_BOUNDS_LOCK = threading.Lock()
# Key: (str(resample_mgr.base_dir), symbol) -> (st_mtime_ns, first_begin_time, last_begin_time)
_RESAMPLE_BOUNDS_CACHE: dict[tuple[str, str], tuple[int, pd.Timestamp, pd.Timestamp]] = {}


def _record_resample_bounds(
    candle_mgr: CandleFileManager,
    symbol: str,
    first_ts: Optional[pd.Timestamp],
    last_ts: Optional[pd.Timestamp],
) -> Optional[tuple[int, pd.Timestamp, pd.Timestamp]]:
    if first_ts is None or last_ts is None:
        return None
    try:
        p = candle_mgr.format_data_file_path(symbol)
        mtime_ns = os.stat(p).st_mtime_ns
    except OSError:
        return None
    entry = (mtime_ns, first_ts, last_ts)
    with _RESAMPLE_BOUNDS_LOCK:
        _RESAMPLE_BOUNDS_CACHE[(str(candle_mgr.base_dir), symbol)] = entry
    return entry


def _get_cached_resample_bounds(
    candle_mgr: CandleFileManager, symbol: str
) -> Optional[tuple[int, pd.Timestamp, pd.Timestamp]]:
    key = (str(candle_mgr.base_dir), symbol)
    with _RESAMPLE_BOUNDS_LOCK:
        entry = _RESAMPLE_BOUNDS_CACHE.get(key)
    if entry is None:
        return None
    try:
        p = candle_mgr.format_data_file_path(symbol)
        if os.stat(p).st_mtime_ns != entry[0]:
            return None
    except OSError:
        return None
    return entry


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


def _resolve_history_trim_ts(handle: BmacHandle) -> Optional[pd.Timestamp]:
    if hasattr(handle, 'history_trim_ts'):
        return handle.history_trim_ts()
    return None


def _resolve_base_5m_retention_bars(handle: BmacHandle, base_delta: timedelta) -> int:
    if hasattr(handle, 'base_5m_retention_bars'):
        return int(handle.base_5m_retention_bars())
    retention_days = int(getattr(handle, 'base_5m_retention_days', 30))
    bars_per_day = int(timedelta(days=1) // base_delta)
    return retention_days * bars_per_day


def _apply_history_bound(
    df_total: pd.DataFrame,
    num_candles: Optional[int],
    min_ts: Optional[pd.Timestamp],
) -> pd.DataFrame:
    if min_ts is not None:
        return df_total[df_total['candle_begin_time'] >= min_ts]
    if num_candles is not None:
        return df_total.tail(num_candles)
    return df_total


def calc_resample(df: pd.DataFrame, resample_interval: str, offset_str: str,
                  theoretical_start: datetime) -> pd.DataFrame:
    # pandas 中 5m 代表 5 个月，需要转化成 5min
    if offset_str[-1] == 'm' and offset_str[:-1].isdigit():
        offset_str = offset_str.replace('m', 'min')

    df1 = df.copy()
    df1['_actual_first_time'] = df1['candle_begin_time']

    # 根据指定的周期对数据进行 resample，并指定如何聚合各列数据
    df_resample = df1.resample(resample_interval, offset=offset_str, on='candle_begin_time').agg({
        '_actual_first_time': 'first',
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'quote_volume': 'sum',
        'trade_num': 'sum',
        'taker_buy_base_asset_volume': 'sum',
        'taker_buy_quote_asset_volume': 'sum',
    }).reset_index()

    # 用实际首条数据时间过滤（老币因下载截断导致的不完整首根会被过滤，新币数据全量保留）
    df_resample = df_resample[df_resample['_actual_first_time'] >= theoretical_start]
    df_resample.drop(columns=['_actual_first_time'], inplace=True)

    return df_resample


def fill_gaps(df: pd.DataFrame, delta: pd.Timedelta) -> pd.DataFrame:
    # 创建一个从开始到结束无空缺的基准
    first = df['candle_begin_time'].min()
    last = df['candle_begin_time'].max()
    time_range = pd.date_range(first, last, freq=delta)

    # 无空缺，不需要填充
    if len(df) == len(time_range) and (df['candle_begin_time'].reset_index(drop=True) == time_range).all():
        return df

    # 记录填补前的整数列 dtype：how='left' merge 会对缺失行注入 NaN，把整数列提升为
    # float64，fillna(0) 填值后 dtype 不回落。整数列（如 trade_num）本应保持 int64，
    # 填补完成、无 NaN 后再 astype 还原，避免下游 Arrow 单 schema 对账出现 dtype 分歧。
    int_dtypes = {col: dt for col, dt in df.dtypes.items() if pd.api.types.is_integer_dtype(dt)}

    benchmark = pd.DataFrame({'candle_begin_time': time_range})

    # 与基准数据合并
    df = pd.merge(left=benchmark, right=df, on='candle_begin_time', how='left', sort=True)

    # 用前一天的收盘价填充价格
    df['close'] = df['close'].ffill()
    df['open'] = df['open'].fillna(df['close'])
    df['high'] = df['high'].fillna(df['close'])
    df['low'] = df['low'].fillna(df['close'])

    # 用开盘价填充 VWAP
    if 'avg_price_1m' in df.columns:
        df['avg_price_1m'] = df['avg_price_1m'].fillna(df['open'])

    # 用 0 填充成交量
    df['volume'] = df['volume'].fillna(0)
    df['quote_volume'] = df['quote_volume'].fillna(0)
    df['trade_num'] = df['trade_num'].fillna(0)
    df['taker_buy_base_asset_volume'] = df['taker_buy_base_asset_volume'].fillna(0)
    df['taker_buy_quote_asset_volume'] = df['taker_buy_quote_asset_volume'].fillna(0)

    # 还原整数列 dtype（此时缺失行已 fillna，无 NaN，astype 安全）
    for col, dt in int_dtypes.items():
        df[col] = df[col].astype(dt)

    df['candle_end_time'] = df['candle_begin_time'] + delta
    df.set_index('candle_end_time', inplace=True)
    return df


def filter_gaps(df: pd.DataFrame, gap_hours_thres=48, price_change_thres=0.1):
    """
    去除（因代币拆分）重上架前的数据
    默认下架 48 小时并且价格变动绝对值超过 10%
    """
    price_change = (df['open'] / df['close'].shift().fillna(df['open']) - 1).fillna(0)
    time_gap = df['candle_begin_time'].diff().fillna(timedelta(minutes=0))
    cond = (time_gap > timedelta(hours=gap_hours_thres)) & (price_change.abs() > price_change_thres)

    if cond.any():
        last_idx = cond[cond].index[-1]
        df = df.loc[last_idx:].copy()

    return df


def _extract_tail_frame(df_total: pd.DataFrame, return_tail_bars: int) -> pd.DataFrame:
    tail = df_total.tail(return_tail_bars)
    if isinstance(tail.index, pd.DatetimeIndex):
        return tail.reset_index(drop=True)
    return tail.reset_index(drop=True)


def update_resample_candle(
    candle_mgr: CandleFileManager,
    symbol: str,
    df_resample: pd.DataFrame,
    num_candles: Optional[int],
    run_time: datetime,
    resample_delta: timedelta,
    *,
    min_ts: Optional[pd.Timestamp] = None,
    return_tail_bars: int = 0,
    df_old_preloaded: Optional[pd.DataFrame] = None,
):
    # 读取历史数据，不存在为 None
    df_old = df_old_preloaded
    if df_old is None and candle_mgr.has_symbol(symbol):
        df_old = candle_mgr.read_candle(symbol)

    if df_old is not None and df_old.empty:
        df_old = None

    # 合并历史数据和实时数据
    df_total = pd.concat([df_old, df_resample]) if df_old is not None else df_resample

    if df_total.empty:
        candle_mgr.set_candle(symbol, run_time, df_total)
        return (None, None, None) if return_tail_bars > 0 else (None, None)

    # 去重
    df_total.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)

    # 排序
    df_total.sort_values('candle_begin_time', inplace=True)

    # 重置索引，避免重复索引导致切片错误
    df_total.reset_index(drop=True, inplace=True)

    # 过滤（代币拆分导致的）大的空缺
    df_total = filter_gaps(df_total)

    # 填充（交易所宕机等导致的）小的空缺
    df_total = fill_gaps(df_total, resample_delta)

    # 按显式 min_ts 或旧 num_candles 裁剪历史头部（均为空时不裁头）
    df_total = _apply_history_bound(df_total, num_candles, min_ts)

    # 写入硬盘
    candle_mgr.set_candle(symbol, run_time, df_total)

    if df_total.empty:
        return (None, None, None) if return_tail_bars > 0 else (None, None)

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]
    _record_resample_bounds(candle_mgr, symbol, first_begin_time, last_begin_time)

    if return_tail_bars > 0:
        # 仅在无历史文件（冷启动）或历史头部未被 filter_gaps 拆分截断且总长不超过 tail 窗口时返回内存尾部；
        # 一旦 48h 拆分过滤改写了 first_begin_time，必须返回 None 让下游重读完整 1h 文件。
        no_head_truncation = (
            df_old is None
            or (
                'candle_begin_time' in df_old.columns
                and not df_old.empty
                and first_begin_time == df_old['candle_begin_time'].iloc[0]
                and len(df_total) <= return_tail_bars
            )
        )
        tail_out = _extract_tail_frame(df_total, return_tail_bars) if no_head_truncation else None
        return first_begin_time, last_begin_time, tail_out
    return first_begin_time, last_begin_time


def update_resample_candle_incremental(
    candle_mgr: CandleFileManager,
    symbol: str,
    df_resample: pd.DataFrame,
    num_candles: Optional[int],
    run_time: datetime,
    resample_delta: timedelta,
    *,
    min_ts: Optional[pd.Timestamp] = None,
    return_tail_bars: int = 0,
):
    if not candle_mgr.has_symbol(symbol):
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars,
        )

    df_old = candle_mgr.read_candle(symbol)
    if df_old.empty or df_resample.empty:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars, df_old_preloaded=df_old,
        )

    old_times = df_old['candle_begin_time']
    new_times = df_resample['candle_begin_time']
    if old_times.duplicated().any() or new_times.duplicated().any():
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars, df_old_preloaded=df_old,
        )
    if not old_times.is_monotonic_increasing or not new_times.is_monotonic_increasing:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars, df_old_preloaded=df_old,
        )

    last_old_time = old_times.iloc[-1]
    first_new_time = new_times.iloc[0]
    if first_new_time <= last_old_time:
        # 常规每小时重采样窗口含 ~7 根 1h bar（前 6 根与 df_old 尾部重叠、末 1 根新增）：
        # 直接定位重叠切点，对不变前缀零拷贝保留，仅对尾部 ~8 根做去重/拆分检测/补空。
        cut_pos = int(old_times.searchsorted(first_new_time))
        if cut_pos >= 1:
            prefix = df_old.iloc[:cut_pos - 1]
            prefix_gap_free = (
                len(prefix) <= 1
                or len(prefix) == int((prefix['candle_begin_time'].iloc[-1] - prefix['candle_begin_time'].iloc[0]) // resample_delta) + 1
            )
            if prefix_gap_free:
                tail_work = pd.concat([df_old.iloc[cut_pos - 1:], df_resample], ignore_index=True)
                tail_work.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
                tail_work.sort_values('candle_begin_time', inplace=True, ignore_index=True)

                t_gap = tail_work['candle_begin_time'].diff().fillna(timedelta(minutes=0))
                p_chg = (tail_work['open'] / tail_work['close'].shift().fillna(tail_work['open']) - 1).fillna(0)
                if not ((t_gap > timedelta(hours=48)) & (p_chg.abs() > 0.1)).any():
                    exp_tail_len = int((tail_work['candle_begin_time'].iloc[-1] - tail_work['candle_begin_time'].iloc[0]) // resample_delta) + 1
                    if len(tail_work) == exp_tail_len:
                        df_total = (
                            pd.concat([prefix.reset_index(drop=True), tail_work], ignore_index=True)
                            if not prefix.empty
                            else tail_work
                        )
                    else:
                        tail_work = fill_gaps(tail_work, resample_delta)
                        if not prefix.empty:
                            prefix_idx = prefix.copy()
                            prefix_idx['candle_end_time'] = prefix_idx['candle_begin_time'] + resample_delta
                            prefix_idx.set_index('candle_end_time', inplace=True)
                            df_total = pd.concat([prefix_idx, tail_work])
                        else:
                            df_total = tail_work

                    df_total = _apply_history_bound(df_total, num_candles, min_ts)
                    candle_mgr.set_candle(symbol, run_time, df_total)
                    if df_total.empty:
                        return (None, None, None) if return_tail_bars > 0 else (None, None)
                    first_begin_time = df_total['candle_begin_time'].iloc[0]
                    last_begin_time = df_total['candle_begin_time'].iloc[-1]
                    _record_resample_bounds(candle_mgr, symbol, first_begin_time, last_begin_time)
                    if return_tail_bars > 0:
                        tail_out = (
                            _extract_tail_frame(df_total, return_tail_bars)
                            if len(tail_work) <= return_tail_bars
                            else None
                        )
                        return first_begin_time, last_begin_time, tail_out
                    return first_begin_time, last_begin_time

        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars, df_old_preloaded=df_old,
        )

    time_gap = new_times.diff().fillna(first_new_time - last_old_time)
    price_change = (
        df_resample['open'] / df_resample['close'].shift().fillna(df_old['close'].iloc[-1]) - 1
    ).fillna(0)
    has_large_gap = ((time_gap > timedelta(hours=48)) & (price_change.abs() > 0.1)).any()
    if has_large_gap:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=return_tail_bars, df_old_preloaded=df_old,
        )

    if (time_gap == resample_delta).all() and not isinstance(df_old.index, pd.DatetimeIndex):
        df_total = pd.concat([df_old, df_resample], ignore_index=True)
        df_total = _apply_history_bound(df_total, num_candles, min_ts)
        candle_mgr.set_candle(symbol, run_time, df_total)
        if df_total.empty:
            return (None, None, None) if return_tail_bars > 0 else (None, None)
        first_begin_time = df_total['candle_begin_time'].iloc[0]
        last_begin_time = df_total['candle_begin_time'].iloc[-1]
        _record_resample_bounds(candle_mgr, symbol, first_begin_time, last_begin_time)
        if return_tail_bars > 0:
            return first_begin_time, last_begin_time, _extract_tail_frame(df_total, return_tail_bars)
        return first_begin_time, last_begin_time

    prefix = df_old.iloc[:-1].copy(deep=True)
    work_df = pd.concat([df_old.tail(1), df_resample], ignore_index=True)
    work_df = fill_gaps(work_df, resample_delta)

    if isinstance(work_df.index, pd.DatetimeIndex):
        if not prefix.empty:
            prefix['candle_end_time'] = prefix['candle_begin_time'] + resample_delta
            prefix.set_index('candle_end_time', inplace=True)
        df_total = pd.concat([prefix, work_df]) if not prefix.empty else work_df
    else:
        df_total = pd.concat([prefix.reset_index(drop=True), work_df], ignore_index=True) if not prefix.empty else work_df

    df_total = _apply_history_bound(df_total, num_candles, min_ts)

    candle_mgr.set_candle(symbol, run_time, df_total)
    if df_total.empty:
        return (None, None, None) if return_tail_bars > 0 else (None, None)

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]
    _record_resample_bounds(candle_mgr, symbol, first_begin_time, last_begin_time)
    if return_tail_bars > 0:
        tail_out = (
            _extract_tail_frame(df_total, return_tail_bars)
            if len(work_df) <= return_tail_bars
            else None
        )
        return first_begin_time, last_begin_time, tail_out
    return first_begin_time, last_begin_time


def _is_healthy_existing_resample(
    resample_mgr: CandleFileManager,
    symbol: str,
    stale_cutoff: pd.Timestamp,
) -> bool:
    """判断某个 offset 下的现有 1h 文件是否健康（存在、可读、非空且最新 bar 未超期）。"""
    if not resample_mgr.has_symbol(symbol):
        return False
    try:
        df_old = resample_mgr.read_candle(symbol)
    except Exception:
        return False
    if not isinstance(df_old, pd.DataFrame) or df_old.empty or 'candle_begin_time' not in df_old.columns:
        return False
    last_ts = df_old['candle_begin_time'].max()
    if pd.isna(last_ts):
        return False
    return bool(last_ts >= stale_cutoff)


def resample_history(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    full_backfill_symbols: Optional[set[str]] = None,
) -> None:
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    resample_delta = convert_interval_to_timedelta(handle.resample_interval)
    logger.info(f'开始 Resample 历史数据，类型={trade_type}，当前时间={now_time_str()}')

    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr_map = market_handle.resample_mgr_map
    base_candle_dir = market_handle.base_candle_dir
    if not base_candle_dir.exists():
        logger.ok(f'Resample 历史数据成功，类型={trade_type}，当前时间={now_time_str()}')
        return

    full_backfill_set = set(full_backfill_symbols or ())
    trim_ts = _resolve_history_trim_ts(handle)
    kline_count_1h = _resolve_kline_count_1h(handle, trade_type, run_time)
    theoretical_start = run_time - kline_count_1h * resample_delta
    retention_bars = _resolve_base_5m_retention_bars(handle, base_delta)
    retention_days = int(getattr(handle, 'base_5m_retention_days', 30))
    stale_cutoff = pd.Timestamp(run_time) - timedelta(days=max(1, retention_days - 2))

    symbols = sorted(item.name for item in base_candle_dir.iterdir() if item.is_dir())

    def _process_symbol(symbol: str):
        symbol_dir = base_candle_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        df: pd.DataFrame | None = ts_mgr.read_all()
        if df is None or df.empty:
            shutil.rmtree(symbol_dir)
            return

        if '_source' in df.columns:
            df = df.drop(columns=['_source'])
        df = df[df['candle_begin_time'] < run_time]
        if df.empty:
            return

        is_full_backfill = symbol in full_backfill_set
        df_min_time = df['candle_begin_time'].min()

        for offset_str, resample_mgr in resample_mgr_map.items():
            is_healthy = _is_healthy_existing_resample(resample_mgr, symbol, stale_cutoff)

            if is_healthy:
                # 健康 offset 始终走增量合并（即便同 symbol 因其他 offset 缺失处于自愈中），
                # 丢弃截断窗口首根不完整 1h bar，绝不覆盖既有历史头部
                df_resample = calc_resample(df, handle.resample_interval, offset_str, theoretical_start)
                if df_resample.empty:
                    continue
                if df_min_time > df_resample['candle_begin_time'].iloc[0]:
                    df_resample = df_resample.iloc[1:]
                if df_resample.empty:
                    continue
                update_resample_candle_incremental(
                    resample_mgr,
                    symbol,
                    df_resample,
                    None,
                    run_time,
                    resample_delta,
                    min_ts=trim_ts,
                )
            else:
                # 缺失/损坏/超期 offset：基于完整 base_5m 全量重采样覆盖写入
                df_resample = calc_resample(df, handle.resample_interval, offset_str, theoretical_start)
                if df_resample.empty:
                    logger.warning(f'{trade_type} {symbol} {offset_str} Resamaple K 线为空，跳过')
                    continue
                df_resample = filter_gaps(df_resample)
                df_resample = fill_gaps(df_resample, resample_delta)
                df_resample = _apply_history_bound(df_resample, None, trim_ts)
                if len(df_resample) > 0:
                    resample_mgr.set_candle(symbol, run_time, df_resample)

        if is_full_backfill:
            # 自愈完成后将临时拉取的深历史 base_5m 全部旧月分区裁回保留期（30 天）
            cutoff = run_time - retention_bars * base_delta
            ts_mgr.trim(cutoff, run_time)

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(_process_symbol, symbols))

    logger.ok(f'Resample 历史数据成功，类型={trade_type}，当前时间={now_time_str()}')


def _read_base_tail(
    base_dir: Path,
    symbol: str,
    run_time: datetime,
    n_closed: int,
) -> pd.DataFrame | None:
    """从 symbol 的 TSManager 月分区，自最新分区往前累计，读到足够 n_closed 根
    闭合 K（candle_begin_time < run_time）为止，返回时间正序、去重后的 DataFrame。
    """
    ts = TSManager(base_dir / symbol)
    parts = ts.list_partitions()  # 升序，如 ['202605', '202606']
    if not parts:
        return None
    collected = []
    closed = 0
    for name in reversed(parts):
        dfp = ts.read_partition(name)
        if dfp is None or dfp.empty:
            continue
        collected.append(dfp)
        closed += int((dfp['candle_begin_time'] < run_time).sum())
        if closed >= n_closed:
            break
    if not collected:
        return None
    df = pd.concat(collected, ignore_index=True)
    df = (
        df.drop_duplicates('candle_begin_time')
        .sort_values('candle_begin_time')
        .reset_index(drop=True)
    )
    if not (df['candle_begin_time'] < run_time).any():
        return None
    return df


def _overlay_ws_window(disk_df: Optional[pd.DataFrame], ws_df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if ws_df is None or ws_df.empty:
        return disk_df
    if disk_df is None or disk_df.empty:
        return ws_df
    merged = pd.concat([disk_df, ws_df], ignore_index=True)
    merged.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
    merged.sort_values('candle_begin_time', inplace=True, ignore_index=True)
    return merged


def _load_closed_base_window(
    base_dir: Path,
    symbol: str,
    run_time: datetime,
    once_update_candles: int,
    ws_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame | None:
    """优先合并实时 ws_cache 与轻量 flat_pkl（默认 240 根）读取闭合 5m 窗口；
    当 ws_cache 已攒满连续无缺口窗口时直接免读盘返回；不足时回退叠加 flat_pkl 或 TSManager 月分区。
    """
    ws_closed: pd.DataFrame | None = None
    if ws_df is not None and not ws_df.empty:
        ws_closed = ws_df[ws_df['candle_begin_time'] < run_time]
        if ws_closed.empty:
            ws_closed = None
        elif len(ws_closed) >= once_update_candles and not (base_dir / f'{symbol}.gap').exists():
            return ws_closed

    df_flat_closed: pd.DataFrame | None = None
    flat_pkl = base_dir / f'{symbol}.pkl'
    if flat_pkl.exists():
        try:
            df_flat = pd.read_pickle(flat_pkl)
        except Exception:
            df_flat = None
        if df_flat is not None and not df_flat.empty and 'candle_begin_time' in df_flat.columns:
            df_flat = df_flat[df_flat['candle_begin_time'] < run_time]
            if '_source' in df_flat.columns:
                df_flat = df_flat.drop(columns=['_source'])
            if not df_flat.empty:
                df_flat_closed = _overlay_ws_window(df_flat, ws_closed)
                if df_flat_closed is not None and len(df_flat_closed) >= once_update_candles:
                    return df_flat_closed

    df_part = _read_base_tail(base_dir, symbol, run_time, once_update_candles)
    if df_part is not None and not df_part.empty:
        df_part = df_part[df_part['candle_begin_time'] < run_time]
        if '_source' in df_part.columns:
            df_part = df_part.drop(columns=['_source'])
        if not df_part.empty:
            df_part_merged = _overlay_ws_window(df_part, ws_closed)
            if df_flat_closed is None or (df_part_merged is not None and len(df_part_merged) >= len(df_flat_closed)):
                return df_part_merged
    return df_flat_closed if df_flat_closed is not None else ws_closed


def _prepare_df_resample_from_base(
    symbol: str,
    base_dir: Path,
    resample_mgr: CandleFileManager,
    resample_interval: str,
    offset_str: str,
    theoretical_start: datetime,
    run_time: datetime,
    once_update_candles: int,
    ws_df: Optional[pd.DataFrame] = None,
) -> Optional[pd.DataFrame]:
    df_base = _load_closed_base_window(base_dir, symbol, run_time, once_update_candles, ws_df=ws_df)
    if df_base is None or df_base.empty:
        return None
    window_truncated = len(df_base) >= once_update_candles
    df_base = df_base.tail(once_update_candles)
    if df_base.empty:
        return None
    df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)
    if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
        df_resample = df_resample.iloc[1:]
    if df_resample.empty:
        return None
    return df_resample


def _resample_one_symbol(
    symbol: str,
    base_dir: Path,
    resample_mgr: CandleFileManager,
    resample_interval: str,
    offset_str: str,
    theoretical_start: datetime,
    run_time: datetime,
    once_update_candles: int,
    resample_delta: timedelta,
    min_ts: Optional[pd.Timestamp] = None,
    return_tail_bars: int = 0,
    ws_df: Optional[pd.DataFrame] = None,
):
    """对单个 symbol：读 base 闭合 K（ws_cache / flat_pkl 优先，月分区回退）→ resample → 增量合并写入 resample_mgr。"""
    df_resample = _prepare_df_resample_from_base(
        symbol, base_dir, resample_mgr, resample_interval, offset_str,
        theoretical_start, run_time, once_update_candles, ws_df=ws_df,
    )
    if df_resample is None:
        return None

    res = update_resample_candle_incremental(
        resample_mgr,
        symbol,
        df_resample,
        None,
        run_time,
        resample_delta,
        min_ts=min_ts,
        return_tail_bars=return_tail_bars,
    )
    if res[0] is not None and res[1] is not None:
        return res
    return None


def _load_ws_windows_for_chunk(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols_chunk: list[str],
    once_update_candles: int,
) -> dict[str, pd.DataFrame]:
    root_dir = getattr(handle, 'base_dir', None)
    if root_dir is None or not symbols_chunk:
        return {}
    ws_db = Path(root_dir) / 'ws_cache' / f'{trade_type}.sqlite'
    if not ws_db.exists():
        return {}
    base_delta = convert_interval_to_timedelta(getattr(handle, 'base_interval', '5m'))
    base_ms = max(1, int(base_delta.total_seconds() * 1000))
    end_excl_ms = int(run_time.timestamp() * 1000)
    # 多取 12 根余量确保覆盖完整 1h 闭合边界
    win_start_ms = end_excl_ms - (int(once_update_candles) + 12) * base_ms
    return read_ws_cache_market_windows(ws_db, trade_type, symbols_chunk, win_start_ms, end_excl_ms)


def _resample_chunk_worker(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols_chunk: list[str],
    return_tail_bars: int = 0,
    include_bounds_meta: bool = False,
):
    resample_interval = handle.resample_interval
    resample_delta = convert_interval_to_timedelta(resample_interval)
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr = market_handle.resample_mgr_map[offset_str]
    once_update_candles = market_handle.once_update_candles
    base_dir = market_handle.base_candle_dir
    kline_count_1h = _resolve_kline_count_1h(handle, trade_type, run_time)
    theoretical_start = run_time - kline_count_1h * resample_delta
    min_ts = _resolve_history_trim_ts(handle)
    ws_windows = _load_ws_windows_for_chunk(handle, trade_type, run_time, symbols_chunk, once_update_candles)

    out_begins: dict = {}
    out_tails: dict = {}
    out_meta: dict = {}
    for symbol in symbols_chunk:
        res = _resample_one_symbol(
            symbol,
            base_dir,
            resample_mgr,
            resample_interval,
            offset_str,
            theoretical_start,
            run_time,
            once_update_candles,
            resample_delta,
            min_ts=min_ts,
            return_tail_bars=return_tail_bars,
            ws_df=ws_windows.get(symbol),
        )
        if res is not None:
            out_begins[symbol] = (res[0], res[1])
            if return_tail_bars > 0 and len(res) > 2 and res[2] is not None:
                out_tails[symbol] = res[2]
            if include_bounds_meta:
                entry = _get_cached_resample_bounds(resample_mgr, symbol)
                if entry is not None:
                    out_meta[symbol] = entry
    if include_bounds_meta:
        return out_begins, out_tails, out_meta
    if return_tail_bars > 0:
        return out_begins, out_tails
    return out_begins


def _compute_first_chunk_worker(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols_chunk: list[str],
    return_tail_bars: int,
    cached_bounds_map: dict,
):
    resample_interval = handle.resample_interval
    resample_delta = convert_interval_to_timedelta(resample_interval)
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr = market_handle.resample_mgr_map[offset_str]
    once_update_candles = market_handle.once_update_candles
    base_dir = market_handle.base_candle_dir
    kline_count_1h = _resolve_kline_count_1h(handle, trade_type, run_time)
    theoretical_start = run_time - kline_count_1h * resample_delta
    min_ts = _resolve_history_trim_ts(handle)
    ws_windows = _load_ws_windows_for_chunk(handle, trade_type, run_time, symbols_chunk, once_update_candles)

    out_items = []
    out_synced_meta = {}
    for symbol in symbols_chunk:
        df_res = _prepare_df_resample_from_base(
            symbol, base_dir, resample_mgr, resample_interval, offset_str,
            theoretical_start, run_time, once_update_candles, ws_df=ws_windows.get(symbol),
        )
        if df_res is None:
            continue
        cached = cached_bounds_map.get(symbol)
        if cached is not None:
            _, first_ts, last_old_ts = cached
            cbt = df_res['candle_begin_time']
            if (
                len(df_res) >= 2
                and cbt.is_monotonic_increasing
                and not cbt.duplicated().any()
                and (min_ts is None or first_ts >= min_ts)
                and cbt.iloc[0] <= last_old_ts
            ):
                t_gap = cbt.diff().fillna(timedelta(minutes=0))
                p_chg = (df_res['open'] / df_res['close'].shift().fillna(df_res['open']) - 1).fillna(0)
                if not ((t_gap > timedelta(hours=48)) & (p_chg.abs() > 0.1)).any():
                    tail_filled = fill_gaps(df_res, resample_delta).reset_index(drop=True)
                    new_last_ts = max(last_old_ts, tail_filled['candle_begin_time'].iloc[-1])
                    t_slice = tail_filled.tail(return_tail_bars).reset_index(drop=True) if return_tail_bars > 0 else None
                    out_items.append(('deferred', symbol, (first_ts, new_last_ts), t_slice, df_res))
                    continue

        res = update_resample_candle_incremental(
            resample_mgr, symbol, df_res, None, run_time, resample_delta,
            min_ts=min_ts, return_tail_bars=max(return_tail_bars, 12),
        )
        if res[0] is not None and res[1] is not None:
            t_slice = res[2].tail(return_tail_bars).reset_index(drop=True) if (return_tail_bars > 0 and res[2] is not None) else None
            out_items.append(('synced', symbol, (res[0], res[1]), t_slice, None))
            entry = _get_cached_resample_bounds(resample_mgr, symbol)
            if entry is not None:
                out_synced_meta[symbol] = entry
    return out_items, out_synced_meta


def _compute_first_resample(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols_trading: list[str],
    return_tail_bars: int,
    executor=None,
) -> tuple[dict, dict, Callable[[], None]]:
    """计算优先重采样：直接内存合成最新 1h 切片供下游 Preprocess 立即开算，
    将 1,287 个 14,700 行 1h_resample/*.pkl 的磁盘合并写入封装为可延后调用的 `flush_fn`。
    """
    resample_interval = handle.resample_interval
    resample_delta = convert_interval_to_timedelta(resample_interval)
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr = market_handle.resample_mgr_map[offset_str]
    min_ts = _resolve_history_trim_ts(handle)

    symbol_begin_time: dict = {}
    symbol_tail_dfs: dict = {}
    deferred_writes: list[tuple[str, pd.DataFrame]] = []
    deferred_lock = threading.Lock()

    cached_all = {}
    for sym in symbols_trading:
        c = _get_cached_resample_bounds(resample_mgr, sym)
        if c is not None:
            cached_all[sym] = c

    items = []
    if executor is not None and len(symbols_trading) > 4:
        pool_workers = int(getattr(executor, '_max_workers', 4) or 4)
        n_chunks = min(len(symbols_trading), max(4, pool_workers * 2))
        chunk_size = (len(symbols_trading) + n_chunks - 1) // n_chunks
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        futs = [
            executor.submit(
                _compute_first_chunk_worker,
                handle, trade_type, run_time, ch, return_tail_bars,
                {s: cached_all[s] for s in ch if s in cached_all},
            )
            for ch in chunks
        ]
        base_key = str(resample_mgr.base_dir)
        for fut in futs:
            c_items, synced_meta = fut.result()
            items.extend(c_items)
            if synced_meta:
                with _RESAMPLE_BOUNDS_LOCK:
                    for sym, entry in synced_meta.items():
                        _RESAMPLE_BOUNDS_CACHE[(base_key, sym)] = entry
    elif len(symbols_trading) <= 4:
        items, _ = _compute_first_chunk_worker(
            handle, trade_type, run_time, symbols_trading, return_tail_bars, cached_all
        )
    else:
        n_workers = 4
        chunk_size = (len(symbols_trading) + n_workers - 1) // n_workers
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        with ThreadPoolExecutor(max_workers=n_workers, thread_name_prefix=f'fast_res_{trade_type}') as pool:
            for c_items, _ in pool.map(
                lambda ch: _compute_first_chunk_worker(
                    handle, trade_type, run_time, ch, return_tail_bars,
                    {s: cached_all[s] for s in ch if s in cached_all},
                ),
                chunks,
            ):
                items.extend(c_items)

    for item in items:
        mode, sym, bounds, tail_df, df_res_raw = item
        symbol_begin_time[sym] = bounds
        if return_tail_bars > 0 and tail_df is not None:
            symbol_tail_dfs[sym] = tail_df
        if mode == 'deferred' and df_res_raw is not None:
            deferred_writes.append((sym, df_res_raw))

    raw_map = dict(deferred_writes)
    flushed = False

    def _flush_to_disk() -> None:
        nonlocal flushed
        with deferred_lock:
            if flushed:
                return
            flushed = True
            pending_items = list(deferred_writes)
            deferred_writes.clear()

        if not pending_items:
            return

        def _write_one(pair: tuple[str, pd.DataFrame]) -> None:
            sym, df_r = pair
            if resample_mgr.check_ready(sym, run_time):
                return
            update_resample_candle_incremental(
                resample_mgr, sym, df_r, None, run_time, resample_delta, min_ts=min_ts
            )

        if len(pending_items) <= 4:
            for pair in pending_items:
                _write_one(pair)
        else:
            with ThreadPoolExecutor(max_workers=2, thread_name_prefix=f'flush_1h_{trade_type}') as fpool:
                list(fpool.map(_write_one, pending_items))

    _flush_to_disk.raw_map = raw_map  # type: ignore[attr-defined]
    logger.ok(
        f'Resample {trade_type} 内存合成就绪: {len(symbol_begin_time)}/{len(symbols_trading)} symbol '
        f'(后台待刷盘={len(deferred_writes)}), 当前时间={now_time_str()}'
    )
    return symbol_begin_time, symbol_tail_dfs, _flush_to_disk


def update_resample_from_base(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    *,
    executor=None,
    return_tail_bars: int = 0,
    defer_disk_write: bool = False,
):
    """从 base 5m 存储增量重采样到 resample_1h。

    - `defer_disk_write=True` 时，直接内存合成最新 1h K 线并返回 `(begin_ts, tail_dfs, flush_fn)`
      （或 `return_tail_bars==0` 时 `(begin_ts, flush_fn)`），使下游 Preprocess 零等待开算、1h 落盘与计算并行重叠；
    - 传入共享常驻 `executor` 时，切分为多块跨进程池真并行；
    - 未传 `executor` 时回退进程内 `ThreadPoolExecutor(max_workers=2)` 保证单测零额外进程开销；
    - `return_tail_bars > 0` 时返回 `(symbol_begin_time, symbol_tail_dfs)` 供下游免读盘增量拼接。
    """
    df_exginfo = handle.exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list() if df_exginfo is not None else []
    if not symbols_trading:
        logger.ok(f'Resample {trade_type} 从 base 存储完成: 0/0 symbol, 当前时间={now_time_str()}')
        if defer_disk_write:
            noop = lambda: None
            return ({}, {}, noop) if return_tail_bars > 0 else ({}, noop)
        return ({}, {}) if return_tail_bars > 0 else {}

    if defer_disk_write:
        begins, tails, flush_fn = _compute_first_resample(
            handle, trade_type, run_time, symbols_trading, return_tail_bars, executor=executor
        )
        if return_tail_bars > 0:
            return begins, tails, flush_fn
        return begins, flush_fn

    symbol_begin_time: dict = {}
    symbol_tail_dfs: dict = {}
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    resample_mgr = _get_market_handle(handle, trade_type).resample_mgr_map[offset_str]

    if executor is not None:
        pool_workers = int(getattr(executor, '_max_workers', 4) or 4)
        n_chunks = min(len(symbols_trading), max(4, pool_workers * 2))
        chunk_size = (len(symbols_trading) + n_chunks - 1) // n_chunks
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        futures = [
            executor.submit(_resample_chunk_worker, handle, trade_type, run_time, ch, return_tail_bars, True)
            for ch in chunks
        ]
        for fut in futures:
            b_part, t_part, m_part = fut.result()
            symbol_begin_time.update(b_part)
            if return_tail_bars > 0:
                symbol_tail_dfs.update(t_part)
            if m_part:
                base_key = str(resample_mgr.base_dir)
                with _RESAMPLE_BOUNDS_LOCK:
                    for sym, entry in m_part.items():
                        _RESAMPLE_BOUNDS_CACHE[(base_key, sym)] = entry
    elif len(symbols_trading) <= 1:
        chunk_res = _resample_chunk_worker(handle, trade_type, run_time, symbols_trading, return_tail_bars)
        if return_tail_bars > 0:
            symbol_begin_time, symbol_tail_dfs = chunk_res
        else:
            symbol_begin_time = chunk_res
    else:
        n_workers = 2
        chunk_size = (len(symbols_trading) + n_workers - 1) // n_workers
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        with ThreadPoolExecutor(max_workers=n_workers) as exe:
            futures = [
                exe.submit(_resample_chunk_worker, handle, trade_type, run_time, ch, return_tail_bars)
                for ch in chunks
            ]
            for fut in futures:
                chunk_res = fut.result()
                if return_tail_bars > 0:
                    symbol_begin_time.update(chunk_res[0])
                    symbol_tail_dfs.update(chunk_res[1])
                else:
                    symbol_begin_time.update(chunk_res)

    logger.ok(
        f'Resample {trade_type} 从 base 存储完成: {len(symbol_begin_time)}/{len(symbols_trading)} symbol, '
        f'当前时间={now_time_str()}'
    )
    if return_tail_bars > 0:
        return symbol_begin_time, symbol_tail_dfs
    return symbol_begin_time
