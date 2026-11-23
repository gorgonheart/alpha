import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from core.handle import BmacHandle
from core.storage.candle_manager import CandleFileManager
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.time import (convert_interval_to_timedelta,
                             get_offset_from_runtime, now_time_str)


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


def update_resample_candle(
    candle_mgr: CandleFileManager,
    symbol: str,
    df_resample: pd.DataFrame,
    num_candles: Optional[int],
    run_time: datetime,
    resample_delta: timedelta,
    *,
    min_ts: Optional[pd.Timestamp] = None,
):
    # 读取历史数据，不存在为 None
    df_old = None
    if candle_mgr.has_symbol(symbol):
        df_old = candle_mgr.read_candle(symbol)

        # 历史数据为空 DF
        if df_old.empty:
            df_old = None

    # 合并历史数据和实时数据
    df_total = pd.concat([df_old, df_resample]) if df_old is not None else df_resample

    if df_total.empty:
        candle_mgr.set_candle(symbol, run_time, df_total)
        return None, None

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
        return None, None

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]

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
):
    if not candle_mgr.has_symbol(symbol):
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )

    df_old = candle_mgr.read_candle(symbol)
    if df_old.empty or df_resample.empty:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )

    old_times = df_old['candle_begin_time']
    new_times = df_resample['candle_begin_time']
    if old_times.duplicated().any() or new_times.duplicated().any():
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )
    if not old_times.is_monotonic_increasing or not new_times.is_monotonic_increasing:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )

    last_old_time = old_times.iloc[-1]
    first_new_time = new_times.iloc[0]
    if first_new_time <= last_old_time:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )

    time_gap = new_times.diff().fillna(first_new_time - last_old_time)
    price_change = (
        df_resample['open'] / df_resample['close'].shift().fillna(df_old['close'].iloc[-1]) - 1
    ).fillna(0)
    has_large_gap = ((time_gap > timedelta(hours=48)) & (price_change.abs() > 0.1)).any()
    if has_large_gap:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta, min_ts=min_ts
        )

    if (time_gap == resample_delta).all():
        df_total = pd.concat([df_old, df_resample], ignore_index=True)
        df_total = _apply_history_bound(df_total, num_candles, min_ts)
        candle_mgr.set_candle(symbol, run_time, df_total)
        if df_total.empty:
            return None, None
        first_begin_time = df_total['candle_begin_time'].iloc[0]
        last_begin_time = df_total['candle_begin_time'].iloc[-1]
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
        df_total = pd.concat([prefix, work_df], ignore_index=True) if not prefix.empty else work_df

    df_total = _apply_history_bound(df_total, num_candles, min_ts)

    candle_mgr.set_candle(symbol, run_time, df_total)
    if df_total.empty:
        return None, None

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]
    return first_begin_time, last_begin_time


def _has_nonempty_existing_resample(resample_mgr: CandleFileManager, symbol: str) -> bool:
    if not resample_mgr.has_symbol(symbol):
        return False
    try:
        df_old = resample_mgr.read_candle(symbol)
    except Exception:
        return False
    return isinstance(df_old, pd.DataFrame) and not df_old.empty and 'candle_begin_time' in df_old.columns


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
            has_existing = _has_nonempty_existing_resample(resample_mgr, symbol)

            if not is_full_backfill and has_existing:
                # 常规快路径：基于 30 天 base_5m 增量合并，丢弃截断窗口首根不完整 1h bar，保留历史头部
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
                # 自愈/缺盘路径：基于完整 base_5m 全量重采样写入
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
):
    """对单个 symbol：读 base 闭合 K（分区 tail 优先，flat pkl 回退）→ resample → 增量合并写入 resample_mgr。"""
    df_base = _read_base_tail(base_dir, symbol, run_time, once_update_candles)
    if df_base is None or df_base.empty:
        flat_pkl = base_dir / f'{symbol}.pkl'
        if not flat_pkl.exists():
            return None
        try:
            df_base = pd.read_pickle(flat_pkl)
        except Exception:
            return None
        if df_base is None or df_base.empty:
            return None

    # 只取闭合 K（candle_begin_time < run_time），丢弃 writer 内部 _source 列
    df_base = df_base[df_base['candle_begin_time'] < run_time]
    if '_source' in df_base.columns:
        df_base = df_base.drop(columns=['_source'])
    window_truncated = len(df_base) >= once_update_candles
    df_base = df_base.tail(once_update_candles)
    if df_base.empty:
        return None

    df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)
    if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
        df_resample = df_resample.iloc[1:]

    first_begin_time, last_begin_time = update_resample_candle_incremental(
        resample_mgr,
        symbol,
        df_resample,
        None,
        run_time,
        resample_delta,
        min_ts=min_ts,
    )
    if first_begin_time is not None and last_begin_time is not None:
        return first_begin_time, last_begin_time
    return None


def _resample_chunk_worker(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols_chunk: list[str],
) -> dict:
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

    out: dict = {}
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
        )
        if res is not None:
            out[symbol] = res
    return out


def update_resample_from_base(handle: BmacHandle, trade_type: str, run_time: datetime) -> dict:
    """从 base 5m 存储增量重采样到 resample_1h（ThreadPoolExecutor(max_workers=2)，零 spawn 子进程）。"""
    df_exginfo = handle.exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list() if df_exginfo is not None else []
    if not symbols_trading:
        logger.ok(f'Resample {trade_type} 从 base 存储完成: 0/0 symbol, 当前时间={now_time_str()}')
        return {}

    n_workers = 2
    if len(symbols_trading) <= 1:
        symbol_begin_time = _resample_chunk_worker(handle, trade_type, run_time, symbols_trading)
    else:
        chunk_size = (len(symbols_trading) + n_workers - 1) // n_workers
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        symbol_begin_time = {}
        with ThreadPoolExecutor(max_workers=n_workers) as exe:
            futures = [exe.submit(_resample_chunk_worker, handle, trade_type, run_time, ch) for ch in chunks]
            for fut in futures:
                symbol_begin_time.update(fut.result())

    logger.ok(
        f'Resample {trade_type} 从 base 存储完成: {len(symbol_begin_time)}/{len(symbols_trading)} symbol, '
        f'当前时间={now_time_str()}'
    )
    return symbol_begin_time
