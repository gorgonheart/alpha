import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

import pandas as pd

from core.candle_manager import CandleFileManager
from core.handle import BmacHandle
from core.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.time import (convert_interval_to_timedelta, now_time,
                             now_time_str)


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


def resample_history(handle: BmacHandle, trade_type: str, run_time: datetime):
    resample_delta = convert_interval_to_timedelta(handle.resample_interval)
    logger.info(f'开始 Resample 历史数据，类型={trade_type}，当前时间={now_time_str()}')

    binance_handle = handle.get_binance_handle(trade_type)
    resample_mgr_map = binance_handle.resample_mgr_map

    symbols = sorted(item.name for item in binance_handle.base_candle_dir.iterdir() if item.is_dir())

    def _process_symbol(symbol):
        symbol_dir = binance_handle.base_candle_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        df: pd.DataFrame = ts_mgr.read_all()
        if df is None:
            shutil.rmtree(symbol_dir)
            return
        theoretical_start = run_time - handle.kline_count_1h * resample_delta
        for offset_str, resample_mgr in resample_mgr_map.items():
            df_resample = calc_resample(df, handle.resample_interval, offset_str, theoretical_start)
            if df_resample.empty:
                logger.warning(f'{trade_type} {symbol} {offset_str} Resamaple K 线为空，跳过')
                continue
            df_resample = filter_gaps(df_resample)
            df_resample = fill_gaps(df_resample, resample_delta)
            if len(df_resample) > 0:
                resample_mgr.set_candle(symbol, run_time, df_resample)

    with ThreadPoolExecutor() as pool:
        list(pool.map(_process_symbol, symbols))

    logger.ok(f'Resample 历史数据成功，类型={trade_type}，当前时间={now_time_str()}')


def fill_gaps(df: pd.DataFrame, delta: pd.Timedelta) -> pd.DataFrame:

    # 创建一个从开始到结束无空缺的基准
    first = df['candle_begin_time'].min()
    last = df['candle_begin_time'].max()
    time_range = pd.date_range(first, last, freq=delta)

    # 无空缺，不需要填充
    if len(df) == len(time_range) and (df['candle_begin_time'].reset_index(drop=True) == time_range).all():
        return df

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

    df['candle_end_time'] = df['candle_begin_time'] + delta
    df.set_index('candle_end_time', inplace=True)
    return df


def filter_gaps(df: pd.DataFrame, gap_hours_thres=48, price_change_thres=0.1):
    '''
    去除（因代币拆分）重上架前的数据
    默认下架 48 小时并且价格变动绝对值超过 10% 
    '''
    price_change = (df['open'] / df['close'].shift().fillna(df['open']) - 1).fillna(0)
    time_gap = df['candle_begin_time'].diff().fillna(timedelta(minutes=0))
    cond = (time_gap > timedelta(hours=gap_hours_thres)) & (price_change.abs() > price_change_thres)

    if cond.any():
        last_idx = cond[cond].index[-1]
        df = df.loc[last_idx:].copy()

    return df


def update_resample_candle(candle_mgr: CandleFileManager, symbol: str, df_resample: pd.DataFrame, num_candles: int,
                           run_time: datetime, resample_delta: timedelta):
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

    # 保留最新数据
    df_total = df_total.tail(num_candles)

    # 写入硬盘
    candle_mgr.set_candle(symbol, run_time, df_total)

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]

    return first_begin_time, last_begin_time


def update_resample_candle_incremental(candle_mgr: CandleFileManager, symbol: str, df_resample: pd.DataFrame,
                                       num_candles: int, run_time: datetime, resample_delta: timedelta):
    if not candle_mgr.has_symbol(symbol):
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    df_old = candle_mgr.read_candle(symbol)
    if df_old.empty or df_resample.empty:
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    old_times = df_old['candle_begin_time']
    new_times = df_resample['candle_begin_time']
    if old_times.duplicated().any() or new_times.duplicated().any():
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)
    if not old_times.is_monotonic_increasing or not new_times.is_monotonic_increasing:
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    last_old_time = old_times.iloc[-1]
    first_new_time = new_times.iloc[0]
    if first_new_time <= last_old_time:
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    time_gap = new_times.diff().fillna(first_new_time - last_old_time)
    price_change = (
        df_resample['open'] / df_resample['close'].shift().fillna(df_old['close'].iloc[-1]) - 1
    ).fillna(0)
    has_large_gap = ((time_gap > timedelta(hours=48)) & (price_change.abs() > 0.1)).any()
    if has_large_gap:
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    if (time_gap == resample_delta).all():
        df_total = pd.concat([df_old, df_resample], ignore_index=True)
        df_total = df_total.tail(num_candles)
        candle_mgr.set_candle(symbol, run_time, df_total)
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

    df_total = df_total.tail(num_candles)

    candle_mgr.set_candle(symbol, run_time, df_total)

    first_begin_time = df_total['candle_begin_time'].iloc[0]
    last_begin_time = df_total['candle_begin_time'].iloc[-1]
    return first_begin_time, last_begin_time
