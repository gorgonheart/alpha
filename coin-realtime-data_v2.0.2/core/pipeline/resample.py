import multiprocessing as mp
import os
import shutil
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from core.storage.candle_manager import CandleFileManager
from core.handle import BmacHandle
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.time import (convert_interval_to_timedelta,
                             get_offset_from_runtime, now_time,
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

    binance_handle = handle.get_market_handle(trade_type)
    resample_mgr_map = binance_handle.resample_mgr_map

    symbols = sorted(item.name for item in binance_handle.base_candle_dir.iterdir() if item.is_dir())
    # 遍历每个交易对，进行 Resample
    for symbol in symbols:
        symbol_dir = binance_handle.base_candle_dir / symbol
        # 从磁盘读取原始 K 线数据
        ts_mgr = TSManager(symbol_dir)

        df: pd.DataFrame = ts_mgr.read_all()
        if df is None:
            shutil.rmtree(symbol_dir)
            continue

        # 对于每个偏移量进行 Resample
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


def _read_base_tail(
    base_dir: Path,
    symbol: str,
    run_time: datetime,
    n_closed: int,
) -> pd.DataFrame | None:
    """从 symbol 的 TSManager 月分区，自最新分区往前累计，读到足够 n_closed 根
    闭合 K（candle_begin_time < run_time）为止，返回时间正序、去重后的 DataFrame。

    典型只读 1 个分区（当月行数远超 n_closed）；月初闭合不足时自动回拉前一分区。
    分区目录不存在 / 全部为空 → 返回 None（调用方回退读 flat pkl）。
    """
    ts = TSManager(base_dir / symbol)
    parts = ts.list_partitions()  # 升序，如 ['202605', '202606']
    if not parts:
        return None
    collected = []
    closed = 0
    for name in reversed(parts):  # 从最新月往前读
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
    df = (df.drop_duplicates('candle_begin_time')
            .sort_values('candle_begin_time')
            .reset_index(drop=True))
    # 无任何闭合 K（全部 >= run_time）→ 视同无数据，返回 None，与 docstring 契约一致
    if not (df['candle_begin_time'] < run_time).any():
        return None
    return df


def _resample_one_symbol(symbol, base_dir, resample_mgr, resample_interval, offset_str,
                         theoretical_start, run_time, once_update_candles, kline_count_1h,
                         resample_delta):
    """对单个 symbol：读 base 闭合 K（分区 tail 优先，flat pkl 回退）→ resample → 增量合并写入 resample_mgr。

    返回 (first_begin_time, last_begin_time) 或 None（无数据/损坏/读不到）。
    """
    # 优先从 TSManager 月分区按需读闭合 K 窗口（避免全量回读 flat pkl）。
    df_base = _read_base_tail(base_dir, symbol, run_time, once_update_candles)
    if df_base is None or df_base.empty:
        # 安全网：分区读不到时回退旧路径（flat pkl 仍由 writer 照常写）。
        flat_pkl = base_dir / f'{symbol}.pkl'
        if not flat_pkl.exists():
            return None
        try:
            df_base = pd.read_pickle(flat_pkl)
        except Exception:
            # 损坏 pkl 跳过；writer 的 corrupt-pkl 路径会处理
            return None
        if df_base is None or df_base.empty:
            return None

    # 只取闭合 K（candle_begin_time < run_time），丢弃 writer 内部 _source 列
    df_base = df_base[df_base['candle_begin_time'] < run_time]
    if '_source' in df_base.columns:
        df_base = df_base.drop(columns=['_source'])
    # 必须在 tail 截断之前判断：闭合 K 满 once_update_candles 根才说明取数窗口被截断
    window_truncated = len(df_base) >= once_update_candles
    # 增量：只取最近 once_update_candles 根，限制 resample 计算量
    df_base = df_base.tail(once_update_candles)
    if df_base.empty:
        return None

    df_resample = calc_resample(df_base, resample_interval, offset_str, theoretical_start)
    # 取数窗口被截断时，首根 resample K 线缺少更早的基础 K 线、不完整，
    # 丢弃以免覆盖已有的完整 K 线；窗口未截断（如新币拿到完整历史）时首根完整，
    # 必须保留以便用闭合后的数据修正先前写入的未闭合值
    if window_truncated and resample_mgr.has_symbol(symbol) and len(df_resample) > 1:
        df_resample = df_resample.iloc[1:]

    first_begin_time, last_begin_time = update_resample_candle(
        resample_mgr, symbol, df_resample, kline_count_1h, run_time, resample_delta)
    if first_begin_time is not None and last_begin_time is not None:
        return first_begin_time, last_begin_time
    return None


def _resample_chunk_worker(handle: BmacHandle, trade_type: str, run_time: datetime,
                           symbols_chunk: list) -> dict:
    """ProcessPoolExecutor worker：对一批 symbol 跑 resample，返回 begin_time 字典。

    每个进程从 handle 重建 resample_mgr / base_dir（pickle-safe）。多进程写不同 symbol
    到同一 offset 目录是安全的（文件名不同）。
    """
    resample_interval = handle.resample_interval
    resample_delta = convert_interval_to_timedelta(resample_interval)
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    binance_handle = handle.get_market_handle(trade_type)
    resample_mgr = binance_handle.resample_mgr_map[offset_str]
    once_update_candles = binance_handle.once_update_candles
    base_dir = binance_handle.base_candle_dir
    theoretical_start = run_time - handle.kline_count_1h * resample_delta

    out: dict = {}
    for symbol in symbols_chunk:
        res = _resample_one_symbol(
            symbol, base_dir, resample_mgr, resample_interval, offset_str,
            theoretical_start, run_time, once_update_candles, handle.kline_count_1h,
            resample_delta)
        if res is not None:
            out[symbol] = res
    return out


def update_resample_from_base(handle: BmacHandle, trade_type: str, run_time: datetime) -> dict:
    """realtime resample 从 ws 优先级合并的 base 5m 存储读，而非 REST。

    与 fetch_update_resample(REST 内联) 的区别：本函数在 update_all 的 barrier 之后调用，
    此时 writer 已把 ws(闭合 K, 优先级最高) + REST 兜底 合并写入 base 5m flat pkl。
    从 base 存储读 → 只取闭合 K (candle_begin_time < run_time) → resample → 增量合并。
    这样 resample 用的是 ws 闭合 K，避免 REST 最近一根未结算 K 污染下游。

    ★ 并行化：1000+ symbol 的 resample 各自独立（读自己的 pkl、写自己的 resample 文件），
    切块丢 ProcessPoolExecutor 并行，把串行 ~6s 降到 ~1.5s。

    返回 symbol_begin_time: {symbol: (first_begin_time, last_begin_time)} 供 preprocess 用。
    """
    df_exginfo = handle.exg_mgr.read_candle(f'exginfo_{trade_type}')
    symbols_trading = df_exginfo['symbol'].to_list() if df_exginfo is not None else []
    if not symbols_trading:
        logger.ok(f'Resample {trade_type} 从 base 存储完成: 0/0 symbol, 当前时间={now_time_str()}')
        return {}

    # 切块：均分到 4 个 worker
    n_workers = min(4, max(1, (os.cpu_count() or 1)))
    if len(symbols_trading) <= n_workers or n_workers <= 1:
        # symbol 太少 / 单核：直接串行，省去进程开销
        symbol_begin_time = _resample_chunk_worker(handle, trade_type, run_time, symbols_trading)
    else:
        chunk_size = (len(symbols_trading) + n_workers - 1) // n_workers
        chunks = [symbols_trading[i:i + chunk_size] for i in range(0, len(symbols_trading), chunk_size)]
        symbol_begin_time = {}
        # WS 模式从线程池调用此处，使用 spawn 避免 fork 继承其他线程持有的锁。
        with ProcessPoolExecutor(max_workers=n_workers, mp_context=mp.get_context("spawn")) as exe:
            futures = [exe.submit(_resample_chunk_worker, handle, trade_type, run_time, ch) for ch in chunks]
            for fut in futures:
                symbol_begin_time.update(fut.result())

    logger.ok(f'Resample {trade_type} 从 base 存储完成: {len(symbol_begin_time)}/{len(symbols_trading)} symbol, '
              f'当前时间={now_time_str()}')
    return symbol_begin_time
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
