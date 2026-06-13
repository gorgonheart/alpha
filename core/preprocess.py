import importlib
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
from typing import Optional

import pandas as pd

import config as bmac_config
from core.utils.log_kit import logger
from core.utils.time import now_time_str

from .candle_manager import CandleFileReader
from .handle import BmacHandle
from .utils.time import get_offset_from_runtime, wait_until_ready

PIVOT_COLUMNS = ['candle_begin_time', 'symbol', 'open', 'close', 'avg_price_1m', 'funding_fee']


def read_candle_data(binance_mgr: CandleFileReader, data_api_mgr: Optional[CandleFileReader], symbol: str,
                     run_time: datetime):

    if binance_mgr.check_ready(symbol, run_time):
        return binance_mgr.read_candle(symbol), 'binance'

    if data_api_mgr is not None:
        if data_api_mgr.check_ready(symbol, run_time):
            return data_api_mgr.read_candle(symbol), 'data_api'

    return None


def add_additional_cols(df: pd.DataFrame, trade_type: str, symbol: str, df_funding: Optional[pd.DataFrame]):
    df['symbol'] = symbol

    if df_funding is None or df_funding.empty or 'candle_begin_time' not in df_funding.columns:
        df['funding_fee'] = 0
    else:
        # 修改后的代码
        df_funding = df_funding.rename(columns={'funding_rate': 'funding_fee'})
        df_funding['candle_begin_hour'] = df_funding['candle_begin_time'].dt.floor('h')
        df_funding = df_funding.set_index('candle_begin_hour')  # 改为按小时列索引
        # 关联时用df的candle_begin_time按小时对齐匹配
        df = df.join(
            df_funding[['funding_fee']],
            on=df['candle_begin_time'].dt.floor('h'),  # 关键修改：双方按小时对齐
            how='left'
        )
        df['funding_fee'] = df['funding_fee'].fillna(0)
    df['avg_price_1m'] = df['open']
    df['avg_price_5m'] = df['open']
    df['是否交易'] = 1
    df['first_candle_time'] = df['candle_begin_time'].min()
    df['last_candle_time'] = df['candle_begin_time'].max()

    df['is_spot'] = 1 if trade_type == 'spot' else 0

    col_self = f'symbol_{trade_type}'
    col_other = 'symbol_spot' if trade_type == 'swap' else 'symbol_swap'
    df[col_self] = symbol
    df[col_other] = ''

    return df


def read_all_fundings(handle: BmacHandle, run_time: datetime, symbols: list[str]):
    funding_mgr = handle.funding_mgr
    fundings = dict()
    for symbol in symbols:
        if funding_mgr.has_symbol(symbol):
            fundings[symbol] = funding_mgr.read_candle(symbol)
    return fundings


def make_market_pivot(market_dict, market_type='spot'):
    df_list = [df[PIVOT_COLUMNS].dropna(subset='symbol') for df in market_dict.values()]
    df_all_market = pd.concat(df_list, ignore_index=True)
    df_all_market['symbol'] = pd.Categorical(df_all_market['symbol'])
    df_open = df_all_market.pivot(values='open', index='candle_begin_time', columns='symbol')
    df_close = df_all_market.pivot(values='close', index='candle_begin_time', columns='symbol')
    df_vwap1m = df_all_market.pivot(values='avg_price_1m', index='candle_begin_time', columns='symbol')
    if market_type == 'swap':
        df_rate = df_all_market.pivot(values='funding_fee', index='candle_begin_time', columns='symbol')
        df_rate.fillna(value=0, inplace=True)
        return {'open': df_open, 'close': df_close, 'funding_rate': df_rate, 'vwap1m': df_vwap1m}
    else:
        return {'open': df_open, 'close': df_close, 'vwap1m': df_vwap1m}


def intersect(spot_first_ts, spot_last_ts, fut_first_ts, fut_last_ts):
    first_ts = max(spot_first_ts, fut_first_ts)
    last_ts = min(spot_last_ts, fut_last_ts)
    if first_ts > last_ts:
        return None, None
    return first_ts, last_ts


def is_valid(symbol, symbol_begin_time):
    # symbol 是 None 或 nan 等, 为无效 symbol
    if pd.isna(symbol):
        return False

    # begin_time 不存在，则行情数据缺失，为无效 symbol
    if symbol not in symbol_begin_time:
        return False

    # 有效 symbol
    return True


def calc_match(symbol_spot, symbol_swap, symbol_begin_time_spot, symbol_begin_time_swap):
    symbol_match = {'spot': symbol_spot, 'swap': symbol_swap, 'first_begin_time': None, 'last_begin_time': None}

    # 单边有效时回退使用自身时间范围，避免匹配为空导致后续年份为空
    spot_valid = is_valid(symbol_spot, symbol_begin_time_spot)
    swap_valid = is_valid(symbol_swap, symbol_begin_time_swap)

    # 两边都无效，无法匹配
    if not spot_valid and not swap_valid:
        return symbol_match

    # 两边都有效，取时间交集
    if spot_valid and swap_valid:
        spot_first_begin_time, spot_last_begin_time = symbol_begin_time_spot[symbol_spot]
        swap_first_begin_time, swap_last_begin_time = symbol_begin_time_swap[symbol_swap]
        itersect_first_begin_time, intersect_last_begin_time = intersect(spot_first_begin_time, spot_last_begin_time,
                                                                         swap_first_begin_time, swap_last_begin_time)
        symbol_match['first_begin_time'] = itersect_first_begin_time
        symbol_match['last_begin_time'] = intersect_last_begin_time
        return symbol_match

    # 仅一边有效，使用该边自身时间范围
    if spot_valid:
        spot_first_begin_time, spot_last_begin_time = symbol_begin_time_spot[symbol_spot]
        symbol_match['first_begin_time'] = spot_first_begin_time
        symbol_match['last_begin_time'] = spot_last_begin_time
    else:
        swap_first_begin_time, swap_last_begin_time = symbol_begin_time_swap[symbol_swap]
        symbol_match['first_begin_time'] = swap_first_begin_time
        symbol_match['last_begin_time'] = swap_last_begin_time

    return symbol_match


def gen_batch_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                   symbol_list: list[str], batch_name: str):
    if not symbol_list:
        return

    symbol_list = sorted(symbol_list)

    t_start = time.perf_counter()
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)

    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    binance_handle = handle.get_binance_handle(trade_type)

    # 获取 Candle Manager
    binance_mgr = binance_handle.resample_mgr_map[offset_str]
    data_api_mgr = None
    if handle.data_api is not None:
        if trade_type == 'spot':
            data_api_mgr = handle.data_api.spot_mgr_map[offset_str]
        elif trade_type == 'swap':
            data_api_mgr = handle.data_api.swap_mgr_map[offset_str]

    # 读取资金费率
    fundings = None
    if trade_type == 'swap' and handle.fetch_funding_rate:
        fundings = read_all_fundings(handle, run_time, symbol_list)

    market_dict: dict[str, pd.DataFrame] = dict()
    sources = defaultdict(int)

    start_year, end_year = None, None

    # Pre-build O(1) lookup: symbol -> match row（替代循环内 O(n²) 全表扫描）
    match_lookup = {}
    for row in df_match.itertuples(index=False):
        key = getattr(row, trade_type)
        if not pd.isna(key):
            match_lookup[key] = row

    def _load_symbol(symbol):
        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time)
        if result is None:
            return None
        df_candle, source = result
        df_funding = fundings.get(symbol, None) if fundings is not None else None
        return symbol, add_additional_cols(df_candle, trade_type, symbol, df_funding), source

    with ThreadPoolExecutor(max_workers=min(len(symbol_list), 16)) as pool:
        for loaded in pool.map(_load_symbol, symbol_list):
            if loaded is None:
                continue
            symbol, df_candle, source = loaded
            sources[source] += 1

            row = match_lookup.get(symbol)
            if row is None:
                market_dict[symbol] = df_candle
                continue

            first_begin_time = row.first_begin_time
            last_begin_time = row.last_begin_time
            symbol_spot = row.spot
            symbol_swap = row.swap

            if pd.isna(first_begin_time) or pd.isna(last_begin_time):
                market_dict[symbol] = df_candle
                continue

            mask = df_candle['candle_begin_time'].between(first_begin_time, last_begin_time)
            df_candle.loc[mask, 'symbol_spot'] = symbol_spot
            df_candle.loc[mask, 'symbol_swap'] = symbol_swap

            market_dict[symbol] = df_candle

            min_year = df_candle['candle_begin_time'].min().year
            if start_year is None or start_year > min_year:
                start_year = min_year

            max_year = df_candle['candle_begin_time'].max().year
            if end_year is None or end_year < max_year:
                end_year = max_year

    full_batch_name = f'{trade_type}_dict_{batch_name}'
    preprocess_mgr.set_candle(full_batch_name, run_time, market_dict)

    year_pivot_name = dict()
    run_utc = run_time.astimezone(timezone.utc)
    cur_year = run_utc.year
    in_new_year_week = run_utc.month == 1 and run_utc.day <= 7
    for year in range(start_year, end_year + 1):
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        # 跨年后一周内需同时更新上一年，之后只更新当前年
        if (Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists()
                and (int(year) < cur_year - 1
                     or (int(year) == cur_year - 1 and not in_new_year_week))):
            continue

        pivot_batch_name = f'{trade_type}_for_pivot_{batch_name}_{year}'
        start_ts = pd.to_datetime(f'{year}-01-01 00:00:00+00:00')
        end_ts = pd.to_datetime(f'{year}-12-31 23:59:59+00:00')
        market_dict_for_pivot = {
            sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
            for sym, df in market_dict.items()
        }
        preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
        year_pivot_name[year] = pivot_batch_name

    time_elapsed = time.perf_counter() - t_start
    logger.debug(f'预处理 Market Dict {trade_type} {batch_name} 完成, '
                 f'交易对={symbol_list[0]} -- {symbol_list[-1]}, '
                 f'数据源={dict(sources)}, 当前时间={now_time_str()}, '
                 f'耗时 {time_elapsed:.1f} 秒')

    return full_batch_name, year_pivot_name


def gen_batch_data_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                               symbol_list: list[str], batch_name: str):
    if not symbol_list:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    full_batch_name = f'{trade_type}_dict_{batch_name}'

    if not preprocess_mgr.has_symbol(full_batch_name):
        return gen_batch_data(handle, run_time, trade_type, df_match, symbol_list, batch_name)

    old_market_dict: dict[str, pd.DataFrame] = preprocess_mgr.read_candle(full_batch_name)

    symbol_list = sorted(symbol_list)
    binance_handle = handle.get_binance_handle(trade_type)
    binance_mgr = binance_handle.resample_mgr_map[offset_str]
    data_api_mgr = None
    if handle.data_api is not None:
        if trade_type == 'spot':
            data_api_mgr = handle.data_api.spot_mgr_map[offset_str]
        elif trade_type == 'swap':
            data_api_mgr = handle.data_api.swap_mgr_map[offset_str]

    fundings = None
    if trade_type == 'swap' and handle.fetch_funding_rate:
        fundings = read_all_fundings(handle, run_time, symbol_list)

    match_lookup = {}
    for row in df_match.itertuples(index=False):
        key = getattr(row, trade_type)
        if not pd.isna(key):
            match_lookup[key] = row

    new_market_dict: dict[str, pd.DataFrame] = {}
    start_year, end_year = None, None

    for symbol in symbol_list:
        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time)
        if result is None:
            if symbol in old_market_dict:
                new_market_dict[symbol] = old_market_dict[symbol]
            continue

        df_candle_full, _ = result

        if symbol not in old_market_dict:
            df_funding = fundings.get(symbol, None) if fundings is not None else None
            df_full_enriched = add_additional_cols(df_candle_full, trade_type, symbol, df_funding)
            row = match_lookup.get(symbol)
            if row is not None:
                first_begin_time = row.first_begin_time
                last_begin_time = row.last_begin_time
                if not pd.isna(first_begin_time) and not pd.isna(last_begin_time):
                    mask = df_full_enriched['candle_begin_time'].between(first_begin_time, last_begin_time)
                    df_full_enriched.loc[mask, 'symbol_spot'] = row.spot
                    df_full_enriched.loc[mask, 'symbol_swap'] = row.swap
            new_market_dict[symbol] = df_full_enriched
        else:
            df_old = old_market_dict[symbol]
            last_ts = df_old['candle_begin_time'].max()
            df_new_rows = df_candle_full[df_candle_full['candle_begin_time'] > last_ts].copy(deep=True)

            if df_new_rows.empty:
                new_market_dict[symbol] = df_old
                continue

            df_funding = fundings.get(symbol, None) if fundings is not None else None
            df_new_enriched = add_additional_cols(df_new_rows, trade_type, symbol, df_funding)

            row = match_lookup.get(symbol)
            if row is not None:
                first_begin_time = row.first_begin_time
                last_begin_time = row.last_begin_time
                if not pd.isna(first_begin_time) and not pd.isna(last_begin_time):
                    mask = df_new_enriched['candle_begin_time'].between(first_begin_time, last_begin_time)
                    df_new_enriched.loc[mask, 'symbol_spot'] = row.spot
                    df_new_enriched.loc[mask, 'symbol_swap'] = row.swap

            inherited_first_candle_time = df_old['first_candle_time'].iloc[0]
            df_combined = pd.concat([df_old, df_new_enriched], ignore_index=True)
            df_combined.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
            df_combined.sort_values('candle_begin_time', inplace=True)
            df_combined.reset_index(drop=True, inplace=True)
            table_max_time = df_combined['candle_begin_time'].max()
            df_combined['first_candle_time'] = inherited_first_candle_time
            df_combined['last_candle_time'] = table_max_time
            new_market_dict[symbol] = df_combined

        df_sym = new_market_dict[symbol]
        min_year = df_sym['candle_begin_time'].min().year
        if start_year is None or start_year > min_year:
            start_year = min_year
        max_year = df_sym['candle_begin_time'].max().year
        if end_year is None or end_year < max_year:
            end_year = max_year

    preprocess_mgr.set_candle(full_batch_name, run_time, new_market_dict)

    year_pivot_name = dict()
    if start_year is not None and end_year is not None:
        run_utc = run_time.astimezone(timezone.utc)
        cur_year = run_utc.year
        in_new_year_week = run_utc.month == 1 and run_utc.day <= 7
        for year in range(start_year, end_year + 1):
            if (Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists()
                    and (int(year) < cur_year - 1
                         or (int(year) == cur_year - 1 and not in_new_year_week))):
                continue
            pivot_batch_name = f'{trade_type}_for_pivot_{batch_name}_{year}'
            start_ts = pd.to_datetime(f'{year}-01-01 00:00:00+00:00')
            end_ts = pd.to_datetime(f'{year}-12-31 23:59:59+00:00')
            market_dict_for_pivot = {
                sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
                for sym, df in new_market_dict.items()
            }
            preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
            year_pivot_name[year] = pivot_batch_name

    return full_batch_name, year_pivot_name


def gen_pivot(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list]):
    preprocess_handle = handle.preprocess_handle
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]

    run_utc = run_time.astimezone(timezone.utc)
    cur_year = run_utc.year
    in_new_year_week = run_utc.month == 1 and run_utc.day <= 7
    for year, pivot_names in year_pivot_names.items():
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        if (Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists()
                and (int(year) < cur_year - 1
                     or (int(year) == cur_year - 1 and not in_new_year_week))):
            continue
        market_dict = dict()
        for name in pivot_names:
            market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)

        market_pivot = make_market_pivot(market_dict, trade_type)
        preprocess_mgr.set_candle(f'market_pivot_{trade_type}_{year}', run_time, market_pivot)
        logger.info(f'生成 Market Pivot {trade_type} {year} 完成')

    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def gen_pivot_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list]):
    """Incremental counterpart of gen_pivot.

    First call (no old pivot): builds pivot identically to gen_pivot.
    Subsequent calls: loads old pivot, appends only new rows (faster than
    re-pivoting the entire history), then saves the updated pivot.

    Falls back to a full rebuild when:
    - The symbol set has changed (addition or removal).
    - There are no new rows (still touches run_time metadata).
    """
    preprocess_handle = handle.preprocess_handle
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]

    run_utc = run_time.astimezone(timezone.utc)
    cur_year = run_utc.year
    in_new_year_week = run_utc.month == 1 and run_utc.day <= 7
    for year, pivot_names in year_pivot_names.items():
        # Skip old years (matching gen_pivot behaviour exactly)
        if (Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists()
                and (int(year) < cur_year - 1
                     or (int(year) == cur_year - 1 and not in_new_year_week))):
            continue

        pivot_key = f'market_pivot_{trade_type}_{year}'

        # Load the new batch (contains ALL rows for this year from new_market_dict)
        new_market_dict: dict[str, pd.DataFrame] = {}
        for name in pivot_names:
            new_market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)

        if not new_market_dict:
            continue

        # ── No old pivot: first-time full build (identical to gen_pivot) ──────
        if not preprocess_mgr.has_symbol(pivot_key):
            market_pivot = make_market_pivot(new_market_dict, trade_type)
            preprocess_mgr.set_candle(pivot_key, run_time, market_pivot)
            logger.info(f'生成 Market Pivot {trade_type} {year} 完成')
            continue

        # ── Old pivot exists: attempt incremental append ───────────────────────
        old_pivot = preprocess_mgr.read_candle(pivot_key)
        last_ts = old_pivot['open'].index.max()

        # Find rows that are strictly newer than the last pivoted timestamp
        new_rows_dict: dict[str, pd.DataFrame] = {
            sym: df.loc[df['candle_begin_time'] > last_ts]
            for sym, df in new_market_dict.items()
        }
        new_rows_dict = {sym: df for sym, df in new_rows_dict.items() if not df.empty}

        # Symbol set changed → full rebuild to keep pivot columns consistent
        # Use only symbols that actually have data in this year (non-empty DataFrames)
        old_symbols = set(old_pivot['open'].columns.tolist())
        all_current_symbols = {sym for sym, df in new_market_dict.items() if not df.empty}
        if all_current_symbols != old_symbols:
            if int(year) < cur_year:
                # Historical year: new symbols only have data in current year,
                # so keep the old pivot unchanged rather than rebuilding with missing history.
                preprocess_mgr.set_candle(pivot_key, run_time, old_pivot)
                continue
            market_pivot = make_market_pivot(new_market_dict, trade_type)
            preprocess_mgr.set_candle(pivot_key, run_time, market_pivot)
            logger.info(f'生成 Market Pivot {trade_type} {year} 完成 (重建)')
            continue

        if not new_rows_dict:
            # Nothing new: just refresh run_time metadata
            preprocess_mgr.set_candle(pivot_key, run_time, old_pivot)
            continue

        # Fast path: mini-pivot for new rows only, concat with old pivot
        mini_pivot = make_market_pivot(new_rows_dict, trade_type)
        updated_pivot: dict[str, pd.DataFrame] = {}
        for pk in old_pivot:
            if pk in mini_pivot:
                updated_pivot[pk] = pd.concat([old_pivot[pk], mini_pivot[pk]])
            else:
                updated_pivot[pk] = old_pivot[pk]
        for pk in mini_pivot:
            if pk not in updated_pivot:
                updated_pivot[pk] = mini_pivot[pk]

        preprocess_mgr.set_candle(pivot_key, run_time, updated_pivot)
        logger.info(f'生成 Market Pivot {trade_type} {year} 完成 (增量)')

    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def preprocess_type_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame):
    num_batch = handle.preprocess_handle.num_batch
    exg_mgr = handle.exg_mgr
    exginfo_name = f'exginfo_{trade_type}'
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    
    if not exg_mgr.check_ready(exginfo_name, run_time):
        logger.error(f'读取 {trade_type} Exchange Info 失败，预处理数据 {trade_type} 失败')
        return

    df_exginfo = exg_mgr.read_candle(exginfo_name)
    symbol_list = sorted(df_exginfo['symbol'].to_list())

    logger.info(f'开始预处理 {trade_type}, 当前时间={now_time_str()}')

    idx = 1
    year_pivot_names = defaultdict(list)
    t_start = time.perf_counter()
    while symbol_list:
        if len(symbol_list) > num_batch:
            batch, symbol_list = symbol_list[:num_batch], symbol_list[num_batch:]
        else:
            batch, symbol_list = symbol_list, []

        full_batch_name, year_pivot_name = gen_batch_data_incremental(handle, run_time, trade_type, df_match, batch, f'batch{idx}')
        idx += 1

        for col, pivot_name in year_pivot_name.items():
            year_pivot_names[col].append(pivot_name)

    preprocess_mgr.save_ready(f'{trade_type}_dict', run_time)
    time_elapsed = time.perf_counter() - t_start
    logger.ok(f'预处理 Market Dict {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {time_elapsed:.1f} 秒')

    gen_factor_data(handle, run_time, trade_type)

    t_start = time.perf_counter()
    gen_pivot_incremental(handle, run_time, trade_type, year_pivot_names)
    time_elapsed = time.perf_counter() - t_start
    logger.ok(f'预处理 Pivot Table {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {time_elapsed:.1f} 秒')


def preprocess_offset_data(handle: BmacHandle, run_time: datetime, symbol_begin_time_spot, symbol_begin_time_swap):
    df_match = preprocess_match_symbols(handle, run_time, symbol_begin_time_spot, symbol_begin_time_swap)

    if df_match is None:
        logger.error('读取现货与永续合约映射失败，预处理失败')
        return

    with ThreadPoolExecutor(max_workers=2) as pool:
        spot_future = pool.submit(preprocess_type_data, handle, run_time, 'spot', df_match)
        swap_future = pool.submit(preprocess_type_data, handle, run_time, 'swap', df_match)
        spot_future.result()
        swap_future.result()


def preprocess_match_symbols(handle: BmacHandle, run_time: datetime, symbol_begin_time_spot, symbol_begin_time_swap):
    exg_mgr = handle.exg_mgr

    if not exg_mgr.check_ready('spot_swap_matches', run_time):
        return None

    df_match = exg_mgr.read_candle('spot_swap_matches')
    match_results = [
        calc_match(match_row.spot, match_row.swap, symbol_begin_time_spot, symbol_begin_time_swap)
        for match_row in df_match.itertuples(index=False)
    ]
    df_match = pd.DataFrame.from_records(match_results)
    return df_match


def _calc_symbol_factors(item, factor_modules_params):
    """Worker: compute all pre-configured factor columns for one symbol DataFrame.
    ctypes/cffi calls release the GIL, so ThreadPoolExecutor achieves true CPU parallelism.
    """
    symbol, df = item
    factor_cols: dict[str, pd.Series] = {}
    for module, factor_name, param_list in factor_modules_params:
        raw = module.signal_multi_params(df, param_list)
        for param_key, series in raw.items():
            factor_cols[f'{factor_name}_{param_key}'] = series
    return symbol, df.assign(**factor_cols)


def _calc_one_factor(
    item: tuple,
    module,
    factor_name: str,
    param_list: list,
) -> tuple:
    """Worker: compute one factor for one symbol. Returns (symbol, factor_name, {param_key: Series})."""
    symbol, df = item
    raw = module.signal_multi_params(df, param_list)
    return symbol, factor_name, raw


def gen_factor_data(handle: BmacHandle, run_time: datetime, trade_type: str):
    """Compute pre-configured factor columns for all market_dict batches and write back.

    Loads all batches into memory first, then fans out (symbol x factor) work units
    to a single ThreadPoolExecutor so batch I/O and factor computation are fully decoupled.
    Called after gen_batch_data_incremental, before gen_pivot_incremental.
    Factor columns are named {factor_name}_{param}, matching factor_calc.py convention.
    """
    factor_configs = getattr(bmac_config, 'precompute_factor_configs', {})
    if not factor_configs:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

    factor_modules_params = [
        (importlib.import_module(f'factors.{factor_name}'), factor_name, param_list)
        for factor_name, param_list in factor_configs.items()
    ]

    # 1. Load all batches into memory (sequential, fast I/O)
    t_start = time.perf_counter()
    all_batches: dict[str, dict] = {}
    batch_idx = 1
    while True:
        batch_key = f'{trade_type}_dict_batch{batch_idx}'
        if not preprocess_mgr.has_symbol(batch_key):
            break
        all_batches[batch_key] = preprocess_mgr.read_candle(batch_key)
        batch_idx += 1

    if not all_batches:
        return

    # 2. Flatten to work units: track (batch_key, symbol) alongside each job
    work_meta: list[tuple[str, str]] = []
    work_args: list[tuple] = []
    for bk, market_dict in all_batches.items():
        for symbol, df in market_dict.items():
            for module, factor_name, param_list in factor_modules_params:
                work_meta.append((bk, symbol))
                work_args.append(((symbol, df), module, factor_name, param_list))

    # 3. Execute all units in a single thread pool (GIL released by cffi C extensions)
    max_workers = os.cpu_count() or 4
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(_calc_one_factor, item, module, factor_name, param_list)
            for item, module, factor_name, param_list in work_args
        ]
        raw_results = [f.result() for f in futures]

    # 4. Merge: accumulate factor columns keyed by (batch_key, symbol)
    factor_cols_map: dict[tuple, dict] = {}
    for (bk, symbol), (_, factor_name, raw) in zip(work_meta, raw_results):
        key = (bk, symbol)
        if key not in factor_cols_map:
            factor_cols_map[key] = {}
        for param_key, series in raw.items():
            factor_cols_map[key][f'{factor_name}_{param_key}'] = series

    # 5. Assign factor columns back into DataFrames and write all batches
    total_symbols = 0
    for bk, market_dict in all_batches.items():
        updated = {
            symbol: df.assign(**factor_cols_map.get((bk, symbol), {}))
            for symbol, df in market_dict.items()
        }
        preprocess_mgr.set_candle(bk, run_time, updated)
        total_symbols += len(updated)

    elapsed = time.perf_counter() - t_start
    logger.ok(
        f'预计算因子 {trade_type} 完成, symbols={total_symbols}, '
        f'当前时间={now_time_str()}, 耗时 {elapsed:.1f} 秒'
    )
