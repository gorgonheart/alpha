import importlib
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Optional

import pandas as pd

import config as bmac_config
from core.utils.log_kit import logger
from core.utils.time import now_time_str

from .candle_manager import CandleFileReader
from .candle_origins import load_origins, remember_ready_origins
from .handle import BmacHandle
from .arrow_publish import cleanup_old_snapshots, publish_arrow_snapshot
from .funding_hours import normalize_funding_hours
from .funding_publication import FundingPublication
from .utils.time import get_offset_from_runtime, wait_until_ready

PIVOT_COLUMNS = ['candle_begin_time', 'symbol', 'open', 'close', 'avg_price_1m', 'funding_fee']


def get_pivot_complete_flag_path(preprocess_mgr, trade_type, year):
    return Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).with_suffix('.complete')


def should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
    return (int(year) < cur_year
            and Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists()
            and get_pivot_complete_flag_path(preprocess_mgr, trade_type, year).exists())


def mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, run_time, pivot, interval):
    """Completion means the final offset-grid bar closed, not gap-free history."""
    now = pd.Timestamp(run_time).tz_convert('UTC')
    if int(year) >= now.year:
        return
    from core.utils.time import convert_interval_to_timedelta
    step = pd.Timedelta(convert_interval_to_timedelta(interval)).value
    offset = now.value % step  # Same UTC epoch grid as get_offset_from_runtime.
    boundary = pd.Timestamp(f'{int(year)+1}-01-01', tz='UTC').value
    final = pd.Timestamp(((boundary-1-offset)//step)*step+offset, tz='UTC')
    flag = get_pivot_complete_flag_path(preprocess_mgr, trade_type, year)
    if now < final + pd.Timedelta(step, unit='ns') or final not in pivot['close'].index:
        flag.unlink(missing_ok=True)
        return
    if not flag.exists():
        flag.write_text(str(final), encoding='utf-8')


def changed_price_years(old, incoming):
    """Small set used to invalidate year completion BEFORE saving raw batches."""
    current = incoming.set_index('candle_begin_time')[['open', 'close']]
    if old is None or old.empty:
        return set(current.index.year)
    previous = old.set_index('candle_begin_time')[['open', 'close']].reindex(current.index)
    same = current.eq(previous) | (current.isna() & previous.isna())
    changed = ~same.all(axis=1) | ~current.index.isin(old['candle_begin_time'])
    return set(current.index[changed].year)


def invalidate_price_years(preprocess_mgr, trade_type, years):
    for year in years:
        get_pivot_complete_flag_path(preprocess_mgr, trade_type, year).unlink(missing_ok=True)


def read_candle_data(binance_mgr: CandleFileReader, data_api_mgr: Optional[CandleFileReader], symbol: str,
                     run_time: datetime, *, source=None):

    if source == 'absent':
        return None
    if source in (None, 'binance') and binance_mgr.check_ready(symbol, run_time):
        return binance_mgr.read_candle(symbol), 'binance'

    if source in (None, 'data_api') and data_api_mgr is not None:
        if data_api_mgr.check_ready(symbol, run_time):
            return data_api_mgr.read_candle(symbol), 'data_api'

    return None


def add_additional_cols(df: pd.DataFrame, trade_type: str, symbol: str, df_funding: Optional[pd.DataFrame],
                        prior_bounds=None):
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
    first, last = df['candle_begin_time'].min(), df['candle_begin_time'].max()
    # Keep age only for a continuing series whose old origin rolled out of the window.
    if prior_bounds is not None and prior_bounds[0] <= first <= prior_bounds[1] and first <= last:
        first = prior_bounds[0]
    df['first_candle_time'] = first
    df['last_candle_time'] = last

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
            fundings[symbol] = normalize_funding_hours(funding_mgr.read_candle(symbol))
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
        symbol_match['swap'] = None
        spot_first_begin_time, spot_last_begin_time = symbol_begin_time_spot[symbol_spot]
        symbol_match['first_begin_time'] = spot_first_begin_time
        symbol_match['last_begin_time'] = spot_last_begin_time
    else:
        symbol_match['spot'] = None
        swap_first_begin_time, swap_last_begin_time = symbol_begin_time_swap[symbol_swap]
        symbol_match['first_begin_time'] = swap_first_begin_time
        symbol_match['last_begin_time'] = swap_last_begin_time

    return symbol_match


def apply_symbol_match(frame, trade_type, symbol, row):
    """Replace the retained mapping, including cells no longer matched.

    Only the other market is optional; this market's own name remains usable.
    A rolling source window must not erase an earlier verified same pairing.
    New/changed partners are restricted to the currently proven overlap.
    """
    other = 'symbol_swap' if trade_type == 'spot' else 'symbol_spot'
    frame[f'symbol_{trade_type}'] = symbol
    previous = frame.get(other)
    frame[other] = ''
    if row is None or pd.isna(row.first_begin_time) or pd.isna(row.last_begin_time):
        return
    partner = row.swap if trade_type == 'spot' else row.spot
    if pd.isna(partner):
        return
    mask = frame['candle_begin_time'].between(row.first_begin_time, row.last_begin_time)
    if previous is not None:
        mask |= frame['candle_begin_time'].lt(row.first_begin_time) & previous.eq(partner)
    frame.loc[mask, other] = partner


def gen_batch_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                   symbol_list: list[str], batch_name: str, with_factors: bool = False, *,
                   funding_publication=None, year_pivot_out=None):
    if not symbol_list:
        return None, dict(), dict()

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

    origins = load_origins(preprocess_mgr.base_dir, trade_type)

    def _load_symbol(symbol):
        choices = getattr(handle, 'publication_sources', {}).get(trade_type)
        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time,
                                  source=None if choices is None else choices.get(symbol, 'absent'))
        if result is None:
            return None
        df_candle, source = result
        if df_candle.empty:
            return None
        df_funding = fundings.get(symbol, None) if fundings is not None else None
        return symbol, add_additional_cols(df_candle, trade_type, symbol, df_funding, origins.get(symbol)), source

    with ThreadPoolExecutor(max_workers=min(len(symbol_list), 16)) as pool:
        for loaded in pool.map(_load_symbol, symbol_list):
            if loaded is None:
                continue
            symbol, df_candle, source = loaded
            sources[source] += 1

            apply_symbol_match(df_candle, trade_type, symbol, match_lookup.get(symbol))
            market_dict[symbol] = df_candle

            min_year = df_candle['candle_begin_time'].min().year
            if start_year is None or start_year > min_year:
                start_year = min_year

            max_year = df_candle['candle_begin_time'].max().year
            if end_year is None or end_year < max_year:
                end_year = max_year

    full_batch_name = f'{trade_type}_dict_{batch_name}'
    if funding_publication is not None:
        for symbol, frame in market_dict.items():
            funding_publication.reconcile(symbol, frame, fundings.get(symbol))
    # Finding 3: persist the RAW batch (OHLCV+meta only). Factor columns are recomputed
    # from OHLCV every cycle and never read back as computational input, so writing them
    # to the batch pkl is dead I/O. They are embedded only into the returned dict (consumed
    # by the Arrow publish) below.
    if start_year is not None:
        invalidate_price_years(preprocess_mgr, trade_type, range(start_year, end_year + 1))
    preprocess_mgr.set_candle(full_batch_name, run_time, market_dict)

    year_pivot_name = dict()
    run_utc = run_time.astimezone(timezone.utc)
    cur_year = run_utc.year
    for year in (range(start_year, end_year + 1) if start_year is not None else ()):
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        # 跨年后一周内需同时更新上一年，之后只更新当前年

        if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
            continue
        pivot_batch_name = f'{trade_type}_for_pivot_{batch_name}_{year}'
        start_ts = pd.to_datetime(f'{year}-01-01 00:00:00+00:00')
        end_ts = pd.to_datetime(f'{year}-12-31 23:59:59+00:00')
        market_dict_for_pivot = {
            sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
            for sym, df in market_dict.items()
        }
        if year_pivot_out is not None:
            year_pivot_out.setdefault(year, {}).update(market_dict_for_pivot)
        else:
            preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
            year_pivot_name[year] = pivot_batch_name

    if with_factors:
        factor_modules_params = _load_factor_modules_params()
        market_dict = {
            symbol: apply_precomputed_factors(df, factor_modules_params)
            for symbol, df in market_dict.items()
        }

    time_elapsed = time.perf_counter() - t_start
    logger.debug(f'预处理 Market Dict {trade_type} {batch_name} 完成, '
                 f'交易对={symbol_list[0]} -- {symbol_list[-1]}, '
                 f'数据源={dict(sources)}, 当前时间={now_time_str()}, '
                 f'耗时 {time_elapsed:.1f} 秒')

    return full_batch_name, year_pivot_name, market_dict


def gen_batch_data_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                               symbol_list: list[str], batch_name: str, with_factors: bool = False, *,
                               funding_publication=None, year_pivot_out=None):
    if not symbol_list:
        return None, dict(), dict()

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    full_batch_name = f'{trade_type}_dict_{batch_name}'
    if funding_publication is None and trade_type == 'swap' and handle.fetch_funding_rate:
        funding_publication = FundingPublication(preprocess_mgr.base_dir, run_time)

    if not preprocess_mgr.has_symbol(full_batch_name):
        return gen_batch_data(handle, run_time, trade_type, df_match, symbol_list, batch_name,
                              with_factors=with_factors, funding_publication=funding_publication,
                              year_pivot_out=year_pivot_out)

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
    price_updates = []
    revised_years = set()
    origins = None
    start_year, end_year = None, None

    for symbol in symbol_list:
        choices = getattr(handle, 'publication_sources', {}).get(trade_type)
        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time,
                                  source=None if choices is None else choices.get(symbol, 'absent'))
        if result is None:
            if symbol in old_market_dict:
                new_market_dict[symbol] = old_market_dict[symbol]
            continue

        df_candle_full, _ = result
        if df_candle_full.empty:
            continue

        df_old = old_market_dict.get(symbol)
        df_funding = fundings.get(symbol, None) if fundings is not None else None
        if origins is None:
            origins = load_origins(preprocess_mgr.base_dir, trade_type)
        incoming = add_additional_cols(df_candle_full, trade_type, symbol, df_funding, origins.get(symbol))
        revised_years.update(changed_price_years(df_old, incoming))
        if df_old is not None and not df_old.empty:
            # Settlements are reconciled separately; absent sources must not erase
            # previously confirmed fees while replacing the price/volume overlap.
            old_rates = df_old.set_index('candle_begin_time')['funding_fee']
            overlap = incoming['candle_begin_time'].isin(old_rates.index)
            incoming.loc[overlap, 'funding_fee'] = incoming.loc[overlap, 'candle_begin_time'].map(old_rates).to_numpy()
            keep = ~df_old['candle_begin_time'].isin(incoming['candle_begin_time'])
            combined = pd.concat([df_old.loc[keep], incoming], ignore_index=True)
            combined.sort_values('candle_begin_time', inplace=True, ignore_index=True)
            combined['first_candle_time'] = df_old['first_candle_time'].iloc[0]
            combined['last_candle_time'] = combined['candle_begin_time'].max()
            new_market_dict[symbol] = combined
        else:
            new_market_dict[symbol] = incoming

        price_updates.append(symbol)

    for symbol, frame in new_market_dict.items():
        apply_symbol_match(frame, trade_type, symbol, match_lookup.get(symbol))

    # Funding-enabled markets also need cached rows from unavailable or
    # unchanged symbols: the pivot may be behind an already durable raw batch.
    for symbol in (new_market_dict if funding_publication is not None else price_updates):
        df_sym = new_market_dict[symbol]
        if funding_publication is not None:
            funding_publication.reconcile(symbol, df_sym, fundings.get(symbol))
        if df_sym.empty:
            continue
        min_year = df_sym['candle_begin_time'].min().year
        if start_year is None or start_year > min_year:
            start_year = min_year
        max_year = df_sym['candle_begin_time'].max().year
        if end_year is None or end_year < max_year:
            end_year = max_year

    # Finding 3: persist the RAW batch (OHLCV+meta only); factors are recomputed each
    # cycle and never read back as input, so they are embedded only into the returned
    # dict (consumed by the Arrow publish) below — not written to the batch pkl.
    invalidate_price_years(preprocess_mgr, trade_type, revised_years)
    preprocess_mgr.set_candle(full_batch_name, run_time, new_market_dict)

    year_pivot_name = dict()
    if start_year is not None and end_year is not None:
        run_utc = run_time.astimezone(timezone.utc)
        cur_year = run_utc.year
        for year in range(start_year, end_year + 1):
            if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
                continue
            pivot_batch_name = f'{trade_type}_for_pivot_{batch_name}_{year}'
            start_ts = pd.to_datetime(f'{year}-01-01 00:00:00+00:00')
            end_ts = pd.to_datetime(f'{year}-12-31 23:59:59+00:00')
            market_dict_for_pivot = {
                sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
                for sym, df in new_market_dict.items()
            }
            if year_pivot_out is not None:
                year_pivot_out.setdefault(year, {}).update(market_dict_for_pivot)
            else:
                preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
                year_pivot_name[year] = pivot_batch_name

    if with_factors:
        factor_modules_params = _load_factor_modules_params()
        new_market_dict = {
            symbol: apply_precomputed_factors(df, factor_modules_params)
            for symbol, df in new_market_dict.items()
        }

    return full_batch_name, year_pivot_name, new_market_dict


def gen_pivot(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list]):
    preprocess_handle = handle.preprocess_handle
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]

    run_utc = run_time.astimezone(timezone.utc)
    cur_year = run_utc.year
    for year, pivot_names in year_pivot_names.items():
        if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
            continue
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        market_dict = dict()
        for name in pivot_names:
            market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)

        market_pivot = make_market_pivot(market_dict, trade_type)
        preprocess_mgr.set_candle(f'market_pivot_{trade_type}_{year}', run_time, market_pivot)
        mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, run_time, market_pivot, handle.resample_interval)
        logger.info(f'生成 Market Pivot {trade_type} {year} 完成')

    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def update_pivot_rows(pivot, market_dict, trade_type):
    """Upsert only actual candle cells; preserve old symbols and older history.

    Called even after a failed publication already saved its raw batch. The source
    rows, not the latest batch diff, are authoritative for retrying a partial write.
    """
    frames = {s: f.set_index('candle_begin_time') for s, f in market_dict.items() if not f.empty}
    if not frames:
        return False
    index = pivot['open'].index
    for frame in frames.values():
        index = index.union(frame.index)
    index = index.sort_values()
    symbols = set(pivot['open'].columns) | set(frames)
    columns = pivot['open'].columns
    if set(columns) != symbols:
        columns = pd.CategoricalIndex(sorted(symbols), name='symbol')
    fields = {'open': 'open', 'close': 'close', 'vwap1m': 'avg_price_1m'}
    if trade_type == 'swap':
        fields['funding_rate'] = 'funding_fee'
    changed = False
    for key, column in fields.items():
        table = pivot[key]
        if not table.index.equals(index) or not table.columns.equals(columns):
            table = table.reindex(index=index, columns=columns)
            pivot[key] = table
            changed = True
        for symbol, frame in frames.items():
            values = frame[column]
            current = table.loc[frame.index, symbol]
            different = ~(current.eq(values) | (current.isna() & values.isna()))
            if different.any():
                table.loc[values.index[different], symbol] = values[different].to_numpy()
                changed = True
        if key == 'funding_rate':
            table.fillna(0, inplace=True)
    return changed


def gen_pivot_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list],
                          *, funding_publication=None, year_pivot_data=None):
    """Append new candles and repair overlap, without rewriting unchanged pivots."""
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]
    years = set(year_pivot_names)
    if year_pivot_data is not None:
        years.update(year_pivot_data)
    if funding_publication is not None:
        years.update(funding_publication.by_year)
    for year in sorted(years):
        pivot_key = f'market_pivot_{trade_type}_{year}'
        market_dict = {}
        if year_pivot_data is not None and year in year_pivot_data:
            market_dict.update(year_pivot_data.pop(year))
        for name in year_pivot_names.get(year, []):
            market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)
        market_dict = {s: f for s, f in market_dict.items() if not f.empty}
        if not preprocess_mgr.has_symbol(pivot_key):
            if not market_dict:
                continue
            pivot = make_market_pivot(market_dict, trade_type)
            changed = True
        else:
            pivot = preprocess_mgr.read_candle(pivot_key)
            changed = update_pivot_rows(pivot, market_dict, trade_type)
        if funding_publication is not None:
            changed = funding_publication.apply_to_pivot(year, pivot) or changed
        if changed:
            preprocess_mgr.set_candle(pivot_key, run_time, pivot)
            logger.info(f'生成 Market Pivot {trade_type} {year} 完成 (增量/修订)')
        mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, run_time, pivot, handle.resample_interval)
    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def publish_arrow_snapshot_for_type(handle: BmacHandle, run_time: datetime, trade_type: str,
                                    market_dict: dict[str, pd.DataFrame], publish_kline_num=None):
    """§3.4 发布步骤：把本轮（已含因子）的内存 ``market_dict`` 写为单个 Arrow IPC 快照。

    输出 ``preprocess_1h_resample/{minute}m/{trade_type}_dict_{epoch}.arrow``，
    与消费者 gate 标志 ``{trade_type}_dict_{epoch}.ready`` 同名（共享 stem）。
    纯加法：失败不影响既有 pkl 批次路径与 gate 标志，消费者可自动 fallback。

    Finding 1：直接复用批次生成时已在内存里的 ``market_dict``，不再从磁盘回读
    ``{type}_dict_batch*`` pkl（消除约 2.56GB/轮/offset 的无谓读盘）。
    """
    if not market_dict:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

    if publish_kline_num is None:
        publish_kline_num = getattr(bmac_config, 'publish_kline_num', 5000)

    epoch = int(run_time.timestamp())
    out_path = Path(preprocess_mgr.base_dir) / f'{trade_type}_dict_{epoch}.arrow'
    try:
        # gate 标志由上层 save_ready 统一管理（清理过期 + 单一信号），此处不写 .ready。
        publish_arrow_snapshot(market_dict, out_path, publish_kline_num, write_ready=False)
        # Finding 2：清理过期 epoch 快照，避免 {type}_dict_{epoch}.arrow 无限堆积。
        cleanup_old_snapshots(preprocess_mgr.base_dir, trade_type,
                              keep=getattr(bmac_config, 'arrow_snapshot_keep', 1), current_epoch=epoch)
    except Exception as exc:
        # 加法步骤：发布失败仅记录，不影响既有 pkl 路径与后续 gate 标志。
        logger.error(f'Arrow 发布 {trade_type} 失败（不影响 pkl 路径）: {exc!r}')


def preprocess_type_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                         *, factor_executor=None, factor_modules_params=None, publish_kline_num=None):
    num_batch = handle.preprocess_handle.num_batch
    exg_mgr = handle.exg_mgr
    exginfo_name = f'exginfo_{trade_type}'
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    if preprocess_mgr.check_ready(f'{trade_type}_dict', run_time) and preprocess_mgr.check_ready(
            f'market_pivot_{trade_type}', run_time):
        return
    
    if not exg_mgr.check_ready(exginfo_name, run_time):
        logger.error(f'读取 {trade_type} Exchange Info 失败，预处理数据 {trade_type} 失败')
        return

    df_exginfo = exg_mgr.read_candle(exginfo_name)
    symbol_list = sorted(df_exginfo['symbol'].to_list())
    if factor_modules_params is None:
        factor_modules_params = _load_factor_modules_params()
    calc_factors = partial(_calc_symbol_factors, factor_modules_params=factor_modules_params)

    logger.info(f'开始预处理 {trade_type}, 当前时间={now_time_str()}')

    idx = 1
    year_pivot_names = defaultdict(list)
    year_pivot_data: dict[int, dict[str, pd.DataFrame]] = {}
    t_start = time.perf_counter()
    market_dict: dict[str, pd.DataFrame] = {}
    funding_publication = (FundingPublication(preprocess_mgr.base_dir, run_time)
                           if trade_type == 'swap' and handle.fetch_funding_rate else None)
    while symbol_list:
        if len(symbol_list) > num_batch:
            batch, symbol_list = symbol_list[:num_batch], symbol_list[num_batch:]
        else:
            batch, symbol_list = symbol_list, []

        # Persist raw batches first; compute full-history factors only in memory.
        # Pass year_pivot_out so pivot slices stay in memory instead of round-tripping
        # through temporary *_for_pivot_batch*_{year}.pkl files on disk.
        _full_batch_name, year_pivot_name, batch_market_dict = gen_batch_data_incremental(
            handle, run_time, trade_type, df_match, batch, f'batch{idx}', with_factors=False,
            funding_publication=funding_publication, year_pivot_out=year_pivot_data)
        if factor_modules_params:
            # Ordered consumption propagates failures before this market is published.
            # Finish this batch before submitting another, bounding queued frame references.
            factor_map = factor_executor.map if factor_executor is not None else map
            batch_market_dict = dict(factor_map(calc_factors, batch_market_dict.items()))
        # Finding 1: keep the in-memory dict so publish never re-reads the batch pkl.
        market_dict.update(batch_market_dict)
        idx += 1

        for col, pivot_name in year_pivot_name.items():
            year_pivot_names[col].append(pivot_name)

    market_dict_elapsed = time.perf_counter() - t_start
    logger.ok(f'预处理 Market Dict (含因子) {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {market_dict_elapsed:.1f} 秒')

    if not market_dict or not any(not frame.empty for frame in market_dict.values()):
        raise ValueError('No current market candles; public ready withheld')

    # §3.4 Arrow 发布：原子写出消费者 mmap 读取的快照（每 symbol 一 batch + manifest）。
    t_publish = time.perf_counter()
    publish_arrow_snapshot_for_type(handle, run_time, trade_type, market_dict, publish_kline_num)
    publish_elapsed = time.perf_counter() - t_publish

    t_start = time.perf_counter()
    gen_pivot_incremental(handle, run_time, trade_type, year_pivot_names,
                          funding_publication=funding_publication, year_pivot_data=year_pivot_data)
    # Consumers open the batch/Arrow and the year pivot on this one signal.
    preprocess_mgr.save_ready(f'{trade_type}_dict', run_time)
    remember_ready_origins(preprocess_mgr.base_dir, trade_type, run_time, market_dict)
    pivot_elapsed = time.perf_counter() - t_start
    logger.ok(f'预处理 Pivot Table {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {pivot_elapsed:.1f} 秒')

    # Lever 1 决策依据：单轮各阶段墙钟分解（grep [io-stat] phase）。market_dict 阶段含
    # 批次读回+富集+因子计算+原始批次写盘；逐文件读写明细见上方 [io-stat] read/write 行。
    n_batches = idx - 1
    logger.info(f'[io-stat] phase {trade_type} symbols={len(market_dict)} batches={n_batches} '
                f'market_dict={market_dict_elapsed:.1f}s publish={publish_elapsed:.1f}s '
                f'pivot={pivot_elapsed:.1f}s')


def preprocess_offset_data(handle: BmacHandle, run_time: datetime, symbol_begin_time_spot, symbol_begin_time_swap):
    df_match = preprocess_match_symbols(handle, run_time, symbol_begin_time_spot, symbol_begin_time_swap)

    if df_match is None:
        logger.error('读取现货与永续合约映射失败，预处理失败')
        return

    # Fix the policy before launching either market; late requests apply next round.
    from .arrow_demand import publication_rows
    depth = getattr(bmac_config, 'publish_kline_num', 5000)
    if getattr(bmac_config, 'arrow_demand_enabled', False):
        offset = get_offset_from_runtime(run_time, handle.resample_interval)
        folder = handle.preprocess_handle.preprocess_mgr_map[offset].base_dir
        depth = publication_rows(folder, depth, enabled=True,
                                 minimum=getattr(bmac_config, 'arrow_demand_min_rows', None))
        logger.info(f'Arrow publication offset={offset} rows={depth} (0=full)')
    factor_modules_params = _load_factor_modules_params()
    workers = getattr(bmac_config, 'precompute_factor_workers', 0)
    # Separate from market threads: sharing their pool would deadlock on nested waits.
    factor_pool = (ThreadPoolExecutor(max_workers=workers, thread_name_prefix='factors')
                   if workers > 0 and factor_modules_params else nullcontext(None))
    with factor_pool as factor_executor, ThreadPoolExecutor(max_workers=2) as pool:
        kwargs = dict(factor_executor=factor_executor, factor_modules_params=factor_modules_params,
                      publish_kline_num=depth)
        spot_future = pool.submit(preprocess_type_data, handle, run_time, 'spot', df_match, **kwargs)
        swap_future = pool.submit(preprocess_type_data, handle, run_time, 'swap', df_match, **kwargs)
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


def _load_factor_modules_params():
    """Resolve precompute_factor_configs into [(module, factor_name, param_list), ...].

    Returns an empty list when no factors are configured. Shared by gen_factor_data
    (two-step path) and the embedded one-step path so both compute identical columns.
    """
    factor_configs = getattr(bmac_config, 'precompute_factor_configs', {})
    if not factor_configs:
        return []
    return [
        (importlib.import_module(f'factors.{factor_name}'), factor_name, param_list)
        for factor_name, param_list in factor_configs.items()
    ]


def apply_precomputed_factors(df: pd.DataFrame, factor_modules_params) -> pd.DataFrame:
    """Compute all configured factor columns for one symbol DataFrame and assign them.

    Columns are named ``{factor_name}_{param}`` (factor_calc.py convention). This is the
    single source of truth for factor embedding: gen_factor_data and the one-step
    gen_batch_data_incremental(with_factors=True) both call it, guaranteeing the
    write-once path is value/dtype-identical to the legacy two-step path.
    """
    if not factor_modules_params:
        return df
    factor_cols: dict[str, pd.Series] = {}
    for module, factor_name, param_list in factor_modules_params:
        raw = module.signal_multi_params(df, param_list)
        for param_key, series in raw.items():
            factor_cols[f'{factor_name}_{param_key}'] = series
    return df.assign(**factor_cols)


def _calc_symbol_factors(item, factor_modules_params):
    """Worker: compute all pre-configured factor columns for one symbol DataFrame.
    ctypes/cffi calls release the GIL, so ThreadPoolExecutor achieves true CPU parallelism.
    """
    symbol, df = item
    return symbol, apply_precomputed_factors(df, factor_modules_params)


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
    factor_modules_params = _load_factor_modules_params()
    if not factor_modules_params:
        return

    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

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
        raw_results = []
        for (bk, symbol), fut in zip(work_meta, futures):
            try:
                raw_results.append(fut.result())
            except Exception as exc:
                logger.error(
                    f'预计算因子 {trade_type} 失败: batch={bk}, symbol={symbol}, error={exc!r}'
                )
                raise

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
