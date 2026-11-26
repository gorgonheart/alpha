import ctypes
import gc
import importlib
import inspect
import os
import sys
import time
import types
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

import config as bmac_config
from core.handle import BmacHandle
from core.storage.candle_manager import CandleFileReader, drop_file_page_cache
from core.utils.log_kit import logger
from core.utils.time import get_offset_from_runtime, now_time_str

from .arrow_publish import (
    cleanup_old_snapshots,
    publish_arrow_ipc_batches,
    publish_arrow_snapshot,
    serialize_market_dict_to_ipc_batches,
)
from .candle_origins import extract_ready_origins, load_origins, remember_ready_origins
from .funding_hours import normalize_funding_hours
from .funding_publication import FundingPublication

PIVOT_COLUMNS = ['candle_begin_time', 'symbol', 'open', 'close', 'avg_price_1m', 'funding_fee']


def _release_memory() -> None:
    """Explicitly reclaim cyclic garbage, PyArrow pool buffers, and free arena pages on Linux."""
    gc.collect()
    try:
        import pyarrow as pa
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    if sys.platform.startswith('linux'):
        try:
            ctypes.CDLL('libc.so.6').malloc_trim(0)
        except Exception:
            pass


def _get_history_trim_ts(handle) -> Optional[pd.Timestamp]:
    trim_fn = getattr(handle, 'history_trim_ts', None)
    if callable(trim_fn):
        return trim_fn()
    return None


def _apply_history_trim(df: pd.DataFrame, min_ts: Optional[pd.Timestamp]) -> pd.DataFrame:
    if min_ts is None or df.empty:
        return df
    return df.loc[df['candle_begin_time'] >= min_ts].reset_index(drop=True)


def _get_market_handle(handle, trade_type: str):
    getter = getattr(handle, 'get_market_handle', None)
    if callable(getter):
        return getter(trade_type)
    return handle.get_binance_handle(trade_type)


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
    final = pd.Timestamp(((boundary - 1 - offset) // step) * step + offset, tz='UTC')
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
    self_col = f'symbol_{trade_type}'
    if self_col not in frame.columns or not (len(frame) == 0 or (frame[self_col].iloc[0] == symbol and frame[self_col].iloc[-1] == symbol)):
        frame[self_col] = symbol
    if frame.empty:
        frame[other] = ''
        return
    if row is None or pd.isna(row.first_begin_time) or pd.isna(row.last_begin_time):
        frame[other] = ''
        return
    partner = row.swap if trade_type == 'spot' else row.spot
    if pd.isna(partner):
        frame[other] = ''
        return
    cbt = frame['candle_begin_time']
    # Fast path: entire sorted candle window lies inside proven [first_begin_time, last_begin_time]
    if cbt.iloc[0] >= row.first_begin_time and cbt.iloc[-1] <= row.last_begin_time:
        if other not in frame.columns or not (frame[other] == partner).all():
            frame[other] = partner
        return
    previous = frame.get(other)
    frame[other] = ''
    mask = cbt.between(row.first_begin_time, row.last_begin_time)
    if previous is not None:
        mask |= cbt.lt(row.first_begin_time) & previous.eq(partner)
    frame.loc[mask, other] = partner


def gen_batch_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                   symbol_list: list[str], batch_name: str, with_factors: bool = False, *,
                   funding_publication=None, year_pivot_out=None, defer_save: bool = False):
    if not symbol_list:
        return (None, dict(), dict(), None) if defer_save else (None, dict(), dict())

    symbol_list = sorted(symbol_list)

    t_start = time.perf_counter()
    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)

    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    binance_handle = _get_market_handle(handle, trade_type)

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

    min_ts = _get_history_trim_ts(handle)
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
        enriched = add_additional_cols(df_candle, trade_type, symbol, df_funding, origins.get(symbol))
        enriched = _apply_history_trim(enriched, min_ts)
        if enriched.empty:
            return None
        enriched['last_candle_time'] = enriched['candle_begin_time'].max()
        return symbol, enriched, source

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
    raw_to_save = dict(market_dict)

    def _save_raw_batch():
        preprocess_mgr.set_candle(full_batch_name, run_time, raw_to_save)
        if hasattr(preprocess_mgr, 'format_data_file_path'):
            drop_file_page_cache(preprocess_mgr.format_data_file_path(full_batch_name))

    if not defer_save:
        _save_raw_batch()

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

    if defer_save:
        return full_batch_name, year_pivot_name, market_dict, _save_raw_batch
    return full_batch_name, year_pivot_name, market_dict


_RAW_OHLCV_COMPARE_COLS = (
    'open', 'high', 'low', 'close', 'volume', 'quote_volume',
    'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume',
)
_TAIL_FAST_OVERLAP_BARS = 16


def _frames_ohlcv_equal(df_a: pd.DataFrame, df_b: pd.DataFrame) -> bool:
    """Fast C/NumPy check that candle_begin_time and all present raw OHLCV columns match bit-for-bit."""
    if len(df_a) != len(df_b):
        return False
    cbt_a = df_a['candle_begin_time']
    cbt_b = df_b['candle_begin_time']
    if cbt_a.dtype != cbt_b.dtype or not np.array_equal(cbt_a.array.asi8, cbt_b.array.asi8):
        return False
    for col in _RAW_OHLCV_COMPARE_COLS:
        if (col in df_a.columns) != (col in df_b.columns):
            return False
        if col in df_a.columns:
            arr_a = df_a[col].to_numpy(copy=False)
            arr_b = df_b[col].to_numpy(copy=False)
            if arr_a.dtype != arr_b.dtype:
                return False
            if pd.api.types.is_float_dtype(arr_a.dtype):
                if not np.array_equal(arr_a, arr_b, equal_nan=True):
                    return False
            elif not np.array_equal(arr_a, arr_b):
                return False
    return True


def _try_tail_enrich_merge(
    df_old: pd.DataFrame,
    df_candle_tail: pd.DataFrame,
    anchor_pos: int,
    trade_type: str,
    symbol: str,
    df_funding: Optional[pd.DataFrame],
    prior_bounds,
    min_ts: Optional[pd.Timestamp],
) -> tuple[Optional[pd.DataFrame], set[int]]:
    """对不变前缀 `df_old.iloc[:anchor_pos]` 执行零重复富集的尾部增量合并。"""
    if anchor_pos <= 0 or anchor_pos >= len(df_old) or df_candle_tail.empty:
        return None, set()
    if min_ts is not None and df_old['candle_begin_time'].iloc[0] < min_ts:
        return None, set()

    prefix = df_old.iloc[:anchor_pos]
    old_tail = df_old.iloc[anchor_pos:]
    tail_incoming = add_additional_cols(
        df_candle_tail.copy(), trade_type, symbol, df_funding, prior_bounds
    )
    rev_years = changed_price_years(old_tail, tail_incoming)

    old_rates = old_tail.set_index('candle_begin_time')['funding_fee']
    overlap = tail_incoming['candle_begin_time'].isin(old_rates.index)
    if overlap.any():
        if pd.api.types.is_float_dtype(old_rates.dtype) and not pd.api.types.is_float_dtype(tail_incoming['funding_fee'].dtype):
            tail_incoming['funding_fee'] = tail_incoming['funding_fee'].astype('float64')
        tail_incoming.loc[overlap, 'funding_fee'] = (
            tail_incoming.loc[overlap, 'candle_begin_time'].map(old_rates).to_numpy()
        )

    keep_old = ~old_tail['candle_begin_time'].isin(tail_incoming['candle_begin_time'])
    if keep_old.any():
        tail_merged = pd.concat([old_tail.loc[keep_old], tail_incoming], ignore_index=True)
        tail_merged.sort_values('candle_begin_time', inplace=True, ignore_index=True)
    else:
        tail_merged = tail_incoming.reset_index(drop=True)

    # 对齐 key_0 与 funding_fee dtype，避免 concat 产生列序或类型漂移
    if 'key_0' in prefix.columns and 'key_0' not in tail_merged.columns:
        tail_merged.insert(0, 'key_0', tail_merged['candle_begin_time'].dt.floor('h'))
    elif 'key_0' in tail_merged.columns and 'key_0' not in prefix.columns:
        prefix = prefix.copy()
        prefix.insert(0, 'key_0', prefix['candle_begin_time'].dt.floor('h'))

    if pd.api.types.is_float_dtype(prefix['funding_fee']) and not pd.api.types.is_float_dtype(tail_merged['funding_fee']):
        tail_merged['funding_fee'] = tail_merged['funding_fee'].astype('float64')

    if list(tail_merged.columns) != list(prefix.columns):
        if set(tail_merged.columns) != set(prefix.columns):
            return None, set()
        tail_merged = tail_merged[list(prefix.columns)]

    combined = pd.concat([prefix, tail_merged], ignore_index=True)
    combined['first_candle_time'] = df_old['first_candle_time'].iloc[0]
    combined['last_candle_time'] = combined['candle_begin_time'].iloc[-1]
    return combined, rev_years


def gen_batch_data_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                               symbol_list: list[str], batch_name: str, with_factors: bool = False, *,
                               funding_publication=None, year_pivot_out=None, resample_tail_hints=None,
                               defer_save: bool = False):
    shorthand = isinstance(run_time, str)
    if shorthand:
        trade_type, run_time = run_time, trade_type
        if not isinstance(symbol_list, (list, tuple)):
            df_exginfo = handle.exg_mgr.read_candle(f'exginfo_{trade_type}')
            symbol_list = sorted(df_exginfo['symbol'].to_list())
        if not isinstance(batch_name, str):
            batch_name = 'batch1'

    if not symbol_list:
        if defer_save:
            return (dict(), None, dict(), None) if shorthand else (None, dict(), dict(), None)
        return (dict(), None, dict()) if shorthand else (None, dict(), dict())

    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    full_batch_name = f'{trade_type}_dict_{batch_name}'
    if funding_publication is None and trade_type == 'swap' and handle.fetch_funding_rate:
        funding_publication = FundingPublication(preprocess_mgr.base_dir, run_time)

    if not preprocess_mgr.has_symbol(full_batch_name):
        if defer_save:
            fb, yp, md, save_fn = gen_batch_data(
                handle, run_time, trade_type, df_match, symbol_list, batch_name,
                with_factors=with_factors, funding_publication=funding_publication,
                year_pivot_out=year_pivot_out, defer_save=True,
            )
            return (md, fb, yp, save_fn) if shorthand else (fb, yp, md, save_fn)
        fb, yp, md = gen_batch_data(handle, run_time, trade_type, df_match, symbol_list, batch_name,
                                    with_factors=with_factors, funding_publication=funding_publication,
                                    year_pivot_out=year_pivot_out)
        return (md, fb, yp) if shorthand else (fb, yp, md)

    old_market_dict: dict[str, pd.DataFrame] = preprocess_mgr.read_candle(full_batch_name)

    symbol_list = sorted(symbol_list)
    binance_handle = _get_market_handle(handle, trade_type)
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

    min_ts = _get_history_trim_ts(handle)

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
    hints_map = resample_tail_hints or {}

    for symbol in symbol_list:
        choices = getattr(handle, 'publication_sources', {}).get(trade_type)
        src_choice = None if choices is None else choices.get(symbol, 'absent')
        df_old = old_market_dict.get(symbol)
        df_funding = fundings.get(symbol, None) if fundings is not None else None
        if origins is None:
            origins = load_origins(preprocess_mgr.base_dir, trade_type)

        # Fast Path 1: 内存重采样尾部直传（校验头部时间与锚点 K 线一致后免除 1.5 万行 1h pkl 读盘）
        hint = hints_map.get(symbol)
        deferred_arg = hint[3] if (hint is not None and len(hint) >= 4) else None
        is_deferred_1h = isinstance(deferred_arg, pd.DataFrame) or bool(deferred_arg)
        if (
            hint is not None
            and src_choice in (None, 'binance')
            and df_old is not None
            and len(df_old) > 2
            and (is_deferred_1h or binance_mgr.check_ready(symbol, run_time))
        ):
            hint_first, _hint_last, df_hint_tail = hint[:3]
            if (
                df_hint_tail is not None
                and len(df_hint_tail) >= 2
                and hint_first >= df_old['candle_begin_time'].iloc[0]
                and hint_first <= df_hint_tail['candle_begin_time'].iloc[0]
            ):
                anchor_ts = df_hint_tail['candle_begin_time'].iloc[0]
                anchor_pos = int(df_old['candle_begin_time'].searchsorted(anchor_ts))
                if (
                    0 < anchor_pos < len(df_old)
                    and _frames_ohlcv_equal(df_old.iloc[anchor_pos:anchor_pos + 1], df_hint_tail.iloc[:1])
                ):
                    fast_comb, fast_rev = _try_tail_enrich_merge(
                        df_old, df_hint_tail, anchor_pos, trade_type, symbol,
                        df_funding, origins.get(symbol), min_ts,
                    )
                    if fast_comb is not None:
                        revised_years.update(fast_rev)
                        new_market_dict[symbol] = fast_comb
                        price_updates.append(symbol)
                        continue

        if is_deferred_1h and src_choice in (None, 'binance') and not binance_mgr.check_ready(symbol, run_time):
            if isinstance(deferred_arg, pd.DataFrame) and not deferred_arg.empty:
                from core.pipeline.resample import update_resample_candle_incremental
                from core.utils.time import convert_interval_to_timedelta
                update_resample_candle_incremental(
                    binance_mgr, symbol, deferred_arg, None, run_time,
                    convert_interval_to_timedelta(resample_interval), min_ts=min_ts,
                )
            else:
                wait_deadline = time.monotonic() + 30.0
                while not binance_mgr.check_ready(symbol, run_time) and time.monotonic() < wait_deadline:
                    time.sleep(0.02)

        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time, source=src_choice)
        if result is None:
            if symbol in old_market_dict:
                retained = _apply_history_trim(old_market_dict[symbol], min_ts)
                if not retained.empty:
                    new_market_dict[symbol] = retained
            continue

        df_candle_full, _ = result
        if df_candle_full.empty:
            continue

        # Fast Path 2: 已读入磁盘 1h 全表，对齐起始位置后前缀 NumPy 快速校验一致时仅对尾部 ~16 根做 pandas 富集
        if (
            df_old is not None
            and len(df_old) > _TAIL_FAST_OVERLAP_BARS + 2
        ):
            anchor_pos = len(df_old) - _TAIL_FAST_OVERLAP_BARS
            first_full_ts = df_candle_full['candle_begin_time'].iloc[0]
            start_pos = int(df_old['candle_begin_time'].searchsorted(first_full_ts))
            full_prefix_len = anchor_pos - start_pos
            if (
                0 <= start_pos < anchor_pos
                and df_old['candle_begin_time'].iloc[start_pos] == first_full_ts
                and len(df_candle_full) > full_prefix_len
                and _frames_ohlcv_equal(df_old.iloc[start_pos:anchor_pos], df_candle_full.iloc[:full_prefix_len])
            ):
                fast_comb, fast_rev = _try_tail_enrich_merge(
                    df_old, df_candle_full.iloc[full_prefix_len:], anchor_pos, trade_type, symbol,
                    df_funding, origins.get(symbol), min_ts,
                )
                if fast_comb is not None:
                    revised_years.update(fast_rev)
                    new_market_dict[symbol] = fast_comb
                    price_updates.append(symbol)
                    continue

        incoming = add_additional_cols(df_candle_full, trade_type, symbol, df_funding, origins.get(symbol))
        revised_years.update(changed_price_years(df_old, incoming))
        if df_old is not None and not df_old.empty:
            # Settlements are reconciled separately; absent sources must not erase
            # previously confirmed fees while replacing the price/volume overlap.
            old_rates = df_old.set_index('candle_begin_time')['funding_fee']
            overlap = incoming['candle_begin_time'].isin(old_rates.index)
            if overlap.any():
                if pd.api.types.is_float_dtype(old_rates.dtype) and not pd.api.types.is_float_dtype(incoming['funding_fee'].dtype):
                    incoming['funding_fee'] = incoming['funding_fee'].astype('float64')
                incoming.loc[overlap, 'funding_fee'] = incoming.loc[overlap, 'candle_begin_time'].map(old_rates).to_numpy()
            keep = ~df_old['candle_begin_time'].isin(incoming['candle_begin_time'])
            combined = pd.concat([df_old.loc[keep], incoming], ignore_index=True)
            combined.sort_values('candle_begin_time', inplace=True, ignore_index=True)
            combined['first_candle_time'] = df_old['first_candle_time'].iloc[0]
            combined = _apply_history_trim(combined, min_ts)
            if combined.empty:
                continue
            combined['last_candle_time'] = combined['candle_begin_time'].max()
            new_market_dict[symbol] = combined
        else:
            incoming = _apply_history_trim(incoming, min_ts)
            if incoming.empty:
                continue
            incoming['last_candle_time'] = incoming['candle_begin_time'].max()
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
    raw_to_save = dict(new_market_dict)

    def _save_raw_batch():
        preprocess_mgr.set_candle(full_batch_name, run_time, raw_to_save)
        if hasattr(preprocess_mgr, 'format_data_file_path'):
            drop_file_page_cache(preprocess_mgr.format_data_file_path(full_batch_name))

    if not defer_save:
        _save_raw_batch()

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

    if defer_save:
        return (new_market_dict, full_batch_name, year_pivot_name, _save_raw_batch) if shorthand else (
            full_batch_name, year_pivot_name, new_market_dict, _save_raw_batch
        )
    if shorthand:
        return new_market_dict, full_batch_name, year_pivot_name
    return full_batch_name, year_pivot_name, new_market_dict


def _normalize_pivot_frame(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    missing = {}
    if 'symbol' not in df.columns:
        missing['symbol'] = symbol
    if 'avg_price_1m' not in df.columns:
        missing['avg_price_1m'] = df['open']
    if 'funding_fee' not in df.columns:
        missing['funding_fee'] = 0.0
    return df.assign(**missing) if missing else df


def _merge_or_build_year_pivot(preprocess_mgr, pivot_key: str, market_dict: dict[str, pd.DataFrame],
                               trade_type: str) -> dict:
    """Append-only merge into existing year pivot so delisted columns are never dropped."""
    if preprocess_mgr.has_symbol(pivot_key):
        market_pivot = preprocess_mgr.read_candle(pivot_key)
        update_pivot_rows(market_pivot, market_dict, trade_type)
        return market_pivot
    return make_market_pivot(market_dict, trade_type)


def gen_pivot(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list]):
    if isinstance(handle, dict):
        # Direct call: gen_pivot(market_dict, trade_type, run_time, preprocess_mgr)
        raw_market_dict = handle
        trade_type_str = run_time
        run_dt = trade_type
        preprocess_mgr = year_pivot_names
        by_year: dict[int, dict[str, pd.DataFrame]] = defaultdict(dict)
        for sym, df in raw_market_dict.items():
            norm = _normalize_pivot_frame(sym, df)
            if norm.empty:
                continue
            for year, grp in norm.groupby(norm['candle_begin_time'].dt.year):
                by_year[int(year)][sym] = grp
        for year, year_dict in sorted(by_year.items()):
            pivot_key = f'market_pivot_{trade_type_str}_{year}'
            market_pivot = _merge_or_build_year_pivot(preprocess_mgr, pivot_key, year_dict, trade_type_str)
            preprocess_mgr.set_candle(pivot_key, run_dt, market_pivot)
        preprocess_mgr.save_ready(f'market_pivot_{trade_type_str}', run_dt)
        return

    preprocess_handle = handle.preprocess_handle
    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
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

        pivot_key = f'market_pivot_{trade_type}_{year}'
        market_pivot = _merge_or_build_year_pivot(preprocess_mgr, pivot_key, market_dict, trade_type)
        preprocess_mgr.set_candle(pivot_key, run_time, market_pivot)
        mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, run_time, market_pivot, resample_interval)
        logger.info(f'生成 Market Pivot {trade_type} {year} 完成')

    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def update_pivot_rows(pivot, market_dict, trade_type):
    """Upsert only actual candle cells; preserve old symbols and older history.

    Called even after a failed publication already saved its raw batch. The source
    rows, not the latest batch diff, are authoritative for retrying a partial write.
    Vectorized 2D NumPy implementation: computes each symbol's time/column indexer
    once and updates dense float64 matrices without per-symbol pandas .loc overhead.
    """
    non_empty = [(s, f) for s, f in market_dict.items() if not f.empty]
    if not non_empty:
        return False

    base_index = pivot['open'].index
    base_i64 = base_index.asi8
    n_base = len(base_i64)

    cleaned_frames: list[tuple[str, pd.DataFrame, np.ndarray]] = []
    extra_i64: list[np.ndarray] = []
    for sym, frame in non_empty:
        cbt = frame['candle_begin_time']
        if cbt.duplicated().any():
            frame = frame.drop_duplicates(subset='candle_begin_time', keep='last')
            cbt = frame['candle_begin_time']
        if cbt.dtype == base_index.dtype:
            f_i64 = cbt.array.asi8
        else:
            f_i64 = pd.DatetimeIndex(cbt).tz_convert(base_index.tz).as_unit(base_index.unit).asi8
        cleaned_frames.append((sym, frame, f_i64))

        n_f = len(f_i64)
        if n_base > 0 and n_f > 0 and f_i64[0] >= base_i64[0]:
            p0 = int(np.searchsorted(base_i64, f_i64[0]))
            if p0 < n_base:
                overlap = min(n_f, n_base - p0)
                if np.array_equal(base_i64[p0:p0 + overlap], f_i64[:overlap]):
                    if overlap < n_f:
                        extra_i64.append(f_i64[overlap:])
                    continue
        extra_i64.append(f_i64)

    if extra_i64:
        extra_unique = np.unique(np.concatenate(extra_i64))
        extra_dt = pd.DatetimeIndex(
            pd.to_datetime(extra_unique, unit=base_index.unit, utc=(base_index.tz is not None)),
            name=base_index.name,
        )
        if base_index.tz is not None and str(extra_dt.tz) != str(base_index.tz):
            extra_dt = extra_dt.tz_convert(base_index.tz)
        index = base_index.union(extra_dt).sort_values()
        idx_i64 = index.asi8
    else:
        sample_dt = pd.DatetimeIndex(cleaned_frames[0][1]['candle_begin_time'])
        index = base_index.union(sample_dt).sort_values()
        idx_i64 = index.asi8

    old_cols = pivot['open'].columns
    symbols = set(old_cols) | {s for s, _, _ in cleaned_frames}
    columns = old_cols
    if set(columns) != symbols:
        columns = pd.CategoricalIndex(sorted(symbols), name='symbol')

    col_map = {sym: pos for pos, sym in enumerate(columns)}
    n_idx = len(idx_i64)
    n_cols = len(columns)

    placements: list[tuple[int, slice | np.ndarray, pd.DataFrame]] = []
    for sym, frame, f_i64 in cleaned_frames:
        n_f = len(f_i64)
        p0 = int(np.searchsorted(idx_i64, f_i64[0]))
        if p0 + n_f <= n_idx and idx_i64[p0 + n_f - 1] == f_i64[-1] and (
            n_f <= 2 or np.array_equal(idx_i64[p0:p0 + n_f], f_i64)
        ):
            row_sel: slice | np.ndarray = slice(p0, p0 + n_f)
        else:
            row_sel = np.searchsorted(idx_i64, f_i64)
        placements.append((col_map[sym], row_sel, frame))

    fields = {'open': 'open', 'close': 'close', 'vwap1m': 'avg_price_1m'}
    if trade_type == 'swap':
        fields['funding_rate'] = 'funding_fee'

    changed = False
    for key, column in fields.items():
        table = pivot[key]
        same_index = table.index.equals(index)
        same_cols = table.columns.equals(columns)
        need_reindex = not (same_index and same_cols)

        if not need_reindex:
            mat = np.array(table.to_numpy(dtype=np.float64, copy=False), dtype=np.float64, copy=True)
        else:
            mat = np.full((n_idx, n_cols), np.nan, dtype=np.float64)
            if not table.empty:
                t_vals = table.to_numpy(dtype=np.float64, copy=False)
                t_i64 = table.index.asi8
                if len(t_i64) <= n_idx and np.array_equal(idx_i64[:len(t_i64)], t_i64):
                    old_r: slice | np.ndarray = slice(0, len(t_i64))
                else:
                    old_r = np.searchsorted(idx_i64, t_i64)
                if same_cols:
                    mat[old_r, :] = t_vals
                else:
                    old_c = np.fromiter((col_map[c] for c in table.columns), dtype=np.intp, count=len(table.columns))
                    mat[np.ix_(np.arange(len(t_i64)) if isinstance(old_r, slice) else old_r, old_c)] = t_vals
            changed = True

        field_changed = need_reindex
        for col_pos, row_sel, frame in placements:
            vals = frame[column].to_numpy(dtype=np.float64, copy=False)
            cur = mat[row_sel, col_pos]
            if not np.array_equal(cur, vals, equal_nan=True):
                mat[row_sel, col_pos] = vals
                field_changed = True

        if key == 'funding_rate':
            nan_mask = np.isnan(mat)
            if nan_mask.any():
                mat[nan_mask] = 0.0
                field_changed = True

        if field_changed:
            pivot[key] = pd.DataFrame(mat, index=index, columns=columns)
            changed = True

    return changed


def gen_pivot_incremental(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list],
                          *, funding_publication=None, year_pivot_data=None):
    """Append new candles and repair overlap, without rewriting unchanged pivots."""
    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
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
        mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, run_time, pivot, resample_interval)
    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def publish_arrow_snapshot_for_type(handle: BmacHandle, run_time: datetime, trade_type: str,
                                    market_dict: dict[str, pd.DataFrame], publish_kline_num=None,
                                    *, history_trim_date: Optional[str] = None):
    """§3.4 发布步骤：把本轮（已含因子）的内存 ``market_dict`` 写为单个 Arrow IPC 快照。

    输出 ``preprocess_1h_resample/{minute}m/{trade_type}_dict_{epoch}.arrow``，
    与消费者 gate 标志 ``{trade_type}_dict_{epoch}.ready`` 同名（共享 stem）。
    纯加法：失败不影响既有 pkl 批次路径与 gate 标志，消费者可自动 fallback。

    Finding 1：直接复用批次生成时已在内存里的 ``market_dict``，不再从磁盘回读
    ``{type}_dict_batch*`` pkl（消除约 2.56GB/轮/offset 的无谓读盘）。
    """
    if not market_dict:
        return

    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]

    if publish_kline_num is None:
        publish_kline_num = getattr(bmac_config, 'publish_kline_num', getattr(handle, 'publish_kline_num', 5000))
    if history_trim_date is None:
        history_trim_date = getattr(handle, 'history_trim_date', None)

    epoch = int(run_time.timestamp())
    out_path = Path(preprocess_mgr.base_dir) / f'{trade_type}_dict_{epoch}.arrow'
    try:
        # gate 标志由上层 save_ready 统一管理（清理过期 + 单一信号），此处不写 .ready。
        publish_arrow_snapshot(
            market_dict,
            out_path,
            publish_kline_num,
            write_ready=False,
            history_trim_date=history_trim_date,
        )
        # Finding 2：清理过期 epoch 快照，避免 {type}_dict_{epoch}.arrow 无限堆积。
        keep = getattr(bmac_config, 'arrow_snapshot_keep', getattr(handle, 'arrow_snapshot_keep', 1))
        cleanup_old_snapshots(preprocess_mgr.base_dir, trade_type, keep=keep, current_epoch=epoch)
    except Exception as exc:
        # 加法步骤：发布失败仅记录，不影响既有 pkl 路径与后续 gate 标志。
        logger.error(f'Arrow 发布 {trade_type} 失败（不影响 pkl 路径）: {exc!r}')


_WORKER_FACTOR_MOD_CACHE: dict[str, types.ModuleType] = {}


def _can_offload_factors_to_process_pool(factor_modules_params) -> bool:
    for mod, factor_name, _ in (factor_modules_params or ()):
        if not isinstance(mod, types.ModuleType):
            return False
        if getattr(mod, '__name__', None) != f'factors.{factor_name}':
            return False
    return True


def _run_batch_worker_task(
    handle: BmacHandle,
    run_time: datetime,
    trade_type: str,
    df_match: pd.DataFrame,
    batch_symbols: list[str],
    batch_name: str,
    factor_specs: list[tuple[str, list]],
    publish_kline_num: int,
    resample_tail_hints: Optional[dict] = None,
):
    """常驻子进程内的单 Batch 全链路任务：
    增量合并 + 持久化无因子 Raw Batch + 全历史 C 因子计算 + 裁切 .tail(publish_kline_num) 序列化 Arrow IPC + 立即释放大表。
    """
    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
    preprocess_mgr = handle.preprocess_handle.preprocess_mgr_map[offset_str]
    funding_publication = (
        FundingPublication(preprocess_mgr.base_dir, run_time)
        if trade_type == 'swap' and handle.fetch_funding_rate
        else None
    )
    year_pivot_slice: dict[int, dict[str, pd.DataFrame]] = {}

    _fb, year_pivot_name, batch_market_dict, save_raw_batch_fn = gen_batch_data_incremental(
        handle, run_time, trade_type, df_match, batch_symbols, batch_name,
        with_factors=False,
        funding_publication=funding_publication,
        year_pivot_out=year_pivot_slice,
        resample_tail_hints=resample_tail_hints,
        defer_save=True,
    )

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='batch_save') as save_pool:
        save_fut = save_pool.submit(save_raw_batch_fn) if save_raw_batch_fn is not None else None
        try:
            if factor_specs:
                worker_fmp = []
                for factor_name, param_list in factor_specs:
                    mod = _WORKER_FACTOR_MOD_CACHE.get(factor_name)
                    if mod is None:
                        mod = importlib.import_module(f'factors.{factor_name}')
                        _WORKER_FACTOR_MOD_CACHE[factor_name] = mod
                    worker_fmp.append((mod, factor_name, param_list))
                batch_market_dict = {
                    sym: apply_precomputed_factors(df, worker_fmp)
                    for sym, df in batch_market_dict.items()
                }

            has_non_empty = any(not frame.empty for frame in batch_market_dict.values())
            batch_origins = extract_ready_origins(batch_market_dict)
            pub_depth = int(publish_kline_num or 0)
            schema_bytes, ipc_items = serialize_market_dict_to_ipc_batches(batch_market_dict, pub_depth)
            fpub_by_year = dict(funding_publication.by_year) if funding_publication is not None else {}
        except BaseException:
            if save_fut is not None:
                try:
                    save_fut.result()
                except Exception:
                    pass
            raise
        if save_fut is not None:
            save_fut.result()

    del batch_market_dict
    _release_memory()
    return (
        year_pivot_name,
        year_pivot_slice,
        fpub_by_year,
        batch_origins,
        schema_bytes,
        ipc_items,
        has_non_empty,
    )


def preprocess_type_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame,
                         *, factor_executor=None, factor_modules_params=None, publish_kline_num=None,
                         batch_executor=None, resample_tail_hints=None,
                         wait_before_submit=None, on_batches_submitted=None):
    submitted_notified = False
    try:
        num_batch = handle.preprocess_handle.num_batch
        exg_mgr = handle.exg_mgr
        exginfo_name = f'exginfo_{trade_type}'
        resample_interval = getattr(handle, 'resample_interval', '1h')
        offset_str = get_offset_from_runtime(run_time, resample_interval)
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

        logger.info(f'开始预处理 {trade_type}, 当前时间={now_time_str()}')

        batches: list[tuple[int, list[str]]] = []
        idx = 1
        rem = symbol_list
        while rem:
            if len(rem) > num_batch:
                batch, rem = rem[:num_batch], rem[num_batch:]
            else:
                batch, rem = rem, []
            batches.append((idx, batch))
            idx += 1

        use_batch_pool = (
            batch_executor is not None
            and _can_offload_factors_to_process_pool(factor_modules_params)
        )

        year_pivot_names = defaultdict(list)
        year_pivot_data: dict[int, dict[str, pd.DataFrame]] = {}
        funding_publication = (FundingPublication(preprocess_mgr.base_dir, run_time)
                               if trade_type == 'swap' and handle.fetch_funding_rate else None)

        if use_batch_pool:
            t_start = time.perf_counter()
            pub_depth = (
                publish_kline_num
                if publish_kline_num is not None
                else getattr(bmac_config, 'publish_kline_num', getattr(handle, 'publish_kline_num', 5000))
            )
            factor_specs = [(fn, pl) for _, fn, pl in (factor_modules_params or ())]
            if wait_before_submit is not None:
                wait_before_submit.wait(timeout=5.0)
            futures = []
            try:
                for b_idx, b_syms in batches:
                    b_hints = (
                        {s: resample_tail_hints[s] for s in b_syms if s in resample_tail_hints}
                        if resample_tail_hints
                        else None
                    )
                    futures.append(
                        batch_executor.submit(
                            _run_batch_worker_task,
                            handle,
                            run_time,
                            trade_type,
                            df_match,
                            b_syms,
                            f'batch{b_idx}',
                            factor_specs,
                            int(pub_depth or 0),
                            b_hints,
                        )
                    )
            finally:
                if on_batches_submitted is not None and not submitted_notified:
                    submitted_notified = True
                    on_batches_submitted()
    finally:
        if on_batches_submitted is not None and not submitted_notified:
            submitted_notified = True
            on_batches_submitted()

    if use_batch_pool:

        ipc_chunks: list[tuple[bytes, list[tuple[str, bytes]]]] = []
        ready_origins: dict = {}
        symbol_count = 0
        any_non_empty = False
        for fut in futures:
            (
                yp_name,
                yp_slice,
                fpub_by_year,
                b_origins,
                schema_bytes,
                ipc_items,
                has_non_empty,
            ) = fut.result()
            for col, pname in yp_name.items():
                year_pivot_names[col].append(pname)
            for yr, ydict in yp_slice.items():
                year_pivot_data.setdefault(yr, {}).update(ydict)
            if funding_publication is not None and fpub_by_year:
                for yr, cells_map in fpub_by_year.items():
                    funding_publication.by_year[yr].update(cells_map)
            ready_origins.update(b_origins)
            if schema_bytes is not None and ipc_items:
                ipc_chunks.append((schema_bytes, ipc_items))
                symbol_count += len(ipc_items)
            if has_non_empty:
                any_non_empty = True

        market_dict_elapsed = time.perf_counter() - t_start
        logger.ok(f'预处理 Market Dict (含因子) {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {market_dict_elapsed:.1f} 秒')

        if not ipc_chunks or not any_non_empty:
            raise ValueError('No current market candles; public ready withheld')

        t_publish = time.perf_counter()
        epoch = int(run_time.timestamp())
        out_path = Path(preprocess_mgr.base_dir) / f'{trade_type}_dict_{epoch}.arrow'
        history_trim_date = getattr(handle, 'history_trim_date', None)
        try:
            publish_arrow_ipc_batches(
                ipc_chunks,
                out_path,
                int(pub_depth or 0),
                write_ready=False,
                history_trim_date=history_trim_date,
            )
            keep = getattr(bmac_config, 'arrow_snapshot_keep', getattr(handle, 'arrow_snapshot_keep', 1))
            cleanup_old_snapshots(preprocess_mgr.base_dir, trade_type, keep=keep, current_epoch=epoch)
        except Exception as exc:
            logger.error(f'Arrow 发布 {trade_type} 失败（不影响 pkl 路径）: {exc!r}')
        publish_elapsed = time.perf_counter() - t_publish
        del ipc_chunks
        _release_memory()
    else:
        calc_factors = partial(_calc_symbol_factors, factor_modules_params=factor_modules_params)
        t_start = time.perf_counter()
        market_dict: dict[str, pd.DataFrame] = {}
        for b_idx, batch in batches:
            # Persist raw batches first; compute full-history factors only in memory.
            _full_batch_name, year_pivot_name, batch_market_dict = gen_batch_data_incremental(
                handle, run_time, trade_type, df_match, batch, f'batch{b_idx}', with_factors=False,
                funding_publication=funding_publication, year_pivot_out=year_pivot_data,
                resample_tail_hints=resample_tail_hints)
            if factor_modules_params:
                factor_map = factor_executor.map if factor_executor is not None else map
                batch_market_dict = dict(factor_map(calc_factors, batch_market_dict.items()))
            market_dict.update(batch_market_dict)
            del batch_market_dict

            for col, pivot_name in year_pivot_name.items():
                year_pivot_names[col].append(pivot_name)

        market_dict_elapsed = time.perf_counter() - t_start
        logger.ok(f'预处理 Market Dict (含因子) {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {market_dict_elapsed:.1f} 秒')

        if not market_dict or not any(not frame.empty for frame in market_dict.values()):
            raise ValueError('No current market candles; public ready withheld')

        t_publish = time.perf_counter()
        publish_arrow_snapshot_for_type(
            handle,
            run_time,
            trade_type,
            market_dict,
            publish_kline_num,
            history_trim_date=getattr(handle, 'history_trim_date', None),
        )
        publish_elapsed = time.perf_counter() - t_publish

        ready_origins = extract_ready_origins(market_dict)
        symbol_count = len(market_dict)
        del market_dict
        _release_memory()

    t_start = time.perf_counter()
    gen_pivot_incremental(handle, run_time, trade_type, year_pivot_names,
                          funding_publication=funding_publication, year_pivot_data=year_pivot_data)
    # Consumers open the batch/Arrow and the year pivot on this one signal.
    preprocess_mgr.save_ready(f'{trade_type}_dict', run_time)
    remember_ready_origins(preprocess_mgr.base_dir, trade_type, run_time, ready_origins)
    pivot_elapsed = time.perf_counter() - t_start
    logger.ok(f'预处理 Pivot Table {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {pivot_elapsed:.1f} 秒')

    n_batches = len(batches)
    logger.info(f'[io-stat] phase {trade_type} symbols={symbol_count} batches={n_batches} '
                f'market_dict={market_dict_elapsed:.1f}s publish={publish_elapsed:.1f}s '
                f'pivot={pivot_elapsed:.1f}s')


def preprocess_offset_data(handle: BmacHandle, run_time: datetime,
                           symbol_begin_time_spot=None, symbol_begin_time_swap=None,
                           *, batch_executor=None, resample_tails=None):
    df_match = preprocess_match_symbols(handle, run_time, symbol_begin_time_spot, symbol_begin_time_swap)

    if df_match is None:
        logger.error('读取现货与永续合约映射失败，预处理失败')
        return

    # Fix the policy before launching either market; late requests apply next round.
    from .arrow_demand import publication_rows
    depth = getattr(bmac_config, 'publish_kline_num', getattr(handle, 'publish_kline_num', 5000))
    demand_enabled = getattr(bmac_config, 'arrow_demand_enabled', getattr(handle, 'arrow_demand_enabled', False))
    if demand_enabled:
        resample_interval = getattr(handle, 'resample_interval', '1h')
        offset = get_offset_from_runtime(run_time, resample_interval)
        folder = handle.preprocess_handle.preprocess_mgr_map[offset].base_dir
        min_rows = getattr(bmac_config, 'arrow_demand_min_rows', getattr(handle, 'arrow_demand_min_rows', None))
        depth = publication_rows(folder, depth, enabled=True, minimum=min_rows)
        logger.info(f'Arrow publication offset={offset} rows={depth} (0=full)')
    factor_modules_params = _load_factor_modules_params()
    workers = getattr(bmac_config, 'precompute_factor_workers', getattr(handle, 'precompute_factor_workers', 0))

    need_thread_factor_pool = (
        workers > 0
        and factor_modules_params
        and not (batch_executor is not None and _can_offload_factors_to_process_pool(factor_modules_params))
    )
    factor_pool = (
        ThreadPoolExecutor(max_workers=workers, thread_name_prefix='factors')
        if need_thread_factor_pool
        else nullcontext(None)
    )

    sig = inspect.signature(preprocess_type_data)
    accepts_extra = (
        any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        or ('batch_executor' in sig.parameters and 'resample_tail_hints' in sig.parameters)
    )
    accepts_gate = (
        any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())
        or ('wait_before_submit' in sig.parameters and 'on_batches_submitted' in sig.parameters)
    )
    tails_map = resample_tails or {}
    import threading
    spot_submitted = threading.Event() if (batch_executor is not None and accepts_gate) else None

    with factor_pool as factor_executor:
        def _invoke_market(tt: str):
            kw = dict(
                factor_executor=factor_executor,
                factor_modules_params=factor_modules_params,
                publish_kline_num=depth,
            )
            if accepts_extra:
                kw['batch_executor'] = batch_executor
                kw['resample_tail_hints'] = tails_map.get(tt)
            if spot_submitted is not None:
                if tt == 'spot':
                    kw['on_batches_submitted'] = spot_submitted.set
                else:
                    kw['wait_before_submit'] = spot_submitted
            return preprocess_type_data(handle, run_time, tt, df_match, **kw)

        # 双市场并发调度：spot（3 批）确定性先于 swap（5 批）入队 4 进程池（总计仍 2 波完成，不拖慢 swap），
        # 确保 spot 100% 在第 1 波提前发 spot_dict.ready 供下游流式抢读。
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix='preprocess_mkt') as mkt_pool:
            fut_spot = mkt_pool.submit(_invoke_market, 'spot')
            fut_swap = mkt_pool.submit(_invoke_market, 'swap')
            fut_spot.result()
            fut_swap.result()
        _release_memory()


def preprocess_match_symbols(handle: BmacHandle, run_time: datetime,
                             symbol_begin_time_spot=None, symbol_begin_time_swap=None):
    exg_mgr = handle.exg_mgr
    if symbol_begin_time_spot is None:
        symbol_begin_time_spot = {}
    if symbol_begin_time_swap is None:
        symbol_begin_time_swap = {}

    if hasattr(exg_mgr, 'check_ready') and not exg_mgr.check_ready('spot_swap_matches', run_time):
        return None

    df_match = exg_mgr.read_candle('spot_swap_matches')
    match_results = [
        calc_match(match_row.spot, match_row.swap, symbol_begin_time_spot, symbol_begin_time_swap)
        for match_row in df_match.itertuples(index=False)
    ]
    df_match = pd.DataFrame.from_records(match_results)
    return df_match


def _default_load_factor_modules_params():
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


def load_factor_modules_params():
    return _default_load_factor_modules_params()


def _load_factor_modules_params():
    return load_factor_modules_params()


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

    resample_interval = getattr(handle, 'resample_interval', '1h')
    offset_str = get_offset_from_runtime(run_time, resample_interval)
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
