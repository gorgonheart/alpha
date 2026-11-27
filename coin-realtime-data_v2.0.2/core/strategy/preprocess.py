import os
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import chain
from pathlib import Path
from typing import Optional

import pandas as pd

from core.utils.log_kit import logger
from core.utils.time import now_time_str

from core.storage.candle_manager import CandleFileReader
from core.handle import BmacHandle
from core.utils.time import get_offset_from_runtime, wait_until_ready

PIVOT_COLUMNS = ['candle_begin_time', 'symbol', 'open', 'close', 'avg_price_1m', 'funding_fee']


def get_pivot_complete_flag_path(preprocess_mgr, trade_type: str, year) -> Path:
    """已闭合年份 pivot 的完成标记文件路径, 删除该文件可强制重建对应年份 pivot"""
    return Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).with_suffix('.complete')


def should_skip_pivot_year(preprocess_mgr, trade_type: str, year, cur_year: int) -> bool:
    """增量更新时判断某年份 pivot 是否可跳过重建

    pivot 文件存在时: 更早年份 (cur_year-2 及之前) 直接跳过;
    上一年需存在完成标记才跳过, 防止跨年停机导致年末数据缺失被永久冻结
    """
    if not Path(preprocess_mgr.format_data_file_path(f'market_pivot_{trade_type}_{year}')).exists():
        return False
    if int(year) < cur_year - 1:
        return True
    return int(year) == cur_year - 1 and get_pivot_complete_flag_path(preprocess_mgr, trade_type, year).exists()


def mark_pivot_complete_if_closed(preprocess_mgr, trade_type: str, year, cur_year: int, market_pivot: dict):
    """年份已闭合且 pivot 数据覆盖到年末最后一根 bar 时写入完成标记, 此后跳过该年份重建

    索引为 bar 开始时间, 完整判据为最后一根 bar 的结束时间 (开始时间 + bar 间隔) 到达次年 1-1 00:00,
    bar 间隔从索引相邻最小间距推断, 兼容任意 resample 周期与 offset
    """
    if int(year) >= cur_year:
        return

    idx = market_pivot['close'].index
    idx_max = idx.max() if len(idx) else None
    step = idx.to_series().diff().min()
    next_year_begin = pd.to_datetime(f'{int(year) + 1}-01-01 00:00:00+00:00')
    if len(idx) < 2 or pd.isna(step) or idx_max + step < next_year_begin:
        # 数据未回补到年末 (如跨年停机后首轮运行), 不冻结, 下轮继续重建
        logger.warning(f'Market Pivot {trade_type} {year} 数据未覆盖到年末 (最新={idx_max}), 暂不冻结')
        return

    flag_path = get_pivot_complete_flag_path(preprocess_mgr, trade_type, year)
    flag_path.write_text(f'{datetime.now()} idx_max={idx_max} step={step}\n')
    logger.info(f'Market Pivot {trade_type} {year} 数据完整, 已冻结, 后续跳过重建 (删除 {flag_path.name} 可强制重建)')


def read_candle_data(binance_mgr: CandleFileReader, data_api_mgr: Optional[CandleFileReader], symbol: str,
                     run_time: datetime, exchange: str = 'binance'):

    if binance_mgr.check_ready(symbol, run_time):
        return binance_mgr.read_candle(symbol), exchange

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
    """生成一批行情及按年拆分的 pivot 输入；年份仅由有效行情决定。

    Args:
        handle: 数据与存储配置；run_time: 本轮时间；trade_type: 市场类型。
        df_match: 现货合约匹配表；symbol_list: 本批交易对；batch_name: 批次名称。
    Returns:
        (行情批次名称, 年份到 pivot 输入名称的映射)。整批无有效行情或时间无效时，
        在写入任何批次文件前抛出 ValueError，避免发布不完整批次。
    """

    symbol_list = sorted(symbol_list)

    t_start = time.perf_counter()
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    context = f'{handle.exchange} {trade_type} {offset_str} {batch_name}'
    if not symbol_list:
        raise ValueError(f'预处理 {context}: 交易对列表为空')

    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]
    binance_handle = handle.get_market_handle(trade_type)

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
    # 逐个 symbol 读取并预处理 K 线
    for symbol in symbol_list:

        # 读取 K 线数据
        result = read_candle_data(binance_mgr, data_api_mgr, symbol, run_time, handle.exchange)
        if result is None:
            continue
        df_candle, source = result
        if df_candle is None or df_candle.empty:
            continue

        # 匹配只决定另一侧 symbol 字段，不能决定本币行情是否计入年份范围。
        candle_times = df_candle.get('candle_begin_time')
        if (candle_times is None or not pd.api.types.is_datetime64_any_dtype(candle_times)
                or candle_times.isna().any()):
            raise ValueError(f'预处理 {context}: {symbol} 行情时间无效')
        min_year = candle_times.min().year
        max_year = candle_times.max().year
        start_year = min_year if start_year is None else min(start_year, min_year)
        end_year = max_year if end_year is None else max(end_year, max_year)

        # 读取资金费数据(如有)
        df_funding = fundings.get(symbol, None) if fundings is not None else None

        # 统计来源
        sources[source] += 1

        # 添加交易框架需要的其他列
        df_candle = add_additional_cols(df_candle, trade_type, symbol, df_funding)

        # 读取现货与永续合约匹配结果
        symbol_match = df_match[df_match[trade_type] == symbol]
        if symbol_match.empty:
            market_dict[symbol] = df_candle
            continue

        # 按 candle_begin_time 匹配
        symbol_match = symbol_match.iloc[0]
        first_begin_time = symbol_match['first_begin_time']
        last_begin_time = symbol_match['last_begin_time']
        symbol_spot = symbol_match['spot']
        symbol_swap = symbol_match['swap']

        if pd.isna(first_begin_time) or pd.isna(last_begin_time):
            market_dict[symbol] = df_candle
            continue

        mask = df_candle['candle_begin_time'].between(first_begin_time, last_begin_time)
        df_candle.loc[mask, 'symbol_spot'] = symbol_spot
        df_candle.loc[mask, 'symbol_swap'] = symbol_swap

        market_dict[symbol] = df_candle

    if not market_dict:
        raise ValueError(f'预处理 {context}: 整批无有效行情')

    full_batch_name = f'{trade_type}_dict_{batch_name}'
    preprocess_mgr.set_candle(full_batch_name, run_time, market_dict)

    year_pivot_name = dict()
    cur_year = run_time.astimezone(timezone.utc).year
    for year in range(start_year, end_year + 1):
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
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


def gen_pivot(handle: BmacHandle, run_time: datetime, trade_type: str, year_pivot_names: dict[str, list]):
    preprocess_handle = handle.preprocess_handle
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]

    cur_year = run_time.astimezone(timezone.utc).year
    for year, pivot_names in year_pivot_names.items():
        # 增量更新，这一块的数据，为了生成 pivot 准备的
        if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
            continue
        market_dict = dict()
        for name in pivot_names:
            market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)

        market_pivot = make_market_pivot(market_dict, trade_type)
        preprocess_mgr.set_candle(f'market_pivot_{trade_type}_{year}', run_time, market_pivot)
        logger.info(f'生成 Market Pivot {trade_type} {year} 完成')
        mark_pivot_complete_if_closed(preprocess_mgr, trade_type, year, cur_year, market_pivot)

    preprocess_mgr.save_ready(f'market_pivot_{trade_type}', run_time)


def preprocess_type_data(handle: BmacHandle, run_time: datetime, trade_type: str, df_match: pd.DataFrame):
    """生成一侧行情和 pivot，全部完成后才发布总 ready。

    Args:
        handle: 数据与存储配置；run_time: 本轮时间；trade_type: 市场类型。
        df_match: 本轮现货合约匹配表。
    Returns:
        无。失败时清理本侧、本 offset、当轮的两个总标记并传播异常；
        不要求每个交易对都有数据，但不允许空交易对列表被发布为成功。
    """
    num_batch = handle.preprocess_handle.num_batch
    exg_mgr = handle.exg_mgr
    exginfo_name = f'exginfo_{trade_type}'
    offset_str = get_offset_from_runtime(run_time, handle.resample_interval)
    preprocess_handle = handle.preprocess_handle
    preprocess_mgr = preprocess_handle.preprocess_mgr_map[offset_str]

    # 同轮重试必须先撤销旧总标记；按精确路径删除，不扫描或影响其他市场/轮次。
    ready_paths = [Path(preprocess_mgr.format_ready_file_path(name, run_time))
                   for name in (f'{trade_type}_dict', f'market_pivot_{trade_type}')]
    for path in ready_paths:
        path.unlink(missing_ok=True)

    try:
        if not exg_mgr.check_ready(exginfo_name, run_time):
            logger.error(f'读取 {trade_type} Exchange Info 失败，预处理数据 {trade_type} 失败')
            return

        df_exginfo = exg_mgr.read_candle(exginfo_name)
        symbol_list = sorted(df_exginfo['symbol'].to_list())
        if not symbol_list:
            raise ValueError(f'预处理 {handle.exchange} {trade_type} {offset_str}: 交易对列表为空')

        logger.info(f'开始预处理 {trade_type}, 当前时间={now_time_str()}')

        batch_name = f'{trade_type}_dict_batch*.pkl'
        for p in preprocess_mgr.base_dir.rglob(batch_name):
            os.remove(p)

        idx = 1
        year_pivot_names = defaultdict(list)
        t_start = time.perf_counter()
        while symbol_list:
            if len(symbol_list) > num_batch:
                batch, symbol_list = symbol_list[:num_batch], symbol_list[num_batch:]
            else:
                batch, symbol_list = symbol_list, []

            full_batch_name, year_pivot_name = gen_batch_data(handle, run_time, trade_type, df_match, batch, f'batch{idx}')
            idx += 1

            for col, pivot_name in year_pivot_name.items():
                year_pivot_names[col].append(pivot_name)

        time_elapsed = time.perf_counter() - t_start
        logger.ok(f'预处理 Market Dict {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {time_elapsed:.1f} 秒')

        t_start = time.perf_counter()
        gen_pivot(handle, run_time, trade_type, year_pivot_names)
        # gen_pivot 已写完全部数据并发布 pivot ready，最后才允许 dict 消费者进入。
        preprocess_mgr.save_ready(f'{trade_type}_dict', run_time)
        time_elapsed = time.perf_counter() - t_start
        logger.ok(f'预处理 Pivot Table {trade_type} 完成, 当前时间={now_time_str()}, 耗时 {time_elapsed:.1f} 秒')
    except Exception:
        for path in ready_paths:
            path.unlink(missing_ok=True)
        raise


def preprocess_offset_data(handle: BmacHandle, run_time: datetime, symbol_begin_time_spot, symbol_begin_time_swap):
    df_match = preprocess_match_symbols(handle, run_time, symbol_begin_time_spot, symbol_begin_time_swap)

    if df_match is None:
        logger.error('读取现货与永续合约映射失败，预处理失败')

    preprocess_type_data(handle, run_time, 'spot', df_match)
    preprocess_type_data(handle, run_time, 'swap', df_match)


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
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
