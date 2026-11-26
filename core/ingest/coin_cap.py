import asyncio
from datetime import datetime, timedelta
import time
from typing import Optional

import pandas as pd

import config as bmac_config
from core.handle import BmacHandle, CoinCapHandle
from core.ingest.coin_cap_schema import COIN_CAP_DTYPES
from core.ingest.quantclass import QuantclassDataApi
from core.utils.log_kit import logger
from core.utils.network import async_retry_getter, create_aiohttp_session
from core.utils.time import now_time_str


_LAST_COIN_CAP_DATE: dict[str, datetime] = {}
_LAST_COIN_CAP_CHECK_MONO: dict[str, float] = {}


def reset_coin_cap_idle_state() -> None:
    """重置进程内市值检查缓存与冷却状态（供测试与显式复位使用）。"""
    _LAST_COIN_CAP_DATE.clear()
    _LAST_COIN_CAP_CHECK_MONO.clear()


def _coin_cap_cache_key(coin_cap_mgr) -> Optional[str]:
    base_dir = getattr(coin_cap_mgr, 'base_dir', None)
    return str(base_dir) if base_dir is not None else None


def _resolve_coin_cap_proxy(handle: BmacHandle) -> Optional[str]:
    use_proxy = getattr(handle, 'coin_cap_use_proxy', None)
    if use_proxy is None:
        use_proxy = getattr(bmac_config, 'coin_cap_use_proxy', False)
    return handle.proxy if use_proxy else None


def convert_coin_cap_csv(coin_cap_handle: CoinCapHandle, run_time: datetime):
    coin_cap_mgr = coin_cap_handle.coin_cap_mgr
    base_dir = coin_cap_mgr.base_dir
    csv_files = base_dir.glob('*.csv')
    for csv_file in csv_files:
        df_cap = pd.read_csv(
            csv_file,
            skiprows=1,
            encoding='gbk',
            parse_dates=['candle_begin_time', 'date_added'],
            dtype=COIN_CAP_DTYPES,
        )
        symbol = csv_file.stem.replace('-', '')
        df_cap['symbol'] = df_cap['symbol'].str.replace('-', '')

        if coin_cap_mgr.has_symbol(symbol):
            df_old = coin_cap_mgr.read_candle(symbol)
            df_cap = pd.concat([df_old, df_cap], ignore_index=True)
            df_cap.drop_duplicates('candle_begin_time', ignore_index=True, inplace=True)
            df_cap.sort_values('candle_begin_time', inplace=True, ignore_index=True)

        coin_cap_mgr.set_candle(symbol, run_time, df_cap)
        csv_file.unlink()


async def coin_cap_update_to_recent(handle: BmacHandle, run_time: datetime):
    coin_cap_mgr = handle.coin_cap_handle.coin_cap_mgr
    cache_key = _coin_cap_cache_key(coin_cap_mgr)

    max_candle_begin_time = _LAST_COIN_CAP_DATE.get(cache_key) if cache_key is not None else None
    if max_candle_begin_time is None:
        symbols = coin_cap_mgr.get_all_symbols()
        if not symbols:
            raise RuntimeError('历史市场数据不存在，请重新上传')
        max_candle_begin_time = max(coin_cap_mgr.read_candle(s)['candle_begin_time'].max() for s in symbols)
        if cache_key is not None:
            _LAST_COIN_CAP_DATE[cache_key] = max_candle_begin_time

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        cfg = handle.coin_cap_handle.cfg
        api = QuantclassDataApi(
            session,
            cfg.data_api_key,
            cfg.data_api_uuid,
            coin_cap_proxy=_resolve_coin_cap_proxy(handle),
        )
        df_product = await async_retry_getter(api.aioreq_product_infos)

    last_update_time: datetime = df_product.loc['coin-cap-daily', 'update_time']

    if last_update_time.date() - max_candle_begin_time.date() > timedelta(days=30):
        raise RuntimeError('历史市场数据与最新数据相差超过 15 天，请重新上传')

    date_range = pd.date_range(max_candle_begin_time.date(), last_update_time.date(), inclusive='neither')
    latest_written = max_candle_begin_time
    for dt in date_range:
        await update_daily_coin_cap(handle, dt, run_time)
        if dt.date() > latest_written.date():
            latest_written = dt
    if cache_key is not None:
        _LAST_COIN_CAP_DATE[cache_key] = latest_written


async def update_daily_coin_cap(handle: BmacHandle, dt: datetime, run_time: datetime):
    coin_cap_mgr = handle.coin_cap_handle.coin_cap_mgr
    logger.debug(f'更新市场数据, 日期={dt.date()}, 当前时间={now_time_str()}')
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        cfg = handle.coin_cap_handle.cfg
        api = QuantclassDataApi(
            session,
            cfg.data_api_key,
            cfg.data_api_uuid,
            coin_cap_proxy=_resolve_coin_cap_proxy(handle),
        )
        data_url = await async_retry_getter(api.aioreq_coincap_url, run_time=dt)
        logger.debug(f'url={data_url}')
        df: Optional[pd.DataFrame] = await async_retry_getter(api.aioreq_coincap_df, url=data_url)

    if df is None or df.empty:
        logger.warning(f'日期={dt.date()}, 获取市值数据为空, 跳过该日期')
        return

    for symbol, df_cap in df.groupby('symbol'):
        if coin_cap_mgr.has_symbol(symbol):
            df_old = coin_cap_mgr.read_candle(symbol)
            df_cap = pd.concat([df_old, df_cap], ignore_index=True)
            df_cap.drop_duplicates('candle_begin_time', ignore_index=True, inplace=True)
            df_cap.sort_values('candle_begin_time', inplace=True, ignore_index=True)
        coin_cap_mgr.set_candle(symbol, run_time, df_cap)


def run_coin_cap_update_to_recent(handle: BmacHandle, run_time: datetime):
    logger.info(f'更新市值数据, 当前时间={now_time_str()}')
    t_start = time.perf_counter()

    try:
        asyncio.run(coin_cap_update_to_recent(handle, run_time))
    except Exception:
        logger.exception('更新市值数据发生错误, 跳过本周期')
        return False

    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.info(f'市值数据更新成功, 当前时间={now_time_str()}, 耗时={time_elapsed:.2f}分钟')

    return True


def maybe_update_coin_cap_idle(
    handle: BmacHandle,
    run_time: datetime,
    *,
    min_interval_sec: float = 1800.0,
) -> Optional[bool]:
    """在非启用 5m 空档按冷却周期轻量检查/更新日频市值，避免占用启用轮关键发布窗口。"""
    coin_cap_handle = getattr(handle, 'coin_cap_handle', None)
    if coin_cap_handle is None:
        return None
    cache_key = _coin_cap_cache_key(coin_cap_handle.coin_cap_mgr) or str(id(coin_cap_handle.coin_cap_mgr))
    now_mono = time.monotonic()
    last_mono = _LAST_COIN_CAP_CHECK_MONO.get(cache_key)
    if last_mono is not None and (now_mono - last_mono) < min_interval_sec:
        return None
    _LAST_COIN_CAP_CHECK_MONO[cache_key] = now_mono
    return run_coin_cap_update_to_recent(handle, run_time)
