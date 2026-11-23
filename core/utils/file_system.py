import os
import re
import shutil
from pathlib import Path
from typing import Optional

import pandas as pd

from core.candle_origins import preserve_published_origins
from core.handle import BmacHandle
from core.utils.log_kit import logger


_PIVOT_FILE_RE = re.compile(r'^market_pivot_.+_(\d{4})\.(pkl|complete)$')


def _get_market_handle(handle, trade_type: str):
    if trade_type == 'spot':
        return getattr(handle, 'spot', None) or getattr(handle, 'binance_spot', None)
    if trade_type == 'swap':
        return getattr(handle, 'swap', None) or getattr(handle, 'binance_swap', None)
    raise ValueError(f'Unknown trade_type {trade_type}')


def ensure_dir(dir_path, remove_exist=False):
    if os.path.exists(dir_path):
        if remove_exist:
            shutil.rmtree(dir_path)
        else:
            return

    os.makedirs(dir_path, exist_ok=True)


def init_offset_dirs(offset_candle_mgr_map: dict):
    for offset_mgr in offset_candle_mgr_map.values():
        ensure_dir(offset_mgr.base_dir, remove_exist=False)


def init_dirs(handle: BmacHandle) -> None:
    preprocess_handle = getattr(handle, 'preprocess_handle', None)
    if preprocess_handle is not None:
        for mgr in preprocess_handle.preprocess_mgr_map.values():
            preserve_published_origins(mgr)

    # 非破坏性确保所有数据目录存在，绝不 rmtree 任何历史产物
    exg_mgr = getattr(handle, 'exg_mgr', None)
    if exg_mgr is not None:
        ensure_dir(exg_mgr.base_dir, remove_exist=False)

    spot_handle = _get_market_handle(handle, 'spot')
    swap_handle = _get_market_handle(handle, 'swap')

    if spot_handle is not None:
        ensure_dir(spot_handle.base_candle_dir, remove_exist=False)
        ensure_dir(spot_handle.resample_dir, remove_exist=False)
        init_offset_dirs(spot_handle.resample_mgr_map)

    if swap_handle is not None:
        ensure_dir(swap_handle.base_candle_dir, remove_exist=False)
        ensure_dir(swap_handle.resample_dir, remove_exist=False)
        init_offset_dirs(swap_handle.resample_mgr_map)

    funding_mgr = getattr(handle, 'funding_mgr', None)
    if funding_mgr is not None and getattr(handle, 'fetch_funding_rate', True):
        ensure_dir(funding_mgr.base_dir, remove_exist=False)

    data_api = getattr(handle, 'data_api', None)
    if data_api is not None:
        ensure_dir(data_api.data_api_spot_dir, remove_exist=False)
        init_offset_dirs(data_api.spot_mgr_map)
        ensure_dir(data_api.data_api_swap_dir, remove_exist=False)
        init_offset_dirs(data_api.swap_mgr_map)

    if preprocess_handle is not None:
        ensure_dir(preprocess_handle.preprocess_dir, remove_exist=False)
        init_offset_dirs(preprocess_handle.preprocess_mgr_map)

    coin_cap_handle = getattr(handle, 'coin_cap_handle', None)
    if coin_cap_handle is not None:
        ensure_dir(coin_cap_handle.coin_cap_mgr.base_dir, remove_exist=False)


def _atomic_write_pickle(obj, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    if isinstance(obj, pd.DataFrame):
        obj.to_pickle(tmp)
    else:
        pd.to_pickle(obj, tmp)
    os.replace(tmp, path)


def _trim_dataframe(df: pd.DataFrame, trim_ts: pd.Timestamp) -> tuple[pd.DataFrame, bool, set[int]]:
    if not isinstance(df, pd.DataFrame) or df.empty or 'candle_begin_time' not in df.columns:
        return df, False, set()
    cbt = pd.to_datetime(df['candle_begin_time'], utc=True)
    mask_old = cbt < trim_ts
    if not mask_old.any():
        return df, False, set()
    affected_years = {int(y) for y in cbt[mask_old].dt.year.unique() if int(y) >= trim_ts.year}
    trimmed = df.loc[~mask_old].reset_index(drop=True)
    return trimmed, True, affected_years


def _trim_symbol_dir(base_dir: Path, trim_ts: pd.Timestamp) -> tuple[int, set[int]]:
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return 0, set()
    trimmed_count = 0
    affected_years: set[int] = set()
    for pkl_path in sorted(base_dir.glob('*.pkl')):
        df = pd.read_pickle(pkl_path)
        trimmed_df, changed, years = _trim_dataframe(df, trim_ts)
        if changed:
            _atomic_write_pickle(trimmed_df, pkl_path)
            trimmed_count += 1
            affected_years |= years
    return trimmed_count, affected_years


def _trim_batch_dir(base_dir: Path, trim_ts: pd.Timestamp) -> tuple[int, set[int]]:
    base_dir = Path(base_dir)
    if not base_dir.exists():
        return 0, set()
    trimmed_count = 0
    affected_years: set[int] = set()
    for pkl_path in sorted(base_dir.glob('*_dict_batch*.pkl')):
        data = pd.read_pickle(pkl_path)
        if not isinstance(data, dict):
            continue
        file_changed = False
        updated = {}
        for sym, df in data.items():
            trimmed_df, changed, years = _trim_dataframe(df, trim_ts)
            if changed:
                file_changed = True
                affected_years |= years
            updated[sym] = trimmed_df
        if file_changed:
            _atomic_write_pickle(updated, pkl_path)
            trimmed_count += 1
    return trimmed_count, affected_years


def apply_explicit_history_trim(handle: BmacHandle, trim_ts: Optional[pd.Timestamp]) -> dict[str, int]:
    """显式按 trim_ts 裁剪本地历史文件。trim_ts is None 时为纯 no-op。"""
    stats = {'resample': 0, 'batch': 0, 'funding': 0, 'data_api': 0, 'pivots_removed': 0}
    if trim_ts is None:
        return stats

    trim_ts = pd.Timestamp(trim_ts)
    if trim_ts.tzinfo is None:
        trim_ts = trim_ts.tz_localize('UTC')
    else:
        trim_ts = trim_ts.tz_convert('UTC')

    affected_years: set[int] = set()

    # 1. resample_1h
    for trade_type in ('spot', 'swap'):
        mkt = _get_market_handle(handle, trade_type)
        if mkt is None:
            continue
        for mgr in getattr(mkt, 'resample_mgr_map', {}).values():
            cnt, years = _trim_symbol_dir(mgr.base_dir, trim_ts)
            stats['resample'] += cnt
            affected_years |= years

    # 2. batch PKL
    preprocess_handle = getattr(handle, 'preprocess_handle', None)
    if preprocess_handle is not None:
        for mgr in getattr(preprocess_handle, 'preprocess_mgr_map', {}).values():
            cnt, years = _trim_batch_dir(mgr.base_dir, trim_ts)
            stats['batch'] += cnt
            affected_years |= years

    # 3. funding
    funding_mgr = getattr(handle, 'funding_mgr', None)
    if funding_mgr is not None:
        cnt, years = _trim_symbol_dir(funding_mgr.base_dir, trim_ts)
        stats['funding'] += cnt
        affected_years |= years

    # 4. data_api
    data_api = getattr(handle, 'data_api', None)
    if data_api is not None:
        for mgr_map in (getattr(data_api, 'spot_mgr_map', {}), getattr(data_api, 'swap_mgr_map', {})):
            for mgr in mgr_map.values():
                cnt, years = _trim_symbol_dir(mgr.base_dir, trim_ts)
                stats['data_api'] += cnt
                affected_years |= years

    # 5. 旧年 Pivot 删除与受影响年份 .complete 失效
    trimmed_any = (stats['resample'] + stats['batch'] + stats['funding'] + stats['data_api']) > 0
    if trimmed_any:
        affected_years.add(int(trim_ts.year))

    if preprocess_handle is not None:
        for mgr in getattr(preprocess_handle, 'preprocess_mgr_map', {}).values():
            base_dir = Path(mgr.base_dir)
            if not base_dir.exists():
                continue
            for entry in sorted(base_dir.iterdir()):
                if not entry.is_file():
                    continue
                m = _PIVOT_FILE_RE.match(entry.name)
                if not m:
                    continue
                year = int(m.group(1))
                ext = m.group(2)
                if year < trim_ts.year:
                    entry.unlink()
                    if ext == 'pkl':
                        stats['pivots_removed'] += 1
                elif ext == 'complete' and trimmed_any and year in affected_years:
                    entry.unlink()

    return stats


def only_ready_files(directory, files):
    # 忽略不以 .ready 结尾的文件
    return [f for f in files if not f.endswith('.ready')]


def copy_data_api_type_candles(binance_resample_dir, data_api_resample_dir):
    offset_strs = os.listdir(binance_resample_dir)
    ignore_ready = shutil.ignore_patterns('*.ready')

    for offset_str in offset_strs:
        binance_offset_dir = os.path.join(binance_resample_dir, offset_str)
        data_api_offset_dir = os.path.join(data_api_resample_dir, offset_str)
        ensure_dir(data_api_offset_dir, remove_exist=False)

        shutil.copytree(binance_offset_dir, data_api_offset_dir, ignore=ignore_ready, dirs_exist_ok=True)
        shutil.copytree(binance_offset_dir, data_api_offset_dir, ignore=only_ready_files, dirs_exist_ok=True)


def copy_data_api_candles(handle: BmacHandle):
    spot_handle = _get_market_handle(handle, 'spot')
    swap_handle = _get_market_handle(handle, 'swap')
    copy_data_api_type_candles(spot_handle.resample_dir, handle.data_api.data_api_spot_dir)
    copy_data_api_type_candles(swap_handle.resample_dir, handle.data_api.data_api_swap_dir)
    logger.ok(f'Binance {handle.resample_interval} 复制到 Data API 成功')
