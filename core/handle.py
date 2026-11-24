import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

import config as bmac_config
from core.exchange.symbol_filter import BaseSymbolFilter, create_symbol_filter
from core.storage.candle_manager import CandleFileManager
from core.utils.log_kit import logger
from core.utils.time import convert_interval_to_timedelta

MIN_HISTORY_TRIM_AGE_HOURS = 14400
EFFECTIVE_HISTORY_SAFETY_BUFFER_HOURS = 48


def _parse_utc_timestamp(value: str | datetime | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        raise ValueError(f'Invalid timestamp value: {value!r}')
    if ts.tzinfo is None:
        return ts.tz_localize('UTC')
    return ts.tz_convert('UTC')


@dataclass
class DataApiCfg:
    # Quantclass Data API 的葫芦 ID (UUID)
    data_api_uuid: str

    # Quantclass Data API 的 apiKey
    data_api_key: str


@dataclass
class DataApiHandle:
    """
    Quantclass Data API 的句柄类，用于管理与 Quantclass 数据源的连接和数据存储
    """

    cfg: DataApiCfg

    # 现货市场重采样周期数据的存储目录路径
    data_api_spot_dir: Path

    # 现货市场重采样周期数据的映射表，键为偏移量 (offset)，值为对应的 CandleFileManager 实例
    spot_mgr_map: dict[str, CandleFileManager]

    # U 本位永续合约市场重采样周期数据的存储目录路径
    data_api_swap_dir: Path

    # U 本位永续合约市场重采样周期数据的映射表，键为偏移量 (offset)，值为对应的 CandleFileManager 实例
    swap_mgr_map: dict[str, CandleFileManager]

    # Data API 的超时时间（单位：秒），如果在时间内 Data API 数据未就绪，则放弃本次请求，默认 30 秒
    data_api_timeout_sec: int = 30

    def get_mgr_map(self, trade_type) -> dict[str, CandleFileManager]:
        if trade_type == 'spot':
            return self.spot_mgr_map
        elif trade_type == 'swap':
            return self.swap_mgr_map
        else:
            raise ValueError(f'Unknown trade_type {trade_type}')


@dataclass
class MarketHandle:
    """
    单个交易所某 trade_type（spot / swap）的通用市场数据句柄。

    被 Binance / OKX 等所有交易所复用承载各自的 spot / swap 数据。前身 BinanceHandle，
    保留 BinanceHandle = MarketHandle 别名以向后兼容。
    """

    # API 类型，可选值为 spot（现货）、coin_futures（币本位合约）和 usdt_futures（U 本位合约）
    api_type: str

    # Symbol 过滤器，用于筛选每个周期需要的交易对 (symbol)
    symbol_filter: BaseSymbolFilter

    # 基础周期（5m）数据的存储目录
    base_candle_dir: Path

    # 重采样周期（1h）数据的存储目录路径
    resample_dir: Path

    # 重采样周期（1h）数据的映射表，键为偏移量 (offset)，值为对应的 CandleFileManager 实例
    resample_mgr_map: dict[str, CandleFileManager]

    # 每次更新时从 REST API 请求的基础周期 K 线数量，默认值为 99
    once_update_candles: int = 99


BinanceHandle = MarketHandle


@dataclass
class PreprocessHandle:
    """
    预处理数据句柄类，用于管理和存储预处理后的市场数据
    """

    # 预处理数据的存储目录路径
    preprocess_dir: Path

    # 预处理数据的重采样周期（1h）映射表，键为偏移量 (offset)，值为对应的 CandleFileManager 实例
    preprocess_mgr_map: dict[str, CandleFileManager]

    # 预处理数据中单个 Batch 包含的 symbol 数量
    num_batch: int


@dataclass
class CoinCapHandle:
    cfg: DataApiCfg
    coin_cap_mgr: CandleFileManager


@dataclass
class BmacHandle:
    """
    BMAC (Binance Marketdata Async Client) 句柄类，用于管理和配置 BMAC
    """

    # BMAC 数据存储的根目录路径
    base_dir: Path

    # 基础周期，表示原始数据的 K 线间隔，例如 "5m" 表示 5 分钟
    base_interval: str

    # 重采样周期，表示对基础数据进行重采样的间隔，例如 "1h" 表示 1 小时
    resample_interval: str

    # 重采样周期 K 线数量（spot/swap 分别配置）
    kline_count_1h_spot: int
    kline_count_1h_swap: int

    # 基础周期 K 线的数量（spot/swap 分别配置）
    num_base_candles_spot: int
    num_base_candles_swap: int

    # 偏移量数量，表示一个重采样周期内包含多少个基础周期
    num_offsets: int

    # 是否获取资金费率数据
    fetch_funding_rate: bool

    # 资金费率数据的管理器，用于存储和读取资金费率数据
    funding_mgr: Optional[CandleFileManager]

    # 交易所信息(Exchange Info)和其他元数据的管理器，用于存储和读取交易所信息
    exg_mgr: CandleFileManager

    # Quantclass Data API 句柄，用于与 Quantclass 数据源进行交互
    data_api: Optional[DataApiHandle]

    # 现货市场子句柄（按 trade_type，交易所无关，兼容 binance_spot property）
    spot: MarketHandle

    # U 本位永续合约市场子句柄（按 trade_type，交易所无关，兼容 binance_swap property）
    swap: MarketHandle

    # 预处理数据句柄，用于生成和管理预处理数据
    preprocess_handle: Optional[PreprocessHandle]

    coin_cap_handle: Optional[CoinCapHandle]

    # 代理设置，用于配置网络请求的代理
    proxy: Optional[str]

    # 分钟偏移配置，只更新如下分钟偏移（hour_offset）
    enabled_hour_offsets: tuple

    # HTTP 请求的超时时间，单位为秒，默认为 8 秒
    http_timeout_sec: int = 8

    # 交易所标识，可选值为 'binance'（默认）或 'okx'
    exchange: str = 'binance'

    # v2.0.2 单值兼容字段
    kline_count_1h: int = 0
    num_base_candles: int = 0

    # 本地定制字段
    coin_cap_use_proxy: bool = False
    precompute_factor_configs: dict = field(default_factory=dict)
    precompute_factor_workers: int = 4
    arrow_demand_enabled: bool = False
    arrow_demand_min_rows: Optional[int] = None

    # 2B WebSocket 与存储保留配置
    use_websocket: bool = True
    rest_fetch_delay_seconds: float = 3.0
    num_writers: int = 4
    ws_wait_sec: float = 14.0
    ws_flat_pkl_bars: int = 240
    base_5m_retention_days: int = 30
    min_history_start_date: Optional[str] = '2025-03-25'
    history_trim_date: Optional[str] = None

    def __post_init__(self):
        if not self.kline_count_1h:
            self.kline_count_1h = max(self.kline_count_1h_spot, self.kline_count_1h_swap)
        if not self.num_base_candles:
            self.num_base_candles = max(self.num_base_candles_spot, self.num_base_candles_swap)

    @property
    def binance_spot(self) -> MarketHandle:
        return self.spot

    @property
    def binance_swap(self) -> MarketHandle:
        return self.swap

    def get_market_handle(self, trade_type: str) -> MarketHandle:
        if trade_type == 'spot':
            return self.spot
        elif trade_type == 'swap':
            return self.swap
        else:
            raise ValueError(f'Unknown trade_type {trade_type}')

    def get_binance_handle(self, trade_type: str) -> MarketHandle:
        return self.get_market_handle(trade_type)

    def kline_count_1h_for(self, trade_type: str) -> int:
        if trade_type == 'spot':
            return self.kline_count_1h_spot
        elif trade_type == 'swap':
            return self.kline_count_1h_swap
        else:
            raise ValueError(f'Unknown trade_type {trade_type}')

    def get_kline_count_1h(self, trade_type: str) -> int:
        if trade_type == 'spot':
            return self.kline_count_1h_spot
        return self.kline_count_1h_swap

    def get_num_base_candles(self, trade_type: str) -> int:
        if trade_type == 'spot':
            return self.num_base_candles_spot
        return self.num_base_candles_swap

    def history_trim_ts(self) -> Optional[pd.Timestamp]:
        if not self.history_trim_date:
            return None
        return _parse_utc_timestamp(self.history_trim_date)

    def base_5m_retention_bars(self) -> int:
        base_delta = convert_interval_to_timedelta(self.base_interval)
        bars_per_day = int(timedelta(days=1) // base_delta)
        return self.base_5m_retention_days * bars_per_day

    def flat_pkl_max_bars(self) -> int:
        return self.ws_flat_pkl_bars

    def effective_kline_count_1h(self, trade_type: str, run_time: datetime) -> int:
        base_count = self.kline_count_1h_for(trade_type)
        min_start_ts = _parse_utc_timestamp(self.min_history_start_date) if self.min_history_start_date else None
        trim_ts = self.history_trim_ts()

        if trim_ts is not None:
            if min_start_ts is None or trim_ts > min_start_ts:
                floor_ts = trim_ts
            else:
                floor_ts = min_start_ts
        else:
            floor_ts = min_start_ts

        if floor_ts is None:
            return base_count

        run_ts = _parse_utc_timestamp(run_time)
        resample_hours = convert_interval_to_timedelta(self.resample_interval).total_seconds() / 3600.0
        delta_hours = max(0.0, (run_ts - floor_ts).total_seconds() / 3600.0)
        anchored_bars = math.ceil(delta_hours / resample_hours) + EFFECTIVE_HISTORY_SAFETY_BUFFER_HOURS
        return max(base_count, anchored_bars)


def create_offset_candle_mgr_map(
    resample_dir: Path,
    hour_offsets: tuple[str],
    cfg=None,
) -> dict[str, CandleFileManager]:
    cfg = bmac_config if cfg is None else cfg
    _ = convert_interval_to_timedelta(cfg.base_interval)
    candle_mgr_map = {}
    for offset_str in hour_offsets:
        offset_candle_dir = os.path.join(resample_dir, offset_str)
        candle_mgr_map[offset_str] = CandleFileManager(offset_candle_dir, 'pickle')
    return candle_mgr_map


def get_coin_cap_dir(cfg=None) -> Path:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)
    return storage_path / 'coin_cap'


def validate_config(base_delta: timedelta, resample_delta: timedelta, cfg=None):
    cfg = bmac_config if cfg is None else cfg

    # 可行性检查 1: 重采样周期必须是基础周期的倍数
    if (resample_delta % base_delta).total_seconds() > 1e-6:
        logger.error(f'重采样周期 {cfg.resample_interval} 不是基础周期 {cfg.base_interval} 的倍数, 请重设')
        sys.exit(-1)

    # 可行性检查 2: Quantclass Data API 仅支持重采样周期为 1h 且基础周期为 5m
    if cfg.use_api.get('kline', False):
        if cfg.resample_interval.lower() != '1h' or cfg.base_interval.lower() != '5m':
            logger.error('Quantclass Data API 仅支持 1h 重采样周期和 5m 基础周期, 请重设')
            sys.exit(-1)

    if cfg.use_api.get('coin_cap', False):
        coin_cap_dir = get_coin_cap_dir(cfg)
        pkl_files = list(coin_cap_dir.glob('*.pkl'))
        csv_files = list(coin_cap_dir.glob('*.csv'))
        if not pkl_files and not csv_files:
            logger.error(f'历史市值数据不存在，请重新上传，目录={coin_cap_dir}')
            sys.exit(-1)

    # 可行性检查 3: 快照恢复与 kline / coin_cap 同走 Data API 鉴权
    if cfg.use_api.get('snapshot_restore', False):
        if not cfg.data_api_uuid or not cfg.data_api_key:
            logger.warning(
                'use_api.snapshot_restore 已开启但未配置 data_api_uuid / data_api_key, '
                '快照下载将鉴权失败并软退化为全量 API 下载'
            )

    # 可行性检查 4: history_trim_date 必须距今 >= 14400 小时 (600 天)
    trim_date = getattr(cfg, 'history_trim_date', None)
    min_start_date = getattr(cfg, 'min_history_start_date', None)
    if trim_date:
        trim_ts = _parse_utc_timestamp(trim_date)
        now_utc = pd.Timestamp.now(tz='UTC')
        age_hours = (now_utc - trim_ts).total_seconds() / 3600.0
        if age_hours < MIN_HISTORY_TRIM_AGE_HOURS:
            raise ValueError(
                f'history_trim_date={trim_date} 距今不足 {MIN_HISTORY_TRIM_AGE_HOURS} 小时 '
                f'(600 天), 当前仅 {age_hours:.1f} 小时'
            )
        if min_start_date:
            min_start_ts = _parse_utc_timestamp(min_start_date)
            if trim_ts > min_start_ts:
                logger.warning(
                    f'history_trim_date ({trim_date}) 晚于 min_history_start_date ({min_start_date}), '
                    '将以 history_trim_date 作为有效历史下限'
                )


def create_data_api_handle(data_api_cfg: DataApiCfg, hour_offsets: tuple[str], cfg=None) -> DataApiHandle:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)

    data_api_spot_dir = storage_path / 'data_api_spot_1h_resample'
    spot_candle_mgr_map = create_offset_candle_mgr_map(data_api_spot_dir, hour_offsets, cfg)

    data_api_swap_dir = storage_path / 'data_api_swap_1h_resample'
    swap_candle_mgr_map = create_offset_candle_mgr_map(data_api_swap_dir, hour_offsets, cfg)

    return DataApiHandle(
        cfg=data_api_cfg,
        data_api_spot_dir=data_api_spot_dir,
        spot_mgr_map=spot_candle_mgr_map,
        data_api_swap_dir=data_api_swap_dir,
        swap_mgr_map=swap_candle_mgr_map,
    )


class OkxSpotFilter(BaseSymbolFilter):
    """OKX 现货 symbol 过滤器，作用于 OkxFetcher.get_exchange_info 产出的 MAPPED 字典。"""

    def is_valid(self, x):
        if x['instType'] != 'SPOT':
            return False
        if x['quote_asset'] != 'USDT':
            return False
        if x['status'] != 'live':
            return False
        return True


class OkxSwapFilter(BaseSymbolFilter):
    """OKX U 本位永续 symbol 过滤器，作用于 OkxFetcher.get_exchange_info 产出的 MAPPED 字典。"""

    def is_valid(self, x):
        if x['instType'] != 'SWAP':
            return False
        if x['quote_asset'] != 'USDT':
            return False
        if x['status'] != 'live':
            return False
        if x['ct_type'] != 'linear':
            return False
        return True


def create_okx_spot_handle(hour_offsets: tuple[str], cfg=None) -> MarketHandle:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)
    spot_filter = OkxSpotFilter()
    base_dir = storage_path / 'okx_spot_5m'
    resample_dir = storage_path / 'okx_spot_1h_resample'
    resample_mgr_map = create_offset_candle_mgr_map(resample_dir, hour_offsets, cfg)
    return MarketHandle(
        api_type='spot',
        symbol_filter=spot_filter,
        base_candle_dir=base_dir,
        resample_dir=resample_dir,
        resample_mgr_map=resample_mgr_map,
        once_update_candles=99,
    )


def create_okx_swap_handle(hour_offsets: tuple[str], cfg=None) -> MarketHandle:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)
    swap_filter = OkxSwapFilter()
    base_dir = storage_path / 'okx_swap_5m'
    resample_dir = storage_path / 'okx_swap_1h_resample'
    resample_mgr_map = create_offset_candle_mgr_map(resample_dir, hour_offsets, cfg)
    return MarketHandle(
        api_type='swap',
        symbol_filter=swap_filter,
        base_candle_dir=base_dir,
        resample_dir=resample_dir,
        resample_mgr_map=resample_mgr_map,
        once_update_candles=99,
    )


def create_binance_spot_handle(hour_offsets: tuple[str], cfg=None) -> MarketHandle:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)

    spot_filter = create_symbol_filter(
        filter_name='TradingSpotFilter',
        params={
            'quote_asset': 'USDT',
            'keep_stablecoins': False,
            'keep_pre_market': False,
        },
    )

    base_dir = storage_path / f'binance_spot_{cfg.base_interval}'
    resample_dir = storage_path / f'binance_spot_{cfg.resample_interval}_resample'
    resample_mgr_map = create_offset_candle_mgr_map(resample_dir, hour_offsets, cfg)

    return MarketHandle(
        api_type='spot',
        symbol_filter=spot_filter,
        base_candle_dir=base_dir,
        resample_dir=resample_dir,
        resample_mgr_map=resample_mgr_map,
    )


def create_binance_swap_handle(hour_offsets: tuple[str], cfg=None) -> MarketHandle:
    cfg = bmac_config if cfg is None else cfg
    storage_path = Path(cfg.storage_path)

    swap_filter = create_symbol_filter(
        filter_name='TradingUsdtFuturesFilter',
        params={
            'quote_asset': 'USDT',
            'types': ['PERPETUAL', 'TRADIFI_PERPETUAL'],
        },
    )

    base_dir = storage_path / f'binance_swap_{cfg.base_interval}'
    resample_dir = storage_path / f'binance_swap_{cfg.resample_interval}_resample'
    resample_mgr_map = create_offset_candle_mgr_map(resample_dir, hour_offsets, cfg)

    return MarketHandle(
        api_type='usdt_futures',
        symbol_filter=swap_filter,
        base_candle_dir=base_dir,
        resample_dir=resample_dir,
        resample_mgr_map=resample_mgr_map,
    )


def _build_handle(cfg, *, exchange, spot_handle, swap_handle, data_root, fetch_funding_rate) -> BmacHandle:
    base_delta = convert_interval_to_timedelta(cfg.base_interval)
    resample_delta = convert_interval_to_timedelta(cfg.resample_interval)

    validate_config(base_delta, resample_delta, cfg)

    num_offsets = resample_delta // base_delta

    fallback_kline_count = getattr(cfg, 'kline_count_1h', 15000)
    kline_count_1h_spot = getattr(cfg, 'kline_count_1h_spot', fallback_kline_count)
    kline_count_1h_swap = getattr(cfg, 'kline_count_1h_swap', fallback_kline_count)

    num_base_candles_spot = num_offsets * (kline_count_1h_spot + 3)
    num_base_candles_swap = num_offsets * (kline_count_1h_swap + 3)

    data_api_cfg = DataApiCfg(data_api_uuid=cfg.data_api_uuid, data_api_key=cfg.data_api_key)

    data_api_handle = None
    if cfg.use_api.get('kline', False):
        data_api_handle = create_data_api_handle(data_api_cfg, cfg.enabled_hour_offsets, cfg)

    exginfo_dir = data_root / 'exginfo'
    exg_mgr = CandleFileManager(exginfo_dir, 'pickle')

    preprocess_handle = None
    if cfg.preprocess:
        preprocess_dir = data_root / 'preprocess_1h_resample'
        preprocess_mgr_map = create_offset_candle_mgr_map(preprocess_dir, cfg.enabled_hour_offsets, cfg)
        preprocess_handle = PreprocessHandle(
            preprocess_dir=preprocess_dir,
            preprocess_mgr_map=preprocess_mgr_map,
            num_batch=cfg.preprocess_num_batch,
        )

    funding_mgr = None
    if fetch_funding_rate:
        funding_dir = data_root / 'funding'
        funding_mgr = CandleFileManager(funding_dir, 'pickle')

    coin_cap_handle = None
    if cfg.use_api.get('coin_cap', False):
        coin_cap_dir = get_coin_cap_dir(cfg)
        mgr = CandleFileManager(coin_cap_dir, 'pickle')
        coin_cap_handle = CoinCapHandle(cfg=data_api_cfg, coin_cap_mgr=mgr)

    return BmacHandle(
        base_dir=Path(cfg.storage_path),
        base_interval=cfg.base_interval,
        resample_interval=cfg.resample_interval,
        kline_count_1h_spot=kline_count_1h_spot,
        kline_count_1h_swap=kline_count_1h_swap,
        num_base_candles_spot=num_base_candles_spot,
        num_base_candles_swap=num_base_candles_swap,
        num_offsets=num_offsets,
        fetch_funding_rate=fetch_funding_rate,
        funding_mgr=funding_mgr,
        exg_mgr=exg_mgr,
        data_api=data_api_handle,
        spot=spot_handle,
        swap=swap_handle,
        preprocess_handle=preprocess_handle,
        coin_cap_handle=coin_cap_handle,
        proxy=cfg.proxy,
        enabled_hour_offsets=cfg.enabled_hour_offsets,
        exchange=exchange,
        coin_cap_use_proxy=getattr(cfg, 'coin_cap_use_proxy', False),
        precompute_factor_configs=dict(getattr(cfg, 'precompute_factor_configs', {})),
        precompute_factor_workers=getattr(cfg, 'precompute_factor_workers', 4),
        arrow_demand_enabled=getattr(cfg, 'arrow_demand_enabled', False),
        arrow_demand_min_rows=getattr(cfg, 'arrow_demand_min_rows', None),
        use_websocket=getattr(cfg, 'use_websocket', True),
        rest_fetch_delay_seconds=getattr(cfg, 'rest_fetch_delay_seconds', 3.0),
        num_writers=getattr(cfg, 'num_writers', 4),
        ws_wait_sec=float(getattr(cfg, 'ws_wait_sec', 14.0)),
        ws_flat_pkl_bars=getattr(cfg, 'ws_flat_pkl_bars', 240),
        base_5m_retention_days=getattr(cfg, 'base_5m_retention_days', 30),
        min_history_start_date=getattr(cfg, 'min_history_start_date', '2025-03-25'),
        history_trim_date=getattr(cfg, 'history_trim_date', None),
    )


def create_okx_handle(cfg=None) -> BmacHandle:
    cfg = bmac_config if cfg is None else cfg
    spot_handle = create_okx_spot_handle(cfg.enabled_hour_offsets, cfg)
    swap_handle = create_okx_swap_handle(cfg.enabled_hour_offsets, cfg)
    return _build_handle(
        cfg,
        exchange='okx',
        spot_handle=spot_handle,
        swap_handle=swap_handle,
        data_root=Path(cfg.storage_path),
        fetch_funding_rate=cfg.funding_rate,
    )


def create_binance_handle(cfg=None) -> BmacHandle:
    cfg = bmac_config if cfg is None else cfg
    spot_handle = create_binance_spot_handle(cfg.enabled_hour_offsets, cfg)
    swap_handle = create_binance_swap_handle(cfg.enabled_hour_offsets, cfg)
    return _build_handle(
        cfg,
        exchange='binance',
        spot_handle=spot_handle,
        swap_handle=swap_handle,
        data_root=Path(cfg.storage_path),
        fetch_funding_rate=cfg.funding_rate,
    )


def create_bmac_handle_from_config(cfg=None) -> BmacHandle:
    """根据配置创建并初始化 BmacHandle 实例（默认使用 config.py）。"""
    return create_binance_handle(cfg)


def exchange_display_name(exchange: str) -> str:
    """将交易所标识映射为用户可见的展示名（经交易所注册表的 display_name；未知交易所原样返回）。"""
    from core.exchange.adapter import get_adapter

    try:
        return get_adapter(exchange).display_name
    except ValueError:
        return str(exchange)


def debug_handle(handle: BmacHandle):
    logger.debug(f'数据目录={handle.base_dir}')
    logger.debug(f'基础周期={handle.base_interval}, Resample 周期={handle.resample_interval}')
    logger.debug((f'spot {handle.resample_interval} 数量={handle.kline_count_1h_spot:,d}, '
                  f'swap {handle.resample_interval} 数量={handle.kline_count_1h_swap:,d}, '
                  f'Offset={handle.enabled_hour_offsets}'))

    name = exchange_display_name(handle.exchange)
    logger.debug(f'Exchange Info 目录={handle.exg_mgr.base_dir}')
    logger.debug(f'{name}现货, {handle.base_interval} 目录={handle.spot.base_candle_dir}')
    logger.debug(f'{name}现货, {handle.resample_interval} 目录={handle.spot.resample_dir}')
    logger.debug(f'{name}永续合约, {handle.base_interval} 目录={handle.swap.base_candle_dir}')
    logger.debug(f'{name}永续合约, {handle.resample_interval} 目录={handle.swap.resample_dir}')

    if handle.fetch_funding_rate:
        logger.debug(f'资金费目录={handle.funding_mgr.base_dir}')
    else:
        logger.debug('不使用资金费率数据')

    if handle.data_api is not None:
        logger.debug(f'DataAPI 现货目录={handle.data_api.data_api_spot_dir}')
        logger.debug(f'DataAPI 永续合约目录={handle.data_api.data_api_swap_dir}')
    else:
        logger.debug('不使用 DataAPI')

    if handle.preprocess_handle is not None:
        logger.debug(f'预处理数据目录={handle.preprocess_handle.preprocess_dir}')
    else:
        logger.debug('不使用预处理数据')

    if handle.coin_cap_handle is not None:
        logger.debug(f'市值数据目录={handle.coin_cap_handle.coin_cap_mgr.base_dir}')
    else:
        logger.debug('不使用市值数据数据')
