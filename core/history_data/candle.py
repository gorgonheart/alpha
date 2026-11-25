import asyncio
import shutil
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from core.exchange.fetcher_factory import create_fetcher
from core.handle import BmacHandle, MarketHandle
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.network import create_aiohttp_session
from core.utils.time import (async_sleep_until_run_time,
                             convert_interval_to_timedelta, next_run_time,
                             now_time_str)


def _is_rate_limited(exc: Exception) -> bool:
    """Duck-typing 判断异常是否为 429 / Too Many Requests。"""
    if getattr(exc, 'status_code', None) == 429:
        return True
    if getattr(exc, 'code', None) in (50011, '50011'):
        return True
    if 'Too Many Requests' in str(exc):
        return True
    return False


def _get_market_handle(handle: BmacHandle, trade_type: str) -> MarketHandle:
    if hasattr(handle, 'get_market_handle'):
        return handle.get_market_handle(trade_type)
    return handle.get_binance_handle(trade_type)


def _resolve_base_5m_retention_bars(handle: BmacHandle, base_delta: timedelta) -> int:
    if hasattr(handle, 'base_5m_retention_bars'):
        return int(handle.base_5m_retention_bars())
    retention_days = int(getattr(handle, 'base_5m_retention_days', 30))
    bars_per_day = int(timedelta(days=1) // base_delta)
    return retention_days * bars_per_day


def _resolve_full_backfill_base_bars(handle: BmacHandle, trade_type: str, run_time: datetime) -> int:
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    resample_delta = convert_interval_to_timedelta(handle.resample_interval)
    num_offsets = resample_delta // base_delta
    if hasattr(handle, 'effective_kline_count_1h'):
        kline_1h = int(handle.effective_kline_count_1h(trade_type, run_time))
        return int(num_offsets * (kline_1h + 3))
    if hasattr(handle, 'get_num_base_candles'):
        return int(handle.get_num_base_candles(trade_type))
    return int(getattr(handle, 'num_base_candles', num_offsets * (15000 + 3)))


def _required_first_begin_for_symbol(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    is_full_backfill: bool,
) -> datetime:
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    if is_full_backfill:
        bars = _resolve_full_backfill_base_bars(handle, trade_type, run_time)
    else:
        bars = _resolve_base_5m_retention_bars(handle, base_delta)
    return run_time - base_delta * bars


def detect_full_backfill_symbols(
    handle: BmacHandle,
    trade_type: str,
    symbols: list[str],
    run_time: datetime,
) -> set[str]:
    """检测需要深历史 5m 自愈补拉的 symbol 集合。

    严格只包含两类条件（绝不因既有 1h 头部较短而触发隐式前置补拉）：
      - 条件 1：任一启用 offset 的 resample_1h 文件缺失、为空、损坏或缺少 candle_begin_time；
      - 条件 2：任一启用 offset 的 resample_1h 最新 bar 早于
        ``run_time - timedelta(days=base_5m_retention_days - 2)``。
    """
    market_handle = _get_market_handle(handle, trade_type)
    resample_mgr_map = market_handle.resample_mgr_map
    offsets = getattr(handle, 'enabled_hour_offsets', tuple(resample_mgr_map.keys()))
    retention_days = int(getattr(handle, 'base_5m_retention_days', 30))
    stale_days = max(1, retention_days - 2)
    stale_cutoff = pd.Timestamp(run_time) - timedelta(days=stale_days)

    flagged: set[str] = set()
    for symbol in symbols:
        for offset_str in offsets:
            mgr = resample_mgr_map.get(offset_str)
            if mgr is None or not mgr.has_symbol(symbol):
                flagged.add(symbol)
                break
            try:
                df = mgr.read_candle(symbol)
            except Exception:
                flagged.add(symbol)
                break
            if not isinstance(df, pd.DataFrame) or df.empty or 'candle_begin_time' not in df.columns:
                flagged.add(symbol)
                break
            last_ts = df['candle_begin_time'].max()
            if pd.isna(last_ts) or last_ts < stale_cutoff:
                flagged.add(symbol)
                break

    return flagged


@dataclass
class CandleInfo:
    is_empty: bool
    first_begin_time: Optional[datetime]
    last_begin_time: Optional[datetime]


class Downloader(ABC):
    # 非 429 异常：同一 symbol 连续失败此次数则本阶段放弃（成功一次即清零）
    MAX_SYMBOL_FAILURES = 3
    # 整批 429 时的整体冷却：30s 起，逐次翻倍，最长 300s
    RATE_LIMIT_COOLDOWN_BASE_SEC = 30
    RATE_LIMIT_COOLDOWN_MAX_SEC = 300

    def __init__(
        self,
        handle: BmacHandle,
        binance_handle: MarketHandle,
        fetcher,
        trade_type: str,
        run_time: datetime,
        stage_name: str,
    ):
        self.handle = handle
        self.binance_handle = binance_handle
        self.fetcher = fetcher
        self.run_time = run_time
        self.trade_type = trade_type
        self.stage_name = stage_name

    @abstractmethod
    async def fetch(self, symbol: str, info: CandleInfo) -> tuple[pd.DataFrame, bool]:
        pass

    @abstractmethod
    def check_finish(self, symbol: str, info: CandleInfo) -> bool:
        pass

    async def _acquire_rate_limit(self, batch_len: int) -> None:
        if hasattr(self.fetcher, 'acquire'):
            await self.fetcher.acquire(batch_len)
            return
        max_minute_weight, _ = self.fetcher.get_api_limits()
        while True:
            _server_time, weight = await self.fetcher.get_time_and_weight()
            if weight > max_minute_weight * 0.9:
                await async_sleep_until_run_time(next_run_time("1m"))
                continue
            return

    def _batch_size(self) -> int:
        if hasattr(self.fetcher, 'batch_size'):
            return int(self.fetcher.batch_size())
        return 80

    def _once_candles(self) -> int:
        if hasattr(self.fetcher, 'once_candles'):
            return int(self.fetcher.once_candles())
        _, once_candles = self.fetcher.get_api_limits()
        return int(once_candles)

    async def run_download(self, candle_infos: dict[str, CandleInfo]):
        time_start = time.perf_counter()
        num_total_symbols = len(candle_infos)

        failures: dict[str, int] = {}
        skipped: set[str] = set()
        cooldown_streak = 0

        left_symbols = sorted(candle_infos.keys())

        download_round = 0
        last_info_ts = 0.0  # 进度日志 30s 节流用（秒）

        # 限制同时在途的请求数，避免瞬时大量连接打穿代理或触发交易所连接拒绝
        semaphore = asyncio.Semaphore(50)

        async def fetch_with_sem(symbol, info):
            async with semaphore:
                return await self.fetch(symbol, info)

        while left_symbols:
            download_round += 1

            fetch_symbols = left_symbols[:self._batch_size()]

            # 限速门控：按本批数量申请配额
            await self._acquire_rate_limit(len(fetch_symbols))

            # 并发获取 K 线，最多 50 个请求同时在途；return_exceptions=True 隔离单 symbol 异常
            tasks = [fetch_with_sem(symbol, candle_infos[symbol]) for symbol in fetch_symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            num_finished_round = 0
            num_rate_limited = 0
            for symbol, result in zip(fetch_symbols, results):
                if isinstance(result, BaseException):
                    if not isinstance(result, Exception):
                        raise result
                    if _is_rate_limited(result):
                        num_rate_limited += 1
                    if self._on_symbol_failed(symbol, result, left_symbols, candle_infos, failures, skipped):
                        num_finished_round += 1
                    continue

                failures.pop(symbol, None)
                df_new, not_enough = result

                symbol_dir = self.binance_handle.base_candle_dir / symbol
                ts_mgr = TSManager(symbol_dir)

                if not df_new.empty:
                    ts_mgr.update(df_new)

                partitions = ts_mgr.list_partitions()

                if not partitions:
                    num_finished_round += 1
                    left_symbols.remove(symbol)
                    del candle_infos[symbol]
                    continue

                first_begin_time = ts_mgr.read_partition(partitions[0])["candle_begin_time"].min()
                last_begin_time = ts_mgr.read_partition(partitions[-1])["candle_begin_time"].max()

                candle_info = CandleInfo(False, first_begin_time, last_begin_time)
                candle_infos[symbol] = candle_info

                if not_enough:
                    num_finished_round += 1
                    left_symbols.remove(symbol)
                    continue

                if self.check_finish(symbol, candle_info):
                    num_finished_round += 1
                    left_symbols.remove(symbol)

            if num_finished_round > 0 or download_round == 1:
                time_elapsed = (time.perf_counter() - time_start) / 60
                num_left = len(left_symbols)
                num_success = num_total_symbols - num_left
                symbol_in_progress = sorted(set(fetch_symbols).intersection(set(left_symbols)))
                num_in_progress = len(symbol_in_progress)

                if num_in_progress > 0:
                    in_progress_str = f", 正在下载: {symbol_in_progress[0]} -- {symbol_in_progress[-1]}"
                else:
                    in_progress_str = ""
                progress = (num_success / num_total_symbols) * 100 if num_total_symbols > 0 else 100.0
                msg = (
                    f"{self.trade_type} {self.stage_name}, "
                    f"进度={progress:.1f}%({num_success}/{num_total_symbols}), "
                    f"已耗时={time_elapsed:.1f}分钟, "
                    f"当前时间={now_time_str()}"
                    f"{in_progress_str}"
                )
                logger.debug(msg)
                # 进度按 30 秒节流输出到 INFO
                if download_round == 1 or time.perf_counter() - last_info_ts >= 30:
                    last_info_ts = time.perf_counter()
                    logger.info(msg)

            # 整批全部 429：整体冷却后再继续（逐次翻倍，最长 300s）
            if fetch_symbols and num_rate_limited == len(fetch_symbols):
                cooldown = min(
                    self.RATE_LIMIT_COOLDOWN_BASE_SEC * (2 ** cooldown_streak),
                    self.RATE_LIMIT_COOLDOWN_MAX_SEC,
                )
                cooldown_streak += 1
                logger.warning(
                    f"{self.trade_type} {self.stage_name}: 本批 {len(fetch_symbols)} 个 symbol 全部 429，"
                    f"整体冷却 {cooldown}s 后继续，剩余 {len(left_symbols)} 个"
                )
                await asyncio.sleep(cooldown)
            else:
                cooldown_streak = 0

        if skipped:
            logger.warning(
                f"{self.trade_type} {self.stage_name}: {len(skipped)} 个 symbol 连续失败 "
                f"{self.MAX_SYMBOL_FAILURES} 次已跳过，下次整体重下或重启时会继续补: {sorted(skipped)}"
            )
        return candle_infos

    def _on_symbol_failed(
        self,
        symbol: str,
        exc: Exception,
        left_symbols: list,
        candle_infos: dict,
        failures: dict,
        skipped: set,
    ) -> bool:
        rate_limited = _is_rate_limited(exc)
        if not rate_limited:
            failures[symbol] = failures.get(symbol, 0) + 1
        n_fail = failures.get(symbol, 0)

        left_symbols.remove(symbol)
        if n_fail >= self.MAX_SYMBOL_FAILURES:
            skipped.add(symbol)
            if not TSManager(self.binance_handle.base_candle_dir / symbol).list_partitions():
                candle_infos.pop(symbol, None)
            logger.warning(
                f"{self.trade_type} {self.stage_name}: {symbol} 连续失败 {n_fail} 次，本阶段跳过: {exc!r}"
            )
            return True

        left_symbols.append(symbol)
        if not rate_limited:
            logger.warning(
                f"{self.trade_type} {self.stage_name}: {symbol} 下载失败"
                f"（第 {n_fail}/{self.MAX_SYMBOL_FAILURES} 次），稍后重试: {exc!r}"
            )
        return False


class RecentDownloader(Downloader):
    """下载直到现在的最新 K线。"""

    def __init__(
        self,
        handle: BmacHandle,
        binance_handle: MarketHandle,
        fetcher,
        trade_type: str,
        run_time: datetime,
    ):
        super().__init__(handle, binance_handle, fetcher, trade_type, run_time, "阶段 2")

    async def fetch(self, symbol: str, info: CandleInfo):
        not_enough = False
        once_candles = self._once_candles()

        if info.is_empty:
            end_ts = int(self.run_time.timestamp()) * 1000
            df_new = await self.fetcher.get_candle(
                symbol, self.handle.base_interval, limit=once_candles, endTime=end_ts
            )
            not_enough = df_new.empty
        else:
            base_delta = convert_interval_to_timedelta(self.handle.base_interval)
            start_ts = int((info.last_begin_time + base_delta).timestamp()) * 1000
            df_new = await self.fetcher.get_candle(
                symbol, self.handle.base_interval, limit=once_candles, startTime=start_ts
            )
            not_enough = df_new.empty or (
                df_new["candle_begin_time"].max() < self.run_time
                and len(df_new) < once_candles
            )

        if not_enough:
            return df_new, not_enough

        df_new = df_new[df_new["candle_begin_time"] < self.run_time]
        return df_new, not_enough

    def check_finish(self, symbol: str, info: CandleInfo) -> bool:
        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        return bool(info.last_begin_time + base_delta >= self.run_time)


class HistoryDownloader(Downloader):
    """下载历史 K线（常规 symbol 30 天，full_backfill_symbols 按 effective_kline_count_1h 下满）。"""

    def __init__(
        self,
        handle: BmacHandle,
        binance_handle: MarketHandle,
        fetcher,
        trade_type: str,
        run_time: datetime,
        full_backfill_symbols: Optional[set[str]] = None,
    ):
        super().__init__(handle, binance_handle, fetcher, trade_type, run_time, "阶段 3")
        self.full_backfill_symbols = set(full_backfill_symbols or ())

    async def fetch(self, symbol: str, info: CandleInfo):
        once_candles = self._once_candles()
        end_ts = int(info.first_begin_time.timestamp()) * 1000
        df_new = await self.fetcher.get_candle(
            symbol, self.handle.base_interval, limit=once_candles, endTime=end_ts
        )
        df_new = df_new[df_new["candle_begin_time"] < self.run_time]

        if len(df_new) < once_candles:
            return df_new, True

        return df_new, False

    def check_finish(self, symbol: str, info: CandleInfo) -> bool:
        required_first_begin = _required_first_begin_for_symbol(
            self.handle,
            self.trade_type,
            self.run_time,
            symbol in self.full_backfill_symbols,
        )
        return bool(info.first_begin_time <= required_first_begin)


def _enable_fail_fast_on_rate_limit(fetcher) -> None:
    if hasattr(fetcher, "fail_fast_on_rate_limit"):
        fetcher.fail_fast_on_rate_limit = True


async def download_recent_until_runtime(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    candle_infos: dict[str, CandleInfo],
):
    market_handle = _get_market_handle(handle, trade_type)
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    pending_infos: dict[str, CandleInfo] = {
        symbol: info
        for symbol, info in candle_infos.items()
        if info.is_empty or info.last_begin_time is None or (info.last_begin_time + base_delta < run_time)
    }

    logger.info(
        f"{trade_type} 初始化历史 K线, 阶段 2, 补全 {handle.base_interval} 最新数据, "
        f"交易对数量={len(pending_infos)}/{len(candle_infos)}, "
        f"当前时间={now_time_str()}"
    )

    if not pending_infos:
        logger.ok(f"{trade_type} 初始化历史 K线, 阶段 2 无需补拉，当前时间={now_time_str()}")
        return candle_infos

    t_start = time.perf_counter()

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        fetcher = create_fetcher(handle, trade_type, session)
        _enable_fail_fast_on_rate_limit(fetcher)
        downloader = RecentDownloader(handle, market_handle, fetcher, trade_type, run_time)
        updated = await downloader.run_download(pending_infos)
        candle_infos.update(updated)

    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(
        f"{trade_type} 初始化历史 K线, 阶段 2 已完成, "
        f"当前时间={now_time_str()}, "
        f"耗时={time_elapsed:.2f}分钟"
    )

    return candle_infos


async def download_enough_history(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    candle_infos: dict[str, CandleInfo],
    full_backfill_symbols: Optional[set[str]] = None,
):
    market_handle = _get_market_handle(handle, trade_type)
    full_backfill_set = set(full_backfill_symbols or ())

    # 过滤掉已经满足目标起始时间的 symbol，避免无意义的阶段 3 请求
    pending_infos: dict[str, CandleInfo] = {}
    for symbol, info in candle_infos.items():
        if info.is_empty or info.first_begin_time is None:
            continue
        required_first_begin = _required_first_begin_for_symbol(
            handle, trade_type, run_time, symbol in full_backfill_set
        )
        if info.first_begin_time > required_first_begin:
            pending_infos[symbol] = info

    logger.info(
        f"{trade_type} 初始化历史 K线, 阶段 3, 补全 {handle.base_interval} K线根数, "
        f"交易对数量={len(pending_infos)}/{len(candle_infos)}, "
        f"全量自愈={len(full_backfill_set)}, "
        f"当前时间={now_time_str()}"
    )

    if not pending_infos:
        logger.ok(f"{trade_type} 初始化历史 K线, 阶段 3 无需补拉，当前时间={now_time_str()}")
        return candle_infos

    t_start = time.perf_counter()

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        fetcher = create_fetcher(handle, trade_type, session)
        _enable_fail_fast_on_rate_limit(fetcher)
        downloader = HistoryDownloader(
            handle, market_handle, fetcher, trade_type, run_time, full_backfill_symbols=full_backfill_set
        )
        updated = await downloader.run_download(pending_infos)
        candle_infos.update(updated)

    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(
        f"{trade_type} 初始化历史 K线, 阶段 3 已完成, "
        f"当前时间={now_time_str()}, "
        f"耗时={time_elapsed:.2f}分钟"
    )

    return candle_infos


def verify_symbol_candle(
    base_candle_dir: Path,
    trade_type: str,
    symbol: str,
    run_time: datetime,
    required_first_begin: datetime,
) -> CandleInfo:
    """校验 trade_type 类型 symbol 交易对的历史 K 线，并根据 required_first_begin 和 run_time 清理过时数据。"""
    symbol_dir = base_candle_dir / symbol

    empty_candle_info = CandleInfo(True, None, None)
    if not symbol_dir.exists():
        return empty_candle_info

    ts_mgr = TSManager(symbol_dir)
    try:
        ts_mgr.trim(required_first_begin, run_time)
    except Exception:
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    df = ts_mgr.read_all()
    if not isinstance(df, pd.DataFrame) or df.empty or "candle_begin_time" not in df.columns:
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    if df["candle_begin_time"].diff().max() > timedelta(days=28):
        logger.error(f"{trade_type} {symbol} 数据损坏，缺失数据超过 28 天")
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    return CandleInfo(False, df["candle_begin_time"].min(), df["candle_begin_time"].max())


def verify_exist_history(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols: list[str],
    full_backfill_symbols: Optional[set[str]] = None,
):
    market_handle = _get_market_handle(handle, trade_type)
    base_candle_dir = market_handle.base_candle_dir
    full_backfill_set = set(full_backfill_symbols or ())

    logger.info(
        f"{trade_type} 初始化历史 K线, 阶段 1, 校验并清除过时数据, "
        f"全量自愈={len(full_backfill_set)}/{len(symbols)}, 当前时间={now_time_str()}"
    )

    t_start = time.perf_counter()

    candle_infos = {}
    for idx, symbol in enumerate(symbols, 1):
        required_first_begin = _required_first_begin_for_symbol(
            handle, trade_type, run_time, symbol in full_backfill_set
        )
        candle_infos[symbol] = verify_symbol_candle(
            base_candle_dir, trade_type, symbol, run_time, required_first_begin
        )
        if idx % 40 == 0:
            time_elapsed = (time.perf_counter() - t_start) / 60
            finish_pct = idx / len(symbols) * 100
            logger.debug(
                f"{trade_type} 已完成 {finish_pct:.1f}%({idx}/{len(symbols)}) 交易对校验, "
                f"耗时={time_elapsed:.2f}分钟"
            )

    time_elapsed = (time.perf_counter() - t_start) / 60
    logger.ok(f"{trade_type} 初始化历史 K线, 阶段 1 已完成, 耗时={time_elapsed:.2f}分钟")
    return candle_infos


def _sync_ws_authority_to_tsmanager(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols: list[str],
) -> dict[str, pd.DataFrame]:
    """将本地 ws_cache（48h WAL）与 flat_pkl（_source=='ws'）中的权威闭合 K 线同步回灌到 TSManager：
    - 对 TSManager 现有连续段及 <= ts_last 的历史段立即覆盖更新，修复关停尾棒并保证分区与 WS 同源；
    - 若存在停机断档后的孤立 WS 尾棒，暂存至 post_gap_ws，待阶段 2 填补断档后再覆盖还原。
    """
    from core.ws.ws_local_cache import read_ws_cache_market_windows

    market_handle = _get_market_handle(handle, trade_type)
    base_candle_dir = Path(market_handle.base_candle_dir)
    if not symbols or not base_candle_dir.exists():
        return {}

    base_delta = convert_interval_to_timedelta(handle.base_interval)
    root_dir = getattr(handle, "base_dir", None)
    ws_cache_map: dict[str, pd.DataFrame] = {}
    if root_dir is not None:
        ws_db = Path(root_dir) / "ws_cache" / f"{trade_type}.sqlite"
        if ws_db.exists():
            end_excl_ms = int(run_time.timestamp() * 1000)
            start_ms = end_excl_ms - int(timedelta(hours=50).total_seconds() * 1000)
            ws_cache_map = read_ws_cache_market_windows(ws_db, trade_type, symbols, start_ms, end_excl_ms)

    post_gap_ws: dict[str, pd.DataFrame] = {}
    for symbol in symbols:
        parts: list[pd.DataFrame] = []
        flat_path = base_candle_dir / f"{symbol}.pkl"
        if flat_path.exists():
            try:
                df_flat = pd.read_pickle(flat_path)
            except Exception:
                df_flat = None
            if (
                isinstance(df_flat, pd.DataFrame)
                and not df_flat.empty
                and "_source" in df_flat.columns
                and "candle_begin_time" in df_flat.columns
            ):
                df_f_ws = df_flat[(df_flat["_source"] == "ws") & (df_flat["candle_begin_time"] < run_time)]
                if not df_f_ws.empty:
                    parts.append(df_f_ws.drop(columns=["_source"]))

        df_c_ws = ws_cache_map.get(symbol)
        if df_c_ws is not None and not df_c_ws.empty:
            df_c_ws = df_c_ws[df_c_ws["candle_begin_time"] < run_time]
            if not df_c_ws.empty:
                parts.append(df_c_ws)

        if not parts:
            continue

        df_ws = (
            pd.concat(parts, ignore_index=True)
            .drop_duplicates(subset="candle_begin_time", keep="last")
            .sort_values("candle_begin_time", ignore_index=True)
            if len(parts) > 1
            else parts[0].drop_duplicates(subset="candle_begin_time", keep="last").sort_values("candle_begin_time", ignore_index=True)
        )
        if df_ws.empty:
            continue

        sym_dir = base_candle_dir / symbol
        ts_mgr = TSManager(sym_dir)
        partitions = ts_mgr.list_partitions() if sym_dir.exists() else []
        if not partitions:
            post_gap_ws[symbol] = df_ws
            continue

        last_part = ts_mgr.read_partition(partitions[-1])
        if last_part is None or last_part.empty or "candle_begin_time" not in last_part.columns:
            post_gap_ws[symbol] = df_ws
            continue

        cur_end = last_part["candle_begin_time"].max()
        for ts in df_ws.loc[df_ws["candle_begin_time"] > cur_end, "candle_begin_time"]:
            if ts - cur_end <= base_delta:
                cur_end = ts
            else:
                break

        df_pre = df_ws[df_ws["candle_begin_time"] <= cur_end]
        if not df_pre.empty:
            ts_mgr.update(df_pre)

        df_post = df_ws[df_ws["candle_begin_time"] > cur_end]
        if not df_post.empty:
            post_gap_ws[symbol] = df_post

    return post_gap_ws


def init_history_candles(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    symbols: list[str],
    full_backfill_symbols: Optional[set[str]] = None,
) -> set[str]:
    if full_backfill_symbols is None:
        full_backfill_symbols = detect_full_backfill_symbols(handle, trade_type, symbols, run_time)
    else:
        full_backfill_symbols = set(full_backfill_symbols)

    # 阶段0: 先将本地权威闭合 WS K 线回灌至 TSManager，消除关停尾差与无缺口重启的假修订
    post_gap_ws = _sync_ws_authority_to_tsmanager(handle, trade_type, run_time, symbols)

    # 阶段1: 校验已保存的 K线数据，并清除过时的 K线（常规 symbol 裁回 30 天）
    candle_infos = verify_exist_history(
        handle, trade_type, run_time, symbols, full_backfill_symbols=full_backfill_symbols
    )

    # 阶段2: 向现在补全缺失的 K 线（仅对存在缺口的 symbol 请求严格开区间 (last_begin_time, run_time)）
    candle_infos = asyncio.run(download_recent_until_runtime(handle, trade_type, run_time, candle_infos))

    if post_gap_ws:
        base_candle_dir = Path(_get_market_handle(handle, trade_type).base_candle_dir)
        for sym, df_post in post_gap_ws.items():
            sym_dir = base_candle_dir / sym
            if sym_dir.exists():
                TSManager(sym_dir).update(df_post)

    # 阶段3: 向过去补全基础 K 线（常规 30 天，自愈 symbol 按 effective_kline_count_1h 下满）
    asyncio.run(
        download_enough_history(
            handle, trade_type, run_time, candle_infos, full_backfill_symbols=full_backfill_symbols
        )
    )
    return full_backfill_symbols
