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
from core.handle import MarketHandle, BmacHandle
from core.storage.ts_manager import TSManager
from core.utils.log_kit import logger
from core.utils.network import is_rate_limited, create_aiohttp_session
from core.utils.time import convert_interval_to_timedelta, now_time_str


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
    def fetch(self, symbol, info) -> tuple[pd.DataFrame, bool]:
        pass

    @abstractmethod
    def check_finish(self, info):
        pass

    async def run_download(self, candle_infos: dict[str, CandleInfo]):
        """分批下载 K 线，并维护可进入后续阶段的 symbol 状态。

        Args:
            candle_infos: 各 symbol 在当前阶段开始时的本地 K 线范围。方法会
                原地更新该字典，并仅删除始终没有任何本地分区的 symbol。

        Returns:
            dict[str, CandleInfo]: 当前阶段结束后仍有可用数据的 symbol 状态。

        Side Effects:
            将新 K 线写入对应 symbol 的 :class:`TSManager` 分区。
        """
        fetcher = self.fetcher
        time_start = time.perf_counter()
        num_total_symbols = len(candle_infos)

        # 单阶段内的失败记录（只在本次 run_download 内有效）：
        # symbol → 连续非 429 失败次数；本阶段放弃的 symbol；连续整批 429 的轮数
        failures: dict[str, int] = {}
        skipped: set[str] = set()
        cooldown_streak = 0

        # 还未完成补全的交易对
        left_symbols = sorted(candle_infos.keys())

        download_round = 0

        # 循环分批获取每个 symbol 历史数据
        while left_symbols:
            download_round += 1

            # 每轮从剩余 symbol 中选择一批
            fetch_symbols = left_symbols[:fetcher.batch_size()]

            # 限速门控：按本批数量申请配额（币安路径基于每分钟权重 sleep 到下一分钟）
            await fetcher.acquire(len(fetch_symbols))

            # 为本轮需要获取的 symbols 创建获取 K 线数据的任务
            tasks = []
            for symbol in fetch_symbols:
                info = candle_infos[symbol]
                tasks.append(self.fetch(symbol, info))

            # 并获取 K 线，预计消耗权重为 160。
            # return_exceptions=True：单个 symbol 重试耗尽抛出的异常（如 429）只影响它自己，
            # 不能中止整个市场的下载，也不能让兄弟协程在 session 关闭后继续飞行。
            results = await asyncio.gather(*tasks, return_exceptions=True)

            num_finished_round = 0
            num_rate_limited = 0
            # 更新每个 symbol 的 K线数据
            for symbol, result in zip(fetch_symbols, results):
                if isinstance(result, BaseException):
                    if not isinstance(result, Exception):
                        raise result  # KeyboardInterrupt / CancelledError 等照常向上传播
                    if is_rate_limited(result):
                        num_rate_limited += 1
                    if self._on_symbol_failed(symbol, result, left_symbols, candle_infos, failures, skipped):
                        num_finished_round += 1
                    continue
                # 成功一次即清零失败计数
                failures.pop(symbol, None)
                df_new, not_enough = result

                symbol_dir = self.binance_handle.base_candle_dir / symbol
                ts_mgr = TSManager(symbol_dir)

                if not df_new.empty:
                    ts_mgr.update(df_new)

                partitions = ts_mgr.list_partitions()

                # 只有磁盘上仍不存在任何分区，才说明该 symbol 无法进入后续阶段。
                # `not_enough` 只负责停止本阶段，不能抹掉已经存在的历史窗口。
                if not partitions:
                    num_finished_round += 1
                    left_symbols.remove(symbol)
                    del candle_infos[symbol]
                    continue

                first_begin_time = ts_mgr.read_partition(partitions[0])["candle_begin_time"].min()
                last_begin_time = ts_mgr.read_partition(partitions[-1])["candle_begin_time"].max()

                candle_info = CandleInfo(False, first_begin_time, last_begin_time)
                candle_infos[symbol] = candle_info

                # 短页或空页表示当前方向不应继续请求；保留刚更新的 CandleInfo，
                # 让后续阶段按新的 num_base_candles 重新核对历史窗口是否足够。
                if not_enough:
                    num_finished_round += 1
                    left_symbols.remove(symbol)
                    continue

                # 已经补全了 K 线数据
                if self.check_finish(candle_info):
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
                progress = (num_success / num_total_symbols) * 100
                logger.debug(
                    f"{self.trade_type} {self.stage_name}, "
                    f"进度={progress:.1f}%({num_success}/{num_total_symbols}), "
                    f"已耗时={time_elapsed:.1f}分钟, "
                    f"当前时间={now_time_str()}"
                    f"{in_progress_str}"
                )

            # 整批全部 429：交易所已对本 IP 限频，整体冷却后再继续（逐次加长，有上限）；
            # 只要有一轮没有整批 429，冷却时长就恢复到初始值。
            if fetch_symbols and num_rate_limited == len(fetch_symbols):
                cooldown = min(self.RATE_LIMIT_COOLDOWN_BASE_SEC * (2 ** cooldown_streak),
                               self.RATE_LIMIT_COOLDOWN_MAX_SEC)
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

    def _on_symbol_failed(self, symbol: str, exc: Exception, left_symbols: list,
                          candle_infos: dict, failures: dict, skipped: set) -> bool:
        """处理单个 symbol 的下载失败；返回 True 表示本阶段放弃该 symbol。

        - 429：不计入失败次数（是整个 IP 被限频，不是 symbol 本身的问题），排到队尾稍后重试。
        - 其他异常：连续失败 MAX_SYMBOL_FAILURES 次才放弃，之前排到队尾重试。
        已下载的分页都已落盘，重试从断点继续。放弃时若磁盘上没有任何数据，
        从 candle_infos 移除，避免后续阶段拿空的 CandleInfo 去请求。
        left_symbols / candle_infos / failures / skipped 均由 run_download 持有，此处原地修改。
        """
        rate_limited = is_rate_limited(exc)
        if not rate_limited:
            failures[symbol] = failures.get(symbol, 0) + 1
        n_fail = failures.get(symbol, 0)

        left_symbols.remove(symbol)
        if n_fail >= self.MAX_SYMBOL_FAILURES:
            skipped.add(symbol)
            if not TSManager(self.binance_handle.base_candle_dir / symbol).list_partitions():
                candle_infos.pop(symbol, None)
            logger.warning(f"{self.trade_type} {self.stage_name}: {symbol} 连续失败 {n_fail} 次，"
                           f"本阶段跳过: {exc!r}")
            return True

        left_symbols.append(symbol)  # 排到队尾，先下载其他 symbol
        if not rate_limited:
            logger.warning(f"{self.trade_type} {self.stage_name}: {symbol} 下载失败"
                           f"（第 {n_fail}/{self.MAX_SYMBOL_FAILURES} 次），稍后重试: {exc!r}")
        return False


class RecentDownloader(Downloader):
    """
    下载直到现在的最新 K线
    """

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
        """从本地最新 K 线向本次运行时间补齐数据。

        Args:
            symbol: 交易所原始交易对标识。
            info: 当前 symbol 已有 K 线的首尾时间信息。

        Returns:
            tuple[pd.DataFrame, bool]: 新取得的 K 线，以及是否应停止当前阶段。
                空页和未追到运行时间的短页都会发出停止信号。
        """
        not_enough = False
        fetcher = self.fetcher
        once_candles = self.fetcher.once_candles()

        if info.is_empty:  # 没有已存在的历史 K线, 根据 run_time 获取最近的
            end_ts = int(self.run_time.timestamp()) * 1000
            df_new = await self.fetcher.get_candle(
                symbol, self.handle.base_interval, limit=once_candles, endTime=end_ts
            )
            not_enough = df_new.empty
        else:  # 有已存在的历史 K线, 从 last_begin_time 向现在获取
            start_ts = int(info.last_begin_time.timestamp()) * 1000
            df_new = await fetcher.get_candle(symbol, self.handle.base_interval, limit=once_candles, startTime=start_ts)
            # 空表的最大时间是 NaT，直接比较会得到 False 并导致阶段 2 无限循环；
            # 因此必须先用 empty 短路，再判断非空短页是否仍未追到 run_time。
            not_enough = df_new.empty or (
                df_new["candle_begin_time"].max() < self.run_time
                and len(df_new) < once_candles
            )

        if not_enough:
            return df_new, not_enough

        df_new = df_new[df_new["candle_begin_time"] < self.run_time]
        return df_new, not_enough

    def check_finish(self, info: CandleInfo):
        # 已经补全了 K 线数据
        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        if info.last_begin_time + base_delta >= self.run_time:
            return True

        return False


class HistoryDownloader(Downloader):
    """
    下载历史 K线
    """

    def __init__(
        self,
        handle: BmacHandle,
        binance_handle: MarketHandle,
        fetcher,
        trade_type: str,
        run_time: datetime,
    ):
        super().__init__(handle, binance_handle, fetcher, trade_type, run_time, "阶段 3")

    async def fetch(self, symbol: str, info: CandleInfo):
        fetcher = self.fetcher
        once_candles = self.fetcher.once_candles()

        # 根据已存在的历史 K线, 从 first_begin_time 向现在获取
        end_ts = int(info.first_begin_time.timestamp()) * 1000
        df_new = await fetcher.get_candle(symbol, self.handle.base_interval, limit=once_candles, endTime=end_ts)
        df_new = df_new[df_new["candle_begin_time"] < self.run_time]

        if len(df_new) < once_candles:
            return df_new, True

        return df_new, False

    def check_finish(self, info: CandleInfo):
        base_delta = convert_interval_to_timedelta(self.handle.base_interval)
        required_first_begin = self.run_time - base_delta * self.handle.num_base_candles

        # 起始 begin_time 已经在期望起始时间之前，则代表补全了 K 线数据
        if info.first_begin_time <= required_first_begin:
            return True

        return False


def _enable_fail_fast_on_rate_limit(fetcher) -> None:
    """历史下载时让支持的 fetcher（目前为 OKX）遇到 429 立即失败，不在原地退避。

    下载循环会把失败的 symbol 排到队尾、整批 429 时整体冷却，单个请求原地退避 5–10s
    反而会拖住同一轮的其他 symbol。不支持该开关的 fetcher（Binance）保持原行为。
    """
    if hasattr(fetcher, "fail_fast_on_rate_limit"):
        fetcher.fail_fast_on_rate_limit = True


async def download_recent_until_runtime(
    handle: BmacHandle, trade_type: str, run_time: datetime, candle_infos: dict[str, CandleInfo]
):
    binance_handle = handle.get_market_handle(trade_type)

    logger.info(
        f"{trade_type} 初始化历史 K线, 阶段 2, 补全 {handle.base_interval} 最新数据, "
        f"交易对数量={len(candle_infos)}, "
        f"当前时间={now_time_str()}"
    )

    t_start = time.perf_counter()

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        fetcher = create_fetcher(handle, trade_type, session)
        _enable_fail_fast_on_rate_limit(fetcher)
        downloader = RecentDownloader(handle, binance_handle, fetcher, trade_type, run_time)
        candle_infos = await downloader.run_download(candle_infos)

    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(
        f"{trade_type} 初始化历史 K线, 阶段 2 已完成, "
        #   f'需要继续补全的交易对数量={len(candle_infos)}, '
        f"当前时间={now_time_str()}, "
        f"耗时={time_elapsed:.2f}分钟"
    )

    return candle_infos


async def download_enough_history(
    handle: BmacHandle, trade_type: str, run_time: datetime, candle_infos: dict[str, CandleInfo]
):
    binance_handle = handle.get_market_handle(trade_type)

    logger.info(
        f"{trade_type} 初始化历史 K线, 阶段 3, 补全 {handle.base_interval} K线根数, "
        f"交易对数量={len(candle_infos)}, "
        f"当前时间={now_time_str()}"
    )

    t_start = time.perf_counter()

    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        fetcher = create_fetcher(handle, trade_type, session)
        _enable_fail_fast_on_rate_limit(fetcher)
        downloader = HistoryDownloader(handle, binance_handle, fetcher, trade_type, run_time)
        candle_infos = await downloader.run_download(candle_infos)

    time_elapsed = (time.perf_counter() - t_start) / 60

    logger.ok(
        f"{trade_type} 初始化历史 K线, 阶段 3 已完成, " f"当前时间={now_time_str()}, " f"耗时={time_elapsed:.2f}分钟"
    )

    return candle_infos


def verify_symbol_candle(
    base_candle_dir: Path, trade_type: str, symbol: str, run_time: datetime, required_first_begin: datetime
) -> CandleInfo:
    """
    校验 trade_type 类型 symbol 交易对的历史 K 线
    根据 required_first_begin 和 run_time 过滤过时数据
    symbol -> CandleInfo
    """
    symbol_dir = base_candle_dir / symbol

    empty_candle_info = CandleInfo(True, None, None)
    if not symbol_dir.exists():
        return empty_candle_info

    ts_mgr = TSManager(symbol_dir)
    try:
        # 仅保留期望起始时间 required_first_begin 到当前运行时间 run_time 之间的 K线
        ts_mgr.trim(required_first_begin, run_time)
    except Exception as ex:
        # 修剪数据报错，移除 symbol 目录
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    df = ts_mgr.read_all()
    if not isinstance(df, pd.DataFrame) or df.empty or "candle_begin_time" not in df.columns:
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    # symbol 缺失数据超过 28 天（一个 monthly partition），认为已经损坏
    if df["candle_begin_time"].diff().max() > timedelta(days=28):
        logger.error(f"{trade_type} {symbol} 数据损坏，缺失数据超过 28 天")
        shutil.rmtree(symbol_dir)
        return empty_candle_info

    candle_info = CandleInfo(False, df["candle_begin_time"].min(), df["candle_begin_time"].max())
    return candle_info


def verify_exist_history(handle: BmacHandle, trade_type: str, run_time: datetime, symbols: list[str]):
    binance_handle = handle.get_market_handle(trade_type)
    base_candle_dir = binance_handle.base_candle_dir
    base_delta = convert_interval_to_timedelta(handle.base_interval)

    # 推算 base 周期(5m) K线起始和终止 candle_begin_time
    required_first_begin = run_time - base_delta * handle.num_base_candles

    logger.info(f"{trade_type} 初始化历史 K线, 阶段 1, 校验并清除过时数据, 当前时间={now_time_str()}")

    t_start = time.perf_counter()

    candle_infos = dict()
    for idx, symbol in enumerate(symbols, 1):
        candle_infos[symbol] = verify_symbol_candle(base_candle_dir, trade_type, symbol, run_time, required_first_begin)
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


def init_history_candles(handle: BmacHandle, trade_type: str, run_time: datetime, symbols: list[str]):
    # 阶段1: 校验已保存的 K线数据，并清除过时的 K线
    candle_infos = verify_exist_history(handle, trade_type, run_time, symbols)

    # 阶段2: 向现在补全最新的 K线, 直到 run_time
    candle_infos = asyncio.run(download_recent_until_runtime(handle, trade_type, run_time, candle_infos))

    # 阶段3: 向过去补全 handle.num_base_candles 根 K线
    asyncio.run(download_enough_history(handle, trade_type, run_time, candle_infos))
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
