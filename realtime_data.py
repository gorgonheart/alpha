# -*- coding: utf-8 -*-
"""
选币策略框架 | 邢不行 | 2024分享会
author: 邢不行
微信: xbx6660
"""
import asyncio
from copy import copy
from core.publication_sources import choose_publication_inputs
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
import config
from core.utils.dingding import send_wechat_work_msg
from core.utils.log_kit import divider, logger
from core.version import VERSION
from core.binance_update import fetch_update_binance_candles, update_binance_base_candles, update_binance_meta
from core.backfill import maybe_backfill
from core.funding_backfill import defer_funding_backfill
from core.data_api_update import update_data_api_candles
from core.handle import BmacHandle, create_bmac_handle_from_config, debug_handle
from core.history_data.history import run_download_all_history
from core.preprocess import preprocess_offset_data
from core.resample_candle import resample_history
from core.symbol_match import update_symbol_match
from core.utils.file_system import copy_data_api_candles, init_dirs
from core.utils.time import next_run_time, now_time_str, sleep_until_run_time
from core.coin_cap import run_coin_cap_update_to_recent

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def update_all(handle: BmacHandle, run_time: datetime):
    if f"{run_time.minute}m" not in handle.enabled_hour_offsets:
        logger.info(f"Runtime={run_time} 未启用该 Offset，刷新市场信息与资金费后检查补算")
        sleep_until_run_time(run_time - timedelta(seconds=15))
        asyncio.run(update_binance_meta(handle, run_time))

        # 首次启动时，除 init 那个分钟归属的 offset 外都还没有预处理 ready。
        # Wait for this base-period tick, instead of rechecking its future time every minute.
        sleep_until_run_time(run_time)
        try:
            maybe_backfill(handle)
        except Exception as e:
            logger.error(f"补算历史预处理失败: {e!r}")
            logger.debug(traceback.format_exc())

        return

    divider(f"Update {handle.base_interval} Runtime={run_time}", with_timestamp=False)

    # 提前 15 秒更新 exchange info
    sleep_until_run_time(run_time - timedelta(seconds=15))

    # 用于记录本周期发生的错误，如有错误发送到企业微信
    errs = []

    is_binance_success, is_data_api_success = False, False

    workers = 1 + int(handle.data_api is not None) + int(handle.coin_cap_handle is not None)
    with ProcessPoolExecutor(max_workers=workers) as exe:
        # Occupy all workers until the target so spawn/import overlaps metadata I/O.
        # No market request runs before metadata succeeds and the target is reached.
        logger.info(f'开始预热采集进程, run_time={run_time}, 当前时间={now_time_str()}')
        warmups = [exe.submit(sleep_until_run_time, run_time) for _ in range(workers)]
        asyncio.run(update_binance_meta(handle, run_time))
        update_symbol_match(handle, run_time)
        logger.ok(f"Exchange Info 与实时资金费获取成功, 本地时间={now_time_str()}")
        sleep_until_run_time(run_time)
        for warmup in warmups:
            warmup.result()
        logger.info(f'采集进程预热就绪, run_time={run_time}, 当前时间={now_time_str()}')

        # 子进程：从币安 REST API 更新数据, 并且更新 Resample
        task_binance = exe.submit(fetch_update_binance_candles, handle, run_time)
        update_tasks = [task_binance]

        # 子进程：从 QuantClass Data API 更新数据
        if handle.data_api is not None:
            task_data_api = exe.submit(update_data_api_candles, handle, run_time)
            update_tasks.append(task_data_api)

        # 子进程：更新市值数据
        task_coin_cap = None
        if handle.coin_cap_handle is not None:
            task_coin_cap = exe.submit(run_coin_cap_update_to_recent, handle, run_time)

        # Binance completion fixes source priority. A missing/failed DataAPI
        # does not delay publication once Binance covers the current market.
        task_preprocess = None
        binance_result = data_api_result = None
        binance_done = False
        data_api_done = handle.data_api is None
        coverage_failed = False
        for future in as_completed(update_tasks):
            # A failed source must not discard a completed alternative or stop
            # the durable 5m update. Coverage still decides whether to publish.
            try:
                result = future.result()
            except Exception as error:
                source_name = 'Binance' if future == task_binance else 'DataAPI'
                logger.warning(f'{source_name}采集任务失败: {type(error).__name__}')
                result = None
            if future == task_binance:
                binance_done = True
                binance_result = result
                if binance_result is not None:
                    is_binance_success = True
                    spot_dfs, swap_dfs, _, _ = binance_result
            else:
                data_api_done = True
                data_api_result = result
                is_data_api_success = data_api_result is not None
            if handle.preprocess_handle is not None and task_preprocess is None and not coverage_failed:
                try:
                    chosen = choose_publication_inputs(handle, run_time, binance_result, data_api_result,
                        binance_done=binance_done, data_api_done=data_api_done)
                except ValueError as error:
                    errs.append(str(error))
                    coverage_failed = True
                    chosen = None
                if chosen is not None:
                    sources, spot_begin_ts, swap_begin_ts = chosen
                    publication_handle = copy(handle)
                    publication_handle.publication_sources = sources
                    task_preprocess = exe.submit(preprocess_offset_data, publication_handle, run_time,
                                                  spot_begin_ts, swap_begin_ts)

        if task_preprocess is not None:
            task_preprocess.result()

        if task_coin_cap is not None:
            is_coin_cap_success = task_coin_cap.result()
            if not is_coin_cap_success:
                errs.append("获取市值数据失败")

    # 更新基础周期(5m) K 线
    if is_binance_success:
        update_binance_base_candles(handle, run_time, spot_dfs, swap_dfs)
    else:
        errs.append("获取 Binance API K线失败")

    # Data API 更新失败，复制币安数据
    if handle.data_api is not None and not is_data_api_success:
        # errs.append('获取 Data API K线失败')  # 2025-01-16报错不提示，不然太烦了@SIMONS
        if is_binance_success:
            copy_data_api_candles(handle)

    if errs:
        errs += [f"run_time={run_time}"]
        err_msg = "\n".join(errs)
        send_wechat_work_msg(err_msg, config.error_webhook_url)


def init_history(handle: BmacHandle):
    # 重启，确保数据目录存在，并删除除了币安 base 周期全量数据以外的所有文件
    init_dirs(handle)

    with ProcessPoolExecutor(max_workers=2) as exe:
        # 下载 base 周期历史数据
        download_task = exe.submit(run_download_all_history, handle)
        run_time = download_task.result()

        divider("开始 Resample 历史数据", sep="-")
        resample_spot_task = exe.submit(resample_history, handle, "spot", run_time)
        resample_swap_task = exe.submit(resample_history, handle, "swap", run_time)

        resample_swap_task.result()
        resample_spot_task.result()

    if handle.data_api is not None:
        copy_data_api_candles(handle)  # 复制一份给 Quantclass Data API


def main():
    # BMAC 彩虹重制版，版本信息
    divider(f"BMAC 版本: {VERSION}")

    # 阶段1：读取配置, 将配置保存在 BmacHandle 中
    handle = create_bmac_handle_from_config()
    debug_handle(handle)

    initialized = False
    while True:
        try:
            # Initialization may clear directories: never re-enter after live updates fail.
            if not initialized:
                init_history(handle)
                initialized = True
                if handle.fetch_funding_rate:
                    defer_funding_backfill()

            # 阶段3: 更新数据
            while True:
                run_time = None
                run_time = next_run_time(handle.base_interval)
                update_all(handle, run_time)
        except Exception as e:
            from config import error_webhook_url

            ex_str = traceback.format_exc()
            phase = f'运行更新 run_time={run_time}' if initialized else '首次初始化'
            recovery = '保留已有产物，1 分钟后继续更新' if initialized else '1 分钟后重试初始化'
            send_wechat_work_msg(f"BMAC {phase}发生错误 {repr(e)}, {ex_str} {recovery}", error_webhook_url)

            logger.debug(ex_str)
            logger.error(f'{phase}: {e!r}；{recovery}')

            divider(recovery, _logger=logger)
            time.sleep(60)


if __name__ == "__main__":
    main()
