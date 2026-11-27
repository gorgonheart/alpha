# -*- coding: utf-8 -*-
"""BMAC 共享核心入口。

与交易所无关的主流程（select_protocol / init_history / update_all / run_realtime），
通过显式 cfg 参数化驱动，不依赖全局 config 单例。各交易所入口
（realtime_okx.py / realtime_binance.py）只需 `from core.app import run_realtime`
并把自己的 config 模块传进来。
"""
import asyncio
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

from core.alert import Alert
from core.exchange.adapter import get_adapter
from core.pipeline.runtime_supervisor import RuntimeSupervisor
from core.pipeline.update_cycle import run_rest_cycle, run_startup_catchup, run_ws_cycle
from core.utils.dingding import send_wechat_work_msg
from core.utils.log_bridge import install_logkit_bridge
from core.utils.log_kit import divider, logger
from core.version import VERSION
from core.pipeline.data_update import update_meta
from core.handle import BmacHandle, debug_handle
from core.history_data.history import run_download_all_history
from core.pipeline.resample import resample_history
from core.exchange.symbol_match import update_symbol_match
from core.utils.file_system import copy_data_api_candles, init_dirs
from core.utils.time import convert_interval_to_timedelta, next_run_time, now_time_str, sleep_until_run_time

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def select_protocol(exchange: Optional[str]):
    """按 exchange 经交易所注册表选出 WS 协议实例（未知 exchange 直接 raise ValueError）。"""
    return get_adapter(exchange).create_protocol()


def update_all(handle: BmacHandle, run_time: datetime, cfg, supervisor: Optional[RuntimeSupervisor] = None):
    if f"{run_time.minute}m" not in handle.enabled_hour_offsets:
        logger.info(f"Runtime={run_time},不在 Offset={handle.enabled_hour_offsets} 中，休息 60s 后，跳过")
        time.sleep(60)  # 必须休息 60s，否则会一直卡在这 1 分钟
        return

    divider(f"Update {handle.base_interval} Runtime={run_time}", with_timestamp=False)

    # 提前 15 秒更新 exchange info
    sleep_until_run_time(run_time - timedelta(seconds=15))

    asyncio.run(update_meta(handle, run_time))

    # 更新现货和永续合约对应关系
    update_symbol_match(handle, run_time)
    logger.ok(f"Exchange Info 与实时资金费获取成功, 本地时间={now_time_str()}")

    # WS 动态订阅：symbol 集变化时重启对应 trade_type 的 ws_runner（缺口 REST 兜底）；
    # 之后做子进程健康检查（writer / ws_runner 任何一个挂掉立刻告警）
    if supervisor is not None:
        supervisor.refresh_ws_subscriptions(handle)
        alert = Alert(webhook_url=cfg.error_webhook_url) if cfg.error_webhook_url else None
        supervisor.health_check(alert)

    # 准点更新 K 线
    sleep_until_run_time(run_time)

    # WS 模式下给 ws_runner 一个 grace window 把准点 K 写入文件，再触发 REST 兜底
    if supervisor is not None:
        time.sleep(RuntimeSupervisor.GRACE_WAIT_SEC)

    # 本周期更新：WS 模式走 run_ws_cycle，纯 REST 模式走 run_rest_cycle
    if supervisor is not None:
        result = run_ws_cycle(handle, run_time, supervisor)
    else:
        result = run_rest_cycle(handle, run_time)

    errs = list(result.errs)
    is_binance_success = result.is_binance_success
    is_data_api_success = result.is_data_api_success

    # ──────────────────────────────────────────────────────────────────────
    # 共用 epilogue
    # ──────────────────────────────────────────────────────────────────────
    if not is_binance_success:
        errs.append("获取 Binance API K线失败")

    # Data API 更新失败，复制币安数据
    if handle.data_api is not None and not is_data_api_success:
        # errs.append('获取 Data API K线失败')  # 2025-01-16报错不提示，不然太烦了@SIMONS
        if is_binance_success:
            copy_data_api_candles(handle)

    if errs:
        errs += [f"run_time={run_time}"]
        err_msg = "\n".join(errs)
        send_wechat_work_msg(err_msg, cfg.error_webhook_url)


def init_history(handle: BmacHandle, cfg=None):
    # 重启，确保数据目录存在，并删除除了币安 base 周期全量数据以外的所有文件
    init_dirs(handle)

    # 冷启动快照恢复（可选优化）：在 init_dirs 之后、三段式全量下载之前，把数据中心打包好的
    # base-5m 月分区快照还原进各市场 base_candle_dir，让随后的下载只需补缺口、不必从零拉全量。
    # restore 内部已对任何失败做软退化（只记日志、不抛），不会阻断冷启动。
    if cfg is not None and getattr(cfg, "use_api", {}).get("snapshot_restore", False):
        from core.snapshot.restore import restore_from_snapshot

        restore_from_snapshot(handle, cfg)

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


def run_realtime(cfg, make_handle):
    """初始化并持续更新数据，在每轮调度前收尾待完成的 WS 重启。

    Args:
        cfg: 交易所配置；make_handle: 从配置创建数据句柄的工厂。
    Returns:
        无；持续运行到退出，最终交由 supervisor 清理子进程。
    """
    # 主进程 stdlib logging 兜底：supervisor / alert / ts_manager 在主进程产生的日志
    # 经 root 桥接进 log_kit，否则 INFO 级别全部丢失（子进程侧由各自入口配置）
    install_logkit_bridge()

    # BMAC 彩虹重制版，版本信息
    divider(f"BMAC 版本: {VERSION}")

    supervisor: Optional[RuntimeSupervisor] = None
    # 启动期（创建 handle + WS bootstrap + 拉起 supervisor）整体包进 try。
    # 这段在下方主循环的 try/except 之外，此前任何异常都直接崩到 stderr、不进日志文件
    # （无 pm2 循环重启时极难察觉——例如首次全量下载在 init_history 抛 KeyError 崩溃）。
    # 此处统一把致命异常写进日志（含完整 traceback）+ 告警，并清理已 spawn 的子进程后
    # 再抛出，保持"启动失败即退出、不进重启循环"的语义（交由 pm2 / 人工处理）。
    try:
        # 阶段1：读取配置, 用入口注入的工厂创建并保存到 BmacHandle 中
        handle = make_handle(cfg)
        debug_handle(handle)

        # WS 模式启动顺序：
        #   1. init_history（init_dirs 会清空 exginfo 目录）
        #   2. bootstrap update_meta 把 exginfo 写回（ws_runner 需要 symbol 列表）
        #   3. 启动 supervisor（writer 子进程 → ws_runner 子进程）
        if cfg.use_websocket:
            init_history(handle, cfg)
            base_delta_for_bootstrap = convert_interval_to_timedelta(handle.base_interval)
            bootstrap_run_time = next_run_time(handle.base_interval) - base_delta_for_bootstrap
            logger.info(f"WS bootstrap: 拉取 exginfo, run_time={bootstrap_run_time}")
            asyncio.run(update_meta(handle, bootstrap_run_time))
            update_symbol_match(handle, bootstrap_run_time)

            protocol = select_protocol(handle.exchange)
            supervisor = RuntimeSupervisor(
                handle=handle,
                alert_webhook_url=cfg.error_webhook_url,
                protocol=protocol,
            )
            supervisor.start_writers()
            supervisor.start_ws_runners()
            logger.info(
                f"WS 模式启动: writer pids={[p.pid for p in supervisor.writer_procs]}, "
                f"ws_procs={[p.pid for procs in supervisor.ws_procs.values() for p in procs]}"
            )
            # 历史下载截止 → WS 订阅生效之间的 K 线 WS 不补发，进主循环前同步 REST 补齐
            run_startup_catchup(handle, supervisor.router)
    except Exception as e:
        import traceback

        ex_str = traceback.format_exc()
        logger.error(f"BMAC 启动期致命错误，进程退出: {repr(e)}")
        logger.debug(ex_str)
        send_wechat_work_msg(f"BMAC 启动失败，进程退出 {repr(e)}\n{ex_str}", cfg.error_webhook_url)
        # 清理可能已 spawn 的子进程，避免孤儿进程持有 mp.Queue 的 semlock
        if supervisor is not None:
            supervisor.shutdown()
        raise

    try:
        while True:
            try:
                # 阶段2: 初始化历史数据（非 WS 模式每次循环重跑；WS 模式上面已做）
                if supervisor is None:
                    init_history(handle, cfg)

                # 阶段3: 更新数据
                while True:
                    if supervisor is not None:
                        supervisor.resume_pending_ws_restarts()
                    run_time = next_run_time(handle.base_interval)
                    update_all(handle, run_time, cfg, supervisor=supervisor)
            except Exception as e:
                import traceback

                ex_str = traceback.format_exc()
                send_wechat_work_msg(f"BMAC 发生错误 {repr(e)}, {ex_str} 1 分钟后重启", cfg.error_webhook_url)

                logger.debug(ex_str)
                logger.error(repr(e))

                divider("1 分钟后重启", _logger=logger)
                time.sleep(60)
    finally:
        if supervisor is not None:
            logger.info("BMAC 关停中, supervisor shutdown ...")
            supervisor.shutdown()
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
