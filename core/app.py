# -*- coding: utf-8 -*-
"""BMAC 共享核心入口。

与交易所无关的主流程（select_protocol / _fetch_wake_time / init_history / update_all / run_realtime），
通过显式 cfg 参数化驱动，不依赖全局 config 单例。各交易所入口（realtime_data.py 等）
只需委派到本模块。
"""
import asyncio
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from typing import Optional

from core.alert import Alert
from core.exchange.adapter import get_adapter
from core.exchange.symbol_match import update_symbol_match
from core.handle import BmacHandle, debug_handle
from core.history_data.history import run_download_all_history_with_backfill
from core.pipeline.backfill import maybe_backfill
from core.pipeline.data_update import update_meta
from core.pipeline.resample import merge_resample_bounds_cache, resample_history
from core.pipeline.runtime_supervisor import RuntimeSupervisor
from core.pipeline.update_cycle import (
    _fetch_wake_time,
    run_rest_cycle,
    run_startup_catchup,
    run_ws_cycle,
)
from core.strategy.funding_backfill import defer_funding_backfill
from core.utils.dingding import send_wechat_work_msg
from core.utils.file_system import apply_explicit_history_trim, copy_data_api_candles, init_dirs
from core.utils.log_bridge import install_logkit_bridge
from core.utils.log_kit import divider, logger
from core.utils.time import (
    convert_interval_to_timedelta,
    next_run_time,
    now_time_str,
    sleep_until_run_time,
)
from core.version import VERSION

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def select_protocol(exchange: Optional[str]):
    """按 exchange 经交易所注册表选出 WS 协议实例（未知 exchange 直接 raise ValueError）。"""
    return get_adapter(exchange).create_protocol()


def update_all(
    handle: BmacHandle,
    run_time: datetime,
    cfg=None,
    supervisor: Optional[RuntimeSupervisor] = None,
):
    if cfg is None:
        import config as _default_cfg
        cfg = _default_cfg

    error_webhook_url = getattr(cfg, "error_webhook_url", "")

    if f"{run_time.minute}m" not in handle.enabled_hour_offsets:
        logger.info(f"Runtime={run_time} 未启用该 Offset，刷新市场信息与资金费后检查补算")
        sleep_until_run_time(run_time - timedelta(seconds=15))
        asyncio.run(update_meta(handle, run_time))
        update_symbol_match(handle, run_time)

        if supervisor is not None:
            supervisor.refresh_ws_subscriptions(handle)
            alert = Alert(webhook_url=error_webhook_url) if error_webhook_url else None
            supervisor.health_check(alert)

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

    if supervisor is not None:
        asyncio.run(update_meta(handle, run_time))
        update_symbol_match(handle, run_time)
        logger.ok(f"Exchange Info 与实时资金费获取成功, 本地时间={now_time_str()}")

        supervisor.refresh_ws_subscriptions(handle)
        alert = Alert(webhook_url=error_webhook_url) if error_webhook_url else None
        supervisor.health_check(alert)

        # 准点更新 K 线
        sleep_until_run_time(run_time)

        # WS 模式下给 ws_runner 一个 grace window 把准点 K 写入文件，再触发 REST 兜底
        time.sleep(RuntimeSupervisor.GRACE_WAIT_SEC)

        result = run_ws_cycle(handle, run_time, supervisor)
    else:
        result = run_rest_cycle(handle, run_time, cfg=cfg)

    errs = list(result.errs)
    is_binance_success = result.is_binance_success
    is_data_api_success = result.is_data_api_success

    if not is_binance_success:
        errs.append("获取 Binance API K线失败")

    # Data API 更新失败，复制币安数据
    if handle.data_api is not None and not is_data_api_success:
        if is_binance_success:
            copy_data_api_candles(handle)

    if errs:
        errs += [f"run_time={run_time}"]
        err_msg = "\n".join(errs)
        send_wechat_work_msg(err_msg, error_webhook_url)


def init_history(handle: BmacHandle, cfg=None):
    # 重启，确保数据目录存在，并删除除了币安 base 周期全量数据以外的所有文件
    init_dirs(handle)

    # 显式历史截断（history_trim_date=None 时为纯 no-op）
    trim_ts = handle.history_trim_ts() if hasattr(handle, "history_trim_ts") else None
    apply_explicit_history_trim(handle, trim_ts)

    # 冷启动快照恢复（可选优化）
    if cfg is not None and getattr(cfg, "use_api", {}).get("snapshot_restore", False):
        from core.snapshot.restore import restore_from_snapshot

        restore_from_snapshot(handle, cfg)

    with ProcessPoolExecutor(max_workers=2) as exe:
        # 下载 base 周期历史数据，返回 (run_time, {"spot": set, "swap": set})
        download_task = exe.submit(run_download_all_history_with_backfill, handle)
        run_time, full_backfill_symbols = download_task.result()

        divider("开始 Resample 历史数据", sep="-")
        spot_backfill = full_backfill_symbols.get("spot") if full_backfill_symbols else None
        swap_backfill = full_backfill_symbols.get("swap") if full_backfill_symbols else None
        resample_spot_task = exe.submit(
            resample_history, handle, "spot", run_time, full_backfill_symbols=spot_backfill
        )
        resample_swap_task = exe.submit(
            resample_history, handle, "swap", run_time, full_backfill_symbols=swap_backfill
        )

        swap_bounds = resample_swap_task.result()
        spot_bounds = resample_spot_task.result()
        if isinstance(swap_bounds, dict):
            merge_resample_bounds_cache(swap_bounds)
        if isinstance(spot_bounds, dict):
            merge_resample_bounds_cache(spot_bounds)

    if handle.data_api is not None:
        copy_data_api_candles(handle)  # 复制一份给 Quantclass Data API


def run_realtime(cfg, make_handle):
    """初始化并持续更新数据，保留 initialized 恢复守卫，并在 WS 模式下托管 supervisor 生命周期。"""
    install_logkit_bridge()

    divider(f"BMAC 版本: {VERSION}")

    handle = make_handle(cfg)
    debug_handle(handle)

    use_ws = bool(getattr(cfg, "use_websocket", False))
    error_webhook_url = getattr(cfg, "error_webhook_url", "")
    supervisor: Optional[RuntimeSupervisor] = None
    initialized = False

    try:
        while True:
            run_time = None
            try:
                # Initialization may clear directories: never re-enter after live updates fail.
                if not initialized:
                    init_history(handle, cfg)
                    initialized = True
                    if getattr(handle, "fetch_funding_rate", False):
                        defer_funding_backfill()

                if use_ws and supervisor is None:
                    base_delta_for_bootstrap = convert_interval_to_timedelta(handle.base_interval)
                    bootstrap_run_time = next_run_time(handle.base_interval) - base_delta_for_bootstrap
                    logger.info(f"WS bootstrap: 拉取 exginfo, run_time={bootstrap_run_time}")
                    asyncio.run(update_meta(handle, bootstrap_run_time))
                    update_symbol_match(handle, bootstrap_run_time)

                    protocol = select_protocol(getattr(handle, "exchange", "binance"))
                    sup = RuntimeSupervisor(
                        handle=handle,
                        alert_webhook_url=error_webhook_url,
                        protocol=protocol,
                    )
                    try:
                        sup.start_writers()
                        sup.start_ws_runners()
                    except Exception:
                        sup.shutdown()
                        raise
                    supervisor = sup
                    logger.info(
                        f"WS 模式启动: writer pids={[p.pid for p in supervisor.writer_procs]}, "
                        f"ws_procs={[p.pid for procs in supervisor.ws_procs.values() for p in procs]}"
                    )
                    run_startup_catchup(handle, supervisor.router)

                # 阶段3: 更新数据
                while True:
                    run_time = None
                    if supervisor is not None:
                        supervisor.resume_pending_ws_restarts()
                    run_time = next_run_time(handle.base_interval)
                    update_all(handle, run_time, cfg, supervisor=supervisor)
            except Exception as e:
                ex_str = traceback.format_exc()
                phase = f"运行更新 run_time={run_time}" if initialized else "首次初始化"
                recovery = "保留已有产物，1 分钟后继续更新" if initialized else "1 分钟后重试初始化"
                send_wechat_work_msg(
                    f"BMAC {phase}发生错误 {repr(e)}, {ex_str} {recovery}",
                    error_webhook_url,
                )

                logger.debug(ex_str)
                logger.error(f"{phase}: {e!r}；{recovery}")

                divider(recovery, _logger=logger)
                time.sleep(60)
    finally:
        if supervisor is not None:
            logger.info("BMAC 关停中, supervisor shutdown ...")
            supervisor.shutdown()
