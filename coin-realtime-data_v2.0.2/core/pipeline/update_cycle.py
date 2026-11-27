"""每个 run_time 周期的数据更新逻辑（WS 模式 与 纯 REST 模式）。

core.app.update_all 只保留对齐时间点、exginfo 更新、health check、准点 sleep、
grace window 与错误 epilogue；具体"抓数→落盘→barrier→resample→preprocess"
两种模式的实现放在这里，各自返回一个 CycleResult。
"""
from __future__ import annotations

import multiprocessing as mp
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import inspect

from core.pipeline.barrier import wait_for_ready_barrier
from core.pipeline.data_update import fetch_update_binance_candles
from core.storage.candle_manager import CandleFileManager, CandleFileReader
from core.ingest.coin_cap import run_coin_cap_update_to_recent
from core.ingest.data_api_update import update_data_api_candles
from core.handle import BmacHandle
from core.strategy.preprocess import preprocess_offset_data
from core.pipeline.resample import update_resample_from_base
from core.pipeline.runtime_supervisor import RuntimeSupervisor
from core.utils.log_kit import logger
from core.utils.time import convert_interval_to_timedelta, next_run_time

# REST 兜底 grace 窗口：复用 barrier 的 ws_wait_sec（同一个值）。
# 兜底线程先睡 grace，让 WS 闭合 K 有机会先落盘并写 .ws_ready，再据此算 missed，
# 只补 WS 真正漏掉的 symbol。grace == barrier.ws_wait_sec(10) <= barrier.timeout_sec(45)，
# REST 兜底数据仍能在 barrier 的 check_ready 回退分支与 deadline 之前落盘。
_REST_FALLBACK_GRACE_SEC: float = (
    inspect.signature(wait_for_ready_barrier).parameters["ws_wait_sec"].default
)
# barrier 软截止：超过它仍在等，说明在等 REST 兜底落盘（见 wait_for_ready_barrier）
_BARRIER_SOFT_TIMEOUT_SEC: float = (
    inspect.signature(wait_for_ready_barrier).parameters["timeout_sec"].default
)

# WS 周期主进程是多线程的（mp.Queue feeder + REST 兜底线程）。
# 在多线程进程里 fork 子进程会继承到无主锁导致死锁，故 preprocess / 辅助任务
# 的子进程池统一用 spawn context（与 runtime_supervisor 的 spawn 决策一致）。
_SPAWN_CTX = mp.get_context("spawn")


@dataclass
class CycleResult:
    """一个更新周期的产出：成败标志、resample begin_time、本周期错误列表。"""

    is_binance_success: bool = False
    is_data_api_success: bool = False
    spot_begin_ts: Optional[dict] = None
    swap_begin_ts: Optional[dict] = None
    errs: list = field(default_factory=list)


# ---------------------------------------------------------------- WS helpers

def _universe(handle: BmacHandle, trade_type: str) -> list:
    """exginfo_{trade_type} 的交易对全集（与 barrier 用的同一来源）。"""
    df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
    return df_exg["symbol"].to_list() if df_exg is not None else []


def _compute_missed_symbols(handle: BmacHandle, trade_type: str, run_time: datetime) -> set:
    """计算该 trade_type 下需要 REST 兜底的 symbol。

    missed = {sym ∈ universe | 无本轮 .ws_ready  或  有 .gap 断档标记}。

    数据完整性不变式：任何没有 .ws_ready 文件的 symbol 都进 missed → 必被 REST 兜底；
    绝不漏掉真正缺失的 symbol。WS 大面积宕机时 missed≈全集 → 退化为旧的全量行为，无数据丢失。
    .gap 由 writer 落盘时检测到 REST 窗口内的历史断档后写入（如 WS 断线重连漏掉的
    非 offset 时刻 K 线），使"本轮正常、中间缺几根"的 symbol 也能被补回。
    """
    universe = _universe(handle, trade_type)
    bh = handle.get_market_handle(trade_type)
    reader = CandleFileManager(bh.base_candle_dir, "pickle")
    gap_symbols = reader.list_gap_marker_symbols()
    # 已下架 symbol 残留的标记直接清掉（exginfo 读空时不清，避免误删）
    stale = gap_symbols - set(universe)
    if universe and stale:
        for sym in stale:
            reader.clear_gap_marker(sym)
    gap_symbols -= stale
    if gap_symbols:
        logger.info(f"REST 兜底[{trade_type}]: {len(gap_symbols)} 个 symbol 有历史断档标记, "
                    f"sample={sorted(gap_symbols)[:10]}")
    return {sym for sym in universe
            if sym in gap_symbols or not reader.check_ws_ready(sym, run_time)}


def _submit_rest_dfs(router, run_time: datetime, spot_dfs: dict, swap_dfs: dict,
                     submitted: Optional[dict] = None) -> int:
    """把 REST 抓到的 base K 以 source='rest' 投递 writer router；返回投递条数。

    submitted（可选 {trade_type: set}）：先记再投，barrier 据此判断"REST 结束后还要不要等这个断档 symbol"。
    """
    count = 0
    for trade_type, symbol_dfs in (("spot", spot_dfs), ("swap", swap_dfs)):
        for symbol, df in symbol_dfs.items():
            if df is None or df.empty:
                continue
            if submitted is not None:
                submitted[trade_type].add(symbol)
            router.put({
                "symbol": symbol,
                "trade_type": trade_type,
                "df": df,
                "source": "rest",
                "run_time": run_time,
            })
            count += 1
    return count


def run_startup_catchup(handle: BmacHandle, router, settle_sec: float = 5.0) -> bool:
    """WS 启动后、进入主循环前，同步全量 REST 一次，补齐"历史下载截止 → WS 订阅生效"的启动缺口。

    WS 不补发订阅前的 K 线，若留到第一个周期再补：OKX 全量 REST 实测 40–54s，会让首轮
    barrier 卡满超时并告警。放在启动阶段同步做，首轮就只剩个别 symbol 需要 REST。

    先等 settle_sec 让 WS 连上，再以"当前已闭合"为 run_time 抓取：此后闭合的 K 由 WS 推送，
    之前的由本次 REST 覆盖。仍有漏网（如 WS 连接慢于 settle 且恰跨边界）由 writer 的 .gap
    标记在首轮兜底。失败只告警不抛，退化为首轮由 .gap 标记全量兜底。
    """
    time.sleep(settle_sec)
    base_delta = convert_interval_to_timedelta(handle.base_interval)
    run_time = next_run_time(handle.base_interval) - base_delta
    logger.info(f"启动补齐: 全量 REST 拉取 base K, run_time={run_time}")
    t0 = time.monotonic()
    try:
        result = fetch_update_binance_candles(handle, run_time, resample_inline=False)
    except Exception:
        logger.exception("启动补齐异常，首轮由断档标记兜底")
        return False
    if result is None:
        logger.warning("启动补齐失败，首轮由断档标记兜底")
        return False
    spot_dfs, swap_dfs, _, _ = result
    n = _submit_rest_dfs(router, run_time, spot_dfs, swap_dfs)
    logger.ok(f"启动补齐完成: 投递 {n} 个 symbol, 耗时={time.monotonic() - t0:.1f}s")
    return True


def _rest_fallback_worker(handle: BmacHandle, run_time: datetime, router, state: dict) -> None:
    """后台线程：先睡 grace 让 WS 落盘，再 REST 只补 WS 漏掉的 symbol，以 source='rest' 投递 router。

    writer 优先级合并 ws(3)>rest(2)，所以即便 REST 晚到也不会覆盖 ws 闭合 K。
    grace 窗口 == barrier ws_wait_sec，过后据 .ws_ready 算 missed：WS 覆盖到的 symbol
    不再 REST 重抓（消除 WS 健康时每周期 671 请求全量 fan-out）；WS 漏的仍被兜底，
    且在 barrier 的 REST 回退分支与 deadline 之前落盘。
    """
    try:
        time.sleep(_REST_FALLBACK_GRACE_SEC)
        missed_per_type = {
            "spot": _compute_missed_symbols(handle, "spot", run_time),
            "swap": _compute_missed_symbols(handle, "swap", run_time),
        }
        n_spot, n_swap = len(missed_per_type["spot"]), len(missed_per_type["swap"])
        logger.info(
            f"REST 兜底 grace({_REST_FALLBACK_GRACE_SEC}s) 后计算 missed: "
            f"spot={n_spot} swap={n_swap} (run_time={run_time})"
        )
        if n_spot == 0 and n_swap == 0:
            state["ok"] = True
            return
        result = fetch_update_binance_candles(
            handle, run_time, resample_inline=False, symbols_subset=missed_per_type)
        if result is not None:
            spot_dfs, swap_dfs, _, _ = result
            _submit_rest_dfs(router, run_time, spot_dfs, swap_dfs, submitted=state["submitted"])
            state["ok"] = True
    except Exception:
        logger.exception("REST fallback 后台线程异常")
    finally:
        # 无论成败都置位，barrier 不再为没拿到 REST 数据的断档 symbol 干等
        state["done"].set()


def _start_rest_fallback(handle: BmacHandle, run_time: datetime, router):
    """启动 REST 兜底线程；返回 (thread, state)。

    state: ok=是否成功；done=线程结束事件；submitted={trade_type: 已投递 REST 数据的 symbol}。
    """
    state = {"ok": False, "done": threading.Event(), "submitted": {"spot": set(), "swap": set()}}
    thread = threading.Thread(
        target=_rest_fallback_worker,
        args=(handle, run_time, router, state),
        name="rest-fallback",
        daemon=False,
    )
    thread.start()
    return thread, state


def _barrier_and_resample(handle: BmacHandle, trade_type: str, run_time: datetime,
                          rest_state: Optional[dict] = None) -> dict:
    """等该 trade_type 的 symbol 落盘（ready-file barrier），通过即从 base 存储 resample。

    spot ws 健康(barrier ~3s)时，spot 的 resample 能在 swap barrier 还在等 REST 时跑完。
    """
    bh = handle.get_market_handle(trade_type)
    df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
    symbols = df_exg["symbol"].to_list() if df_exg is not None else []
    logger.info(f"barrier[{trade_type}] 等待 {len(symbols)} symbol ready (run_time={run_time}) ...")
    t0 = time.monotonic()
    # 有 REST 兜底状态时启用断档等待：带 .gap 的 symbol 等 REST 补齐后才放行
    rest_done = rest_state.get("done") if rest_state else None
    rest_submitted = rest_state["submitted"].get(trade_type) if rest_done is not None else None
    success, skipped = wait_for_ready_barrier(
        symbols=symbols, base_5m_dir=bh.base_candle_dir, run_time=run_time,
        rest_done=rest_done, rest_submitted=rest_submitted,
    )
    elapsed = time.monotonic() - t0
    logger.info(
        f"barrier[{trade_type}] 完成: success={len(success)}/{len(symbols)} "
        f"skipped={len(skipped)} 耗时={elapsed:.2f}s"
    )
    if rest_done is not None and elapsed > _BARRIER_SOFT_TIMEOUT_SEC:
        logger.warning(
            f"barrier[{trade_type}] 超过 {_BARRIER_SOFT_TIMEOUT_SEC:.0f}s，已等待 REST 兜底写入后再 resample"
            f"（多等 {elapsed - _BARRIER_SOFT_TIMEOUT_SEC:.1f}s）"
        )
    # 一根 K 线都没有的 symbol（刚上线未开盘、尚无成交等）：WS 不推、REST 也拿不到，
    # 并非数据中心漏数，不计入 timeout 告警，只记日志；开始有成交、落盘后自动恢复正常判断。
    reader = CandleFileReader(Path(bh.base_candle_dir), "pickle")
    no_data = {s for s in skipped if not reader.has_symbol(s)}
    if no_data:
        logger.info(
            f"barrier[{trade_type}] {len(no_data)} 个 symbol 还没有任何 K 线（新上线未开盘等），"
            f"不计入 timeout: {sorted(no_data)[:10]}"
        )
    begin_ts: dict = {}
    if success:
        # barrier 通过即 resample（ws 闭合 K，从 base 存储读）
        begin_ts = update_resample_from_base(handle, trade_type, run_time)
    return {
        "trade_type": trade_type, "success": len(success),
        "total": len(symbols), "skipped": sorted(skipped - no_data), "begin_ts": begin_ts,
    }


def _run_preprocess(handle: BmacHandle, run_time: datetime, spot_begin_ts, swap_begin_ts) -> None:
    """在 worker 进程里跑 preprocess（两种模式共用）。"""
    if handle.preprocess_handle is not None:
        with ProcessPoolExecutor(max_workers=1, mp_context=_SPAWN_CTX) as exe:
            exe.submit(preprocess_offset_data, handle, run_time, spot_begin_ts, swap_begin_ts).result()


# ----------------------------------------------------------------- entrypoints

def run_ws_cycle(handle: BmacHandle, run_time: datetime, supervisor: RuntimeSupervisor) -> CycleResult:
    """WS 模式（方案 A：REST 后台并发 + barrier 等齐）。

    1. REST 抓全量 base 5m 丢后台线程，抓完 submit writer router 兜底
    2. 主线程并发跑 barrier：ws 覆盖的 symbol ~2s 就绪；ws 漏的等 REST 落盘
    3. barrier 完成 → resample 从 base 存储（ws 闭合 K）→ preprocess
    4. finally join REST 线程（清理，不影响 preprocess 产出延迟）
    """
    result = CycleResult()
    router = supervisor.router

    rest_thread, rest_state = _start_rest_fallback(handle, run_time, router)
    try:
        # data_api + coin_cap 并发跑（多数用户不用；保留兼容）
        with ProcessPoolExecutor(max_workers=2, mp_context=_SPAWN_CTX) as aux_exe:
            task_data_api = aux_exe.submit(update_data_api_candles, handle, run_time) \
                if handle.data_api is not None else None
            task_coin_cap = aux_exe.submit(run_coin_cap_update_to_recent, handle, run_time) \
                if handle.coin_cap_handle is not None else None

            # spot / swap 的 barrier+resample 并发流水线
            with ThreadPoolExecutor(max_workers=2) as br_exe:
                results = list(br_exe.map(
                    lambda tt: _barrier_and_resample(handle, tt, run_time, rest_state),
                    ("spot", "swap"),
                ))

            for r in results:
                if r["success"]:
                    result.is_binance_success = True
                if r["trade_type"] == "spot":
                    result.spot_begin_ts = r["begin_ts"]
                else:
                    result.swap_begin_ts = r["begin_ts"]
                if r["skipped"]:
                    logger.warning(
                        f"barrier {r['trade_type']} timeout: {len(r['skipped'])}/{r['total']} "
                        f"skipped, sample={r['skipped'][:10]}"
                    )
                    result.errs.append(f"barrier {r['trade_type']} timeout {len(r['skipped'])}/{r['total']}")

            if task_data_api is not None:
                if task_data_api.result() is not None:
                    result.is_data_api_success = True
            if task_coin_cap is not None:
                if not task_coin_cap.result():
                    result.errs.append("获取市值数据失败")

        # preprocess
        if result.is_binance_success or result.is_data_api_success:
            _run_preprocess(handle, run_time, result.spot_begin_ts, result.swap_begin_ts)
    finally:
        # 清理 REST 后台线程（preprocess 已完成，join 不影响产出延迟）
        rest_thread.join(timeout=60)
        if rest_thread.is_alive():
            logger.warning("REST fallback 后台线程超过 60s 未结束")

    return result


def run_rest_cycle(handle: BmacHandle, run_time: datetime) -> CycleResult:
    """纯 REST 模式（use_websocket=False，旧行为：内联 resample）。"""
    result = CycleResult()
    with ProcessPoolExecutor(max_workers=3) as exe:
        task_binance = exe.submit(fetch_update_binance_candles, handle, run_time, True)
        task_data_api = exe.submit(update_data_api_candles, handle, run_time) \
            if handle.data_api is not None else None
        task_coin_cap = exe.submit(run_coin_cap_update_to_recent, handle, run_time) \
            if handle.coin_cap_handle is not None else None

        binance_result = task_binance.result()
        if binance_result is not None:
            result.is_binance_success = True
            _, _, result.spot_begin_ts, result.swap_begin_ts = binance_result
        if task_data_api is not None:
            data_api_result = task_data_api.result()
            if data_api_result is not None:
                result.is_data_api_success = True
                if not result.is_binance_success:
                    result.spot_begin_ts, result.swap_begin_ts = data_api_result
        if task_coin_cap is not None:
            if not task_coin_cap.result():
                result.errs.append("获取市值数据失败")

    if result.is_binance_success or result.is_data_api_success:
        _run_preprocess(handle, run_time, result.spot_begin_ts, result.swap_begin_ts)

    return result
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
