"""每个 run_time 周期的数据更新逻辑（WS 模式 与 纯 REST 模式）。

core.app.update_all 只保留对齐时间点、非启用 offset 维护、WS 模式下的 exginfo/health check、
以及共用错误 epilogue；具体"抓数→落盘→barrier→resample→preprocess"两种模式的实现放在这里，
各自返回一个 CycleResult。
"""
from __future__ import annotations

import asyncio
import inspect
import multiprocessing as mp
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from copy import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from core.exchange.symbol_match import update_symbol_match
from core.handle import BmacHandle
from core.ingest.coin_cap import run_coin_cap_update_to_recent
from core.ingest.data_api_update import update_data_api_candles
from core.pipeline.barrier import wait_for_ready_barrier
from core.ws.ws_local_cache import query_ws_cache_status
from core.pipeline.data_update import (
    fetch_update_binance_candles,
    update_binance_base_candles,
    update_meta,
)
from core.pipeline.publication_sources import choose_publication_inputs
from core.pipeline.resample import update_resample_from_base
from core.pipeline.runtime_supervisor import RuntimeSupervisor
from core.storage.candle_manager import CandleFileManager, CandleFileReader
from core.strategy.preprocess import preprocess_offset_data
from core.utils.log_kit import logger
from core.utils.time import convert_interval_to_timedelta, next_run_time, now_time_str, sleep_until_run_time

# 默认 WS 等待/REST 兜底 grace 秒数（显式配置，避免 inspect 默认值隐式耦合；可通过 handle.ws_wait_sec 覆盖）
_DEFAULT_WS_WAIT_SEC: float = 14.0
_REST_FALLBACK_GRACE_SEC: float = _DEFAULT_WS_WAIT_SEC
# 无 .gap 历史断档、仅本轮单根 WS 漏推时的轻量 REST 拉取根数
_SINGLE_BAR_FALLBACK_LIMIT: int = 5
# barrier 软截止：超过它仍在等，说明在等 REST 兜底落盘（见 wait_for_ready_barrier）
_BARRIER_SOFT_TIMEOUT_SEC: float = (
    inspect.signature(wait_for_ready_barrier).parameters["timeout_sec"].default
)


def _resolve_ws_wait_sec(handle: BmacHandle) -> float:
    val = getattr(handle, "ws_wait_sec", None)
    if val is None or isinstance(val, bool):
        return _REST_FALLBACK_GRACE_SEC
    return float(val)

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


def _fetch_wake_time(run_time: datetime, cfg=None) -> datetime:
    """计算 REST 采集实际唤醒时间，逻辑 run_time 及闭合边界契约保持不变。"""
    if cfg is None:
        import config as _default_cfg
        cfg = _default_cfg
    delay = getattr(cfg, "rest_fetch_delay_seconds", 0.0)
    if isinstance(delay, bool) or not isinstance(delay, (int, float)) or delay < 0:
        raise ValueError(f"rest_fetch_delay_seconds must be a non-negative number, got {delay!r}")
    return run_time + timedelta(seconds=float(delay)) if delay else run_time


# ---------------------------------------------------------------- WS helpers

def _get_market_handle(handle: BmacHandle, trade_type: str):
    if hasattr(handle, "get_market_handle"):
        return handle.get_market_handle(trade_type)
    return handle.get_binance_handle(trade_type)


def _universe(handle: BmacHandle, trade_type: str) -> list:
    """exginfo_{trade_type} 的交易对全集（与 barrier 用的同一来源）。"""
    df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
    return df_exg["symbol"].to_list() if df_exg is not None else []


def _resolve_ws_cache_db(handle: BmacHandle, trade_type: str) -> Optional[Path]:
    base_dir = getattr(handle, "base_dir", None)
    if base_dir is None:
        return None
    p = Path(base_dir) / "ws_cache" / f"{trade_type}.sqlite"
    return p if p.exists() else None


def _compute_missed_and_gaps(
    handle: BmacHandle, trade_type: str, run_time: datetime
) -> tuple[set[str], set[str]]:
    """计算该 trade_type 下需要 REST 兜底的 symbol 集合及其中带历史 .gap 标记的子集。"""
    universe = _universe(handle, trade_type)
    bh = _get_market_handle(handle, trade_type)
    reader = CandleFileManager(bh.base_candle_dir, "pickle")
    gap_symbols = reader.list_gap_marker_symbols()
    stale = gap_symbols - set(universe)
    if universe and stale:
        for sym in stale:
            reader.clear_gap_marker(sym)
    gap_symbols -= stale

    ws_ready_contig: set[str] = set()
    ws_full_contig: set[str] = set()
    ws_gapped: set[str] = set()
    ws_cache_db = _resolve_ws_cache_db(handle, trade_type)
    if ws_cache_db is not None:
        base_delta = convert_interval_to_timedelta(getattr(handle, "base_interval", "5m"))
        base_ms = max(1, int(base_delta.total_seconds() * 1000))
        target_ms = int((run_time - base_delta).timestamp() * 1000)
        once_bars = int(getattr(bh, "once_update_candles", 99))
        ws_ready_contig, ws_full_contig, ws_gapped = query_ws_cache_status(
            ws_cache_db, trade_type, target_ms, base_ms, check_bars=once_bars
        )

    universe_set = set(universe)
    for sym in (ws_gapped & universe_set):
        if not reader.check_ws_ready(sym, run_time):
            gap_symbols.add(sym)

    if gap_symbols:
        logger.info(
            f"REST 兜底[{trade_type}]: {len(gap_symbols)} 个 symbol 有历史断档标记, "
            f"sample={sorted(gap_symbols)[:10]}"
        )

    missed: set[str] = set()
    for sym in universe:
        if sym in gap_symbols:
            missed.add(sym)
            continue
        if reader.check_ws_ready(sym, run_time):
            continue
        if sym in ws_ready_contig and (sym in ws_full_contig or reader.has_symbol(sym)):
            continue
        missed.add(sym)
    return missed, gap_symbols


def _compute_missed_symbols(handle: BmacHandle, trade_type: str, run_time: datetime) -> set:
    """计算该 trade_type 下需要 REST 兜底的 symbol。

    missed = {sym ∈ universe | 无本轮 .ws_ready  或  有 .gap 断档标记}。
    """
    missed, _ = _compute_missed_and_gaps(handle, trade_type, run_time)
    return missed


def _submit_rest_dfs(
    router,
    run_time: datetime,
    spot_dfs: dict,
    swap_dfs: dict,
    submitted: Optional[dict] = None,
    confirmed_empty: Optional[dict] = None,
    failed: Optional[dict] = None,
    requested: Optional[dict] = None,
) -> int:
    """把 REST 抓到的 base K 以 source='rest' 投递 writer router；返回投递条数。

    submitted（可选 {trade_type: set}）：先记再投，barrier 据此判断"REST 结束后还要不要等这个 symbol"。
    confirmed_empty（可选 {trade_type: set}）：仅在 REST 明确返回空 DataFrame 时记录（未开盘新币）。
    failed（可选 {trade_type: set}）：请求集合中未返回或为 None 的 symbol。
    """
    count = 0
    for trade_type, symbol_dfs in (("spot", spot_dfs), ("swap", swap_dfs)):
        dfs = symbol_dfs or {}
        if failed is not None and requested is not None:
            for sym in requested.get(trade_type, ()):
                if sym not in dfs or dfs[sym] is None:
                    failed[trade_type].add(sym)
        for symbol, df in dfs.items():
            if df is None:
                if failed is not None:
                    failed[trade_type].add(symbol)
                continue
            if df.empty:
                if confirmed_empty is not None:
                    confirmed_empty[trade_type].add(symbol)
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
    """WS 启动后、进入主循环前，同步全量 REST 一次，补齐"历史下载截止 → WS 订阅生效"的启动缺口。"""
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
    """后台线程：先等待可取消 grace 窗口让 WS 落盘；若屏障已 100% 放行则立即退出，否则按缺口分级 REST 兜底。"""
    missed_per_type = {"spot": set(), "swap": set()}
    try:
        grace_sec = _resolve_ws_wait_sec(handle)
        cancel_ev = state.get("cancel")
        if cancel_ev is not None:
            if cancel_ev.wait(timeout=grace_sec):
                state["ok"] = True
                return
        else:
            time.sleep(grace_sec)

        spot_missed, spot_gaps = _compute_missed_and_gaps(handle, "spot", run_time)
        swap_missed, swap_gaps = _compute_missed_and_gaps(handle, "swap", run_time)
        missed_per_type = {"spot": spot_missed, "swap": swap_missed}
        n_spot, n_swap = len(spot_missed), len(swap_missed)
        logger.info(
            f"REST 兜底 grace({grace_sec}s) 后计算 missed: "
            f"spot={n_spot} swap={n_swap} (run_time={run_time})"
        )
        if n_spot == 0 and n_swap == 0:
            state["ok"] = True
            return

        symbol_limits: dict[str, dict[str, int]] = {"spot": {}, "swap": {}}
        for tt, m_set, g_set in (("spot", spot_missed, spot_gaps), ("swap", swap_missed, swap_gaps)):
            mh = _get_market_handle(handle, tt)
            full_lim = int(getattr(mh, "once_update_candles", 99))
            single_lim = min(_SINGLE_BAR_FALLBACK_LIMIT, full_lim)
            symbol_limits[tt] = {
                sym: (full_lim if sym in g_set else single_lim)
                for sym in m_set
            }

        call_kwargs = {}
        sig = inspect.signature(fetch_update_binance_candles)
        if "symbol_limits" in sig.parameters:
            call_kwargs["symbol_limits"] = symbol_limits

        result = fetch_update_binance_candles(
            handle, run_time, resample_inline=False, symbols_subset=missed_per_type, **call_kwargs
        )
        if result is not None:
            spot_dfs, swap_dfs, _, _ = result
            _submit_rest_dfs(
                router,
                run_time,
                spot_dfs,
                swap_dfs,
                submitted=state["submitted"],
                confirmed_empty=state["confirmed_empty"],
                failed=state["failed"],
                requested=missed_per_type,
            )
            state["ok"] = True
        else:
            for tt in ("spot", "swap"):
                state["failed"][tt].update(missed_per_type[tt])
    except Exception:
        for tt in ("spot", "swap"):
            state["failed"][tt].update(missed_per_type[tt])
        logger.exception("REST fallback 后台线程异常")
    finally:
        # 无论成败都置位，barrier 不再为没拿到 REST 数据的 symbol 干等
        state["done"].set()


def _start_rest_fallback(handle: BmacHandle, run_time: datetime, router):
    """启动 REST 兜底线程；返回 (thread, state)。

    state:
      - ok: 是否成功
      - done: 线程结束事件
      - cancel: 屏障 100% 完成且无断档时提前取消等待事件
      - submitted: {trade_type: 已投递 REST 数据的 symbol}
      - confirmed_empty: {trade_type: REST 明确返回空 DataFrame 的 symbol}
      - failed: {trade_type: REST 拉取失败的 symbol}
    """
    state = {
        "ok": False,
        "done": threading.Event(),
        "cancel": threading.Event(),
        "submitted": {"spot": set(), "swap": set()},
        "confirmed_empty": {"spot": set(), "swap": set()},
        "failed": {"spot": set(), "swap": set()},
    }
    thread = threading.Thread(
        target=_rest_fallback_worker,
        args=(handle, run_time, router, state),
        name="rest-fallback",
        daemon=False,
    )
    thread.start()
    return thread, state


def _barrier_and_resample(
    handle: BmacHandle,
    trade_type: str,
    run_time: datetime,
    rest_state: Optional[dict] = None,
    *,
    compute_pool=None,
    return_tail_bars: int = 0,
) -> dict:
    """等该 trade_type 的 symbol 落盘（ready-file barrier），通过即从 base 存储 resample。"""
    bh = _get_market_handle(handle, trade_type)
    df_exg = handle.exg_mgr.read_candle(f"exginfo_{trade_type}")
    symbols = df_exg["symbol"].to_list() if df_exg is not None else []
    logger.info(f"barrier[{trade_type}] 等待 {len(symbols)} symbol ready (run_time={run_time}) ...")
    t0 = time.monotonic()
    rest_done = rest_state.get("done") if rest_state else None
    rest_submitted = rest_state["submitted"].get(trade_type) if rest_done is not None else None
    rest_confirmed_empty = (
        rest_state.get("confirmed_empty", {}).get(trade_type) if rest_done is not None else None
    )
    ws_wait_sec = _resolve_ws_wait_sec(handle)
    bar_kwargs: dict = dict(
        symbols=symbols,
        base_5m_dir=bh.base_candle_dir,
        run_time=run_time,
        ws_wait_sec=ws_wait_sec,
        rest_done=rest_done,
        rest_submitted=rest_submitted,
        rest_confirmed_empty=rest_confirmed_empty,
    )
    bar_sig = inspect.signature(wait_for_ready_barrier)
    if "ws_cache_db" in bar_sig.parameters:
        bar_kwargs["ws_cache_db"] = _resolve_ws_cache_db(handle, trade_type)
        bar_kwargs["trade_type"] = trade_type
        bar_kwargs["base_delta"] = convert_interval_to_timedelta(getattr(handle, "base_interval", "5m"))
        bar_kwargs["once_update_candles"] = int(getattr(bh, "once_update_candles", 99))
    success, skipped = wait_for_ready_barrier(**bar_kwargs)
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
    reader = CandleFileReader(Path(bh.base_candle_dir), "pickle")
    has_gaps = bool(reader.list_gap_marker_symbols())
    no_data = {s for s in skipped if not reader.has_symbol(s)}
    if no_data:
        logger.info(
            f"barrier[{trade_type}] {len(no_data)} 个 symbol 还没有任何 K 线（新上线未开盘等），"
            f"不计入 timeout: {sorted(no_data)[:10]}"
        )
    begin_ts: dict = {}
    tail_dfs: dict = {}
    if success:
        sig = inspect.signature(update_resample_from_base)
        if "executor" in sig.parameters and "return_tail_bars" in sig.parameters:
            res_out = update_resample_from_base(
                handle, trade_type, run_time, executor=compute_pool, return_tail_bars=return_tail_bars
            )
            if return_tail_bars > 0:
                begin_ts, tail_dfs = res_out
            else:
                begin_ts = res_out
        else:
            begin_ts = update_resample_from_base(handle, trade_type, run_time)
    return {
        "trade_type": trade_type,
        "success": len(success),
        "total": len(symbols),
        "skipped": sorted(skipped - no_data),
        "has_gaps": has_gaps,
        "begin_ts": begin_ts,
        "tail_dfs": tail_dfs,
    }


def _run_preprocess(
    handle: BmacHandle,
    run_time: datetime,
    spot_begin_ts,
    swap_begin_ts,
    *,
    batch_executor=None,
    resample_tails=None,
) -> None:
    """运行 preprocess：有常驻计算池时在当前协调线程直接分发各 batch，否则使用单 worker 子进程隔离。"""
    if handle.preprocess_handle is None:
        return
    if batch_executor is not None:
        pp_sig = inspect.signature(preprocess_offset_data)
        accepts_kw = (
            any(p.kind == inspect.Parameter.VAR_KEYWORD for p in pp_sig.parameters.values())
            or ("batch_executor" in pp_sig.parameters and "resample_tails" in pp_sig.parameters)
        )
        if accepts_kw:
            preprocess_offset_data(
                handle,
                run_time,
                spot_begin_ts,
                swap_begin_ts,
                batch_executor=batch_executor,
                resample_tails=resample_tails,
            )
        else:
            preprocess_offset_data(handle, run_time, spot_begin_ts, swap_begin_ts)
    else:
        with ProcessPoolExecutor(max_workers=1, mp_context=_SPAWN_CTX) as exe:
            exe.submit(preprocess_offset_data, handle, run_time, spot_begin_ts, swap_begin_ts).result()


# ----------------------------------------------------------------- entrypoints

def run_ws_cycle(handle: BmacHandle, run_time: datetime, supervisor: RuntimeSupervisor) -> CycleResult:
    """WS 模式（非阻塞发布拓扑）：

    1. 后台线程延迟 grace 后 REST 兜底漏网 symbol，并记录 submitted / confirmed_empty / failed。
    2. aux_exe 异步提交 task_data_api / task_coin_cap。
    3. br_exe 并发跑 spot / swap 的 barrier + resample。
    4. Binance 100% 覆盖时立刻带固定 publication_sources 启动 _run_preprocess，
       不等待慢速的 task_data_api / task_coin_cap；仅在覆盖不足且有 task_data_api 时才等
       task_data_api 并做二次裁决。
    5. 收尾 task_coin_cap / task_data_api，并在 finally 中 join REST 线程。
    """
    result = CycleResult()
    router = supervisor.router

    compute_pool = (
        supervisor.get_compute_pool()
        if callable(getattr(supervisor, "get_compute_pool", None))
        else None
    )
    rest_thread, rest_state = _start_rest_fallback(handle, run_time, router)
    try:
        with ProcessPoolExecutor(max_workers=2, mp_context=_SPAWN_CTX) as aux_exe:
            task_data_api = (
                aux_exe.submit(update_data_api_candles, handle, run_time)
                if handle.data_api is not None
                else None
            )
            task_coin_cap = (
                aux_exe.submit(run_coin_cap_update_to_recent, handle, run_time)
                if handle.coin_cap_handle is not None
                else None
            )

            br_sig = inspect.signature(_barrier_and_resample)
            supports_pool_kw = "compute_pool" in br_sig.parameters
            with ThreadPoolExecutor(max_workers=2) as br_exe:
                results = list(
                    br_exe.map(
                        lambda tt: (
                            _barrier_and_resample(
                                handle, tt, run_time, rest_state,
                                compute_pool=compute_pool,
                                return_tail_bars=12 if compute_pool is not None else 0,
                            )
                            if supports_pool_kw
                            else _barrier_and_resample(handle, tt, run_time, rest_state)
                        ),
                        ("spot", "swap"),
                    )
                )

            if rest_state and "cancel" in rest_state:
                if all(r["success"] == r["total"] and not r.get("has_gaps", False) for r in results):
                    rest_state["cancel"].set()

            resample_tails: dict[str, dict] = {}
            for r in results:
                if r["success"]:
                    result.is_binance_success = True
                if r["trade_type"] == "spot":
                    result.spot_begin_ts = r["begin_ts"]
                else:
                    result.swap_begin_ts = r["begin_ts"]
                r_tails = r.get("tail_dfs") or {}
                r_begins = r.get("begin_ts") or {}
                if r_tails and r_begins:
                    resample_tails[r["trade_type"]] = {
                        sym: (r_begins[sym][0], r_begins[sym][1], tdf)
                        for sym, tdf in r_tails.items()
                        if tdf is not None and sym in r_begins
                    }
                if r["skipped"]:
                    logger.warning(
                        f"barrier {r['trade_type']} timeout: {len(r['skipped'])}/{r['total']} "
                        f"skipped, sample={r['skipped'][:10]}"
                    )
                    result.errs.append(f"barrier {r['trade_type']} timeout {len(r['skipped'])}/{r['total']}")

            ws_binance_result = (
                ({}, {}, result.spot_begin_ts or {}, result.swap_begin_ts or {})
                if result.is_binance_success
                else None
            )
            ws_confirmed_empty = rest_state.get("confirmed_empty") if rest_state else None
            data_api_done = task_data_api is None
            data_api_result = None
            published = False

            rp_sig = inspect.signature(_run_preprocess)
            rp_supports_pool = (
                any(p.kind == inspect.Parameter.VAR_KEYWORD for p in rp_sig.parameters.values())
                or ("batch_executor" in rp_sig.parameters and "resample_tails" in rp_sig.parameters)
            )

            def _invoke_preprocess(pub_handle, sp_bt, sw_bt):
                if rp_supports_pool:
                    _run_preprocess(
                        pub_handle,
                        run_time,
                        sp_bt,
                        sw_bt,
                        batch_executor=compute_pool,
                        resample_tails=resample_tails or None,
                    )
                else:
                    _run_preprocess(pub_handle, run_time, sp_bt, sw_bt)

            if handle.preprocess_handle is not None:
                try:
                    chosen = choose_publication_inputs(
                        handle,
                        run_time,
                        ws_binance_result,
                        None,
                        binance_done=True,
                        data_api_done=data_api_done,
                        confirmed_empty=ws_confirmed_empty,
                    )
                except ValueError as error:
                    result.errs.append(str(error))
                    chosen = None
                    published = True

                if chosen is not None:
                    sources, spot_begin_ts, swap_begin_ts = chosen
                    publication_handle = copy(handle)
                    publication_handle.publication_sources = sources
                    _invoke_preprocess(publication_handle, spot_begin_ts, swap_begin_ts)
                    published = True

                if not published and task_data_api is not None:
                    try:
                        data_api_result = task_data_api.result()
                    except Exception as error:
                        logger.warning(f"DataAPI采集任务失败: {type(error).__name__}")
                        data_api_result = None
                    data_api_done = True
                    result.is_data_api_success = data_api_result is not None

                    try:
                        chosen = choose_publication_inputs(
                            handle,
                            run_time,
                            ws_binance_result,
                            data_api_result,
                            binance_done=True,
                            data_api_done=True,
                            confirmed_empty=ws_confirmed_empty,
                        )
                    except ValueError as error:
                        result.errs.append(str(error))
                        chosen = None

                    if chosen is not None:
                        sources, spot_begin_ts, swap_begin_ts = chosen
                        publication_handle = copy(handle)
                        publication_handle.publication_sources = sources
                        _invoke_preprocess(publication_handle, spot_begin_ts, swap_begin_ts)
                        published = True

            if task_coin_cap is not None:
                if not task_coin_cap.result():
                    result.errs.append("获取市值数据失败")

            if task_data_api is not None and not data_api_done:
                try:
                    data_api_result = task_data_api.result()
                except Exception as error:
                    logger.warning(f"DataAPI采集任务失败: {type(error).__name__}")
                    data_api_result = None
                result.is_data_api_success = data_api_result is not None
    finally:
        rest_thread.join(timeout=60)
        if rest_thread.is_alive():
            logger.warning("REST fallback 后台线程超过 60s 未结束")

    return result


def run_rest_cycle(handle: BmacHandle, run_time: datetime, cfg=None) -> CycleResult:
    """纯 REST 模式（use_websocket=False）：保留预热进程池、_fetch_wake_time、
    as_completed 非阻塞发布与 update_binance_base_candles。
    """
    result = CycleResult()
    spot_dfs = swap_dfs = None

    workers = 1 + int(handle.data_api is not None) + int(handle.coin_cap_handle is not None)
    fetch_wake_time = _fetch_wake_time(run_time, cfg)
    with ProcessPoolExecutor(max_workers=workers) as exe:
        # Occupy all workers until the target so spawn/import overlaps metadata I/O.
        # No market request runs before metadata succeeds and the target is reached.
        logger.info(f"开始预热采集进程, run_time={run_time}, wake_time={fetch_wake_time}, 当前时间={now_time_str()}")
        warmups = [exe.submit(sleep_until_run_time, fetch_wake_time) for _ in range(workers)]
        asyncio.run(update_meta(handle, run_time))
        update_symbol_match(handle, run_time)
        logger.ok(f"Exchange Info 与实时资金费获取成功, 本地时间={now_time_str()}")
        sleep_until_run_time(fetch_wake_time)
        for warmup in warmups:
            warmup.result()
        logger.info(f"采集进程预热就绪, run_time={run_time}, wake_time={fetch_wake_time}, 当前时间={now_time_str()}")

        task_binance = exe.submit(fetch_update_binance_candles, handle, run_time)
        update_tasks = [task_binance]

        if handle.data_api is not None:
            task_data_api = exe.submit(update_data_api_candles, handle, run_time)
            update_tasks.append(task_data_api)

        task_coin_cap = None
        if handle.coin_cap_handle is not None:
            task_coin_cap = exe.submit(run_coin_cap_update_to_recent, handle, run_time)

        task_preprocess = None
        binance_result = data_api_result = None
        binance_done = False
        data_api_done = handle.data_api is None
        coverage_failed = False

        for future in as_completed(update_tasks):
            try:
                fut_res = future.result()
            except Exception as error:
                source_name = "Binance" if future == task_binance else "DataAPI"
                logger.warning(f"{source_name}采集任务失败: {type(error).__name__}")
                fut_res = None

            if future == task_binance:
                binance_done = True
                binance_result = fut_res
                if binance_result is not None:
                    result.is_binance_success = True
                    spot_dfs, swap_dfs, result.spot_begin_ts, result.swap_begin_ts = binance_result
            else:
                data_api_done = True
                data_api_result = fut_res
                result.is_data_api_success = data_api_result is not None

            if handle.preprocess_handle is not None and task_preprocess is None and not coverage_failed:
                try:
                    chosen = choose_publication_inputs(
                        handle,
                        run_time,
                        binance_result,
                        data_api_result,
                        binance_done=binance_done,
                        data_api_done=data_api_done,
                    )
                except ValueError as error:
                    result.errs.append(str(error))
                    coverage_failed = True
                    chosen = None

                if chosen is not None:
                    sources, spot_begin_ts, swap_begin_ts = chosen
                    publication_handle = copy(handle)
                    publication_handle.publication_sources = sources
                    task_preprocess = exe.submit(
                        preprocess_offset_data,
                        publication_handle,
                        run_time,
                        spot_begin_ts,
                        swap_begin_ts,
                    )

        if task_preprocess is not None:
            task_preprocess.result()

        if task_coin_cap is not None:
            is_coin_cap_success = task_coin_cap.result()
            if not is_coin_cap_success:
                result.errs.append("获取市值数据失败")

    # 更新基础周期(5m) K 线
    if result.is_binance_success:
        update_binance_base_candles(handle, run_time, spot_dfs, swap_dfs)

    return result
