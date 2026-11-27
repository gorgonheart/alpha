"""update_all 用的"ready 文件轮询屏障"：在触发下游 resample/preprocess 前，
同步等待 writer 完成。

工作机制：
  - writer 原子写完 pkl 后调 save_ready(symbol, run_time)。
  - update_all 轮询 `{symbol}_{int(run_time.timestamp())}.ready` 文件。
  - 超时窗口内 ready 文件出现的 symbol → success。
  - 截止时间仍缺的 symbol → 标记 skipped（不生成填充数据）。调用方记日志 +
    留缺口；由下一周期的 REST 兜底自然补齐。

本模块刻意保持无副作用且同步（无线程、无 asyncio）。只能由 main 进程的
update_all 调用。

对外 API：
  - wait_for_ready_barrier(symbols, base_5m_dir, run_time,
        timeout_sec=45.0, poll_interval_sec=0.1, ws_wait_sec=10.0) -> tuple[set[str], set[str]]
"""
from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

from core.storage.candle_manager import CandleFileReader


def wait_for_ready_barrier(
    symbols: Iterable[str],
    base_5m_dir: Path,
    run_time: datetime,
    timeout_sec: float = 45.0,
    poll_interval_sec: float = 0.1,
    ws_wait_sec: float = 10.0,
    rest_done=None,
    rest_submitted=None,
    hard_timeout_sec: float = 180.0,
) -> tuple[set[str], set[str]]:
    """轮询 ready 文件，直到每个 symbol 都齐 或 截止时间到。

    Ready 文件命名约定（CandleFileReader.format_ready_file_path）：
      `{base_5m_dir}/{symbol}_{int(run_time.timestamp())}.ready`

    轮询循环（优先认 WS 最终值，WS 缺则回退 REST）：
      - 每轮对所有仍 pending 的 symbol 先调 `check_ws_ready`：WS 闭合 K 已落盘
        （base 为 WS 最终值）→ 立即移入 `success`，不等满 ws_wait_sec。
      - 若 `check_ws_ready` 仍为 False 且已过 ws_wait_sec，则回退查 `check_ready`
        （REST 的 .ready 兜底）；满足即移入 `success`。
      - 当 `pending` 为空 或 `time.monotonic()` 超过 deadline 时退出循环。
      - 两轮之间睡 `poll_interval_sec`，仅在距 deadline 仍有时间且还有 pending
        symbol 时才睡。

    不变式：应保持 ws_wait_sec < timeout_sec；否则 REST 回退分支永不触发，
    仅有 .ready（无 .ws_ready）的 symbol 会被 skip。生产调用用默认
    （ws_wait_sec=10 < timeout_sec=45）满足此不变式。

    超时处理：
      - 截止时仍 pending 的 symbol 放入 `skipped` 返回。
      - 本函数不生成填充数据；调用方负责打日志/告警，并依赖下一周期 REST 兜底。

    断档等待（rest_done 非 None 时启用）：
      - 带 `{symbol}.gap` 标记的 symbol 说明 base 中间缺 K、正等 REST 兜底补齐，
        此时即便有 .ws_ready 也不放行，等 writer 写入 REST 数据后清掉标记再放行，
        避免下游 resample/preprocess 读到带洞的 base。
      - 放弃等待：REST 兜底线程已结束（rest_done.is_set()）且该 symbol 不在
        rest_submitted（REST 没给它数据，如请求失败）→ 退回原逻辑，不拖到 deadline。
      - 无标记时与原逻辑完全一致，健康路径零额外等待。

    REST 兜底未落盘时延长等待（rest_done 非 None 时启用）：
      - 到 timeout_sec（软截止）仍有 pending，不立即放弃：只要 REST 兜底线程还在跑，
        或 REST 已投递数据给某个 pending symbol 但 writer 还没写完，就继续等，
        保证 resample 读到的 base 已含 REST 补回的最新 K，而不是先 resample 后补数据。
      - REST 已结束、剩下的 pending 都没拿到 REST 数据（请求失败等）→ 立即结束，记 skipped。
      - hard_timeout_sec（硬截止）兜底：writer 挂掉、REST 卡死等异常时也一定结束，
        不拖进下一个 5m 周期。
      - rest_done 为 None 时与原逻辑一致：到 timeout_sec 即结束。

    symbol 集合为空时立即短路（无 IO、无 sleep）。
    """
    pending: set[str] = set(symbols)
    success: set[str] = set()
    if not pending:
        return success, set()

    reader = CandleFileReader(Path(base_5m_dir), "pickle")
    start = time.monotonic()
    deadline = start + timeout_sec
    hard_deadline = start + max(timeout_sec, hard_timeout_sec) if rest_done is not None else deadline
    while True:
        ws_grace_passed = (time.monotonic() - start) >= ws_wait_sec
        # 每轮一次目录遍历取断档标记；REST 已结束则只等它真正投递过数据的 symbol
        waiting_gap: set[str] = set()
        if rest_done is not None:
            gap_syms = reader.list_gap_marker_symbols() & pending
            if gap_syms and rest_done.is_set():
                gap_syms &= set(rest_submitted or ())
            waiting_gap = gap_syms
        for sym in list(pending):
            if sym in waiting_gap:
                continue
            # 优先认 WS 已落盘（base 为 WS 最终值）；WS 缺则过 ws_wait 后回退认 REST 的 .ready
            if reader.check_ws_ready(sym, run_time) or (
                ws_grace_passed and reader.check_ready(sym, run_time)
            ):
                pending.discard(sym)
                success.add(sym)
        if not pending:
            break
        now = time.monotonic()
        if now >= hard_deadline:
            break
        if now >= deadline:
            # 软截止已过：只为 REST 还可能补上的 symbol 继续等
            if rest_done is None:
                break
            if rest_done.is_set() and not (pending & set(rest_submitted or ())):
                break
        time.sleep(min(poll_interval_sec, max(hard_deadline - now, 0.0)))
    return success, pending
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
