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
        timeout_sec=45.0, poll_interval_sec=0.1, ws_wait_sec=10.0,
        rest_done=None, rest_submitted=None, rest_confirmed_empty=None,
        hard_timeout_sec=180.0) -> tuple[set[str], set[str]]
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
    rest_confirmed_empty=None,
    hard_timeout_sec: float = 180.0,
) -> tuple[set[str], set[str]]:
    """轮询 ready 文件，直到每个 symbol 都齐、REST 确认无待落盘项，或截止时间到。

    竞态安全提前退出：
      - 在每次 while 循环顶部（扫描 pending 之前）先采样
        `rest_done_was_set = rest_done is not None and rest_done.is_set()`。
      - 只有在 `rest_done_was_set` 已经为 True 的整轮 `pending` 扫描完成之后，
        若 `ws_grace_passed` 且剩余 pending 与 `rest_submitted` 无交集（包括
        `rest_confirmed_empty` 中的未开盘新币或 REST 失败项），才立即提前退出，
        避免在 `rest_done.set()` 瞬间漏扫刚落盘的 `.ready` 文件。
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
        # 必须在扫描 pending 之前采样 rest_done，确保 rest_done.set() 之前写入的 .ready
        # 至少被完整扫描一轮，绝不会因竞态被误判为 skipped。
        rest_done_was_set = rest_done is not None and rest_done.is_set()
        ws_grace_passed = (time.monotonic() - start) >= ws_wait_sec
        submitted_set = set(rest_submitted or ())

        # 每轮一次目录遍历取断档标记；REST 已结束则只等它真正投递过数据的 symbol
        waiting_gap: set[str] = set()
        if rest_done is not None:
            gap_syms = reader.list_gap_marker_symbols() & pending
            if gap_syms and rest_done_was_set:
                gap_syms &= submitted_set
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

        # 仅在 rest_done_was_set 已为 True 的完整扫描后，且没有剩余 pending 等待 writer 落盘时提前退出
        if ws_grace_passed and rest_done_was_set and not (pending & submitted_set):
            break

        now = time.monotonic()
        if now >= hard_deadline:
            break
        if now >= deadline:
            # 软截止已过：只为 REST 还可能补上的 symbol 继续等
            if rest_done is None:
                break
            if rest_done_was_set and not (pending & submitted_set):
                break
        time.sleep(min(poll_interval_sec, max(hard_deadline - now, 0.0)))
    return success, pending
