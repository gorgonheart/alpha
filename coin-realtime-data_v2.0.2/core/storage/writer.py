"""
单 writer 进程：从 mp.Queue 消费 (symbol, df, source, run_time) item，按优先级合并后落盘。

对外接口：
    - Writer(base_candle_dir, ...)
    - .handle_item(item: dict) -> None   （同步，可测）
    - .consume_loop(queue, shutdown_event) （在 multiprocessing.Process 内运行）

同一 candle_begin_time 的优先级顺序：
    ws (3) > rest (2) > snapshot (1)
"""
from __future__ import annotations

import logging
import os
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from core.storage.candle_manager import CandleFileManager
from core.storage.ts_manager import TSManager


log = logging.getLogger("core.storage.writer")

_SOURCE_PRIO: Dict[str, int] = {
    "ws": 3,
    "rest": 2,
    "snapshot": 1,
}


class _WriterObservability:
    """Writer 的全部进度/可见性日志，与数据路径隔离。

    持有"每来源首条" 标志和 "每 (trade_type, source) 详细步骤" 标志，使热路径
    （merge → pkl → TSManager → ready）读起来是纯逻辑。日志文本与触发条件与此前
    内联在 Writer 里时完全一致。
    """

    def __init__(self) -> None:
        self._first_logged_per_source: Dict[str, bool] = {}
        self._step_logged: Dict[tuple, bool] = {}

    def log_consumed(self, writer: "Writer", item: dict, consumed: int) -> None:
        """consume_loop 的每来源首条 + 每 50 条里程碑日志。

        解析产物路径（flat pkl + ready 文件），让运维能核对下游契约确实落到了磁盘。
        """
        src = item.get("source", "?")
        sym = item.get("symbol", "?")
        tt = item.get("trade_type", "?")
        run_time = item.get("run_time")
        pkl_path = None
        ready_path_str = None
        pkl_exists = False
        ready_exists = False
        pkl_size = 0
        try:
            base_dir, cfm = writer._route(tt, sym)
            pkl_path = writer._pkl_path(base_dir, sym)
            ready_path_str = cfm.format_ready_file_path(sym, run_time) if run_time else None
            pkl_exists = pkl_path.exists()
            pkl_size = pkl_path.stat().st_size if pkl_exists else 0
            ready_exists = os.path.exists(ready_path_str) if ready_path_str else False
        except Exception as e:
            log.warning("writer: artifact-path resolve failed for %s: %r", sym, e)

        if not self._first_logged_per_source.get(src):
            self._first_logged_per_source[src] = True
            log.info(
                "writer: ✅ FIRST handled source=%s tt=%s sym=%s rows=%d -> "
                "pkl=%s (exists=%s size=%d) ready=%s (exists=%s)",
                src, tt, sym, len(item.get("df", [])),
                pkl_path, pkl_exists, pkl_size,
                ready_path_str, ready_exists,
            )
        elif consumed % 50 == 0:
            log.info(
                "writer: handled cumulative=%d (last src=%s tt=%s sym=%s "
                "pkl=%s ready_ok=%s)",
                consumed, src, tt, sym,
                pkl_path, ready_exists,
            )

    def log_handle_start(self, symbol, trade_type, source, df_new) -> bool:
        """返回这个 (trade_type, source) 步骤是否需要打详细日志。

        每个 (trade_type, source) 的首条 item 会逐步打日志；该标志只在 item 完全处理
        完后才置位（见 log_after_ready）。
        """
        verbose = (trade_type, source) not in self._step_logged
        if verbose:
            log.info(
                "writer.handle_item: sym=%s tt=%s src=%s df_new rows=%d "
                "candle_begin_time min=%s max=%s",
                symbol, trade_type, source, len(df_new),
                df_new["candle_begin_time"].min(), df_new["candle_begin_time"].max(),
            )
        return verbose

    def log_after_merge(self, verbose: bool, symbol, df_merged) -> None:
        if verbose:
            log.info(
                "writer.handle_item: [merge done] sym=%s merged rows=%d",
                symbol, len(df_merged),
            )

    def log_after_pkl(self, verbose: bool, symbol, pkl_path: Path) -> None:
        if verbose:
            log.info(
                "writer.handle_item: [pkl atomic write done] sym=%s path=%s size=%d",
                symbol, pkl_path, pkl_path.stat().st_size if pkl_path.exists() else -1,
            )

    def log_after_ts(self, verbose: bool, symbol, sym_dir: Path) -> None:
        if verbose:
            part_files = sorted(sym_dir.glob("*.pkl")) if sym_dir.exists() else []
            log.info(
                "writer.handle_item: [TSManager partition done] sym=%s sym_dir=%s "
                "partitions=%d (latest=%s)",
                symbol, sym_dir, len(part_files),
                part_files[-1].name if part_files else None,
            )

    def log_after_ready(self, verbose: bool, symbol, trade_type, source, cfm, run_time) -> None:
        if not verbose:
            return
        ready_path = cfm.format_ready_file_path(symbol, run_time)
        log.info(
            "writer.handle_item: [save_ready done] sym=%s ready=%s exists=%s",
            symbol, ready_path, os.path.exists(ready_path),
        )
        log.info(
            "writer.handle_item: ✅ FIRST item for (%s/%s) fully processed; "
            "subsequent items log every 50",
            trade_type, source,
        )
        self._step_logged[(trade_type, source)] = True


class Writer:
    """把多个提交合并到按 symbol 的 pkl 文件上的单 writer。

    每个 pkl 携带一个内部 `_source` 列，使后来的提交能决定是否覆盖同一
    candle_begin_time 的行。没有 `_source` 的旧 pkl 视为 `rest`（优先级 2）。

    两种构造形式：
      Writer(base_candle_dir=path)             # 遗留的单目录形式
      Writer(base_candle_dirs={"spot": ..., "swap": ...})  # 按 trade_type 路由
    """

    # 同一缺口 REST 尝试达到此次数仍未补上 → 认定交易所无数据，不再标记
    GAP_MAX_REST_ATTEMPTS = 2
    # 最早分区的裁剪间隔（每个 symbol）。见 _update_tsmanager。
    PARTITION_TRIM_INTERVAL = timedelta(days=1)

    def __init__(
        self,
        base_candle_dir: Optional[Path] = None,
        base_candle_dirs: Optional[Dict[str, Path]] = None,
        num_base_candles: Optional[int] = None,
        base_delta: timedelta = timedelta(minutes=5),
        alert: Optional[Any] = None,
        gap_check_bars: int = 99,
    ) -> None:
        if base_candle_dirs is not None:
            if base_candle_dir is not None:
                raise ValueError(
                    "pass either base_candle_dir OR base_candle_dirs, not both"
                )
            self.base_candle_dirs: Dict[str, Path] = {
                tt: Path(d) for tt, d in base_candle_dirs.items()
            }
            self._single_dir_mode = False
        elif base_candle_dir is not None:
            # 遗留的单目录形式：存到哨兵 key None 下，这样 item 缺 trade_type 时
            # 路由也能取到它。
            self.base_candle_dirs = {None: Path(base_candle_dir)}  # type: ignore[dict-item]
            self._single_dir_mode = True
        else:
            raise ValueError(
                "Writer requires either base_candle_dir or base_candle_dirs"
            )

        # 复用 CandleFileManager 来按目录维护 ready 文件约定。
        self._cfms: Dict = {
            tt: CandleFileManager(d, "pickle")
            for tt, d in self.base_candle_dirs.items()
        }
        # 可配置的滚动窗口。None = 不裁剪（向后兼容）。
        self.num_base_candles = num_base_candles
        self.base_delta = base_delta
        # 损坏 pkl + 未来运维事件的可选 Alert 接收端。
        self._alert = alert
        # 全部进度/可见性日志放这里，离开数据路径。
        self._obs = _WriterObservability()
        # 断档检测窗口（根数）：= REST 单次拉取根数，只标记 REST 能补回的缺口。
        self.gap_check_bars = gap_check_bars
        # (trade_type, symbol) → REST 补过后仍存在的最新缺口终点（交易所本身无数据，如停牌）。
        # 不超过它的缺口不再标记，避免对补不回来的缺口每轮重复 REST。
        # 同一 symbol 固定路由到同一 writer，进程内状态即可；writer 重启丢失只多一次 REST。
        self._unfillable_gap_end: Dict[tuple, pd.Timestamp] = {}
        # (trade_type, symbol) → (gap_end, 已 REST 尝试次数)：REST 够不到缺口起点时
        # （如停牌超过窗口后复牌）无法用"跨过缺口"判定，按次数兜底认定补不回来。
        self._gap_rest_attempts: Dict[tuple, tuple] = {}
        # (base_dir, symbol) → 上次裁剪最早分区时的 run_time；进程内状态，writer 重启后首写即裁一次。
        self._last_partition_trim: Dict[tuple, datetime] = {}

    # ---- 公共同步入口（可测）----

    def consume_loop(self, queue) -> None:
        """从 `queue` 拉取 item，直到收到 None 哨兵。

        item 结构：{symbol, df, source, run_time}。队列上的 None 表示"干净关停"。

        设计为在 multiprocessing.Process 内运行。queue 以参数传入，这样测试夹具可以
        用全新的 mp.Queue，生产接线则可以传入 main 的共享队列。
        """
        log.info("writer: consume_loop started")
        consumed = 0
        while True:
            item = queue.get()
            if item is None:
                log.info("writer: sentinel received (consumed=%d), exit", consumed)
                break
            try:
                self.handle_item(item)
                consumed += 1
                self._obs.log_consumed(self, item, consumed)
            except Exception:
                # 打日志并继续，让一条坏 item 永远不会杀掉 writer 循环。
                #（生产里应走 alert.critical；测试目前还不需要。）
                traceback.print_exc()

    def handle_item(self, item: dict) -> None:
        symbol = item["symbol"]
        df_new = item["df"]
        source = item["source"]
        run_time: datetime = item["run_time"]
        # trade_type 把 item 路由到正确的 base 目录。
        trade_type = item.get("trade_type")

        if source not in _SOURCE_PRIO:
            raise ValueError(f"unknown source {source!r}, expected one of {list(_SOURCE_PRIO)}")
        if df_new is None or df_new.empty:
            return

        # writer 入口类型断言：candle_begin_time 必须是 tz-aware 的 UTC。
        cbt = df_new["candle_begin_time"]
        cbt_tz = getattr(cbt.dt, "tz", None) if hasattr(cbt, "dt") else None
        if cbt_tz is None:
            raise TypeError(
                f"writer.handle_item: candle_begin_time must be tz-aware (UTC); "
                f"got tz-naive series for symbol={symbol!r}"
            )

        # 为这条 item 的 trade_type 取目录 + cfm。
        base_dir, cfm = self._route(trade_type, symbol)

        # 流水线：merge → pkl 原子重命名 → TSManager 更新 → 最后 save_ready
        #（save_ready 表示"本次提交的所有产物都已稳定"）。
        verbose = self._obs.log_handle_start(symbol, trade_type, source, df_new)

        df_merged = self._merge(base_dir, symbol, df_new, source)
        self._obs.log_after_merge(verbose, symbol, df_merged)

        self._write_pkl(base_dir, symbol, df_merged)
        self._obs.log_after_pkl(verbose, symbol, self._pkl_path(base_dir, symbol))

        # 分区与 pkl 同源：只把本 item 触碰的 cbt（df_merged 中已按优先级解析、ws 赢）
        # 写入分区，避免晚到的 rest 经 TSManager last-write-wins 覆盖分区里的 ws 最终值。
        # resample 读分区（resample_candle._read_base_tail），分区被覆盖会冻结非最终 close。
        df_for_partition = df_merged[
            df_merged["candle_begin_time"].isin(df_new["candle_begin_time"])
        ]
        self._update_tsmanager(base_dir, symbol, df_for_partition, run_time)
        self._obs.log_after_ts(verbose, symbol, base_dir / symbol)

        # 先更新断档标记再写 ready：barrier 看到 ready 时标记已是最新。
        # 标记是尽力而为的辅助信息，异常只记日志，绝不能挡住下面 ready / ws_ready 的写入。
        try:
            self._update_gap_marker(cfm, trade_type, symbol, df_merged, df_new, source)
        except Exception:
            log.exception("writer: 更新断档标记失败 sym=%s/%s source=%s（不影响落盘）",
                          trade_type, symbol, source)

        cfm.save_ready(symbol, run_time)
        # WS 闭合 K 已落盘 → 额外写 ws_ready，供 barrier 优先等 WS 最终值（非 REST 边界值）
        if source == "ws":
            cfm.save_ws_ready(symbol, run_time)
        self._obs.log_after_ready(verbose, symbol, trade_type, source, cfm, run_time)

    def _route(self, trade_type: Optional[str], symbol: str) -> tuple[Path, CandleFileManager]:
        if self._single_dir_mode:
            base_dir = self.base_candle_dirs[None]  # type: ignore[index]
            return base_dir, self._cfms[None]
        if trade_type not in self.base_candle_dirs:
            raise KeyError(
                f"writer: unknown trade_type {trade_type!r} for symbol {symbol!r}; "
                f"configured: {sorted(k for k in self.base_candle_dirs if k is not None)}"
            )
        return self.base_candle_dirs[trade_type], self._cfms[trade_type]

    # ---- 内部 ----

    def _latest_gap(self, df: pd.DataFrame) -> Optional[tuple]:
        """返回检测窗口内最新一个缺口 (缺口前最后一根, 缺口后第一根) 的 candle_begin_time；无缺口返回 None。

        窗口 = 最新一根往前 gap_check_bars 根（按时间），df 已由 _merge 按时间升序去重。
        窗口内最多 gap_check_bars 根，故只取最后 gap_check_bars+1 行（多 1 行是窗口内最早一根的前驱）
        即与全表等价；耗时与 pkl 大小无关（30 万行 pkl 也约 0.1ms），不读额外文件。
        """
        cbt = df["candle_begin_time"].iloc[-(self.gap_check_bars + 1):].reset_index(drop=True)
        if len(cbt) < 2:
            return None
        window_start = cbt.iloc[-1] - (self.gap_check_bars - 1) * self.base_delta
        is_gap_end = (cbt.diff() > self.base_delta) & (cbt >= window_start)
        if not is_gap_end.any():
            return None
        pos = is_gap_end[is_gap_end].index[-1]
        return cbt.iloc[pos - 1], cbt.iloc[pos]

    def _update_gap_marker(self, cfm: CandleFileManager, trade_type, symbol: str,
                           df_merged: pd.DataFrame, df_new: pd.DataFrame, source: str) -> None:
        """维护 {symbol}.gap：WS 落盘发现可补缺口 → 标记，本周期 REST 兜底补；
        REST 落盘后仍存在、且 REST 数据完整跨过的缺口，视为交易所无数据，记住不再重复请求。"""
        gap = self._latest_gap(df_merged)
        gap_end = gap[1] if gap is not None else None
        key = (trade_type, symbol)
        if source == "rest":
            if gap is not None:
                # REST 数据完整跨过缺口仍补不上 → 交易所无数据，立即认定；
                # 跨不过（接口抖动返回不全 / 缺口起点在 REST 窗口外）→ 重试，满 GAP_MAX_REST_ATTEMPTS 次再认定
                rest_cbt = df_new["candle_begin_time"]
                spanned = rest_cbt.min() <= gap[0] + self.base_delta and rest_cbt.max() >= gap_end
                last_end, n = self._gap_rest_attempts.get(key, (None, 0))
                n = n + 1 if last_end == gap_end else 1
                if spanned or n >= self.GAP_MAX_REST_ATTEMPTS:
                    prev = self._unfillable_gap_end.get(key)
                    self._unfillable_gap_end[key] = gap_end if prev is None else max(prev, gap_end)
                    self._gap_rest_attempts.pop(key, None)
                else:
                    self._gap_rest_attempts[key] = (gap_end, n)
            else:
                self._gap_rest_attempts.pop(key, None)
            # 总是清标记：REST 已尝试过，barrier 不应再等它
            cfm.clear_gap_marker(symbol)
        elif source == "ws":
            acked = self._unfillable_gap_end.get(key)
            if gap_end is not None and (acked is None or gap_end > acked):
                if not os.path.exists(cfm.format_gap_file_path(symbol)):
                    log.info("writer: 检测到断档 sym=%s/%s gap_end=%s，标记待 REST 补齐",
                             trade_type, symbol, gap_end)
                cfm.save_gap_marker(symbol)
            else:
                cfm.clear_gap_marker(symbol)

    def _pkl_path(self, base_dir: Path, symbol: str) -> Path:
        return base_dir / f"{symbol}.pkl"

    def _read_existing(self, base_dir: Path, symbol: str) -> Optional[pd.DataFrame]:
        p = self._pkl_path(base_dir, symbol)
        if not p.exists():
            return None
        try:
            return pd.read_pickle(p)
        except Exception as e:
            # 保留损坏文件以便取证（重命名，不删除），并发一条 critical 告警。
            # 下一次写入会从头重建 pkl。
            bak = p.with_suffix(p.suffix + f".corrupt.{int(time.time())}")
            try:
                p.rename(bak)
            except Exception:
                # 重命名失败则退化为 unlink，让 writer 能恢复。
                #（例如某些拒绝重命名的特殊文件系统 —— POSIX/macOS 上不该发生，
                # 这里只是防御。）
                try:
                    p.unlink()
                except Exception:
                    pass
            if self._alert is not None:
                try:
                    self._alert.critical(
                        f"writer.corrupt_pkl.{symbol}",
                        f"writer found corrupt pkl for {symbol}, preserved as {bak.name}: {e!r}",
                    )
                except Exception:  # pragma: no cover - defensive
                    pass
            return None

    def _merge(
        self, base_dir: Path, symbol: str, df_new: pd.DataFrame, source: str
    ) -> pd.DataFrame:
        df_new = df_new.copy()
        df_new["_source"] = source

        df_old = self._read_existing(base_dir, symbol)
        if df_old is not None and not df_old.empty:
            if "_source" not in df_old.columns:
                df_old = df_old.copy()
                df_old["_source"] = "rest"
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined["_prio"] = df_combined["_source"].map(_SOURCE_PRIO).fillna(0).astype(int)
        df_combined = (
            df_combined.sort_values(["candle_begin_time", "_prio"], kind="stable")
            .drop_duplicates(subset="candle_begin_time", keep="last")
            .drop(columns="_prio")
            .reset_index(drop=True)
        )
        # 把 flat pkl 裁剪到 num_base_candles 根最新行的滚动窗口。
        # 没有这步，文件会随 ws 不断推 K 而无限增长。
        if self.num_base_candles is not None and len(df_combined) > self.num_base_candles:
            df_combined = df_combined.tail(self.num_base_candles).reset_index(drop=True)
        return df_combined

    def _write_pkl(self, base_dir: Path, symbol: str, df: pd.DataFrame) -> None:
        """原子写：先写 <path>.tmp，再 os.replace → 最终路径。"""
        p = self._pkl_path(base_dir, symbol)
        tmp = p.with_suffix(p.suffix + ".tmp")
        df.to_pickle(tmp)
        os.replace(tmp, p)

    def _update_tsmanager(
        self,
        base_dir: Path,
        symbol: str,
        df_rows: pd.DataFrame,
        run_time: datetime,
    ) -> None:
        """把按 symbol 的 TSManager 月度分区与新行同步。

        df_rows 必须是【优先级已解析】的行（df_merged 的子集），不可传原始 df_new——
        否则 TSManager.update 的 last-write-wins 会让晚到的 rest 覆盖分区里的 ws 最终值，
        而 resample 读分区会冻结这个非最终 close。
        同时裁剪 partitions[0]，使历史分区保持有界，逻辑与 binance_update.py 里的
        `update_binance_base_candles` 一致。
        """
        symbol_dir = base_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        df_for_partition = df_rows
        if "_source" in df_for_partition.columns:
            df_for_partition = df_for_partition.drop(columns=["_source"])
        ts_mgr.update(df_for_partition)
        # 裁剪最早的分区窗口。仅当调用方通过 num_base_candles 提供了有限滚动窗口时有效。
        # 每个 symbol 至多每 PARTITION_TRIM_INTERVAL 裁一次：窗口每根 K 只滑 1 行，逐根裁剪
        # 意味着每次都整体读写最早那个月分区（约 0.6MB），占单次落盘读写量的 ~70%。
        # 少裁的 ≤1 天旧数据不影响读方：实时 resample 只从最新分区往前读所需根数，
        # 重启时历史校验会精确裁回窗口、resample_history 也按窗口起点过滤。
        if self.num_base_candles is not None:
            key = (base_dir, symbol)
            last = self._last_partition_trim.get(key)
            if last is None or run_time - last >= self.PARTITION_TRIM_INTERVAL:
                partitions = ts_mgr.list_partitions()
                if partitions:
                    required_first_begin = run_time - self.num_base_candles * self.base_delta
                    ts_mgr.trim_partition(partitions[0], required_first_begin, run_time)
                self._last_partition_trim[key] = run_time
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
