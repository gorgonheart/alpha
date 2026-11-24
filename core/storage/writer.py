"""
单 writer 进程：从 mp.Queue 消费 (symbol, df, source, run_time) item，按优先级合并后落盘。

对外接口：
    - Writer(base_candle_dirs, flat_pkl_max_bars=240, partition_retention_bars=8640, ...)
    - .handle_item(item: dict) -> None   （同步，可测；别名 .process_one）
    - .consume_loop(queue)               （在 multiprocessing.Process 内运行）

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
from core.storage.ts_manager import TSManager, get_partition


log = logging.getLogger("core.storage.writer")

_SOURCE_PRIO: Dict[str, int] = {
    "ws": 3,
    "rest": 2,
    "snapshot": 1,
}

_UNSET = object()


def _kline_dict_to_df(kline: dict) -> pd.DataFrame:
    """将轻量 WS kline 字典转换为标准 UTC ns 单行 DataFrame。"""
    begin_dt = pd.Timestamp(int(kline["begin_time_ms"]), unit="ms", tz="UTC").as_unit("ns")
    return pd.DataFrame({
        "candle_begin_time": pd.Series([begin_dt], dtype="datetime64[ns, UTC]"),
        "open": [float(kline["open"])],
        "high": [float(kline["high"])],
        "low": [float(kline["low"])],
        "close": [float(kline["close"])],
        "volume": [float(kline["volume"])],
        "quote_volume": [float(kline["quote_volume"])],
        "trade_num": pd.Series([int(kline["trade_num"])], dtype="int64"),
        "taker_buy_base_asset_volume": [float(kline["taker_buy_base"])],
        "taker_buy_quote_asset_volume": [float(kline["taker_buy_quote"])],
    })


def _normalize_candle_begin_time(df: pd.DataFrame) -> pd.DataFrame:
    """将 df["candle_begin_time"] 统一转为 tz-aware UTC 的 datetime64[ns, UTC]。

    兼容历史遗留 flat pkl 中的 tz-naive 与 ms / us / s 时间精度，避免合并排序时类型冲突。
    """
    if df.empty or "candle_begin_time" not in df.columns:
        return df
    cbt = df["candle_begin_time"]
    if str(cbt.dtype) == "datetime64[ns, UTC]":
        return df
    df = df.copy()
    ts = pd.to_datetime(cbt, utc=True)
    if hasattr(ts.dt, "as_unit"):
        ts = ts.dt.as_unit("ns")
    else:
        ts = ts.astype("datetime64[ns, UTC]")
    df["candle_begin_time"] = ts
    return df


class _WriterObservability:
    """Writer 的全部进度/可见性日志，与数据路径隔离。"""

    def __init__(self) -> None:
        self._first_logged_per_source: Dict[str, bool] = {}
        self._step_logged: Dict[tuple, bool] = {}

    def log_consumed(self, writer: "Writer", item: dict, consumed: int) -> None:
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
            n_rows = len(item["df"]) if "df" in item and item["df"] is not None else (1 if "kline" in item else 0)
            log.info(
                "writer: FIRST handled source=%s tt=%s sym=%s rows=%d -> "
                "pkl=%s (exists=%s size=%d) ready=%s (exists=%s)",
                src, tt, sym, n_rows,
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
            "writer.handle_item: FIRST item for (%s/%s) fully processed; "
            "subsequent items log every 50",
            trade_type, source,
        )
        self._step_logged[(trade_type, source)] = True


class Writer:
    """把多个提交合并到按 symbol 的 pkl 文件与 TSManager 月度分区上的单 writer。

    每个 flat pkl 携带一个内部 `_source` 列，使后来的提交能决定是否覆盖同一
    candle_begin_time 的行。没有 `_source` 的旧 pkl 视为 `rest`（优先级 2）。
    flat pkl 固定小窗（默认 240 根），月度分区保留窗口独立配置（默认 8640 根 = 30 天 5m）。
    """

    GAP_MAX_REST_ATTEMPTS = 2
    PARTITION_TRIM_INTERVAL = timedelta(days=1)

    def __init__(
        self,
        base_candle_dirs: Optional[Dict[str, Path]] = None,
        flat_pkl_max_bars: Optional[int] = 240,
        partition_retention_bars: Any = _UNSET,
        base_delta: timedelta = timedelta(minutes=5),
        alert: Optional[Any] = None,
        gap_check_bars: int = 99,
        *,
        base_candle_dir: Optional[Path] = None,
        num_base_candles: Optional[int] = None,
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
            self.base_candle_dirs = {None: Path(base_candle_dir)}  # type: ignore[dict-item]
            self._single_dir_mode = True
        else:
            raise ValueError(
                "Writer requires either base_candle_dir or base_candle_dirs"
            )

        self._cfms: Dict = {
            tt: CandleFileManager(d, "pickle")
            for tt, d in self.base_candle_dirs.items()
        }

        self.flat_pkl_max_bars: Optional[int] = flat_pkl_max_bars
        if partition_retention_bars is _UNSET:
            self.partition_retention_bars: Optional[int] = (
                num_base_candles if num_base_candles is not None else 8640
            )
        else:
            self.partition_retention_bars = partition_retention_bars
        # 向后兼容属性：映射到月度分区保留根数
        self.num_base_candles: Optional[int] = self.partition_retention_bars

        self.base_delta = base_delta
        self._alert = alert
        self._obs = _WriterObservability()
        self.gap_check_bars = gap_check_bars
        self._unfillable_gap_end: Dict[tuple, pd.Timestamp] = {}
        self._gap_rest_attempts: Dict[tuple, tuple] = {}
        self._last_partition_trim: Dict[tuple, datetime] = {}

    # ---- 公共同步入口（可测）----

    def consume_loop(self, queue) -> None:
        """从 `queue` 拉取 item，直到收到 None 哨兵。"""
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
                traceback.print_exc()

    def process_one(self, item: dict) -> None:
        """handle_item 的便捷别名。"""
        self.handle_item(item)

    def handle_item(self, item: dict) -> None:
        symbol = item["symbol"]
        source = item["source"]
        run_time: datetime = item["run_time"]
        trade_type = item.get("trade_type")

        if source not in _SOURCE_PRIO:
            raise ValueError(f"unknown source {source!r}, expected one of {list(_SOURCE_PRIO)}")
        if "df" in item:
            df_new = item["df"]
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
            df_new = _normalize_candle_begin_time(df_new)
        elif "kline" in item and item["kline"] is not None:
            df_new = _kline_dict_to_df(item["kline"])
        else:
            return

        base_dir, cfm = self._route(trade_type, symbol)
        verbose = self._obs.log_handle_start(symbol, trade_type, source, df_new)

        df_merged = self._merge(base_dir, symbol, df_new, source)
        if self.flat_pkl_max_bars is not None and len(df_merged) > self.flat_pkl_max_bars:
            df_flat = df_merged.tail(self.flat_pkl_max_bars).reset_index(drop=True)
        else:
            df_flat = df_merged
        self._obs.log_after_merge(verbose, symbol, df_flat)

        self._write_pkl(base_dir, symbol, df_flat)
        self._obs.log_after_pkl(verbose, symbol, self._pkl_path(base_dir, symbol))

        try:
            self._update_gap_marker(cfm, trade_type, symbol, df_merged, df_new, source)
        except Exception:
            log.exception(
                "writer: 更新断档标记失败 sym=%s/%s source=%s（不影响落盘）",
                trade_type, symbol, source,
            )

        # flat_pkl 与 .gap 均已原子落盘后立即发布 .ready / .ws_ready 放行下游屏障，
        # 不让 ~7000 行月度分区的读写阻塞重采样启动。
        cfm.save_ready(symbol, run_time)
        if source == "ws":
            cfm.save_ws_ready(symbol, run_time)
        self._obs.log_after_ready(verbose, symbol, trade_type, source, cfm, run_time)

        # 从未裁剪的 df_merged 中提取本批次触碰的行写入月度分区，避免 >flat_pkl_max_bars 的批量补齐被截断
        df_for_partition = df_merged[
            df_merged["candle_begin_time"].isin(df_new["candle_begin_time"])
        ]
        try:
            self._update_tsmanager(base_dir, symbol, df_for_partition, run_time)
            self._obs.log_after_ts(verbose, symbol, base_dir / symbol)
        except Exception:
            log.exception(
                "writer: 更新月度分区失败 sym=%s/%s source=%s（flat_pkl 与 ready 已落盘）",
                trade_type, symbol, source,
            )

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
        cbt = df["candle_begin_time"].iloc[-(self.gap_check_bars + 1):].reset_index(drop=True)
        if len(cbt) < 2:
            return None
        window_start = cbt.iloc[-1] - (self.gap_check_bars - 1) * self.base_delta
        is_gap_end = (cbt.diff() > self.base_delta) & (cbt >= window_start)
        if not is_gap_end.any():
            return None
        pos = is_gap_end[is_gap_end].index[-1]
        return cbt.iloc[pos - 1], cbt.iloc[pos]

    def _update_gap_marker(
        self,
        cfm: CandleFileManager,
        trade_type,
        symbol: str,
        df_merged: pd.DataFrame,
        df_new: pd.DataFrame,
        source: str,
    ) -> None:
        gap = self._latest_gap(df_merged)
        gap_end = gap[1] if gap is not None else None
        key = (trade_type, symbol)
        if source == "rest":
            if gap is not None:
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
            cfm.clear_gap_marker(symbol)
        elif source == "ws":
            acked = self._unfillable_gap_end.get(key)
            if gap_end is not None and (acked is None or gap_end > acked):
                if not os.path.exists(cfm.format_gap_file_path(symbol)):
                    log.info(
                        "writer: 检测到断档 sym=%s/%s gap_end=%s，标记待 REST 补齐",
                        trade_type, symbol, gap_end,
                    )
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
            bak = p.with_suffix(p.suffix + f".corrupt.{int(time.time())}")
            try:
                p.rename(bak)
            except Exception:
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
        df_new = _normalize_candle_begin_time(df_new).copy()
        df_new["_source"] = source

        df_old = self._read_existing(base_dir, symbol)
        if df_old is not None and not df_old.empty:
            df_old = _normalize_candle_begin_time(df_old)
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
        return df_combined

    def _write_pkl(self, base_dir: Path, symbol: str, df: pd.DataFrame) -> None:
        """原子写：先写 <path>.tmp，再 os.replace -> 最终路径。"""
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
        """把按 symbol 的 TSManager 月度分区与新行同步，并裁剪所有早于保留窗口的旧分区。"""
        symbol_dir = base_dir / symbol
        ts_mgr = TSManager(symbol_dir)
        df_for_partition = df_rows
        if "_source" in df_for_partition.columns:
            df_for_partition = df_for_partition.drop(columns=["_source"])
        ts_mgr.update(df_for_partition)

        if self.partition_retention_bars is not None:
            key = (base_dir, symbol)
            last = self._last_partition_trim.get(key)
            if last is None or run_time - last >= self.PARTITION_TRIM_INTERVAL:
                partitions = ts_mgr.list_partitions()
                if partitions:
                    required_first_begin = run_time - self.partition_retention_bars * self.base_delta
                    cutoff_part = get_partition(required_first_begin, ts_mgr.data_freq)
                    for part in partitions:
                        if part <= cutoff_part:
                            ts_mgr.trim_partition(part, required_first_begin, run_time)
                self._last_partition_trim[key] = run_time
