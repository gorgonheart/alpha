"""WS 本地缓存：ws_runner 进程内部的 SQLite WAL 缓冲。

WS 本地缓存语义：
    - 它是 ws_runner 进程内部的 SQLite 文件，只有 ws_runner 读写它。
    - 用途：故障兜底缓冲。当队列投递失败或 writer 进程挂掉时，WS 数据不丢。
    - 体量：保留最近 48h 闭合 K 线，由 ws_runner 启动后及每小时调用 prune_before 清理。
    - 不是主数据源。update_all 不读这个缓存。
    - 必须落盘且 fsync 安全（WAL）—— ws_runner 不能纯内存缓冲。

表结构：
    table kline(
        symbol TEXT, trade_type TEXT, begin_time_ms INTEGER,
        open REAL, high REAL, low REAL, close REAL,
        volume REAL, quote_volume REAL, trade_num INTEGER,
        taker_buy_base REAL, taker_buy_quote REAL,
        PRIMARY KEY (symbol, trade_type, begin_time_ms)
    )
    schema 版本记录在 SQLite 原生 PRAGMA user_version 里（不再用独立表）。

迁移策略：
    本缓存是临时崩溃恢复缓冲、非数据源。当 schema 版本（_SCHEMA_VERSION）变更时，
    打开旧库直接 DROP kline 重建，而不做迁移 —— 至多丢几根缓冲 K，下个周期 REST
    兜底自然补回。这样新增字段只需把 _SCHEMA_VERSION 加一，旧本地库自动失效重建。

PRAGMA：
    journal_mode = WAL
    synchronous  = NORMAL
    wal_autocheckpoint = 100
"""
from __future__ import annotations

import pathlib
import sqlite3
from typing import Iterable, Optional

import pandas as pd


_SCHEMA_VERSION = 1

_KLINE_COLUMNS = (
    "begin_time_ms",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base",
    "taker_buy_quote",
)


class WsLocalCache:
    """缓存从交易所 WS 拉到的闭合 K 线的 SQLite（WAL）缓冲。

    仅单进程、单线程使用（ws_runner）。该缓存能在 writer 进程崩溃后存活，因为每次
    写入都会 commit 到 fsync 落盘的 WAL 文件；下次 ws_runner 启动时，新实例可以重放
    比主 pkl/分区文件更新的那些行。
    """

    def __init__(self, db_path: pathlib.Path) -> None:
        self.db_path = pathlib.Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row

        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=100")

        self._init_schema()

    # ---- 表结构 ----

    def _init_schema(self) -> None:
        """建 kline 表；用 PRAGMA user_version 记 schema 版本，版本不匹配则丢弃重建。"""
        on_disk = self._conn.execute("PRAGMA user_version").fetchone()[0]
        with self._conn:
            self._conn.execute("DROP TABLE IF EXISTS schema_version")
            if on_disk != 0 and on_disk != _SCHEMA_VERSION:
                self._conn.execute("DROP TABLE IF EXISTS kline")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kline (
                    symbol TEXT NOT NULL,
                    trade_type TEXT NOT NULL,
                    begin_time_ms INTEGER NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    quote_volume REAL,
                    trade_num INTEGER,
                    taker_buy_base REAL,
                    taker_buy_quote REAL,
                    PRIMARY KEY (symbol, trade_type, begin_time_ms)
                )
                """
            )
            self._conn.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")

    # ---- 写入 ----

    def write(self, symbol: str, trade_type: str, kline: dict) -> None:
        """按 (symbol, trade_type, begin_time_ms) 主键 upsert 单根 K 线。

        同一主键以最后一次调用为准。
        """
        values = (
            symbol,
            trade_type,
            int(kline["begin_time_ms"]),
            float(kline["open"]),
            float(kline["high"]),
            float(kline["low"]),
            float(kline["close"]),
            float(kline["volume"]),
            float(kline["quote_volume"]),
            int(kline["trade_num"]),
            float(kline["taker_buy_base"]),
            float(kline["taker_buy_quote"]),
        )
        with self._conn:
            self._conn.execute(
                """
                INSERT INTO kline (
                    symbol, trade_type, begin_time_ms,
                    open, high, low, close,
                    volume, quote_volume, trade_num,
                    taker_buy_base, taker_buy_quote
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, trade_type, begin_time_ms) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    quote_volume=excluded.quote_volume,
                    trade_num=excluded.trade_num,
                    taker_buy_base=excluded.taker_buy_base,
                    taker_buy_quote=excluded.taker_buy_quote
                """,
                values,
            )

    # ---- 清理 ----

    def prune_before(self, cutoff_ms: int) -> int:
        """删除所有 begin_time_ms < cutoff_ms 的历史缓存行，防止 SQLite 无限膨胀。

        Returns:
            被删除的行数。
        """
        with self._conn:
            cursor = self._conn.execute(
                "DELETE FROM kline WHERE begin_time_ms < ?",
                (int(cutoff_ms),),
            )
            deleted = cursor.rowcount
        self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return deleted

    # ---- 读取 ----

    def replay_after(
        self, symbol: str, trade_type: str, begin_time_ms: int
    ) -> list[dict]:
        """返回严格晚于 `begin_time_ms` 的行，按时间从旧到新（ASC）。

        用于 ws_runner 重启后：新进程读取磁盘（pkl / 分区）上最新的时间戳，把缓存里
        超过这个水位线的行重放回 writer 队列。
        """
        cursor = self._conn.execute(
            """
            SELECT begin_time_ms, open, high, low, close,
                   volume, quote_volume, trade_num,
                   taker_buy_base, taker_buy_quote
              FROM kline
             WHERE symbol = ? AND trade_type = ? AND begin_time_ms > ?
             ORDER BY begin_time_ms ASC
            """,
            (symbol, trade_type, int(begin_time_ms)),
        )
        return [_row_to_dict(r) for r in cursor.fetchall()]

    # ---- 生命周期 ----

    def close(self) -> None:
        """把 WAL checkpoint 进主 db 文件，再关闭连接。

        TRUNCATE 模式确保干净关停时 WAL 文件被收缩到零 —— 这样下次重开无需重放 WAL。
        """
        try:
            self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            self._conn.close()


def _row_to_dict(row: sqlite3.Row) -> dict:
    """把 sqlite3.Row 转成按规范 K 线键顺序排列的普通 dict。"""
    return {col: row[col] for col in _KLINE_COLUMNS}


def _open_readonly_conn(db_path: pathlib.Path) -> Optional[sqlite3.Connection]:
    p = pathlib.Path(db_path)
    if not p.exists():
        return None
    uri = p.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2.0)
    return conn


def query_ws_cache_status(
    db_path: pathlib.Path,
    trade_type: str,
    target_begin_ms: int,
    base_delta_ms: int,
    check_bars: int = 99,
) -> tuple[set[str], set[str], set[str]]:
    """只读扫描 SQLite WAL 缓存，返回 `(ready_contiguous, full_window_contiguous, gapped_at_target)`。

    - `ready_contiguous`: 已收到 `target_begin_ms`，且无紧邻前序缺口（`check_bars <= 1` 或同时存在 `target - base_delta`）
    - `full_window_contiguous`: 在 `[target - (check_bars-1)*base_delta, target]` 内已攒满 `check_bars` 根闭合 K 线
    - `gapped_at_target`: 已收到 `target_begin_ms`，但缺失紧邻的上一根 `target - base_delta`（提示存在 WS 断连缺口）
    """
    ready_contig: set[str] = set()
    full_contig: set[str] = set()
    gapped_latest: set[str] = set()
    if db_path is None:
        return ready_contig, full_contig, gapped_latest
    try:
        conn = _open_readonly_conn(pathlib.Path(db_path))
    except Exception:
        return ready_contig, full_contig, gapped_latest
    if conn is None:
        return ready_contig, full_contig, gapped_latest
    try:
        target_ms = int(target_begin_ms)
        step_ms = max(1, int(base_delta_ms))
        n_check = max(1, int(check_bars))
        win_start_ms = target_ms - (n_check - 1) * step_ms
        prev_ms = target_ms - step_ms
        cur = conn.execute(
            """
            SELECT
                symbol,
                COUNT(*) AS n_win,
                SUM(CASE WHEN begin_time_ms = ? THEN 1 ELSE 0 END) AS has_target,
                SUM(CASE WHEN begin_time_ms = ? THEN 1 ELSE 0 END) AS has_prev
            FROM kline
            WHERE trade_type = ? AND begin_time_ms >= ? AND begin_time_ms <= ?
            GROUP BY symbol
            """,
            (target_ms, prev_ms, trade_type, win_start_ms, target_ms),
        )
        for sym, n_win, has_target, has_prev in cur.fetchall():
            if not has_target:
                continue
            if n_win >= n_check:
                full_contig.add(sym)
                ready_contig.add(sym)
            elif n_check <= 1 or has_prev:
                ready_contig.add(sym)
            else:
                gapped_latest.add(sym)
    except Exception:
        pass
    finally:
        conn.close()
    return ready_contig, full_contig, gapped_latest


def read_ws_cache_market_windows(
    db_path: pathlib.Path,
    trade_type: str,
    symbols: Optional[Iterable[str]],
    window_start_ms: int,
    end_exclusive_ms: int,
) -> dict[str, pd.DataFrame]:
    """从 SQLite WAL 批量只读提取 `[window_start_ms, end_exclusive_ms)` 内的闭合 5m K 线表字典。"""
    out: dict[str, pd.DataFrame] = {}
    if db_path is None:
        return out
    try:
        conn = _open_readonly_conn(pathlib.Path(db_path))
    except Exception:
        return out
    if conn is None:
        return out
    try:
        sym_filter = set(symbols) if symbols is not None else None
        if sym_filter is not None and not sym_filter:
            return out
        cur = conn.execute(
            """
            SELECT symbol, begin_time_ms, open, high, low, close,
                   volume, quote_volume, trade_num, taker_buy_base, taker_buy_quote
              FROM kline
             WHERE trade_type = ? AND begin_time_ms >= ? AND begin_time_ms < ?
             ORDER BY symbol ASC, begin_time_ms ASC
            """,
            (trade_type, int(window_start_ms), int(end_exclusive_ms)),
        )
        buckets: dict[str, list[tuple]] = {}
        for row in cur.fetchall():
            sym = row[0]
            if sym_filter is not None and sym not in sym_filter:
                continue
            buckets.setdefault(sym, []).append(row)

        for sym, rows in buckets.items():
            out[sym] = pd.DataFrame(
                {
                    "candle_begin_time": pd.to_datetime([r[1] for r in rows], unit="ms", utc=True),
                    "open": pd.Series([float(r[2]) for r in rows], dtype="float64"),
                    "high": pd.Series([float(r[3]) for r in rows], dtype="float64"),
                    "low": pd.Series([float(r[4]) for r in rows], dtype="float64"),
                    "close": pd.Series([float(r[5]) for r in rows], dtype="float64"),
                    "volume": pd.Series([float(r[6]) for r in rows], dtype="float64"),
                    "quote_volume": pd.Series([float(r[7]) for r in rows], dtype="float64"),
                    "trade_num": pd.Series([int(r[8]) for r in rows], dtype="int64"),
                    "taker_buy_base_asset_volume": pd.Series([float(r[9]) for r in rows], dtype="float64"),
                    "taker_buy_quote_asset_volume": pd.Series([float(r[10]) for r in rows], dtype="float64"),
                }
            )
    except Exception:
        pass
    finally:
        conn.close()
    return out
