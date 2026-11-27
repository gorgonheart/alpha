"""WS 本地缓存：ws_runner 进程内部的 SQLite WAL 缓冲。

WS 本地缓存语义：
    - 它是 ws_runner 进程内部的 SQLite 文件，只有 ws_runner 读写它。
    - 用途：故障兜底缓冲。当队列投递失败或 writer 进程挂掉时，WS 数据不丢。
    - 体量：约 24 根 K 线 × 1500 symbol ≈ 36K 行，约 10MB。
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
    """缓存从 binance WS 拉到的闭合 K 线的 SQLite（WAL）缓冲。

    仅单进程、单线程使用（ws_runner）。该缓存能在 writer 进程崩溃后存活，因为每次
    写入都会 commit 到 fsync 落盘的 WAL 文件；下次 ws_runner 启动时，新实例可以重放
    比主 pkl/分区文件更新的那些行。
    """

    def __init__(self, db_path: pathlib.Path) -> None:
        self.db_path = pathlib.Path(db_path)
        # 确保父目录存在，这样调用方可以传入嵌套路径。
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # isolation_level=None 即自动提交；在需要的地方用连接的上下文管理器开显式事务。
        self._conn = sqlite3.connect(str(self.db_path))
        self._conn.row_factory = sqlite3.Row

        # 按规范配置 PRAGMA。
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA wal_autocheckpoint=100")

        self._init_schema()

    # ---- 表结构 ----

    def _init_schema(self) -> None:
        """建 kline 表；用 PRAGMA user_version 记 schema 版本，版本不匹配则丢弃重建。"""
        # 全新库的 user_version 默认为 0。
        on_disk = self._conn.execute("PRAGMA user_version").fetchone()[0]
        with self._conn:
            # 旧版用独立的 schema_version 表记版本，这里顺手清掉。
            self._conn.execute("DROP TABLE IF EXISTS schema_version")
            # 版本不匹配（且非全新库）→ 丢弃旧 kline 重建（临时缓冲不做迁移）。
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
            # PRAGMA 不支持绑定参数；_SCHEMA_VERSION 是本模块的整数常量，可安全内插。
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
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
