"""
快照存储格式 (snapshot format) 的纯函数与常量定义。

数据中心侧将磁盘上已有的 base-5m 月分区 ({base}/{symbol}/{YYYYMM}.pkl) 打包为
可上传到快照存储的结构 (manifest + 按月聚合的 parquet)；数据消费侧再反向 ingest 回落盘。

本模块只包含「纯函数」与常量, 不做任何文件系统扫描 / 时间戳生成 / 网络 IO,
以便被测试无副作用地驱动。扫描 / CLI 落在 scripts/build_snapshot.py, ingest 落在
core/snapshot/ingest.py。

设计契约 (build 与 ingest 双方必须一致):
- 每个月度 parquet: 当月所有 symbol 合并; 列 = ['symbol'] + BASE_COLUMNS;
  按 (symbol, candle_begin_time) 排序; candle_begin_time 以 parquet 原生
  timestamp[us, tz=UTC] 写入, round-trip 回 pandas 须无损还原为
  datetime64[ns, UTC] (5m K 线均为整分钟, us 精度足以精确表达)。
- 单个 symbol 还原后须逐列等同其原始分区 pkl: 10 列顺序 / dtype / 值全等,
  其中 candle_begin_time 为 datetime64[ns, UTC], trade_num 为 int64。
"""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# 时间列名 (与 TSManager.time_key 默认值保持一致)
TIME_KEY = "candle_begin_time"

# 落盘分区 pkl 的 10 列业务列 (不含时间列以外的顺序约定见 BASE_COLUMNS)。
# BASE_COLUMNS 是分区 pkl 的「完整 10 列」, 顺序与磁盘文件严格一致。
BASE_COLUMNS = [
    "candle_begin_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]

# 每列在 round-trip 还原时须强制还原到的 dtype。candle_begin_time 用 tz-aware
# datetime64[ns, UTC]; trade_num 为 int64; 其余为 float64。
COLUMN_DTYPES = {
    "candle_begin_time": "datetime64[ns, UTC]",
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "float64",
    "quote_volume": "float64",
    "trade_num": "int64",
    "taker_buy_base_asset_volume": "float64",
    "taker_buy_quote_asset_volume": "float64",
}

# 月度 parquet / 内存 month_df 的完整列序: symbol 在最前, 后接 10 列业务列。
MONTH_DF_COLUMNS = ["symbol"] + BASE_COLUMNS

# 交易所/市场 → 源目录 (相对名)。源目录在磁盘上的根由调用方 (CLI) 给出:
# binance 系挂在 --data-root (默认 'data') 下, okx 系挂在 --okx-storage (默认 'data_okx') 下。
# 这里只记录 (exchange, market) → 相对目录名, 不绑定具体根。
SOURCE_DIRS = {
    ("binance", "spot"): "binance_spot_5m",
    ("binance", "swap"): "binance_swap_5m",
    ("okx", "spot"): "okx_spot_5m",
    ("okx", "swap"): "okx_swap_5m",
}

# (exchange, market) → 该源目录挂在哪个根: 'data_root' 或 'okx_storage'。
SOURCE_ROOT_KIND = {
    ("binance", "spot"): "data_root",
    ("binance", "swap"): "data_root",
    ("okx", "spot"): "okx_storage",
    ("okx", "swap"): "okx_storage",
}


def snapshot_rel_path(exchange: str, market: str, month: str) -> str:
    """
    返回某交易所/市场/月份在快照存储 (快照输出树) 中的相对路径。

    例: snapshot_rel_path('binance', 'spot', '202606') -> 'binance/spot_5m/202606.parquet'

    Args:
        exchange: 交易所名, 如 'binance' / 'okx'。
        market: 市场类型, 'spot' / 'swap'。
        month: 月份分区名, 格式 YYYYMM。

    Returns:
        str: 形如 '{exchange}/{market}_5m/{month}.parquet' 的相对路径。
    """
    return f"{exchange}/{market}_5m/{month}.parquet"


def _empty_month_df() -> pd.DataFrame:
    """构造一个空的、列序/列名正确的 month_df (无行)。"""
    return pd.DataFrame(columns=MONTH_DF_COLUMNS)


def partitions_to_month_df(base_dir: str | Path, month: str) -> pd.DataFrame:
    """
    读取 {base_dir} 下所有 symbol 在指定 month 的分区 pkl, 合并为一个 month_df。

    扫描 {base_dir}/{symbol}/{month}.pkl, 给每个 symbol 的 frame 加上 'symbol' 列,
    concat 后按 (symbol, candle_begin_time) 排序。列序固定为 ['symbol'] + BASE_COLUMNS。

    Args:
        base_dir: 某 (exchange, market) 的源根目录, 形如 'data/binance_spot_5m'。
        month: 月份分区名, 格式 YYYYMM。

    Returns:
        pd.DataFrame: 当月全部 symbol 合并后的 month_df。若无任何分区则返回空 month_df。
    """
    base = Path(base_dir)
    frames: list[pd.DataFrame] = []

    if base.is_dir():
        # 每个 symbol 是 base 下一个子目录, 其中 {month}.pkl 是该月分区。
        for symbol_dir in sorted(p for p in base.iterdir() if p.is_dir()):
            part = symbol_dir / f"{month}.pkl"
            if not part.is_file():
                continue
            df = pd.read_pickle(part)
            # 在最前插入 symbol 列, 保持后续业务列原样。
            df = df.copy()
            df.insert(0, "symbol", symbol_dir.name)
            frames.append(df)

    if not frames:
        return _empty_month_df()

    out = pd.concat(frames, ignore_index=True)
    out = out[MONTH_DF_COLUMNS]
    out = out.sort_values(["symbol", TIME_KEY], kind="mergesort").reset_index(drop=True)
    return out


def month_df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """
    将 month_df 序列化为 parquet 字节串, candle_begin_time 写为 timestamp[us, tz=UTC]。

    用显式 pyarrow schema 固定每列物理类型, 保证跨机/跨版本可复现, 且时间列以
    微秒精度 + UTC 时区写入 (5m K 线为整分钟, us 无损)。

    Args:
        df: month_df (列 = ['symbol'] + BASE_COLUMNS)。

    Returns:
        bytes: parquet 文件内容。
    """
    # 强制列序, 避免调用方传入乱序列。
    df = df[MONTH_DF_COLUMNS]

    fields = [pa.field("symbol", pa.string())]
    for col in BASE_COLUMNS:
        if col == TIME_KEY:
            fields.append(pa.field(col, pa.timestamp("us", tz="UTC")))
        elif col == "trade_num":
            fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.float64()))
    schema = pa.schema(fields)

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def parquet_bytes_to_month_df(b: bytes) -> pd.DataFrame:
    """
    将 parquet 字节串反序列化回 month_df, candle_begin_time 还原为 datetime64[ns, UTC]。

    Args:
        b: parquet 文件内容 (来自 month_df_to_parquet_bytes)。

    Returns:
        pd.DataFrame: month_df (列 = ['symbol'] + BASE_COLUMNS), 时间列为 tz-aware ns。
    """
    table = pq.read_table(io.BytesIO(b))
    # to_pandas: timestamp[us, tz=UTC] -> datetime64[ns, UTC] (pandas 默认 ns)。
    df = table.to_pandas()
    df = df[MONTH_DF_COLUMNS]
    # 显式归一化 dtype, 防御 pandas/pyarrow 版本差异。
    df[TIME_KEY] = df[TIME_KEY].astype("datetime64[ns, UTC]")
    return df


def month_parquet_to_symbol_frames(df_or_bytes) -> dict[str, pd.DataFrame]:
    """
    将一个月度 parquet (字节或已解析的 month_df) 拆回 {symbol: 分区 frame}。

    每个 symbol 的 frame 严格还原为原始分区 pkl 的形态:
    - 列 = BASE_COLUMNS (10 列), 顺序一致, 不含 'symbol' 列。
    - dtype 按 COLUMN_DTYPES 还原 (时间列 datetime64[ns, UTC], trade_num int64)。
    - 索引 reset 为 0..n-1 的 RangeIndex (注意: 原始落盘 pkl 的索引可能非 0 起,
      但其值无业务含义; TSManager.update 写回时同样会 reset, 故以 reset 为准)。

    Args:
        df_or_bytes: 月度 parquet 的 bytes, 或已解析的 month_df。

    Returns:
        dict[str, pd.DataFrame]: symbol → 分区 frame。
    """
    if isinstance(df_or_bytes, (bytes, bytearray)):
        df = parquet_bytes_to_month_df(bytes(df_or_bytes))
    else:
        df = df_or_bytes

    result: dict[str, pd.DataFrame] = {}
    for symbol, group in df.groupby("symbol", sort=True):
        frame = group.drop(columns=["symbol"]).reset_index(drop=True)
        frame = frame[BASE_COLUMNS]
        # 逐列还原 dtype (防御性: parquet round-trip 通常已正确, 此处兜底)。
        for col, dtype in COLUMN_DTYPES.items():
            if frame[col].dtype != dtype:
                frame[col] = frame[col].astype(dtype)
        result[str(symbol)] = frame
    return result
