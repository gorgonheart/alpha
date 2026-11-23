"""
快照 ingest: 将一个月度 parquet 反向落盘为 {base_dir}/{symbol}/{YYYYMM}.pkl。

消费侧冷启动时, writer 尚未运行, 故直接用 TSManager(...).update(df) 写文件。
TSManager 内部会读已有分区 + 按 candle_begin_time 去重 (keep last) + 排序 + 原子写,
与运行时落盘格式完全一致。
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.snapshot.snapshot_format import month_parquet_to_symbol_frames
from core.storage.ts_manager import DataFrequency, SaveType, TSManager


def ingest_month_parquet(parquet_path_or_bytes, base_dir: str | Path) -> dict:
    """
    将一个月度 parquet ingest 到 {base_dir} 下, 每个 symbol 写一个月分区 pkl。

    对每个 symbol, 用 TSManager(data_dir={base_dir}/{symbol}, monthly, pickle).update(frame)
    落盘, 与运行时格式一致 (月分区 + 去重 + 排序 + 原子写)。

    Args:
        parquet_path_or_bytes: 月度 parquet 的文件路径 (str | Path) 或字节内容 (bytes)。
        base_dir: 目标源根目录, 形如 '<tmp>/binance_spot_5m'。

    Returns:
        dict: {'symbols': <写入的 symbol 数>, 'rows': <写入的总行数>}。
    """
    if isinstance(parquet_path_or_bytes, (bytes, bytearray)):
        payload: object = bytes(parquet_path_or_bytes)
    else:
        payload = Path(parquet_path_or_bytes).read_bytes()

    symbol_frames = month_parquet_to_symbol_frames(payload)

    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    for symbol, frame in symbol_frames.items():
        mgr = TSManager(
            data_dir=base / symbol,
            data_freq=DataFrequency.monthly,
            save_type=SaveType.pickle,
        )
        mgr.update(frame)
        total_rows += len(frame)

    return {"symbols": len(symbol_frames), "rows": total_rows}
