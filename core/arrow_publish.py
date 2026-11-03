# -*- coding: utf-8 -*-
"""Arrow 发布步骤（设计文档 §3.4）。

把一轮预处理产出的 ``{symbol: DataFrame}`` 快照写成单个 Arrow IPC 文件，供下游
``d:/alphaonline`` 消费者 mmap 零拷贝重建。布局：

- **每 symbol 一个 record batch**（顺序写入）。
- **manifest**：record batch index -> symbol，存于 schema 的 metadata（``b"symbols"``，
  JSON 数组），消费者据此把第 i 个 batch 还原成 ``market_dict[symbols[i]]``。

发布是**原子**的：写 ``*.arrow.tmp`` -> ``fsync`` -> ``os.replace`` 原子改名为
``*.arrow`` -> 写 ``*.ready`` 标志。消费者要么看到完整快照，要么看到上一轮，绝不会
读到半成品。

本模块是**纯加法**：失败不影响既有 pkl 路径，消费者可自动 fallback。
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa

_MANIFEST_KEY = b"symbols"


def publish_arrow_snapshot(market_dict: dict[str, pd.DataFrame], out_path, publish_kline_num: int,
                           write_ready: bool = True) -> None:
    """原子发布一个 trade_type 的 Arrow 快照。

    :param market_dict: ``{symbol: DataFrame}``，各 symbol 列布局需一致。
    :param out_path:    目标 ``*.arrow`` 路径。
    :param publish_kline_num: 0 发布完整保留历史；正数仅发布末 N 行。
    :param write_ready: 是否同时写 ``.ready`` 标志。流水线中由上层统一管理单一 gate
        标志（并清理过期标志），此时传 False，仅做原子写 + 改名。
    """
    if not market_dict:
        return  # 没有数据可发布，不创建文件（消费者继续用上一轮）

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    symbols = list(market_dict.keys())

    if publish_kline_num < 0:
        raise ValueError('publish_kline_num must be >= 0 (0 means full retained history)')

    # Convert/write one symbol at a time: full publication must not retain a
    # second market-sized list of Arrow buffers alongside market_dict.
    def batch_for(symbol, schema=None):
        df = market_dict[symbol]
        if publish_kline_num:
            df = df.tail(publish_kline_num)
        return pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)

    first = batch_for(symbols[0])
    ref_schema = first.schema
    metadata = dict(ref_schema.metadata or {})
    metadata[_MANIFEST_KEY] = json.dumps(symbols).encode("utf-8")
    metadata[b'full_history'] = b'1' if publish_kline_num == 0 else b'0'
    schema_with_manifest = ref_schema.with_metadata(metadata)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(tmp_path, "wb") as fout:
        with pa.ipc.new_file(fout, schema_with_manifest) as writer:
            writer.write_batch(first)
            del first
            for symbol in symbols[1:]:
                writer.write_batch(batch_for(symbol, ref_schema))
        fout.flush()
        os.fsync(fout.fileno())

    # 原子改名：消费者要么看到完整快照，要么看到上一轮。
    os.replace(tmp_path, out_path)

    # ready 标志（与既有 CandleFileManager 约定一致：同名 .ready 旁车文件）
    if write_ready:
        ready_path = out_path.with_suffix(".ready")
        with open(ready_path, "w") as fready:
            fready.write(str(pd.Timestamp.now().timestamp()))


def cleanup_old_snapshots(base_dir, trade_type: str, keep: int = 2) -> None:
    """删除某 trade_type 过期的 ``{trade_type}_dict_{epoch}.arrow`` 快照（设计文档 Finding 2）。

    epoch 戳的快照只会累积（每轮新写一个文件，``.ready`` 标志被覆盖但旧 ``.arrow``
    不会被清理），在 8GB 服务器上约 1.2GB/轮/offset 泄漏。这里按 epoch 升序保留最新的
    ``keep`` 个，删除更旧的；同时清理崩溃残留的 ``*.arrow.tmp``。

    :param base_dir:   快照所在目录。
    :param trade_type: 仅清理该 trade_type（如 ``"spot"``），其他 trade_type 不受影响。
    :param keep:       保留最新的几个 epoch（默认 2，给可能正在 mmap 读上一轮的消费者留余量）。
    """
    base_dir = Path(base_dir)

    # 清理崩溃残留的半写文件
    for tmp in base_dir.glob(f"{trade_type}_dict_*.arrow.tmp"):
        tmp.unlink()

    snaps = list(base_dir.glob(f"{trade_type}_dict_*.arrow"))

    def _epoch(p: Path) -> int:
        # 文件名形如 {trade_type}_dict_{epoch}.arrow
        stem = p.name[len(f"{trade_type}_dict_"):-len(".arrow")]
        try:
            return int(stem)
        except ValueError:
            return -1

    snaps.sort(key=_epoch)  # 升序，最旧在前
    for old in snaps[:-keep] if keep > 0 else snaps:
        old.unlink()
        ready = old.with_suffix(".ready")
        if ready.exists():
            ready.unlink()


def read_arrow_snapshot(path) -> dict[str, pd.DataFrame]:
    """读取 :func:`publish_arrow_snapshot` 产出的快照，重建 ``{symbol: DataFrame}``。"""
    path = Path(path)
    with pa.memory_map(str(path), "r") as source:
        reader = pa.ipc.open_file(source)
        manifest = reader.schema.metadata.get(_MANIFEST_KEY)
        symbols = json.loads(manifest.decode("utf-8"))
        result: dict[str, pd.DataFrame] = {}
        for i in range(reader.num_record_batches):
            result[symbols[i]] = reader.get_batch(i).to_pandas()
    return result
