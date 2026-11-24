# -*- coding: utf-8 -*-
"""Arrow 发布步骤（设计文档 §3.4）。

把一轮预处理产出的 ``{symbol: DataFrame}`` 快照写成单个 Arrow IPC 文件，供下游
``d:/alphaonline`` 消费者 mmap 零拷贝重建。布局：

- **每 symbol 一个 record batch**（顺序写入）。
- **manifest**：record batch index -> symbol，存于 schema 的 metadata（``b"symbols"``，
  JSON 数组），消费者据此把第 i 个 batch 还原成 ``market_dict[symbols[i]]``。
- **history_trim_date**（可选）：当 ``history_trim_date`` 非 None 时写入 schema metadata
  ``b"history_trim_date"``，供消费者诊断历史裁剪策略。

发布是**原子**的：写 ``*.arrow.tmp`` -> ``fsync`` -> ``os.replace`` 原子改名为
``*.arrow`` -> 写 ``*.ready`` 标志。消费者要么看到完整快照，要么看到上一轮，绝不会
读到半成品。

本模块是**纯加法**：失败不影响既有 pkl 路径，消费者可自动 fallback。
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa

_MANIFEST_KEY = b"symbols"
_HISTORY_TRIM_DATE_KEY = b"history_trim_date"


def publish_arrow_snapshot(
    market_dict: dict[str, pd.DataFrame],
    out_path,
    publish_kline_num: int = 5000,
    write_ready: bool = True,
    *,
    history_trim_date: Optional[str] = None,
) -> Optional[Path]:
    """原子发布一个 trade_type 的 Arrow 快照。

    :param market_dict: ``{symbol: DataFrame}``，各 symbol 列布局需一致。
    :param out_path:    目标 ``*.arrow`` 路径。
    :param publish_kline_num: 0 发布完整保留历史；正数仅发布末 N 行。
    :param write_ready: 是否同时写 ``.ready`` 标志。流水线中由上层统一管理单一 gate
        标志（并清理过期标志），此时传 False，仅做原子写 + 改名。
    :param history_trim_date: 可选的历史截断日期字符串（如 ``"2025-06-01"``），
        非 None 时写入 schema metadata ``b"history_trim_date"``。
    """
    if not market_dict:
        return None  # 没有数据可发布，不创建文件（消费者继续用上一轮）

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
    if history_trim_date is not None:
        metadata[_HISTORY_TRIM_DATE_KEY] = str(history_trim_date).encode("utf-8")
    schema_with_manifest = ref_schema.with_metadata(metadata)
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        with open(tmp_path, "wb") as fout:
            with pa.ipc.new_file(fout, schema_with_manifest) as writer:
                writer.write_batch(first)
                del first
                for symbol in symbols[1:]:
                    writer.write_batch(batch_for(symbol, ref_schema))
            fout.flush()
            os.fsync(fout.fileno())

        # 保留旧文件直到新文件完整落盘；同轮重跑也是原子替换。
        os.replace(tmp_path, out_path)
    finally:
        # 发布失败（如磁盘写满）也回收本轮半成品，不动已有完整快照。
        tmp_path.unlink(missing_ok=True)

    # ready 标志（与既有 CandleFileManager 约定一致：同名 .ready 旁车文件）
    if write_ready:
        ready_path = out_path.with_suffix(".ready")
        with open(ready_path, "w") as fready:
            fready.write(str(pd.Timestamp.now().timestamp()))

    return out_path


def serialize_market_dict_to_ipc_batches(
    market_dict: dict[str, pd.DataFrame],
    publish_kline_num: int = 5000,
) -> tuple[Optional[bytes], list[tuple[str, bytes]]]:
    """在 Worker 进程内将 `{symbol: DataFrame}` 裁切尾部并序列化为 IPC RecordBatch 字节流。"""
    if not market_dict:
        return None, []
    if publish_kline_num < 0:
        raise ValueError('publish_kline_num must be >= 0 (0 means full retained history)')

    symbols = list(market_dict.keys())

    def _to_batch(sym: str, schema=None) -> pa.RecordBatch:
        df = market_dict[sym]
        if publish_kline_num:
            df = df.tail(publish_kline_num)
        return pa.RecordBatch.from_pandas(df, schema=schema, preserve_index=False)

    first_rb = _to_batch(symbols[0])
    ref_schema = first_rb.schema
    schema_bytes = ref_schema.serialize().to_pybytes()
    items: list[tuple[str, bytes]] = [(symbols[0], first_rb.serialize().to_pybytes())]
    del first_rb
    for sym in symbols[1:]:
        rb = _to_batch(sym, ref_schema)
        items.append((sym, rb.serialize().to_pybytes()))
    return schema_bytes, items


def publish_arrow_ipc_batches(
    batch_chunks: list[tuple[bytes, list[tuple[str, bytes]]]],
    out_path,
    publish_kline_num: int = 5000,
    write_ready: bool = False,
    *,
    history_trim_date: Optional[str] = None,
) -> Optional[Path]:
    """将多个 Batch Worker 返回的预序列化 RecordBatch IPC 字节流零拷贝组装并原子落盘为单个 .arrow 文件。"""
    non_empty_chunks = [(sb, items) for sb, items in batch_chunks if sb is not None and items]
    if not non_empty_chunks:
        return None

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    symbols = [sym for _, items in non_empty_chunks for sym, _ in items]
    ref_schema = pa.ipc.read_schema(pa.py_buffer(non_empty_chunks[0][0]))
    metadata = dict(ref_schema.metadata or {})
    metadata[_MANIFEST_KEY] = json.dumps(symbols).encode("utf-8")
    metadata[b'full_history'] = b'1' if publish_kline_num == 0 else b'0'
    if history_trim_date is not None:
        metadata[_HISTORY_TRIM_DATE_KEY] = str(history_trim_date).encode("utf-8")
    schema_with_manifest = ref_schema.with_metadata(metadata)

    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    try:
        with open(tmp_path, "wb") as fout:
            with pa.ipc.new_file(fout, schema_with_manifest) as writer:
                for schema_bytes, items in non_empty_chunks:
                    chunk_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
                    same_schema = chunk_schema.equals(ref_schema)
                    for _, rb_bytes in items:
                        rb = pa.ipc.read_record_batch(pa.py_buffer(rb_bytes), chunk_schema)
                        if not same_schema:
                            rb = pa.Table.from_batches([rb]).cast(ref_schema).to_batches()[0]
                        writer.write_batch(rb)
            fout.flush()
            os.fsync(fout.fileno())
        os.replace(tmp_path, out_path)
    finally:
        tmp_path.unlink(missing_ok=True)

    if write_ready:
        ready_path = out_path.with_suffix(".ready")
        with open(ready_path, "w") as fready:
            fready.write(str(pd.Timestamp.now().timestamp()))

    return out_path


def cleanup_old_snapshots(base_dir, trade_type: str, keep: int = 1, *, current_epoch: int | None = None) -> None:
    """删除某 trade_type 过期的 ``{trade_type}_dict_{epoch}.arrow`` 快照（设计文档 Finding 2）。

    epoch 戳的快照只会累积（每轮新写一个文件，``.ready`` 标志被覆盖但旧 ``.arrow``
    不会被清理），在 8GB 服务器上约 1.2GB/轮/offset 泄漏。这里按 epoch 升序保留最新的
    ``keep`` 个，删除更旧的；同时清理崩溃残留的 ``*.arrow.tmp``。

    :param base_dir:   快照所在目录。
    :param trade_type: 仅清理该 trade_type（如 ``"spot"``），其他 trade_type 不受影响。
    :param keep:       保留轮数，默认 1，必须是正整数。
    :param current_epoch: 已成功发布的当前轮，补算旧轮时也必须保留，供同轮消费者读取。
    """
    if type(keep) is not int or keep < 1:
        raise ValueError('keep must be a positive integer')
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

    # 当前发布轮优先；补算时间早于磁盘残留轮次时，不能删掉刚发布的文件。
    snaps.sort(key=lambda p: (_epoch(p) == current_epoch, _epoch(p)))
    for old in snaps[:-keep]:
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
