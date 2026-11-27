"""
快照本地缓存与本地数据存在性判定 (纯本地, 不触网)。

本地优先恢复的两个关键判断都在这里:
- ``market_has_base_data``: 某市场的 base 5m 分区是否已存在 —— 在则跳过快照恢复
  (杜绝重启重拉), 缺失才进恢复流程。判定只看磁盘上 {symbol}/{YYYYMM}.pkl 是否存在,
  绝不构造 TSManager (其 __init__ 会 mkdir, 污染判定)。
- ``snapshot_cache`` 目录: 下载过的月度 parquet 长期保留在 {storage_path}/snapshot_cache/,
  与 base 5m 目录同级, 不随某市场 5m 子目录被删而消失, 供二次冷启动直接复用。

缓存目录结构 (镜像快照存储相对路径, 复用 snapshot_rel_path):
  {storage_path}/snapshot_cache/manifest.json
  {storage_path}/snapshot_cache/{exchange}/{market}_5m/{YYYYMM}.parquet
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from core.snapshot.snapshot_format import snapshot_rel_path

CACHE_DIR_NAME = "snapshot_cache"


def get_snapshot_cache_dir(storage_path) -> Path:
    """返回快照缓存根目录 {storage_path}/snapshot_cache/。"""
    return Path(storage_path) / CACHE_DIR_NAME


def market_has_base_data(base_candle_dir) -> bool:
    """
    判断某市场 base 5m 目录是否已有月分区数据 (任一 {symbol}/{YYYYMM}.pkl 存在即 True)。

    只看磁盘文件, 不构造 TSManager (避免其 mkdir 副作用)。月分区名严格为 6 位数字 YYYYMM,
    忽略扁平 {symbol}.pkl 滚动文件 —— 与 build_snapshot.discover_months 判定口径一致。
    """
    base = Path(base_candle_dir)
    if not base.is_dir():
        return False
    for symbol_dir in base.iterdir():
        if not symbol_dir.is_dir():
            continue
        for pkl in symbol_dir.glob("*.pkl"):
            stem = pkl.stem
            if len(stem) == 6 and stem.isdigit():
                return True
    return False


def cache_month_path(cache_dir, exchange: str, market: str, month: str) -> Path:
    """某 (exchange, market, month) 在缓存里的 parquet 路径。"""
    return Path(cache_dir) / snapshot_rel_path(exchange, market, month)


def read_cache_manifest(cache_dir) -> dict:
    """读取缓存 manifest.json; 不存在或损坏时返回空骨架 {'exchanges': {}}。"""
    p = Path(cache_dir) / "manifest.json"
    if not p.is_file():
        return {"exchanges": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data.setdefault("exchanges", {})
            return data
    except Exception:  # noqa: BLE001 — 缓存 manifest 损坏视为无缓存, 不抛
        pass
    return {"exchanges": {}}


def write_cache_manifest(cache_dir, manifest: dict) -> None:
    """把缓存 manifest 写到 {cache_dir}/manifest.json。"""
    cdir = Path(cache_dir)
    cdir.mkdir(parents=True, exist_ok=True)
    (cdir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def market_block(manifest: dict, exchange: str, market: str) -> dict | None:
    """取 manifest 里某 (exchange, market) 的块 {months, symbols, files}; 无则 None。"""
    return manifest.get("exchanges", {}).get(exchange, {}).get(f"{market}_5m")


def _to_utc_ts(value):
    """解析为 tz-aware UTC Timestamp (naive 视为 UTC), 保证两侧落后判定相减不会因 tz 不一致报错。"""
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")


def latest_data_end_time(block: dict | None, generated_at_fallback=None):
    """
    某市场块的「最新数据时间」: 取 files[*]['data_end_time'] 的最大值 (统一为 tz-aware UTC)。

    若各月均无 data_end_time (旧版 manifest), 回退到 manifest 顶层 generated_at;
    两者都拿不到时返回 None。结果恒为 tz-aware UTC, 供本地缓存 vs 线上的 >15 天落后判定。
    """
    if not block:
        return None
    ts_list = []
    for entry in (block.get("files", {}) or {}).values():
        det = (entry or {}).get("data_end_time")
        if det:
            try:
                ts_list.append(_to_utc_ts(det))
            except Exception:  # noqa: BLE001 — 单月时间戳解析失败忽略
                continue
    if ts_list:
        return max(ts_list)
    if generated_at_fallback:
        try:
            return _to_utc_ts(generated_at_fallback)
        except Exception:  # noqa: BLE001
            return None
    return None
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
