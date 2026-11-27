"""
快照消费侧 (restore) —— 本地优先冷启动恢复。

核心行为 (本地优先, 杜绝重启重拉):
- 仅当某市场 base 5m 数据【缺失】时才恢复; 已存在则跳过 (走正常三段式增量下载)。
- 下载过的月度 parquet 长期保留在 {storage_path}/snapshot_cache/ (与 5m 目录同级,
  不随某市场 5m 子目录被删而消失), 供二次冷启动直接复用。
- 缺失且有缓存时: 比对线上 manifest 最新数据时间 vs 本地缓存最新数据时间, 落后超 15 天
  才刷新 (重下缓存最新月 + 之后新月; 老月不动); 否则直接 ingest 缓存。
- 缺失且无缓存时 (真首次): 按当前需求窗口选月, 逐月下载入缓存, 再 ingest。
- 下载、刷新和 ingest 均过滤窗口外月份; 旧缓存保留。

下载: manifest 走国内/国外双源容错 (附时间戳防 CDN 缓存); 每月数据先经鉴权端点
(coin-cap 式 data_api_uuid 参数 + api-key 头) 换取签名下载链接再下载; 逐月串行、
每次间隔 1s 节流; sha256 仅做完整性校验。

restore 是冷启动优化而非硬依赖: 整段被 try/except 兜底, 任何失败只记日志并返回,
冷启动继续走正常三段式下载, 绝不因 restore 崩溃。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
from pathlib import Path

from core.snapshot.cache import (
    cache_month_path,
    get_snapshot_cache_dir,
    latest_data_end_time,
    market_block,
    market_has_base_data,
    read_cache_manifest,
    write_cache_manifest,
)
from core.ingest.quantclass import QuantclassDataApi
from core.snapshot.ingest import ingest_month_parquet
from core.snapshot.snapshot_format import SOURCE_DIRS, snapshot_rel_path
from core.utils.log_kit import logger
from core.utils.network import async_retry_getter, create_aiohttp_session
from core.utils.time import convert_interval_to_timedelta, next_run_time

import pandas as pd


# 快照 HTTP 会话总超时 (秒)。aiohttp 的 timeout 是「总时长」语义 (不同于 urllib 的
# socket 空闲超时), 单月 parquet 可达数十 MB, 不能复用 handle.http_timeout_sec (默认
# 8s), 否则大文件下载必被掐断。
_SNAPSHOT_HTTP_TIMEOUT_SEC = 120

# 逐月下载串行节流: 每次下载后等待秒数 (满足「1s/次」限速)。
_DOWNLOAD_THROTTLE_SEC = 1.0


def partition_selector(
    available_months: list[str],
    required_start,
    run_time,
) -> list[str]:
    """选出与 [required_start, run_time) 相交的 UTC 月分区, 不向旧月份凑数量。

    naive 边界视为 UTC; 月初恰等于窗口终点或下月月初恰等于窗口起点时不相交。
    """
    required_start = pd.to_datetime(required_start, utc=True)
    run_time = pd.to_datetime(run_time, utc=True)
    if required_start >= run_time:
        return []

    selected = []
    for month in sorted(set(available_months)):
        month_start = pd.to_datetime(month, format="%Y%m", utc=True)
        month_end = month_start + pd.offsets.MonthBegin(1)
        if month_start < run_time and month_end > required_start:
            selected.append(month)
    return selected


def fetch_manifest(*, uuid: str = "", key: str = "", proxy=None) -> dict:
    """
    拉取线上快照 manifest (复用 QuantclassDataApi: 国内/国外双源容错 + 时间戳防 CDN 缓存,
    与产品列表 json 同机制), 外层再套 async_retry_getter 重试。失败向上抛 (调用方软退化兜底)。
    """

    async def _fetch():
        async with create_aiohttp_session(_SNAPSHOT_HTTP_TIMEOUT_SEC) as session:
            api = QuantclassDataApi(session, key, uuid, proxy)
            return await async_retry_getter(api.aioreq_snapshot_manifest)

    return asyncio.run(_fetch())


def fetch_month_bytes(
    exchange: str, market: str, month: str, *, uuid: str = "", key: str = "", proxy=None
) -> bytes:
    """
    取某月 parquet 字节并节流 1s (复用 QuantclassDataApi, 与 coin-cap 同链路):
    鉴权端点换签名下载链接 (uuid 参数 + api-key 头) → 从签名链接下载。
    """

    async def _fetch():
        async with create_aiohttp_session(_SNAPSHOT_HTTP_TIMEOUT_SEC) as session:
            api = QuantclassDataApi(session, key, uuid, proxy)
            url = await async_retry_getter(
                api.aioreq_snapshot_data_url, exchange=exchange, market=market, month=month
            )
            return await async_retry_getter(api.aioreq_snapshot_bytes, url=url)

    data = asyncio.run(_fetch())
    time.sleep(_DOWNLOAD_THROTTLE_SEC)  # 逐月串行 1s/次节流
    return data


def _read_month_bytes(source_dir, exchange, market, month, *, uuid, key, proxy) -> bytes:
    """取某月 parquet 字节: source_dir 给定时读本地 (测试旁路), 否则走 fetch_month_bytes 远端。"""
    if source_dir is not None:
        return (Path(source_dir) / snapshot_rel_path(exchange, market, month)).read_bytes()
    return fetch_month_bytes(exchange, market, month, uuid=uuid, key=key, proxy=proxy)


def _restore_one_market(
    *,
    exchange: str,
    market: str,
    online_manifest: dict,
    cache_dir: Path,
    required_start,
    run_time,
    base_candle_dir,
    source_dir,
    uuid: str,
    key: str,
    proxy,
) -> list[dict]:
    """
    恢复单个 (exchange, market): 按需求窗口选月 → 下载入缓存 → ingest 窗口内缓存。

    下载月集合 (均限定在需求窗口内):
    - 已有缓存且线上最新数据时间 − 缓存最新 > 15 天: 重下缓存最新月 + 之后新月 (老月不动)。
    - 已有缓存且未落后: 不下载, 直接 ingest 窗口内缓存。
    - 无缓存 (真首次): partition_selector 按需求窗口选月下载。
    单月下载/ingest 失败只跳过该月、记日志, 不打断其余。
    """
    market_key = f"{market}_5m"
    online_block = market_block(online_manifest, exchange, market)
    if not online_block:
        logger.warning(f"快照恢复: manifest 无 {exchange}/{market_key}, 跳过该市场")
        return []
    online_months = sorted(online_block.get("months", []))
    online_files = online_block.get("files", {}) or {}
    if not online_months:
        logger.warning(f"快照恢复: {exchange}/{market_key} 无可用月份, 跳过")
        return []

    cache_manifest = read_cache_manifest(cache_dir)
    cache_blk = market_block(cache_manifest, exchange, market) or {"months": [], "files": {}}
    cache_files: dict = dict(cache_blk.get("files", {}) or {})
    cached_months = sorted(cache_files.keys())

    # 决定下载哪些月
    if cached_months:
        cache_latest = latest_data_end_time(cache_blk, cache_manifest.get("generated_at"))
        online_latest = latest_data_end_time(online_block, online_manifest.get("generated_at"))
        # 本地缓存落后线上超过 15 天则刷新 (15 为内联阈值, 非配置项/非命名常量):
        # 重下缓存最新月 (它当初是半截当月、需补全) + 之后新出现的月; 更老的月保持不动。
        if (
            cache_latest is not None
            and online_latest is not None
            and online_latest - cache_latest > pd.Timedelta(days=15)
        ):
            cache_newest = cached_months[-1]
            months_to_dl = [m for m in online_months if m >= cache_newest]
            months_to_dl = partition_selector(months_to_dl, required_start, run_time)
            logger.info(
                f"快照恢复: {exchange}/{market_key} 缓存落后线上 >15 天 "
                f"(缓存={cache_latest}, 线上={online_latest}), 刷新月份={months_to_dl}"
            )
        else:
            months_to_dl = []
            logger.info(f"快照恢复: {exchange}/{market_key} 缓存新鲜 (≤15 天), 直接复用缓存")
    else:
        months_to_dl = partition_selector(online_months, required_start, run_time)

    # 下载选中月入缓存 (sha256 校验对照线上 manifest), 并更新缓存 manifest
    wrote_any = False
    for month in months_to_dl:
        entry = online_files.get(month) or {}
        try:
            b = _read_month_bytes(source_dir, exchange, market, month,
                                  uuid=uuid, key=key, proxy=proxy)
        except Exception as e:  # noqa: BLE001 — 取字节失败只跳过该月
            logger.error(f"快照恢复: 下载 {exchange}/{market_key}/{month} 失败, 跳过: {repr(e)}")
            continue
        expected = entry.get("sha256")
        if expected is not None and hashlib.sha256(b).hexdigest() != expected:
            logger.error(f"快照恢复: {exchange}/{market_key}/{month} 下载 sha256 不符, 跳过该月")
            continue
        dest = cache_month_path(cache_dir, exchange, market, month)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(b)
        cache_files[month] = entry  # 携带 sha256/rows/bytes/data_end_time
        wrote_any = True

    # 持久化缓存 manifest (镜像线上该市场块)
    cache_manifest.setdefault("exchanges", {}).setdefault(exchange, {})[market_key] = {
        "months": sorted(cache_files.keys()),
        "files": cache_files,
    }
    # 仅在真正下载了新数据时才把缓存 generated_at 更新为线上值; 纯复用 (零下载) 时保留缓存原值,
    # 以免缺 data_end_time 的旧 manifest 在 fallback 路径下被误判为"刚生成、永不刷新"。
    if wrote_any:
        cache_manifest["generated_at"] = online_manifest.get(
            "generated_at", cache_manifest.get("generated_at")
        )
    write_cache_manifest(cache_dir, cache_manifest)

    # 仅 ingest 窗口内的缓存月 (再次 sha256 完整性校验), 窗口外缓存不读不删。
    restored: list[dict] = []
    for month in partition_selector(list(cache_files), required_start, run_time):
        p = cache_month_path(cache_dir, exchange, market, month)
        if not p.is_file():
            continue
        b = p.read_bytes()
        expected = (cache_files.get(month) or {}).get("sha256")
        if expected is not None and hashlib.sha256(b).hexdigest() != expected:
            logger.error(f"快照恢复: 缓存 {exchange}/{market_key}/{month} sha256 不符, 跳过 ingest")
            continue
        try:
            summary = ingest_month_parquet(b, base_candle_dir)
        except Exception as e:  # noqa: BLE001 — 单月 ingest 失败只跳过该月
            logger.error(f"快照恢复: ingest {exchange}/{market_key}/{month} 失败, 跳过: {repr(e)}")
            continue
        logger.ok(
            f"快照恢复: {exchange}/{market_key}/{month} 完成, "
            f"symbols={summary['symbols']}, rows={summary['rows']}"
        )
        restored.append(
            {
                "exchange": exchange,
                "market": market,
                "month": month,
                "symbols": summary["symbols"],
                "rows": summary["rows"],
            }
        )

    return restored


def restore_from_snapshot(handle, cfg=None, *, source_dir=None) -> dict:
    """
    本地优先地从快照恢复各市场 base 5m 数据 (冷启动优化, 失败即软退化)。

    流程 (每市场独立):
    1. 门控: 该市场 base 5m 已有数据 → 跳过 (杜绝重启重拉); 缺失 → 进入恢复。
    2. 用当前基础周期边界与 handle.num_base_candles 固定本次需求窗口。
    3. 沿用缓存刷新策略, 仅下载和 ingest 与需求窗口相交的月份。

    Args:
        handle: BmacHandle (须含 exchange, base_interval, num_base_candles, proxy,
                get_market_handle)。
        cfg: 提供 data_api_uuid / data_api_key (coin-cap 式鉴权); 可为 None。
        source_dir: 本地快照根目录 (测试旁路, 含 manifest.json + 各 parquet); None 表示走远端。

    Returns:
        dict: {'restored': [{'exchange','market','month','symbols','rows'}, ...]}。
    """
    restored: list[dict] = []
    try:
        exchange = getattr(handle, "exchange", None) or "binance"
        uuid = getattr(cfg, "data_api_uuid", "") or "" if cfg is not None else ""
        key = getattr(cfg, "data_api_key", "") or "" if cfg is not None else ""
        proxy = getattr(handle, "proxy", None)

        # 门控: 仅恢复 base 5m 缺失的市场
        pending: list[tuple[str, Path]] = []
        for market in ("spot", "swap"):
            base_candle_dir = Path(handle.get_market_handle(market).base_candle_dir)
            if market_has_base_data(base_candle_dir):
                logger.ok(f"快照恢复: {exchange}/{market} 本地 5m 数据已存在, 跳过恢复 (走增量下载)")
                continue
            pending.append((market, base_candle_dir))

        if not pending:
            return {"restored": []}

        base_delta = convert_interval_to_timedelta(handle.base_interval)
        run_time = next_run_time(handle.base_interval) - base_delta
        required_start = run_time - base_delta * handle.num_base_candles

        # 缓存根: {storage_path}/snapshot_cache/ (base_candle_dir 的父目录即 storage_path)
        cache_dir = get_snapshot_cache_dir(pending[0][1].parent)

        # 线上 manifest: source_dir 给定时读本地 (测试旁路), 否则远端拉取
        if source_dir is not None:
            online_manifest = json.loads((Path(source_dir) / "manifest.json").read_text(encoding="utf-8"))
        else:
            online_manifest = fetch_manifest(uuid=uuid, key=key, proxy=proxy)

        for market, base_candle_dir in pending:
            restored.extend(
                _restore_one_market(
                    exchange=exchange,
                    market=market,
                    online_manifest=online_manifest,
                    cache_dir=cache_dir,
                    required_start=required_start,
                    run_time=run_time,
                    base_candle_dir=base_candle_dir,
                    source_dir=source_dir,
                    uuid=uuid,
                    key=key,
                    proxy=proxy,
                )
            )
    except Exception as e:  # noqa: BLE001 — restore 是优化, 任何失败软退化, 不崩冷启动
        logger.error(f"快照恢复失败 (软退化, 冷启动继续走正常下载): {repr(e)}")

    return {"restored": restored}


# SOURCE_DIRS 在此被引用以保持依赖显式 (映射表对外可见), 防止 lint 误删导入。
_ = SOURCE_DIRS
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
