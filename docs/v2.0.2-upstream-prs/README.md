# `coin-realtime-data_v2.0.2` 通用性能优化回馈 PR 清单

本目录管理从 `alpha` 仓库提炼出的**完全通用、与下游消费端解耦**的性能优化 PR 草案，目标是安全回馈至上游基线 `coin-realtime-data_v2.0.2`（同时兼容 Binance 与 OKX 双交易所）。

每份 PR 文档均包含：**适用场景、量化目标、v2.0.2 现状与根因、项目文档分析与实测证据、面向 v2.0.2 源码的逐文件改动方案与验收用例**。

---

## 1. PR 总览与合入顺序

### 第 1 批（零架构破坏、彼此独立，可按任意顺序或直接顺序合入）

| PR 编号 | 标题 | 核心改动文件（v2.0.2） | 实测数据支撑 | 前置依赖 | 文档 |
|---|---|---|---|---|---|
| **PR-01** | `CandleFileManager` `O(1)` `scandir` 索引缓存（消除 `glob` 风暴） | `core/storage/candle_manager.py` | 组合上线协助 barrier 压至 `0.13~2.37s`；带 2s TTL + `__getstate__` 后跨进程 `.ready` 零泄漏（稳定 14 文件） | 无 | [PR-01](./PR-01-candle-manager-scandir-cache.md) |
| **PR-02** | Writer 轻量化 `kline` dict + `flat_pkl=240` 解耦 + 先发 `.ws_ready` + 全旧分区裁剪 | `core/ws/ws_runner.py`, `core/storage/writer.py`, `core/pipeline/runtime_supervisor.py`, `core/handle.py`, `config_*.py` | 实测 2-writer 排空占 barrier `8~11s`（`86 条/s`）；一期组合上线推动 `25m` `.ready` 由 **`+149.1s → +74.0s`（-50.4%）** | 无 | [PR-02](./PR-02-writer-lightweight-flat-pkl-and-early-ws-ready.md) |
| **PR-03** | Barrier 竞态安全早退 + 可取消 14s REST 兜底 + 分级 `limit(5/99)` + 按需启动补齐 | `core/pipeline/barrier.py`, `core/pipeline/update_cycle.py`, `core/pipeline/data_update.py`, `core/handle.py`, `config_*.py` | 83 轮实测消除中位 **`290` 个假缺口**（42.5% 纯冗余，单轮省 REST `5.4s` + 写盘队列 `2~4s`）；重启阶段 2 降至 **0 REST 覆盖** | 无（建议在 PR-02 后合入） | [PR-03](./PR-03-barrier-early-exit-and-tiered-rest-fallback.md) |
| **PR-04** | `O(1)` 重叠增量重采样 + `flat_pkl` 优先读 + 多线程 `resample_history` + `fill_gaps` 整型还原 | `core/pipeline/resample.py` | 离线合成 `resample` **`4.10s → 1.45s`（2.83×）**；线上由 `30.5s` 压至 `16~21s`（叠加 Compute-First 后内存合成 `5s`） | 无（与 PR-02 的 `flat_pkl=240` 协同最佳） | [PR-04](./PR-04-incremental-resample-and-flat-pkl-first.md) |
| **PR-05** | Pivot 年度切片内存直传（消除临时 `*_for_pivot_batch*` pkl）+ `POSIX_FADV_DONTNEED` 页缓存治理 | `core/strategy/preprocess.py`, `core/storage/candle_manager.py`, `core/pipeline/resample.py` | 消除全部临时 Pivot pkl 写-读-删死 I/O；测试机轮内 `MemAvailable` 地板 `4,218 MiB`、落盘主缺页仅 `653 次/s` | 无 | [PR-05](./PR-05-in-memory-pivot-and-page-cache-hygiene.md) |
| **PR-06** | `coin_cap` 日期缓存与空档冷却 + 按需 `aux_exe` + 非启用轮跳过实时资金费写盘 + `T-22s` 唤醒 | `core/ingest/coin_cap.py`, `core/pipeline/update_cycle.py`, `core/pipeline/data_update.py`, `core/exchange/adapter.py`, `core/app.py` | 线上非启用 5m 空档资金费落盘 **`0.000s`**；启用轮 `update_meta` 留足 **`11~15s`** 余量；`data_api=None` 时省去无谓 spawn 子进程 | 无 | [PR-06](./PR-06-idle-coin-cap-and-skip-idle-funding.md) |

### 第 2 批（架构级并发与零等待链路，存在明确前置依赖）

| PR 编号 | 标题 | 核心改动文件（v2.0.2） | 实测数据支撑 | 前置依赖 | 文档 |
|---|---|---|---|---|---|
| **PR-07** | 单市场单 `ws_runner` 进程多协程 + 48h SQLite 清理 + `idx_kline_tt_time` 覆盖索引 | `core/ws/ws_runner.py`, `core/ws/ws_local_cache.py`, `core/pipeline/runtime_supervisor.py` | WS 常驻进程由 `6~8` 压至 **`2` 个**；`ws_cache` 全市场体积恒定 **`86.4 MiB`**；彻底消除 SQLite 全表 `SCAN kline` | 建议在 PR-02 后合入 | [PR-07](./PR-07-single-proc-multichunk-ws-and-cache-index.md) |
| **PR-08** | WAL 只读直通放行 Barrier & Resample 实时叠加（含严格时间连续性校验） | `core/pipeline/barrier.py`, `core/pipeline/update_cycle.py`, `core/pipeline/resample.py` | `barrier` 由 `13.0s / 16.0s`（p90 `21.2s`）骤降至 **`1.12s / 2.31s`**（正式机 `0.13s / 1.39s`），`missed` **`290 → 0/0`（11/11 轮全零）** | **强依赖 PR-07**（单市场单 SQLite + 索引） | [PR-08](./PR-08-wal-direct-barrier-and-resample-overlay.md) |
| **PR-09** | 周期内共享 `compute_pool`（`T-22s` 预热 + `.ready` 后立即释放）+ 非阻塞 `data_api` 发布 | `core/pipeline/runtime_supervisor.py`, `core/pipeline/update_cycle.py`, `core/pipeline/resample.py`, `core/pipeline/publication_sources.py`, `core/app.py` | 消灭每轮 8-spawn 内存尖峰；Windows 预热验证晚启动 **`3.96s → 2.3ms`**；空闲常驻回落至 **`254.6 MB`**（对比旧版峰值 `5.1 GB`） | 建议在 PR-04、PR-06 后合入 | [PR-09](./PR-09-shared-compute-pool-and-nonblocking-publish.md) |

---

## 2. 依赖关系图（DAG）

```text
[独立基础层]
  PR-01 (CandleFileManager scandir 缓存)
  PR-02 (Writer 轻量化 + flat_pkl=240 + 早发 .ws_ready) ──┬──▶ PR-03 (Barrier 早退 + 可取消 14s 分级 REST)
                                                          ├──▶ PR-04 (增量 Resample + flat_pkl 优先读 + 多线程)
                                                          └──▶ PR-07 (单市场单 ws_runner + 48h 清理 + SQLite 索引)
                                                                 │
                                                                 ▼
                                                               PR-08 (WAL 直通 Barrier + 连续性校验 Resample 叠加)
  PR-05 (Pivot 内存直传 + POSIX_FADV_DONTNEED)
  PR-06 (coin_cap 空档化 + 按需 aux_exe + 空档跳过资金费) ──────▶ PR-09 (共享 compute_pool 生命周期 + 非阻塞发布)
                                                                 ▲
                                                       (PR-04) ──┘
```

---

## 3. 多交易所兼容性声明（Binance & OKX）

`coin-realtime-data_v2.0.2` 原生支持 Binance 与 OKX（经 `core/exchange/adapter.py` 与 `core/exchange/protocol.py` 解耦）：

* **PR-01 / PR-02 / PR-04 / PR-05 / PR-07 / PR-08 / PR-09**：完全基于交易所无关的存储、协议与调度层，对 Binance 与 OKX **零分叉生效**。
* **PR-03**：`fetch_update_resample(..., symbol_limits=...)` 同时适用于 `BinanceFetcher` 与 `OkxFetcher`（均按 `params_list[i]['limit']` 传参）。
* **PR-06**：`ExchangeAdapter.update_meta(..., include_realtime_funding=True)` 默认实现委派给 `update_exchange_meta`，对 Binance 与 OKX 统一生效。

---

## 4. 明确不纳入本清单的项目（范围边界）

以下两类优化不在本目录 PR 范围内：

1. **涉及存储/重启语义策略调整的“第 3 批”候选**（需上游产品决策确认后再单独提 PR）：
   * 非破坏性 `init_dirs()` + `base_5m` 固定 30 天保留解耦（实测省 `11.0 GiB / -91.25%` 磁盘，但改变了 `v2.0.2` 保留全量 5m 历史的语义）；
   * 增量 `gen_batch_data_incremental` + 2D NumPy `update_pivot_rows` + Compute-First 延后 1h 刷盘（强依赖非破坏性保留历史 batch/pivot）。
2. **`alpha` 专属下游契约优化**（不回馈上游）：
   * 预计算 C 扩展因子（`动量V2` / `ILLQ` / `CciV3`）、Worker 内 Arrow IPC 序列化发布、`.arrow-demand` 协议；
   * `has_active_consumer`（扫描 `/proc` 匹配特定交易进程）及 `spot` 强制第 1 波抢跑。

---

## 5. 关键反证提醒（评审必读）

实测证明（见 `alphaonline/docs/2026-11-24-grace-tuning-analysis.md` 与 `docs/kb/performance-analysis.md` §5）：
* **切忌单独调小 `ws_wait_sec`（如从 10s 降到 3s）**：在未合入 PR-07/PR-08（WAL 直通）之前，调短 grace 会将 55–77% 的 swap 推给 REST 兜底，反而把写盘队列拖慢 5–6s。
* **必须保留两个已踩坑的正确性护栏**：
  1. PR-01 中 `CandleFileManager` 的 `2.0s` TTL 与 `__getstate__` 清空（对应提交 `18dadf77`，防跨进程 `.ready` 文件泄漏）；
  2. PR-08 中 `_is_contiguous_closed_window()` 严格时间网格校验（对应提交 `15205e78`，防重启跨根空洞被 48h `ws_cache` 行数掩盖）。
