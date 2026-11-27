# `coin-realtime-data_v2.0.2` 通用性能优化回馈 PR 清单

本目录管理从 `alpha` 仓库提炼出的**完全通用、与下游消费端解耦**的性能优化 PR 草案，目标是安全回馈至上游基线 `coin-realtime-data_v2.0.2`（同时兼容 Binance 与 OKX 双交易所、兼容 **`2c4g` / `2c8g` / `4c8g` 多档硬件规格**，并兼容**强依赖 PKL 消费端**与 Arrow 消费端）。

每份 PR 文档均包含：**适用场景、量化目标、v2.0.2 现状与根因、项目文档分析与实测证据、硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限及自适应护栏、面向 v2.0.2 源码的逐文件改动方案与验收用例**。

---

## 1. PR 总览与合入顺序

### 第 1 批（零架构破坏、彼此独立，可按任意顺序或直接顺序合入）

| PR 编号 | 标题 | 核心改动文件（v2.0.2） | 实测数据支撑 | 硬件与消费端适用范围 | 前置依赖 | 文档 |
|---|---|---|---|---|---|---|
| **PR-01** | `CandleFileManager` `O(1)` `scandir` 索引缓存（消除 `glob` 风暴） | `core/storage/candle_manager.py` | 组合上线协助 barrier 压至 `0.13~2.37s`；带 2s TTL + `__getstate__` 后跨进程 `.ready` 零泄漏（稳定 14 文件） | **全规格通吃**（`2c4g`/`2c8g`/`4c8g`）；PKL/Arrow 均成立 | 无 | [PR-01](./PR-01-candle-manager-scandir-cache.md) |
| **PR-02** | Writer 轻量化 `kline` dict + `flat_pkl=240` 解耦 + 先发 `.ws_ready` + 全旧分区裁剪（核数自适应） | `core/ws/ws_runner.py`, `core/storage/writer.py`, `core/pipeline/runtime_supervisor.py`, `core/handle.py`, `config_*.py` | 实测 2-writer 排空占 barrier `8~11s`（`86 条/s`）；一期组合上线推动 `25m` `.ready` 由 **`+149.1s → +74.0s`（-50.4%）** | `num_writers` 须核数自适应（`2c=2`、`4c+=4`）；5m 直读策略需改读月分区 | 无 | [PR-02](./PR-02-writer-lightweight-flat-pkl-and-early-ws-ready.md) |
| **PR-03** | Barrier 竞态安全早退 + 可取消 14s REST 兜底 + 分级 `limit(5/99)` + 按需启动补齐 | `core/pipeline/barrier.py`, `core/pipeline/update_cycle.py`, `core/pipeline/data_update.py`, `core/handle.py`, `config_*.py` | 83 轮实测消除中位 **`290` 个假缺口**（42.5% 纯冗余，单轮省 REST `5.4s` + 写盘队列 `2~4s`）；重启阶段 2 降至 **0 REST 覆盖** | 机制全规格成立；`14s` 为 `4c8g` 标定，`2c` 机器建议配 PR-08 WAL 判缺口或调大阈值 | 无（建议在 PR-02 后合入） | [PR-03](./PR-03-barrier-early-exit-and-tiered-rest-fallback.md) |
| **PR-04** | `O(1)` 重叠增量重采样 + `flat_pkl` 优先读 + 核数自适应多线程 `resample_history` + `fill_gaps` 整型还原 | `core/pipeline/resample.py` | 离线合成 `resample` **`4.10s → 1.45s`（2.83×）**；线上由 `30.5s` 压至 `16~21s`（叠加 Compute-First 后内存合成 `5s`） | `resample_history` 线程须降为 `max(1, cpu//2)` 防 `2c4g` 重启 OOM；PKL 消费端完全成立 | 无（与 PR-02 的 `flat_pkl=240` 协同最佳） | [PR-04](./PR-04-incremental-resample-and-flat-pkl-first.md) |
| **PR-05** | 年度 Pivot 切片按内存门控直传 + PKL 原子写防护（**已撤回 `POSIX_FADV_DONTNEED`**） | `core/strategy/preprocess.py`, `core/storage/candle_manager.py` | 消除全部临时 Pivot pkl 写-读-删死 I/O；原子写根治 PKL 消费端慢读 `EOFError` | 内存 Pivot 仅 `>=6 GiB` 且单活跃年启用；**严禁在 PKL 体系启用 `fadvise` 逐出热缓存** | 无 | [PR-05](./PR-05-in-memory-pivot-and-page-cache-hygiene.md) |
| **PR-06** | `coin_cap` 日期缓存与空档冷却 + 按需 `aux_exe` + 非启用轮跳过实时资金费写盘 + `T-22s` 唤醒 | `core/ingest/coin_cap.py`, `core/pipeline/update_cycle.py`, `core/pipeline/data_update.py`, `core/exchange/adapter.py`, `core/app.py` | 线上非启用 5m 空档资金费落盘 **`0.000s`**；启用轮 `update_meta` 留足 **`11~15s`** 余量；`data_api=None` 时零 spawn | **全规格强正收益**（低配收益更高）；仅在外部脚本非启用轮查预测资金费时需关闭跳过 | 无 | [PR-06](./PR-06-idle-coin-cap-and-skip-idle-funding.md) |

### 第 2 批（架构级并发与零等待链路，存在明确前置依赖与规格门控）

| PR 编号 | 标题 | 核心改动文件（v2.0.2） | 实测数据支撑 | 硬件与消费端适用范围 | 前置依赖 | 文档 |
|---|---|---|---|---|---|---|
| **PR-07** | 单市场单 `ws_runner` 进程多协程 + 48h SQLite 清理 + `idx_kline_tt_time` 覆盖索引 | `core/ws/ws_runner.py`, `core/ws/ws_local_cache.py`, `core/pipeline/runtime_supervisor.py` | WS 常驻进程由 `6~8` 压至 **`2` 个**（省 `400~800 MB`）；`ws_cache` 恒定 **`86.4 MiB`**；零 `SCAN kline` | **`2c4g` 与 PKL 消费端救命级正收益**（把物理内存让给反序列化与 Page Cache） | 建议在 PR-02 后合入 | [PR-07](./PR-07-single-proc-multichunk-ws-and-cache-index.md) |
| **PR-08** | WAL 只读查缺口（全规格）+ WAL 直通放行与重采样叠加（`4c8g+` 门控，含连续性校验） | `core/pipeline/barrier.py`, `core/pipeline/update_cycle.py`, `core/pipeline/resample.py` | `barrier` 由 `13.0s / 16.0s` 降至 **`1.12s / 2.31s`**（正式机 `0.13s / 1.39s`），`missed` **`290 → 0/0`** | **分层启用**：WAL 判缺口全规格开启；提前放行重叠仅 `4c8g+` 开启，防 `2c4g` 抢核叠峰 | **强依赖 PR-07**（单市场单 SQLite + 索引） | [PR-08](./PR-08-wal-direct-barrier-and-resample-overlay.md) |
| **PR-09** | 周期内共享 `compute_pool`（按规格预热 + `.ready` 后立即释放）+ 内存门控非阻塞 `data_api` | `core/pipeline/runtime_supervisor.py`, `core/pipeline/update_cycle.py`, `core/pipeline/resample.py`, `core/pipeline/publication_sources.py`, `core/app.py` | 消灭每轮 8-spawn 内存尖峰；Windows 预热验证 **`3.96s → 2.3ms`**；空闲常驻回落 **`254.6 MB`** | `4c8g+` 全开；`2c8g` 降 2 worker；`2c4g` 退回线程池禁预热；发布后释放池对 PKL 消费端极关键 | 建议在 PR-04、PR-06 后合入 | [PR-09](./PR-09-shared-compute-pool-and-nonblocking-publish.md) |

---

## 2. 服务器规格适配矩阵（`4c8g` vs `2c8g` vs `2c4g`）

以下总结各 PR 在三档常见云服务器上的真实表现与推荐配置组合：

| PR 编号 | `4c8g`（4 核 8 GiB） | `2c8g`（2 核 8 GiB） | `2c4g`（2 核 4 GiB，可用 ~3.3 GiB） | 关键自适应护栏 |
|---|---|---|---|---|
| **PR-01** | 正收益 | 强正收益 | 强正收益 | 零风险，默认全开 |
| **PR-02** | 强正收益 | 须降 `num_writers=2` | 须降 `num_writers=2` | `num_writers = min(4, max(2, cpu_count))` |
| **PR-03** | 强正收益 | 正收益（注意 14s 阈值） | 正收益（须配 PR-08 判缺口） | 2c 机器未开 WAL 判缺口时建议放宽 `ws_wait_sec` |
| **PR-04** | 强正收益 | 须降每市场 1 线程 | **不降线程重启易 OOM** | `resample_history` 用 `max(1, cpu_count // 2)` 线程 |
| **PR-05** | 仅内存 Pivot 有效 | 仅内存 Pivot 有效 | 须关闭内存 Pivot 回退临时文件 | **全规格删除 `fadvise`**；内存 Pivot 仅 `RAM>=6 GiB` 且单活跃年启用 |
| **PR-06** | 强正收益 | 强正收益 | 强正收益 | 零风险，默认全开 |
| **PR-07** | 强正收益 | 强正收益 | **救命级正收益** | 压减 4~6 个常驻进程，直接省 `400~800 MB` |
| **PR-08** | 强正收益 | WAL 判缺口有效；抢跑重叠易抖动 | **仅开 WAL 判缺口，关闭提前放行** | `wal_direct_barrier` 默认仅在 `cpu>=4 && RAM>=6 GiB` 启用 |
| **PR-09** | 强正收益 | `compute_workers=2`，慎开非阻塞 `data_api` | **禁用多进程池/预热/非阻塞 `data_api`** | `RAM<6 GiB` 回退 `ThreadPoolExecutor(2)`，省 `300~500 MB` |

### 分档推荐落地方案
* **`2c4g` 最优安全组合**：PR-01 + PR-07（必选省内存）+ PR-06 + PR-02（`num_writers=2`）+ PR-03 + PR-04（每市场 1 线程）+ PR-05（仅原子写，不启内存 Pivot）+ PR-08（仅开 WAL 判缺口，不开提前放行重叠）。关闭 PR-09 的多进程预热与非阻塞 `data_api`。
* **`2c8g` 最优组合**：在 `2c4g` 基础上，开启 PR-05 的单年内存 Pivot 直传与 PR-09 的 `2-worker` 共享计算池；PR-08 提前放行需按云盘 IOPS 实测决定。
* **`4c8g+` 全速组合**：开启 PR-01 ~ PR-09 全部特性（PR-05 已撤回 `POSIX_FADV_DONTNEED`），可完整复现线上 `barrier < 2s`、`.ready` 提前 40–50s 的实测收益。

---

## 3. 强依赖 PKL 消费端（无 Arrow）的适用性与局限边界

`alpha` 下游（`alphaonline`）通过 Arrow IPC `memory_map` 实现跨进程零拷贝共享页缓存；而 `coin-realtime-data_v2.0.2` 的默认消费端强依赖 **`pd.read_pickle()`**（将磁盘字节流经 OS Page Cache 读入后，在消费进程私有堆中完整反序列化出新对象）。在这一前提下：

1. **PR-01 ~ PR-04、PR-06 ~ PR-09 全部成立，且两项内存优化对 PKL 消费端价值更高**：
   * **PR-07**（2 个 WS 进程）与 **PR-09**（`.ready` 后立刻 `release_compute_pool` 归还 1–2 GB 匿名堆页）将物理内存让给 OS Page Cache 和消费端 `pd.read_pickle()` 私有堆尖峰，直接压低同机 Swap/OOM 风险。
2. **PR-05 必须撤回 `POSIX_FADV_DONTNEED`，并替换为原子写防护**：
   * 在 `v2.0.2` 中，`1h_resample/*.pkl` 写完 1 秒后即被 `gen_batch_data` 读回，`*_dict_batch*.pkl` 写完后即被 PKL 消费端读取。调用 `POSIX_FADV_DONTNEED` 会人为破坏热页缓存，造成全市场冷盘随机读风暴（在 `2c4g` 弱云盘上尤其致命）。
   * 相反，给 `CandleFileManager.save_data_file` 补上 `.tmp` + `os.replace` 原子写（详见 PR-05），能根治 PKL 消费端慢读跨轮踩到半写文件报 `EOFError` 的问题。
3. **两项数据读取契约前提需核对**：
   * **PR-02（`flat_pkl=240`）**：要求消费端不直读 `base_5m/{symbol}.pkl` 获取超过 240 根（20h）的历史；若有 5m 级策略直读，需改读 `TSManager` 月分区或调大 `ws_flat_pkl_bars`。
   * **PR-06（非启用轮跳过实时资金费写盘）**：若存在外部脚本在非启用 5m 分钟轮询预测资金费，可通过配置开关关闭跳过。
4. **第 3 批扩展项在 PKL 消费端下的失效边界（未纳入本批 PR）**：
   * **剥离预计算因子不写 `batch*.pkl`**：在无 Arrow 的 PKL 体系下直接失效（下游只能被迫全量重算）；
   * **Compute-First 延迟 1h 刷盘（`defer_1h_flush`）**：仅当消费端只读 `preprocess` 产物时成立；若消费端（如 `BmacMultiFactorCalculator` 或 `preprocess=False`）直读 `1h_resample/{offset}/{symbol}.pkl`，延后刷盘会导致超时或读旧盘，禁止启用；
   * **Batch 内异步写盘（`defer_save=True`）**：无预计算因子与 Arrow 序列化可重叠时无净收益。

---

## 4. 依赖关系图（DAG）

```text
[独立基础层]
  PR-01 (CandleFileManager scandir 缓存)
  PR-02 (Writer 轻量化 + flat_pkl=240 + 核数自适应) ──┬──▶ PR-03 (Barrier 早退 + 可取消分级 REST)
                                                     ├──▶ PR-04 (增量 Resample + flat_pkl 优先 + 自适应线程)
                                                     └──▶ PR-07 (单市场单 ws_runner + 48h 清理 + SQLite 索引)
                                                            │
                                                            ▼
                                                          PR-08 (WAL 判缺口[全规格] + 直通放行叠加[4c8g+])
  PR-05 (内存门控 Pivot 直传 + PKL 原子写; 已撤 fadvise)
  PR-06 (coin_cap 空档化 + 按需 aux_exe + 跳过空档资金费) ──▶ PR-09 (规格自适应 compute_pool + 内存门控非阻塞发布)
                                                            ▲
                                                  (PR-04) ──┘
```

---

## 5. 多交易所兼容性声明（Binance & OKX）

`coin-realtime-data_v2.0.2` 原生支持 Binance 与 OKX（经 `core/exchange/adapter.py` 与 `core/exchange/protocol.py` 解耦）：

* **PR-01 / PR-02 / PR-04 / PR-05 / PR-07 / PR-08 / PR-09**：完全基于交易所无关的存储、协议与调度层，对 Binance 与 OKX **零分叉生效**。
* **PR-03**：`fetch_update_resample(..., symbol_limits=...)` 同时适用于 `BinanceFetcher` 与 `OkxFetcher`（均按 `params_list[i]['limit']` 传参）。
* **PR-06**：`ExchangeAdapter.update_meta(..., include_realtime_funding=True)` 默认实现委派给 `update_exchange_meta`，对 Binance 与 OKX 统一生效。

---

## 6. 明确不纳入本清单的项目与关键反证提醒

### 不纳入范围
1. **涉及存储/重启语义策略调整的第 3 批候选**：非破坏性 `init_dirs()` + `base_5m` 固定 30 天保留解耦（实测省 `11.0 GiB / -91.25%` 磁盘，但改变了 `v2.0.2` 保留全量 5m 历史语义）；增量 `gen_batch_data_incremental` + 2D NumPy `update_pivot_rows` + Compute-First 延后 1h 刷盘。
2. **`alpha` 专属下游契约优化**：预计算 C 扩展因子、Worker Arrow IPC 发布、`.arrow-demand` 协议、`has_active_consumer`（扫 `/proc`）及 `spot` 强制第 1 波抢跑。

### 评审必读反证与护栏
1. **切忌单独调小 `ws_wait_sec`（如 `10s → 3s`）**：无 WAL 状态判定时，调短 grace 会把 55–77% 的 swap 推给 REST 兜底，反而把写盘队列拖慢 5–6s（见 `alphaonline/docs/2026-11-24-grace-tuning-analysis.md`）。
2. **切忌在 `v2.0.2` PKL 管线中启用 `POSIX_FADV_DONTNEED`**：`1h_resample` 与 `batch*.pkl` 在写入后数秒内即会被重读，逐出页缓存属于明显负优化。
3. **必须保留两个已踩坑的正确性护栏**：
   * PR-01 中 `CandleFileManager` 的 `2.0s` TTL 与 `__getstate__` 清空（`18dadf77`，防跨进程 `.ready` 文件泄漏）；
   * PR-08 中 `_is_contiguous_closed_window()` 严格时间网格校验（`15205e78`，防重启跨根空洞被 48h `ws_cache` 行数掩盖）。
