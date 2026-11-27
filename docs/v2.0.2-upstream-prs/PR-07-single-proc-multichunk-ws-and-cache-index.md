# PR-07: 单市场单 `ws_runner` 进程多协程、48h SQLite 清理与覆盖索引

* **建议分支**：`perf/single-proc-multichunk-ws-and-cache-index`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：建议在 PR-02 之后合入（本 PR 是 **PR-08 WAL 判缺口与直通屏障** 的强前置依赖）
* **参考源提交（`alpha`）**：`ecade69`、`ffd9db9`、`18dadf7`

---

## 1. 场景

Binance 全市场约 520 spot + 767 swap（合计 ~1,287 币），按单连接上限 `max_streams_per_conn = 200` 切分需要 7 条 WS 连接（spot 3 条 + swap 4 条）；OKX 同样需要多条分片连接。在 `2c4g` / `2c8g` / `4c8g` 云服务器上长期 7×24 小时运行。

## 2. 目标

1. **WS 进程瘦身至固定 2 个（spot / swap 各 1 个）**：在单个 `ws_runner` 子进程的事件循环内并发运行多个 `WsKlineClient` chunk 协程，单市场共享 1 份 `ws_cache/{trade_type}.sqlite`，大幅降低常驻进程内存与 OS 上下文切换。
2. **chunk 级故障隔离**：单个 chunk 在初始 `start()` 握手失败或运行期断线时，独立按指数退避重连，绝不崩溃或阻塞同市场其他 chunk。
3. **防止 SQLite 本地缓存无限膨胀**：新增 `WsLocalCache.prune_before(cutoff_ms)`，在启动重放后及运行期每小时自动清理 48 小时之前的过期行并执行 `PRAGMA wal_checkpoint(PASSIVE)`。
4. **为 SQLite 增加覆盖索引与高效批量查询**：新增 `idx_kline_tt_time(trade_type, begin_time_ms, symbol)` 覆盖索引，以及走主键 B-Tree 范围查找（`WHERE symbol IN (...)`）的 `read_ws_cache_market_windows()` 与 `query_ws_cache_status()`（配合 NumPy 列构造），彻底杜绝全表 `SCAN kline`，为 PR-08 铺平道路。

## 3. 问题（v2.0.2 现状与根因）

1. **多进程膨胀（暗雷二）**（`v2.0.2/core/pipeline/runtime_supervisor.py:259-319`）：
   * `_build_ws_runner_specs()` 对每个 200-stream chunk `spawn` 一个独立 Python 进程（导入 `pandas`/`numpy`/`aiohttp`），Binance 需要 `3 + 4 = 7` 个 `ws_runner` 进程，加上 4 个 `writer` + 1 个 `main` = **12 个常驻 Python 进程**，光常驻内存就吃掉 `~0.9–1.4 GiB`。
   * 同市场数据被拆散到 `spot-0.sqlite`、`spot-1.sqlite`、`spot-2.sqlite`、`swap-0..3.sqlite` 等多个分片库，外部无法统一高效查询。
2. **SQLite 缓存只写不删**（`v2.0.2/core/ws/ws_local_cache.py`）：
   * 仅有 `write()` 和 `replay_after()`，完全没有过期清理逻辑，长跑数周后数据库文件与 WAL 会持续膨胀。
3. **缺少按时间窗口查询索引**：
   * `kline` 表主键为 `(symbol, trade_type, begin_time_ms)`，按 `(trade_type, begin_time_ms)` 查询全市场最新闭合状态时被迫全表扫描。

## 4. 项目文档中的分析与实测证据

* **暗雷二评估**：`alphaonline/docs/2026-11-23-phase5-and-v202-migration-plan.md`（§阶段 2B-1 暗雷二）测算原版 10–12 个常驻进程占用 `~0.9–1.4 GiB` RAM，收拢至单市场 1 进程后大幅降低常驻水位。
* **线上长期运行实测**：
  * `alphaonline/docs/2026-11-25-prod-vs-test-comparison.md`（§5.1）精确字节统计：启用 48h 定时清理后，全市场 `ws_cache` 目录总大小恒定在 **`86.4 MiB`**（`spot.sqlite 32.7 MiB` + `swap.sqlite 48.7 MiB` + WAL/shm）。
  * `alphaonline/docs/2026-11-27-prod-vs-test-and-resource-efficiency.md`（§7.1）：WS + Writer 采集子进程常驻内存仅 **`29–71 MB`/进程**。
  * `EXPLAIN QUERY PLAN` 回归测试（`18dadf7`）验证：升级已有 v1 数据库时零数据丢失自动创建 `idx_kline_tt_time`，状态查询 100% 走覆盖索引（零 `SCAN kline`），子集窗口查询 100% 走主键 `SEARCH kline USING`。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

* **对 `2c4g` 属于“救命级”正收益**：
  * 原版 7 个 `ws_runner` 进程在 4 GB 机器上直接吃掉可用物理内存的 25%~35%。本 PR 砍掉 5 个常驻 Python 进程，直接腾出 **`400–800 MB` 物理内存**，并显著降低 2 核上的调度切换成本，是 `2c4g` 能否稳定运行 WS 模式的前提。
* **对强依赖 PKL 消费端价值进一步放大**：
  * PKL 消费端（`pd.read_pickle()`）无法像 Arrow mmap 那样零拷贝共享页缓存，会在消费进程私有堆膨胀 1.5–3 GB。生产端通过本 PR 压低常驻 RSS，能把更多物理内存留给 OS Page Cache 与消费端反序列化，直接降低同机 Swap/OOM 概率。
* **兼容性提醒**：原版 `v2.0.2` 若已有历史 `ws_cache/{spot,swap}-0.sqlite` 等分片文件，本 PR 启用新路径 `ws_cache/{spot,swap}.sqlite` 后，历史分片文件仅做冗余残留（不再读写，可在维护窗口手动清理）。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/ws/ws_local_cache.py`
1. `_init_schema()` 在建表后增加幂等覆盖索引（兼容已有 v1 库无缝升级）：
   ```sql
   CREATE INDEX IF NOT EXISTS idx_kline_tt_time
   ON kline (trade_type, begin_time_ms, symbol)
   ```
2. 新增 `prune_before(self, cutoff_ms: int) -> int`：
   ```python
   def prune_before(self, cutoff_ms: int) -> int:
       with self._conn:
           cursor = self._conn.execute(
               "DELETE FROM kline WHERE begin_time_ms < ?",
               (int(cutoff_ms),),
           )
           deleted = cursor.rowcount
       self._conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
       return deleted
   ```
3. 新增模块级只读 WAL 查询函数（供 PR-08 使用）：
   * `_open_readonly_conn(db_path)`：使用 `sqlite3.connect(p.resolve().as_uri() + "?mode=ro", uri=True, timeout=2.0)` 零写锁并发读。
   * `query_ws_cache_status(db_path, trade_type, target_begin_ms, base_delta_ms, check_bars=99)`：利用 `idx_kline_tt_time` 覆盖索引单次 SQL 返回 `(ready_contiguous, full_window_contiguous, gapped_at_target)`。
   * `read_ws_cache_market_windows(db_path, trade_type, symbols, window_start_ms, end_exclusive_ms)`：当传入 `symbols` 时按 450 个一组走主键 `WHERE symbol IN (...) AND trade_type = ? AND begin_time_ms >= ? AND begin_time_ms < ?`，并用 `zip(*rows)` + `np.asarray(..., dtype=...)` 批量构造 DataFrame。

### 6.2 `core/ws/ws_runner.py`
1. `WsRunner` 由管理单个 `WsKlineClient` 升级为管理 `self._chunks: list[tuple[int, Any, list[str]]]`（按 `self._protocol.max_streams_per_conn` 自动切片，兼容 Binance 与 OKX）。
2. `start(symbols, replay_on_start=True)`：
   * 空 `symbols=[]` 安全返回不挂起；
   * 逐 chunk 创建 `WsKlineClient`（URL 为 `self._protocol.ws_url(self.trade_type, chunk_idx)`），单 chunk 初始 `connect()/subscribe()` 失败捕获异常记入 `self._pending_connect_chunks`，绝不中断其他 chunk；
   * 先全部连接订阅、再 `replay_cache(all_symbols)`、再执行一次 `self._prune_cache()`。
3. `run()`：使用 `asyncio.gather()` 并发运行所有 chunk 的 `_run_chunk_loop()`（各自独立指数退避重连，上限 30s）与定时 `_prune_loop()`（默认每小时清理 48h 前过期行）。
4. `stop()`：优雅关闭所有 chunk client 并关闭共享 SQLite。

### 6.3 `core/pipeline/runtime_supervisor.py`
* `_build_ws_runner_specs(trade_type)` 由每 chunk 返回 1 个 `WsRunnerSpec` 改为每市场返回 **1 个**包含全量 `symbols` 的 `WsRunnerSpec`，`cache_path = self.ws_cache_dir / f"{trade_type}.sqlite"`，进程命名为 `bmac-{exchange}-ws-{trade_type}`。

## 7. 测试与验收清单

参考 `alpha/tests/test_2b_ws_supervisor.py` 与 `tests/test_compute_io_overlap_phase2.py`：
1. `test_ws_local_cache_prune_before_deletes_old_rows_only`：验证 48h 清理精确删除旧行、保留新行。
2. `test_ws_runner_multichunk_single_process_isolates_chunk_failure_and_empty_symbols`：450 symbols 自动拆 3 chunk（200+200+50），chunk-1 断线不中断 chunk-0；空 symbols 启动/运行/退出不挂起。
3. `test_ws_runner_isolates_initial_connect_failure_per_chunk_and_recovers_in_run`：chunk-1 在 `start()` 握手失败不影响 chunk-0/2，并在 `run()` 循环内自动退避恢复。
4. `test_ws_runner_prunes_stale_cache_on_start_and_periodically`：验证启动与运行期定时清理生效。
5. `test_ws_cache_index_and_symbol_subset_query_avoid_full_table_scan`：验证旧 v1 库零丢数升级建索引，且 `EXPLAIN QUERY PLAN` 断言零 `SCAN kline`。
6. `test_runtime_supervisor_spawns_one_ws_spec_per_market_and_two_writers`：验证每市场仅生成 1 个 WS 进程 spec 与单库路径 `{trade_type}.sqlite`。
