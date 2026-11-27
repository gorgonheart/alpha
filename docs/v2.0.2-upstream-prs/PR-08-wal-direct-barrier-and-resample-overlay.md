# PR-08: WAL 只读判缺口（全规格）与直通放行/重采样叠加（`4c8g+` 硬件门控，含连续性校验）

* **建议分支**：`perf/wal-direct-barrier-and-resample-overlay`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：**强依赖 PR-07**（单市场单 SQLite + `idx_kline_tt_time` 覆盖索引 + `query_ws_cache_status` / `read_ws_cache_market_windows`），建议同时已合入 PR-03、PR-04
* **参考源提交（`alpha`）**：`f44d7ee`、`29e50fc`、`15205e7`

---

## 1. 场景

在 5 分钟边界突发 1,287 币写入时，`ws_runner` 在 `+1~4s` 内即可将全部闭合 K 线 fsync 持久化到 SQLite WAL 缓存，而 Writer 排空磁盘队列（`flat_pkl` + 月度分区）仍需数秒至十余秒。

## 2. 目标（按硬件规格分层启用）

1. **Layer 1 — WAL 只读缺口判定（全规格 `2c4g`/`2c8g`/`4c8g` 无条件启用）**：`_compute_missed_and_gaps` 直接查询 `ws_cache/{trade_type}.sqlite`，只要目标闭合 K 线已入 WAL 且无紧邻断档，就不再误判为缺失而发起冗余 REST，从根本上解决 PR-03 在低配机上 `ws_wait_sec` 阈值漂移的问题。
2. **Layer 2 — WAL 直通放行 Barrier + Resample 实时叠加（仅 `4c8g+` 默认启用，可显式配置开关）**：在 CPU 与内存充裕的机器上，Barrier 免等 Writer 排空磁盘队列即刻放行，Resample 优先叠加 `ws_cache` 实时窗口与 Writer 后台落盘并行重叠；在 `2c4g` / `2c8g` 机器上默认等待磁盘 `.ws_ready` 排空后再重采样，避免 2 核 CPU/IOPS 互殴与内存叠峰。
3. **强制时间连续性防伪护栏（关键）**：新增 `_is_contiguous_closed_window()` 严格校验末尾 `once_update_candles` 根 K 线等步长连续且精确闭合在 `run_time - base_delta`，彻底防止重启跨根空洞被 48h `ws_cache` 行数掩盖而跳过已由 REST 补齐的 `flat_pkl`。

## 3. 问题（v2.0.2 现状与根因）

1. **判定信号错配**（`v2.0.2/core/pipeline/barrier.py:98`、`update_cycle.py:86`）：
   * Barrier 与 `_compute_missed_symbols` 均以磁盘 `.ws_ready`（“Writer 写盘完成”）作为“WS 是否收到”的判定信号，导致滞后币种被误判触发冗余 REST，且在 4 核机器上白白浪费 10~14s 等待磁盘排空。
2. **重采样读源滞后于 WS 接收**：
   * 若 Barrier 提前放行，`_load_closed_base_window` 只读磁盘 `flat_pkl`/月分区会漏掉 Writer 尚未刷盘的当轮最新闭合 K 线。

## 4. 项目文档中的分析、实测收益与踩坑修复

* **设计来源**：`alphaonline/docs/2026-11-24-grace-tuning-analysis.md`（§6.2 P1 建议）与 `docs/superpowers/plans/2026-11-25-producer-compute-io-overlap.md`（Task 1 & Task 2）。
* **线上强实测收益（`4c8g` 环境，`alphaonline/docs/2026-11-25-prod-vs-test-comparison.md` §3.2、`2026-11-27-prod-vs-test-and-resource-efficiency.md` §3）**：
  * 测试机（4c8g E5 v4）：`barrier` 中位数由 `13.0s / 16.0s`（一期 `6.51s / 13.21s`）骤降至 **`1.12s / 2.31s`**（最低 `0.40s`）；`missed` 假缺口从 83 轮中位 `290` 归零至 **`0/0`（11/11 轮全零）**；
  * 正式机（4c8g Xeon 8255C，11-27 切同版本）：同批次 `barrier[spot] = 0.13s`、`barrier[swap] = 1.39s`、`missed = 0/0`。
* **线上踩坑与根治（必须一并合入）**：
  * 2026-11-25 11:25 UTC 复现一次真实边界 Bug：进程重启在 48h `ws_cache` 中留下 1 根内部空洞（如缺 `12:10`），`run_startup_catchup` 用 REST 补齐了 `flat_pkl`（无 `.gap` 标记）；旧版仅判断 `len(ws_closed) >= once_update_candles`，因 48h 缓存有 500+ 行误判完整而跳过 `flat_pkl`，导致 `12:10` 成交量丢失。
  * 提交 `15205e78` 新增 `_is_contiguous_closed_window()` 逐根校验时间网格，根治该问题。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

### 5.1 为什么必须拆分 Layer 1 与 Layer 2（`2c4g`/`2c8g` 反效果分析）
* **Layer 1（WAL 判缺口）**：单次 SQLite 覆盖索引聚合仅耗时数毫秒、内存几乎为零，在 **`2c4g` / `2c8g` / `4c8g`** 上都是无条件强正收益，且彻底消除 2 核排空慢导致的假缺口 REST 风暴。
* **Layer 2（WAL 提前放行 Barrier + Resample 实时叠加）在 `2c4g` / `2c8g` 的反效果**：
  1. **2 核 CPU 与云盘 IOPS 互殴**：5m 边界 `+2s` 提前放行后，`2 ws_runner` + `2 Writer`（正反序列化/重写 1,287 个 7,000 行月分区）+ `Resample`（读 SQLite + 合并写 1,287 个 1h pkl）同时挤在 2 个 vCPU 与低突发 IOPS 云盘上，省掉的 barrier 等待会被转移并放大到 Resample/Preprocess 耗时中。
  2. **`2c4g` 内存叠峰**：`read_ws_cache_market_windows()` 一次将 `111 根 × 1,287 币 ≈ 14.3 万行` 构造为 1,287 个独立 DataFrame 驻留内存，与后台正在写盘的 Writer 叠峰极易压穿 4 GB 物理内存触发 Swap。
* **修正护栏**：引入硬件门控函数 `_enable_wal_direct_barrier(handle)`——当 `cfg.ws_wal_direct_barrier` 未显式指定时，仅在 **`cpu_count >= 4` 且总物理内存 `>= 6 GiB`** 时默认启用 Layer 2；`2c4g`/`2c8g` 默认仅启用 Layer 1（WAL 判缺口），让 Writer 先排空磁盘后再启动 Resample。

### 5.2 强依赖 PKL 消费端评估
* **完全成立**。本 PR 处于 `base_5m → 1h_resample` 内部管线，不改变 `1h_resample/*.pkl` 或 `preprocess` 产物格式，PKL 消费端零感知。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/pipeline/update_cycle.py` — 硬件门控与两层分流

```python
_MIN_WAL_DIRECT_MEM_BYTES = 6 * 1024 * 1024 * 1024  # 6 GiB

def _enable_wal_direct_barrier(handle: BmacHandle) -> bool:
    explicit = getattr(handle, "ws_wal_direct_barrier", None)
    if explicit is not None:
        return bool(explicit)
    if (os.cpu_count() or 1) < 4:
        return False
    try:
        import psutil
        return psutil.virtual_memory().total >= _MIN_WAL_DIRECT_MEM_BYTES
    except Exception:
        return False
```

* **Layer 1（始终开启）**：`_compute_missed_and_gaps()` 总是调用 `query_ws_cache_status()`，WAL 中已连续收齐的币种不计入 missed，有 `ws_gapped` 的币种并入 `gap_symbols`。
* **Layer 2（按 `_enable_wal_direct_barrier(handle)` 门控）**：仅在门控返回 `True` 时，`_barrier_and_resample()` 才向 `wait_for_ready_barrier()` 透传 `ws_cache_db`（提前放行）并在 `update_resample_from_base()` 中启用 `ws_cache` 叠加。

### 6.2 `core/pipeline/barrier.py`
* `wait_for_ready_barrier` 新增可选关键字参数 `ws_cache_db=None, trade_type=None, base_delta=timedelta(minutes=5), once_update_candles=99`：当 `ws_cache_db` 非空时启用 `ws_fast_ok` 提前放行，为 `None` 时自动退回纯磁盘 `.ws_ready` 等待。

### 6.3 `core/pipeline/resample.py`
1. 新增 `_overlay_ws_window(disk_df, ws_df)`（WS 优先 `keep='last'` 叠加去重）与 `_load_ws_windows_for_chunk()`（仅在开启 `use_ws_overlay=True` 时读取 WAL，低配机零额外内存开销）。
2. 新增 `_is_contiguous_closed_window(df, run_time, once_update_candles, base_delta)` 严格连续性校验：

```python
def _is_contiguous_closed_window(
    df: Optional[pd.DataFrame],
    run_time: datetime,
    once_update_candles: int,
    base_delta: timedelta,
) -> bool:
    if df is None or once_update_candles <= 0 or len(df) < once_update_candles:
        return False
    tail_cbt = df['candle_begin_time'].iloc[-once_update_candles:]
    if not tail_cbt.is_monotonic_increasing or tail_cbt.duplicated().any():
        return False
    step = pd.Timedelta(base_delta)
    ts_last = tail_cbt.iloc[-1]
    expected_last = pd.Timestamp(run_time) - step
    if ts_last.tzinfo is not None and expected_last.tzinfo is None:
        expected_last = expected_last.tz_localize(ts_last.tzinfo)
    elif ts_last.tzinfo is None and expected_last.tzinfo is not None:
        expected_last = expected_last.tz_convert('UTC').tz_localize(None)
    if ts_last != expected_last:
        return False
    expected_first = expected_last - (once_update_candles - 1) * step
    if tail_cbt.iloc[0] != expected_first:
        return False
    if once_update_candles > 1 and not (tail_cbt.diff().iloc[1:] == step).all():
        return False
    return True
```

3. `_load_closed_base_window` 优先级逻辑：
   * 若无 `.gap` 且 `_is_contiguous_closed_window(ws_closed, ...)` 为真 → 直接免读盘返回 `ws_closed`；
   * 否则读 `flat_pkl` 并 `_overlay_ws_window(df_flat, ws_closed)`，若严格连续 → 返回；
   * 否则回退月分区并执行三层叠加 `_overlay_ws_window(_overlay_ws_window(df_part, df_flat_closed), ws_closed)`。

## 7. 测试与验收清单

参考 `alpha/tests/test_compute_io_overlap_phase2.py` 并补充低配门控单测：
1. `test_ws_local_cache_readonly_status_and_window_frames`：验证 WAL 并发只读准确区分 `ready_contiguous` / `full_window_contiguous` / `gapped_at_target`。
2. `test_barrier_and_missed_check_release_immediately_via_ws_cache_when_writer_lags`：模拟磁盘滞后 1 根，验证开启 Layer 2 时 Barrier `<0.5s` 放行、`_compute_missed_and_gaps` 不误报正常币且抓出断档币。
3. `test_resample_overlays_ws_cache_when_writer_lags_behind_disk`：磁盘少最新 1 根而 `ws_cache` 已有最新 2 根，验证重采样输出与完整 Oracle `check_exact=True`。
4. `test_load_closed_base_window_rejects_gapped_ws_cache_and_heals_from_flat_pkl`：验证 48h `ws_cache` 缺中间 1 根时绝不跳过 `flat_pkl`（精准保留 REST 补齐行且维持 WS 权威），而真正连续的 `ws_cache` 仍走零读盘快路径。
5. 新增门控单测：模拟 `2c4g` / `2c8g` 环境验证 `_enable_wal_direct_barrier()` 返回 `False`（仍执行 Layer 1 防误报，跳过 Layer 2 抢跑叠加），`4c8g` 返回 `True`。
