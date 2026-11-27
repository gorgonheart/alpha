# PR-08: WAL 只读直通放行 Barrier 与 `ws_cache` 连续性校验叠加重采样

* **建议分支**：`perf/wal-direct-barrier-and-resample-overlay`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：**强依赖 PR-07**（单市场单 SQLite + `idx_kline_tt_time` 覆盖索引 + `query_ws_cache_status` / `read_ws_cache_market_windows`），建议同时已合入 PR-03、PR-04
* **参考源提交（`alpha`）**：`f44d7ee`、`29e50fc`、`15205e7`

---

## 1. 场景

即使将 Writer 扩至 4 进程并轻量化载荷（PR-02），在 5 分钟边界突发 1,287 币写入时，磁盘 I/O 队列仍存在数秒至十余秒的排空延迟；而此时 `ws_runner` 早已在 `+1~4s` 内将全部闭合 K 线 fsync 持久化到了 SQLite WAL 缓存中。

## 2. 目标

1. **Barrier 毫秒级放行**：`wait_for_ready_barrier` 与 `_compute_missed_and_gaps` 直接只读查询 `ws_cache/{trade_type}.sqlite`（SQLite WAL 支持无锁并发读），只要目标闭合 K 线已入 WAL 且无紧邻断档，**免等 Writer 排空磁盘队列**即刻放行（Writer 在后台与下游计算并行落盘）。
2. **Resample 优先叠加 `ws_cache` 实时闭合窗口**：当 Writer 尚未将最新 1 根写入 `flat_pkl` 时，`_load_closed_base_window` 自动用 `ws_cache` 叠加磁盘窗口，保证重采样立刻拿到最新闭合 K 线。
3. **强制时间连续性防伪护栏（关键）**：新增 `_is_contiguous_closed_window()` 严格校验末尾 `once_update_candles` 根 K 线等步长连续且精确闭合在 `run_time - base_delta`，彻底防止“重启留下的单根内部空洞被 48h `ws_cache` 行数掩盖而跳过已由 REST 补齐的 `flat_pkl`”。

## 3. 问题（v2.0.2 现状与根因）

1. **判定信号错配**（`v2.0.2/core/pipeline/barrier.py:98`、`update_cycle.py:86`）：
   * Barrier 与 `_compute_missed_symbols` 均以磁盘 `.ws_ready`（“Writer 写盘完成”）作为“WS 是否收到”的判定信号，强迫后续 Resample 与 Preprocess 干等磁盘队列排空，同时导致滞后币种被误判触发冗余 REST。
2. **重采样读源滞后于 WS 接收**：
   * 即使放行了 Barrier，若 `_load_closed_base_window` 只读磁盘 `flat_pkl`/月分区，会读不到 Writer 尚未刷盘的当轮最新闭合 K 线。

## 4. 项目文档中的分析、实测收益与踩坑修复

* **设计来源**：`alphaonline/docs/2026-11-24-grace-tuning-analysis.md`（§6.2 P1 建议）与 `docs/superpowers/plans/2026-11-25-producer-compute-io-overlap.md`（Task 1 & Task 2）。
* **线上强实测收益（`alphaonline/docs/2026-11-25-prod-vs-test-comparison.md` §3.2、`2026-11-27-prod-vs-test-and-resource-efficiency.md` §3）**：
  * 测试机（E5 v4）：`barrier` 中位数由 `13.0s / 16.0s`（一期 `6.51s / 13.21s`）骤降至 **`1.12s / 2.31s`**（最低 `0.40s`）；`missed` 假缺口从 83 轮中位 `290` 归零至 **`0/0`（11/11 轮全零）**；
  * 正式机（Xeon 8255C，11-27 切同版本）：同批次 `barrier[spot] = 0.13s`、`barrier[swap] = 1.39s`、`missed = 0/0`。
* **线上踩坑与根治（必须一并合入）**：
  * 2026-11-25 11:25 UTC 复现一次真实边界 Bug：进程重启在 48h `ws_cache` 中留下 1 根内部空洞（如缺 `12:10`），`run_startup_catchup` 用 REST 补齐了 `flat_pkl`（无 `.gap` 标记）；旧版 `_load_closed_base_window` 仅判断 `len(ws_closed) >= once_update_candles`（因为 48h 缓存有 500+ 行轻松满足 `>= 99`），误判 `ws_cache` 完整而跳过 `flat_pkl`，导致 `12:10` 的成交量丢失。
  * 提交 `15205e78` 新增 `_is_contiguous_closed_window()` 逐根校验末尾 `once_update_candles` 根等步长连续且末根精确等于 `run_time - base_delta`：若 `ws_cache` 有内部空洞，立即回退叠加 `flat_pkl` / 月分区并保持 WS 优先级覆盖非空洞行；若完全连续，仍走零读盘快路径。

## 5. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 5.1 `core/pipeline/barrier.py`
* `wait_for_ready_barrier` 新增可选关键字参数 `ws_cache_db=None, trade_type=None, base_delta=timedelta(minutes=5), once_update_candles=99`：

```python
        ws_ready_contig: set[str] = set()
        ws_full_contig: set[str] = set()
        ws_gapped: set[str] = set()
        if ws_cache_db is not None and trade_type is not None:
            ws_ready_contig, ws_full_contig, ws_gapped = query_ws_cache_status(
                Path(ws_cache_db), trade_type, target_ms, base_ms, check_bars=once_update_candles,
            )

        disk_gaps = reader.list_gap_marker_symbols() & pending
        waiting_gap: set[str] = set()
        if rest_done is not None:
            gap_syms = disk_gaps | (ws_gapped & pending)
            if gap_syms and rest_done_was_set:
                gap_syms &= submitted_set
            waiting_gap = gap_syms
        else:
            waiting_gap = disk_gaps

        for sym in list(pending):
            if sym in waiting_gap:
                continue
            ws_fast_ok = (
                sym in ws_ready_contig
                and sym not in disk_gaps
                and (sym in ws_full_contig or reader.has_symbol(sym))
            )
            if ws_fast_ok or reader.check_ws_ready(sym, run_time) or (
                ws_grace_passed and reader.check_ready(sym, run_time)
            ):
                pending.discard(sym)
                success.add(sym)
```

### 5.2 `core/pipeline/update_cycle.py`
* 新增 `_resolve_ws_cache_db(handle, trade_type)` 定位 `{handle.base_dir}/ws_cache/{trade_type}.sqlite`。
* `_compute_missed_and_gaps()` 同步查询 `query_ws_cache_status()`：WAL 中 `sym in ws_ready_contig` 且已有本地数据（或满窗）则不视为 missed；若 `sym in ws_gapped` 且尚未 `.ws_ready` 则并入 `gap_symbols` 触发全窗 REST 补齐。
* `_barrier_and_resample()` 向 `wait_for_ready_barrier()` 透传 `ws_cache_db`、`trade_type`、`base_delta`、`once_update_candles`。

### 5.3 `core/pipeline/resample.py`
1. 新增 `_overlay_ws_window(disk_df, ws_df)`（WS 优先 `keep='last'` 叠加去重）与 `_load_ws_windows_for_chunk()`（按 chunk 批量一次只读 WAL，多取 12 根余量）。
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

## 6. 测试与验收清单

参考 `alpha/tests/test_compute_io_overlap_phase2.py`：
1. `test_ws_local_cache_readonly_status_and_window_frames`：验证在 Writer 持有 WAL 连接期间只读并发查询准确区分 `ready_contiguous` / `full_window_contiguous` / `gapped_at_target`，且读取帧与 `_kline_dict_to_df` 逐位一致。
2. `test_barrier_and_missed_check_release_immediately_via_ws_cache_when_writer_lags`：模拟磁盘滞后 1 根（0 个当轮 `.ws_ready`），验证 Barrier `<0.5s` 立即放行、`_compute_missed_and_gaps` 不误报正常币且精确抓出断档币。
3. `test_resample_overlays_ws_cache_when_writer_lags_behind_disk`：磁盘少最新 1 根而 `ws_cache` 已有最新 2 根，验证重采样输出与磁盘完整 Oracle `check_exact=True`。
4. `test_load_closed_base_window_rejects_gapped_ws_cache_and_heals_from_flat_pkl`：验证 48h `ws_cache` 缺中间 1 根时绝不跳过 `flat_pkl`（精准保留 REST 补齐行且非空洞行维持 WS 权威），而真正连续的 `ws_cache` 仍走零读盘快路径。
