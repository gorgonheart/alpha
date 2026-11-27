# PR-02: Writer 轻量化 `kline` 载荷、`flat_pkl` 小窗解耦（240 根）、提前发布 `.ws_ready` 与全旧分区裁剪

* **建议分支**：`perf/writer-lightweight-and-early-ready`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（第 1 批优先合入；与 PR-01、PR-03、PR-04 协同效果最佳）
* **参考源提交（`alpha`）**：`f5e4cd2`、`65f78ae`、`3e8972b`、`7a44299`

---

## 1. 场景

`v2.0.2` 启用 WebSocket 模式（`use_websocket=True`）时，每 5 分钟边界有 1,287+ 个交易对的闭合 K 线几乎同时涌入 `mp.Queue`，由 `Writer` 子进程完成优先级合并、写 `flat_pkl`、更新 `TSManager` 月度分区、更新 `.gap` 标记并发布 `.ready` / `.ws_ready`。当用户配置较大历史深度（如 `kline_count_1h = 10000~25000`）时，此热路径成为整个系统最严重的延迟与磁盘写放大瓶颈。

## 2. 目标

1. **解除 `flat_pkl` 磁盘与 I/O 炸弹**：将 `flat_pkl` 滚动上限与 `TSManager` 历史保留期彻底解耦，固定为轻量小窗（默认 `ws_flat_pkl_bars = 240` 根 ≈ 20 小时，单文件 ~22 KB），同时保证大批量补齐（`>240` 根）写入月度分区时不丢数据。
2. **削减 IPC 序列化开销**：`WsRunner → Writer` 队列改传原生轻量 `kline` 字典，消除 WS 进程每根构造单行 `DataFrame` + pickle 序列化开销，且 `Writer` 保持向后兼容 `df` 载荷（供 REST 兜底使用）。
3. **将慢速月分区写移出屏障热路径**：`Writer` 在原子写完 `flat_pkl` 并更新 `.gap` 后**立即发布 `.ready` / `.ws_ready`**，再异步执行 `TSManager` 月度分区更新（且月分区异常不回滚已落盘的 `flat_pkl`）。
4. **修复 3 个工程隐患**：
   * 裁剪所有早于保留窗口的历史月分区（修复 `v2.0.2` 仅裁 `partitions[0]` 导致多月积累无法回收的 Bug）；
   * 统一 `candle_begin_time` 为 `datetime64[ns, UTC]`，兼容遗留 tz-naive 或 ms 精度 pkl；
   * `_write_pkl` 使用 `{pid}.{tid}.tmp` 并增加 Windows `PermissionError` 退避重试。

## 3. 问题（v2.0.2 现状与根因）

1. **`flat_pkl` 随 `kline_count_1h` 无限膨胀**（`v2.0.2/core/storage/writer.py:415`、`runtime_supervisor.py:249`）：
   * `Writer` 直接使用 `num_base_candles = 12 × (kline_count_1h + 3)` 裁剪 `flat_pkl`。若 `kline_count_1h=15000`，单币 `flat_pkl` 高达 **180,000 行（~15 MB）**，全市场 1,287 币额外吞噬 **15+ GiB 磁盘**，且每 5 分钟逐币整文件反序列化 + 排序 + 序列化重写。
2. **WS 进程构造 DataFrame 经 `mp.Queue` 跨进程传输**（`v2.0.2/core/ws/ws_runner.py:49-68`）：
   * 每根闭合 K 线在 WS 事件循环线程中构造 `pd.DataFrame` 并做 pandas pickle 序列化，占用 WS 接收循环 CPU 与队列带宽。
3. **月度分区读写阻塞 `.ws_ready` 释放**（`v2.0.2/core/storage/writer.py:268-291`）：
   * `handle_item` 顺序执行 `flat_pkl` → `TSManager`（读写 ~7,000~8,640 行的当月分区 pkl）→ `.gap` → `save_ws_ready`，导致屏障必须等待笨重的月分区写盘。
4. **仅裁剪 `partitions[0]`**（`v2.0.2/core/storage/writer.py:456`）：
   * 停机或深历史回补后若存在多个过期月分区，每天仅裁最早 1 个分区，且 `trim_partition` 对未空分区持续重写。

## 4. 项目文档中的分析与设计证据

* **暗雷诊断**：`alphaonline/docs/2026-11-23-phase5-and-v202-migration-plan.md`（§阶段 2B-1 暗雷一）指出未解耦 `flat_pkl` 会导致 `~15+ GiB` 额外磁盘占用并冲刷页缓存。
* **排空速率实测**：`alphaonline/docs/2026-11-24-grace-tuning-analysis.md`（§3）实测证明，WS 客户端在 `+1.1s`（spot）/ `+3.7s`（swap）即收齐整轮并入队，但 `.ws_ready` 滞后到 `+8~15s`（2 个 writer 排空 1,287 币仅 `75~100 条/s`，耗时 `8~11s`）。**Writer 吞吐是屏障延迟的头号主因**。
* **上线组合收益**：`docs/superpowers/plans/2026-11-24-producer-4core-parallel-pipeline.md`（Task 1）及 `docs/kb/performance-analysis.md`（§2、§5.8）记录：一期 4 核流水线（含本 PR 全部 Writer 改造）上线测试机 5 小时实测，`25m` 轮 `.ready` 中位数由 **`+149.1s` 降至 `+74.0s`（-50.4%）**，活跃 7 父策略 157/157 选币与 538/538 合约方向 100% 一致。
* **临界顺序护栏**：`flat_pkl` 必须使用未裁剪的 `df_merged` 过滤 `df_for_partition` 再写月分区（提交 `65f78ae`），否则当单次 REST 批量写入行数超过 `flat_pkl_max_bars=240` 时会导致历史补齐行丢失。

## 5. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 5.1 `config_binance.py` / `config_okx.py` & `core/handle.py`
* 新增可配置参数（保持向后兼容默认值）：
  * `ws_flat_pkl_bars = 240`（`BmacHandle.ws_flat_pkl_bars: int = 240`）
  * `num_writers = 4`（允许在配置中覆盖 `RuntimeSupervisor.NUM_WRITERS`）
* `BmacHandle` 提供 `flat_pkl_max_bars()` 方法：`max(int(self.ws_flat_pkl_bars), int(self.spot.once_update_candles) * 2)` 或直接返回 `int(self.ws_flat_pkl_bars)`。

### 5.2 `core/ws/ws_runner.py`
* `_kline_to_writer_item()` 不再构造 `pd.DataFrame`，改为输出纯 Python 字典：

```python
def _kline_to_writer_item(
    symbol: str,
    trade_type: str,
    kline: dict,
    base_delta: timedelta,
) -> dict:
    """把归一化的缓存结构 K 线 dict 转成轻量 writer 队列 item（不在 WS 进程内构造 DataFrame）。"""
    begin_dt = pd.Timestamp(kline["begin_time_ms"], unit="ms", tz="UTC")
    run_time = (begin_dt + pd.Timedelta(base_delta)).to_pydatetime()
    return {
        "symbol": symbol,
        "trade_type": trade_type,
        "kline": {
            "begin_time_ms": int(kline["begin_time_ms"]),
            "open": float(kline["open"]),
            "high": float(kline["high"]),
            "low": float(kline["low"]),
            "close": float(kline["close"]),
            "volume": float(kline["volume"]),
            "quote_volume": float(kline["quote_volume"]),
            "trade_num": int(kline["trade_num"]),
            "taker_buy_base": float(kline["taker_buy_base"]),
            "taker_buy_quote": float(kline["taker_buy_quote"]),
        },
        "source": "ws",
        "run_time": run_time,
    }
```

### 5.3 `core/storage/writer.py`
1. 新增 `_kline_dict_to_df(kline)` 与 `_normalize_candle_begin_time(df)`：

```python
def _kline_dict_to_df(kline: dict) -> pd.DataFrame:
    begin_dt = pd.Timestamp(int(kline["begin_time_ms"]), unit="ms", tz="UTC").as_unit("ns")
    return pd.DataFrame({
        "candle_begin_time": pd.Series([begin_dt], dtype="datetime64[ns, UTC]"),
        "open": [float(kline["open"])],
        "high": [float(kline["high"])],
        "low": [float(kline["low"])],
        "close": [float(kline["close"])],
        "volume": [float(kline["volume"])],
        "quote_volume": [float(kline["quote_volume"])],
        "trade_num": pd.Series([int(kline["trade_num"])], dtype="int64"),
        "taker_buy_base_asset_volume": [float(kline["taker_buy_base"])],
        "taker_buy_quote_asset_volume": [float(kline["taker_buy_quote"])],
    })
```

2. `Writer.__init__` 新增 `flat_pkl_max_bars: Optional[int] = 240` 与 `partition_retention_bars`（向后兼容旧 `num_base_candles`）。
3. `Writer.handle_item` 调整执行顺序：
   * 同时支持 `item["df"]`（REST/旧调用方）与 `item["kline"]`（WS 轻量载荷）；
   * `_merge()` 返回**未裁剪的** `df_merged`；
   * 仅对 `df_flat = df_merged.tail(self.flat_pkl_max_bars)` 执行 `_write_pkl()`；
   * 执行 `_update_gap_marker(...)`；
   * **立即发布** `cfm.save_ready(symbol, run_time)` 与 `if source == "ws": cfm.save_ws_ready(symbol, run_time)`；
   * 最后在 `try/except` 保护下从**未裁剪的** `df_merged` 切片 `df_for_partition` 更新 `_update_tsmanager()`。
4. `Writer._write_pkl` 原子写防冲突与重试：

```python
    def _write_pkl(self, base_dir: Path, symbol: str, df: pd.DataFrame) -> None:
        p = self._pkl_path(base_dir, symbol)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        df.to_pickle(tmp)
        for attempt in range(6):
            try:
                os.replace(tmp, p)
                return
            except PermissionError:
                if attempt == 5:
                    tmp.unlink(missing_ok=True)
                    raise
                time.sleep(0.005 * (attempt + 1))
```

5. `Writer._update_tsmanager` 裁剪全部过期月分区：

```python
        if self.partition_retention_bars is not None:
            key = (base_dir, symbol)
            last = self._last_partition_trim.get(key)
            if last is None or run_time - last >= self.PARTITION_TRIM_INTERVAL:
                partitions = ts_mgr.list_partitions()
                if partitions:
                    required_first_begin = run_time - self.partition_retention_bars * self.base_delta
                    cutoff_part = get_partition(required_first_begin, ts_mgr.data_freq)
                    for part in partitions:
                        if part <= cutoff_part:
                            ts_mgr.trim_partition(part, required_first_begin, run_time)
                self._last_partition_trim[key] = run_time
```

### 5.4 `core/pipeline/runtime_supervisor.py`
* `run_writer_subprocess` 与 `start_writers` 透传 `flat_pkl_max_bars` 与 `partition_retention_bars`（默认取 `handle.num_base_candles`，不改变 `v2.0.2` 的 5m 历史保留策略）。

## 6. 测试与验收清单

参考 `alpha/tests/test_2b_writer_and_storage.py`、`tests/test_parallel_pipeline_phase1.py`、`tests/test_compute_io_overlap_phase2.py`：
1. `test_ws_runner_emits_lightweight_kline_dict_and_writer_accepts_both_kline_and_df`：断言 `kline` 字典与 `df` 两种输入落盘的 `flat_pkl` 和 `TSManager` 逐位一致（`check_exact=True`）。
2. `test_writer_caps_flat_pkl_at_240_and_trims_all_old_partitions`：推入 300 根 K 线，验证 `flat_pkl` 严格上限 240 根、月分区完整保留全部 300 根、多个旧月分区全部被裁掉。
3. `test_writer_publishes_ws_ready_after_flat_pkl_and_gap_before_tsmanager`：在 `_update_tsmanager` 内断言 `flat_pkl`/`.gap`/`.ws_ready` 均已落盘，且即使月分区抛出 `OSError` 也不影响已发布的 `.ws_ready`。
4. `test_writer_normalizes_mixed_naive_and_ms_timestamps_in_legacy_flat_pkl`：验证混合时区与 ms 精度遗留 pkl 合并后统一为 `datetime64[ns, UTC]`。
5. `test_writer_atomic_replace_retries_transient_permission_error`：模拟前 2 次 `os.replace` 抛 `PermissionError`，第 3 次成功并保证内容一致。
