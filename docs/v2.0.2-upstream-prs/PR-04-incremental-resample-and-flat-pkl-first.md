# PR-04: `O(1)` 重叠增量重采样、`flat_pkl` 优先读、核数自适应 `resample_history` 与 `fill_gaps` 整型还原

* **建议分支**：`perf/incremental-resample-and-flat-pkl-first`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（与 PR-02 的 `flat_pkl_max_bars=240` 协同收益最大）
* **参考源提交（`alpha`）**：`423bb49`、`33d0854`、`29e50fc`

---

## 1. 场景

每 5 分钟启用 offset 轮次（以及进程冷启动初始化阶段），系统需将 1,287+ 个交易对的 5m 基础 K 线重采样并合并写入 `1h_resample/{offset}/{symbol}.pkl`（当 `kline_count_1h=10000~15000` 时，每个 1h 文件含上万行）。

## 2. 目标

1. **实时重采样优先读 240 根轻量 `flat_pkl`**：避开读取 7,000~8,640 行的月度分区 pkl，仅在 `flat_pkl` 缺失或闭合根数不足 `once_update_candles` 时安全回退 `TSManager` 月分区。
2. **新增 `update_resample_candle_incremental()` 实现 $O(1)$ 重叠窗口合并**：常规每小时重采样仅产出 ~7 根 1h bar（前 6 根与历史尾部重叠、末 1 根新增）；通过 `searchsorted` 定位切点，零拷贝保留不变历史前缀，仅对尾部 ~8 根执行去重、48h 拆分检测与补空；遇任何异常自动回退全量 `update_resample_candle()`。
3. **启动期 `resample_history()` 按核数自适应并发**：取代 `v2.0.2` 的单线程串行遍历，同时严格控制每进程线程数防止 `2c4g` 重启 OOM。
4. **修复 `fill_gaps()` 整型列类型漂移**：防止 `how='left'` merge 注入 `NaN` 把 `trade_num`（`int64`）静默提升为 `float64`。

## 3. 问题（v2.0.2 现状与根因）

定位：`coin-realtime-data_v2.0.2/core/pipeline/resample.py`

1. **`_resample_one_symbol`（L216–L231）强制先读月分区**：每轮先调用 `_read_base_tail` 读取数千行月分区（跨月初甚至读 2 个月分区），而 `Writer` 已经在根目录写好了包含最新闭合 K 线的 `flat_pkl`（在 PR-02 解耦后仅 240 行，体量缩小 30 倍）。
2. **只有全量 `update_resample_candle`（L137–L177）**：每次合并仅为了追加 1 根 1h bar，都要对 15,000 行全表执行 `concat` → `drop_duplicates` → `sort_values` → `filter_gaps` → `fill_gaps`（构造全量 `date_range` 并左连接）。
3. **`resample_history`（L47–L79）纯单线程循环**：冷启或重启时逐币串行读取 `TSManager.read_all()` 并逐个 offset 重采样落盘。
4. **`fill_gaps`（L82–L118）未还原整数 dtype**：只要存在 1 小时停机缺口，`pd.merge(..., how='left')` 会把 `trade_num` 从 `int64` 变成 `float64`，导致不同币种 schema 分裂（后续写入 Parquet/Arrow 或严格对账报错）。

## 4. 项目文档中的分析与实测证据

* **线上热点定位**：`alphaonline/docs/kb/performance-analysis.md`（§5.1–5.2）显示，`v2.0.2` 底座初版采集链路 47s 中，**`resample` 单项就占了 30.5s**（超过整条采集链路的一半）。
* **合成基准与线上实测**：
  * `docs/kb/performance-analysis.md`（§5.8）：Windows 240 序列 × 6,000 根合成对拍，接入 `flat_pkl` 优先读与计算池后，`resample` 从 **`4.10s → 1.45s`（2.83×）**，`check_exact=True` 逐位一致。
  * `docs/superpowers/plans/2026-11-25-producer-compute-io-overlap.md`（Task 2）指出：早先的 `update_resample_candle_incremental` 在 `first_new_time <= last_old_time` 时直接退回全量，而常规 99 根 5m 重采样产出的 ~7 根 1h bar **100% 满足重叠条件**，导致增量快路径几乎从未命中！引入切点 `cut_pos = searchsorted(first_new_time)` 后，每轮仅处理尾部 ~8 根，与全量 Oracle `check_exact=True` 逐位零差。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

### 5.1 硬件规格局限与防 OOM 修正
* **`update_resample_candle_incremental` + `flat_pkl` 优先读 + `fill_gaps` 整型还原**：**全规格强正收益**（大幅减少 CPU、内存拷贝与磁盘读量）。
* **`resample_history` 多线程在 `2c4g` 上的 OOM 高危点**：
  * `v2.0.2/core/app.py` 的 `init_history` 外层已通过 `ProcessPoolExecutor(max_workers=2)` 并发执行 `spot` 和 `swap` 两个 `resample_history` 子进程。
  * 若内部硬编码 `ThreadPoolExecutor(max_workers=4)`，会叠加出 **`2 进程 × 4 线程 = 8 个并发流`** 同时调用 `TSManager.read_all()`。由于 `v2.0.2` 保留完整 5m 月分区（未做 30 天裁剪），当 `kline_count_1h` 较大时，单币全量 5m DataFrame 可达数十 MB，8 份叠加 concat/copy 极易瞬间击穿 `2c4g`（~3.3 GiB 可用内存）诱发 OOM Killer。
* **修正护栏**：`resample_history` 线程数改为按核数自适应：`n_threads = min(4, max(1, (os.cpu_count() or 2) // 2))`——
  * `2c4g` / `2c8g`：每市场 `1` 线程（spot + swap 合计正好 `2` 并发，既吃满 2 核又把内存峰值砍半）；
  * `4c8g`：每市场 `2` 线程（合计 `4` 并发，吃满 4 核）；
  * `8c+`：每市场上限 `4` 线程。

### 5.2 强依赖 PKL 消费端评估
* **完全成立**。产出 `1h_resample/{offset}/{symbol}.pkl` 与旧全量版 `check_exact=True`；`fill_gaps` 恢复 `trade_num` 的 `int64` 类型对 PKL 消费端（防止跨币种 `concat` 隐式转 `float64`）同样是正向修复。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/pipeline/resample.py` — 修复 `fill_gaps` 整型 dtype 还原

```python
    int_dtypes = {col: dt for col, dt in df.dtypes.items() if pd.api.types.is_integer_dtype(dt)}
    benchmark = pd.DataFrame({'candle_begin_time': time_range})
    df = pd.merge(left=benchmark, right=df, on='candle_begin_time', how='left', sort=True)
    ...
    df['trade_num'] = df['trade_num'].fillna(0)
    for col, dt in int_dtypes.items():
        df[col] = df[col].astype(dt)
```

### 6.2 `core/pipeline/resample.py` — 新增 `update_resample_candle_incremental`

关键快路径逻辑（支持严格追加 + 常规重叠窗口，异常自动回退 `update_resample_candle(..., df_old_preloaded=df_old)` 避免重复读盘）：

```python
def update_resample_candle_incremental(
    candle_mgr: CandleFileManager,
    symbol: str,
    df_resample: pd.DataFrame,
    num_candles: Optional[int],
    run_time: datetime,
    resample_delta: timedelta,
    *,
    df_old_preloaded: Optional[pd.DataFrame] = None,
):
    if df_old_preloaded is None and not candle_mgr.has_symbol(symbol):
        return update_resample_candle(candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta)

    df_old = df_old_preloaded if df_old_preloaded is not None else candle_mgr.read_candle(symbol)
    if df_old.empty or df_resample.empty:
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            df_old_preloaded=df_old,
        )

    old_times = df_old['candle_begin_time']
    new_times = df_resample['candle_begin_time']
    if (
        old_times.duplicated().any()
        or new_times.duplicated().any()
        or not old_times.is_monotonic_increasing
        or not new_times.is_monotonic_increasing
    ):
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            df_old_preloaded=df_old,
        )

    last_old_time = old_times.iloc[-1]
    first_new_time = new_times.iloc[0]

    # 常规 ~7 根重叠窗口：定位切点，不变前缀零拷贝，仅对尾部 ~8 根做去重/拆分检测/补空
    if first_new_time <= last_old_time:
        cut_pos = int(old_times.searchsorted(first_new_time))
        if cut_pos >= 1:
            prefix = df_old.iloc[:cut_pos - 1]
            prefix_gap_free = (
                len(prefix) <= 1
                or len(prefix) == int((prefix['candle_begin_time'].iloc[-1] - prefix['candle_begin_time'].iloc[0]) // resample_delta) + 1
            )
            if prefix_gap_free:
                tail_work = pd.concat([df_old.iloc[cut_pos - 1:], df_resample], ignore_index=True)
                tail_work.drop_duplicates(subset='candle_begin_time', keep='last', inplace=True)
                tail_work.sort_values('candle_begin_time', inplace=True, ignore_index=True)

                t_gap = tail_work['candle_begin_time'].diff().fillna(timedelta(minutes=0))
                p_chg = (tail_work['open'] / tail_work['close'].shift().fillna(tail_work['open']) - 1).fillna(0)
                if not ((t_gap > timedelta(hours=48)) & (p_chg.abs() > 0.1)).any():
                    exp_tail_len = int((tail_work['candle_begin_time'].iloc[-1] - tail_work['candle_begin_time'].iloc[0]) // resample_delta) + 1
                    if len(tail_work) == exp_tail_len:
                        df_total = pd.concat([prefix.reset_index(drop=True), tail_work], ignore_index=True) if not prefix.empty else tail_work
                    else:
                        tail_work = fill_gaps(tail_work, resample_delta)
                        if not prefix.empty:
                            prefix_idx = prefix.copy()
                            prefix_idx['candle_end_time'] = prefix_idx['candle_begin_time'] + resample_delta
                            prefix_idx.set_index('candle_end_time', inplace=True)
                            df_total = pd.concat([prefix_idx, tail_work])
                        else:
                            df_total = tail_work
                    if num_candles is not None:
                        df_total = df_total.tail(num_candles)
                    candle_mgr.set_candle(symbol, run_time, df_total)
                    if df_total.empty:
                        return None, None
                    return df_total['candle_begin_time'].iloc[0], df_total['candle_begin_time'].iloc[-1]

        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            df_old_preloaded=df_old,
        )

    # 严格递增无重叠尾部快路径（含大缺口检测与单尾补空）
    time_gap = new_times.diff().fillna(first_new_time - last_old_time)
    price_change = (df_resample['open'] / df_resample['close'].shift().fillna(df_old['close'].iloc[-1]) - 1).fillna(0)
    if ((time_gap > timedelta(hours=48)) & (price_change.abs() > 0.1)).any():
        return update_resample_candle(
            candle_mgr, symbol, df_resample, num_candles, run_time, resample_delta,
            df_old_preloaded=df_old,
        )

    if (time_gap == resample_delta).all() and not isinstance(df_old.index, pd.DatetimeIndex):
        df_total = pd.concat([df_old, df_resample], ignore_index=True)
    else:
        prefix = df_old.iloc[:-1].copy(deep=True)
        work_df = fill_gaps(pd.concat([df_old.tail(1), df_resample], ignore_index=True), resample_delta)
        if isinstance(work_df.index, pd.DatetimeIndex) and not prefix.empty:
            prefix['candle_end_time'] = prefix['candle_begin_time'] + resample_delta
            prefix.set_index('candle_end_time', inplace=True)
            df_total = pd.concat([prefix, work_df])
        else:
            df_total = pd.concat([prefix.reset_index(drop=True), work_df], ignore_index=True) if not prefix.empty else work_df

    if num_candles is not None:
        df_total = df_total.tail(num_candles)
    candle_mgr.set_candle(symbol, run_time, df_total)
    if df_total.empty:
        return None, None
    return df_total['candle_begin_time'].iloc[0], df_total['candle_begin_time'].iloc[-1]
```

### 6.3 `core/pipeline/resample.py` — `_load_closed_base_window`（`flat_pkl` 优先 + 月分区回退）与自适应 `resample_history`

```python
def _load_closed_base_window(
    base_dir: Path,
    symbol: str,
    run_time: datetime,
    once_update_candles: int,
) -> pd.DataFrame | None:
    df_flat_closed: pd.DataFrame | None = None
    flat_pkl = base_dir / f'{symbol}.pkl'
    if flat_pkl.exists():
        try:
            df_flat = pd.read_pickle(flat_pkl)
        except Exception:
            df_flat = None
        if df_flat is not None and not df_flat.empty and 'candle_begin_time' in df_flat.columns:
            df_flat = df_flat[df_flat['candle_begin_time'] < run_time]
            if '_source' in df_flat.columns:
                df_flat = df_flat.drop(columns=['_source'])
            if not df_flat.empty:
                df_flat_closed = df_flat
                if len(df_flat_closed) >= once_update_candles:
                    return df_flat_closed

    df_part = _read_base_tail(base_dir, symbol, run_time, once_update_candles)
    if df_part is not None and not df_part.empty:
        df_part = df_part[df_part['candle_begin_time'] < run_time]
        if '_source' in df_part.columns:
            df_part = df_part.drop(columns=['_source'])
        if not df_part.empty:
            if df_flat_closed is None or len(df_part) >= len(df_flat_closed):
                return df_part
    return df_flat_closed
```

* `_resample_one_symbol` 改用 `_load_closed_base_window` + `update_resample_candle_incremental`。
* `resample_history` 线程并发改为按核自适应：

```python
    n_threads = min(4, max(1, (os.cpu_count() or 2) // 2))
    if n_threads <= 1 or len(symbols) <= 1:
        for sym in symbols:
            _process_symbol(sym)
    else:
        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            list(pool.map(_process_symbol, symbols))
```

## 7. 测试与验收清单

参考 `alpha/tests/test_2b_history_and_resample.py`、`tests/test_parallel_pipeline_phase1.py`、`tests/test_compute_io_overlap_phase2.py`：
1. `test_fill_gaps_restores_integer_trade_num_dtype`：存在缺口补空后，`trade_num` 保持 `int64` 且缺口填 0。
2. `test_update_resample_candle_incremental_overlapping_7_bars_matches_oracle_and_returns_tail`：覆盖常规 7 根重叠、含 1h 缺口、代币拆分（>48h + >10% 跳空）三种场景，与 `update_resample_candle` 全量 Oracle `check_exact=True`。
3. `test_resample_prefers_flat_pkl_without_reading_tsmanager_when_window_sufficient`：当 `flat_pkl` 有 240 根闭合 K 线时，monkeypatch `TSManager.read_partition` 抛错断言零触发，结果与纯月分区 Oracle 逐位一致。
4. `test_resample_falls_back_to_tsmanager_when_flat_pkl_shorter_than_window`：当 `flat_pkl` 仅 24 根（`< 99`）而月分区有 150 根时，自动回退月分区保证窗口完整。
5. 新增并发上限单测：在 `os.cpu_count() == 2` 时 `resample_history` 串行运行（配合外层 2 进程总并发为 2），`os.cpu_count() == 4` 时每市场 2 线程（总并发为 4）。
