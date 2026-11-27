# PR-05: 年度 Pivot 切片内存直传与 Linux Page Cache 逐出治理

* **建议分支**：`perf/in-memory-pivot-and-page-cache`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（第 1 批独立合入）
* **参考源提交（`alpha`）**：`7c10b63`、`baa6e94`

---

## 1. 场景

在 4 核 8 GB Linux 服务器（`4c8g`）上运行 `v2.0.2` 生产端与下游实盘交易端时：
1. 预处理阶段为生成年度 `market_pivot_{trade_type}_{year}.pkl`，每轮都要按 batch 将当年切片写入临时 pkl，再在紧接着的 `gen_pivot` 中读回并立即删除；
2. 生产端每轮高频重写 1,287 个 `1h_resample/*.pkl` 与 8~10 个 `*_dict_batch*.pkl`（总写入数百 MB~数 GB），这些只写/冷读文件长期占据 Linux OS Page Cache，将下游交易进程真正需要热读的 `market_pivot_*.pkl` 挤出缓存，诱发换页与主缺页飙升。

## 2. 目标

1. **消除全部临时 `*_for_pivot_batch*_{year}.pkl` 磁盘 I/O**：`gen_batch_data` 支持通过内存字典 `year_pivot_out` 直接向 `gen_pivot` 传递年度切片（保留旧磁盘路径作为参数缺省时的兼容回退）。
2. **引入跨平台安全的 `drop_file_page_cache(path)`**：在 Linux 上调用 `os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)` 仅逐出冷写文件（`1h_resample/*.pkl` 与 `*_dict_batch*.pkl`）的内核页缓存，刻意保留热读的 `market_pivot_*.pkl` 在页缓存中（Windows/macOS 自动 no-op）。

## 3. 问题（v2.0.2 现状与根因）

1. **临时 Pivot 文件死 I/O**（`v2.0.2/core/strategy/preprocess.py:293-325`）：
   * `gen_batch_data` 对每个未冻结年份写出 `{trade_type}_for_pivot_{batch_name}_{year}.pkl` + `.ready`；
   * 几秒后 `gen_pivot` 在**同一个进程内**立刻 `read_candle(name)` 读回并 `remove_symbol(name)` 删除。
   * 同一份数据无谓经历「pickle 序列化 → 磁盘写 → 反序列化 → unlink」全套开销。
2. **冷写 pkl 污染 OS Page Cache**：
   * `v2.0.2` 没有任何 `posix_fadvise` 提示；`1h_resample/*.pkl` 与 `batch*.pkl` 写入后在下一小时前极少被重读，但内核默认将其保留在 page cache 中，8 GB 宿主机内存紧张时直接挤压工作集与热 Pivot 缓存。

## 4. 项目文档中的分析与实测证据

* **Pivot 内存直传**：列为 Phase 4 `P4-4` 专项（`alphaonline/docs/kb/performance-analysis.md` §2），合入 `alpha 7c10b63`，通过 `test_pivot_skips_temp_for_pivot_batch_disk_io_and_matches_oracle` 断言临时 `*_for_pivot_*` 读写严格归零且 `market_pivot` 逐位一致。
* **Page Cache 治理实测**：`alphaonline/docs/2026-11-27-latest-round-resource-profile.md`（§2.3、§3.4）1 Hz 采样显示：
  * 启用页缓存治理与计算池释放后的测试机，轮内 `MemAvailable` 地板达 **`4,218 MB`**，落盘阶段主缺页仅 **`653 次/s`**；
  * 对照未治理且叠加旧栈的正式机，`MemAvailable` 跌至 `1,461 MB`，page cache 被挤空导致落盘阶段主缺页飙升至 **`3,430 次/s`**（高出 5.2 倍）。

## 5. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 5.1 `core/storage/candle_manager.py` — 新增 `drop_file_page_cache` 与可选参数

```python
def drop_file_page_cache(path) -> bool:
    """Hint Linux kernel to evict write-only file pages from OS page cache (no-op on Windows)."""
    fadvise = getattr(os, 'posix_fadvise', None)
    dontneed = getattr(os, 'POSIX_FADV_DONTNEED', None)
    if fadvise is None or dontneed is None:
        return False
    fd = None
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY)
        fadvise(fd, 0, 0, dontneed)
        return True
    except OSError:
        return False
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
```

* `CandleFileManager.save_data_file(self, symbol, df, *, drop_page_cache: bool = False)` 与 `set_candle(..., drop_page_cache: bool = False)`：在写盘完成后若 `drop_page_cache=True` 则调用 `drop_file_page_cache(df_path)`。

### 5.2 `core/pipeline/resample.py`
* 在 `update_resample_candle`（及 PR-04 的 `update_resample_candle_incremental`）写完 `1h_resample` 文件后调用 `drop_file_page_cache(candle_mgr.format_data_file_path(symbol))`。

### 5.3 `core/strategy/preprocess.py`
1. `gen_batch_data` 写完 `full_batch_name` 后调用 `drop_file_page_cache(preprocess_mgr.format_data_file_path(full_batch_name))`。
2. `gen_batch_data` 新增可选参数 `year_pivot_out=None`：

```python
        market_dict_for_pivot = {
            sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
            for sym, df in market_dict.items()
        }
        if year_pivot_out is not None:
            year_pivot_out.setdefault(year, {}).update(market_dict_for_pivot)
        else:
            preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
            year_pivot_name[year] = pivot_batch_name
```

3. `gen_pivot` 新增可选参数 `year_pivot_data=None`，优先从内存消费年度切片；写出 `market_pivot_{trade_type}_{year}` 时**刻意不调用 `drop_file_page_cache`**，保持热读缓存：

```python
def gen_pivot(handle: BmacHandle, run_time: datetime, trade_type: str,
              year_pivot_names: dict[str, list], *, year_pivot_data=None):
    ...
    years = set(year_pivot_names)
    if year_pivot_data is not None:
        years.update(year_pivot_data)
    for year in sorted(years):
        if should_skip_pivot_year(preprocess_mgr, trade_type, year, cur_year):
            continue
        market_dict = dict()
        if year_pivot_data is not None and year in year_pivot_data:
            market_dict.update(year_pivot_data.pop(year))
        for name in year_pivot_names.get(year, []):
            market_dict.update(preprocess_mgr.read_candle(name))
            preprocess_mgr.remove_symbol(name)
        market_pivot = make_market_pivot(market_dict, trade_type)
        preprocess_mgr.set_candle(f'market_pivot_{trade_type}_{year}', run_time, market_pivot)
        ...
```

4. `preprocess_type_data` 创建局部 `year_pivot_data: dict[int, dict[str, pd.DataFrame]] = {}`，传入 `gen_batch_data(..., year_pivot_out=year_pivot_data)` 并转交给 `gen_pivot(..., year_pivot_data=year_pivot_data)`。

## 6. 测试与验收清单

参考 `alpha/tests/test_module10_preprocess_pipeline.py` 与 `tests/test_producer_memory_hygiene.py`：
1. `test_pivot_skips_temp_for_pivot_batch_disk_io_and_matches_oracle`：spy `set_candle`/`read_candle`，断言 `*_for_pivot_*` 读写次数为 0，且生成的 `market_pivot` 与磁盘版逐位一致。
2. `test_drop_file_page_cache_invokes_posix_fadvise_and_handles_missing_api`：验证 Linux 正常调用 `posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED)`，缺失 API（Windows）静默返回 `False`。
3. `test_raw_batch_and_1h_resample_drop_page_cache_while_pivot_stays_hot`：验证 `1h_resample` 与 `batch*.pkl` 触发页缓存逐出，而 `market_pivot_*.pkl` 不触发。
