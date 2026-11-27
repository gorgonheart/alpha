# PR-05: 年度 Pivot 切片按内存门控直传与 PKL 原子写防护（已撤回 `POSIX_FADV_DONTNEED`）

* **建议分支**：`perf/in-memory-pivot-and-atomic-pkl`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（第 1 批独立合入）
* **参考源提交（`alpha`）**：`7c10b63`（Pivot 内存直传）；**`baa6e94` 中的 `POSIX_FADV_DONTNEED` 经复审后已从本 PR 撤回**

---

## 1. 场景

1. 在内存充裕（`>= 6 GiB`）的 `2c8g` / `4c8g` 服务器上，常规运行期每年仅有 1 个未冻结的当年 Pivot，预处理阶段按 batch 将年度切片写临时 pkl 再读回删除属于纯冗余 I/O。
2. 在**强依赖 PKL 消费端**场景下，消费端读取数百 MB 级 `*_dict_batch*.pkl` 与 `market_pivot_*.pkl` 耗时较长，若生产者用非原子“先删后写”覆盖文件，极易触发消费端 `EOFError: Ran out of input` 或 `FileNotFoundError`。

## 2. 目标

1. **有条件消除临时 `*_for_pivot_batch*_{year}.pkl` 磁盘 I/O**：当可用内存充裕（总物理内存 `>= 6 GiB`）且待重建的未冻结年份不超过 1 个时，通过内存字典 `year_pivot_out` 直接向 `gen_pivot` 传递年度切片；在 `2c4g` 低内存机器或跨年多年度冷启动重建时，**自动回退原磁盘临时文件模式**，避免将机器推入 Swap/OOM。
2. **为 `CandleFileManager.save_data_file` 补齐原子写（`.tmp` + `os.replace`）**：零性能损耗地消除 PKL 消费端读到半写文件的竞态。
3. **显式保留 OS Page Cache（撤回 `POSIX_FADV_DONTNEED`）**：保护 `1h_resample/*.pkl`、`*_dict_batch*.pkl` 与 `market_pivot_*.pkl` 在页缓存中的热读性能。

## 3. 问题（v2.0.2 现状与根因）

1. **临时 Pivot 文件死 I/O**（`v2.0.2/core/strategy/preprocess.py:293-325`）：
   * `gen_batch_data` 对每个未冻结年份写出 `{trade_type}_for_pivot_{batch_name}_{year}.pkl` + `.ready`；
   * 几秒后 `gen_pivot` 在同一个进程内立刻 `read_candle(name)` 读回并 `remove_symbol(name)` 删除。
2. **非原子覆盖写盘威胁 PKL 消费端**（`v2.0.2/core/storage/candle_manager.py:123-141`）：
   ```python
   if os.path.exists(df_path):
       os.remove(df_path)
   ...
   df.to_pickle(df_path)
   ```
   在写入耗时数秒的 `batch*.pkl` 期间，文件处于不存在或半写状态；强依赖 PKL 的消费端只要读取稍慢或重试重叠，就会立刻报错。

## 4. 重要复审修正：为何在 `v2.0.2` 必须撤回 `POSIX_FADV_DONTNEED`

`alpha` 仓库（提交 `baa6e94`）曾使用 `os.posix_fadvise(..., POSIX_FADV_DONTNEED)` 逐出 `1h_resample/*.pkl` 和 `batch*.pkl` 的页缓存，并在测试机录得 `MemAvailable` 地板 `4,218 MB`、落盘主缺页 `653 次/s`。但该收益严格依赖 **`v2.0.2` 并不具备的两个前提**：
1. `alpha` 拥有内存 `resample_tail_hints`（Fast Path 1），写完 `1h_resample/*.pkl` 后当轮预处理**不会回读磁盘**；
2. `alpha` 消费端走 Arrow IPC mmap 快路径，写完 `batch*.pkl` 后下游**不会读取 PKL**。

而在 `coin-realtime-data_v2.0.2`（及任何强依赖 PKL 的消费端架构）中：
* `update_resample_from_base` 刚把 1,287 个 `1h_resample/*.pkl` 写完，**1 秒后 `gen_batch_data` 就要全部读回**；
* `gen_batch_data` 刚把 `*_dict_batch*.pkl` 写完，**紧接着 PKL 消费端就要立即 `pd.read_pickle()` 读走**。

如果在 `v2.0.2` 中移植 `POSIX_FADV_DONTNEED`，等于**人为销毁热页缓存、制造两波全市场冷盘随机读风暴**，在 `4c8g` 上拖慢预处理与消费端，在低 IOPS 的 `2c4g` 云盘上更是灾难性负优化。因此本 PR **彻底移除 `POSIX_FADV_DONTNEED`**。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

* **`2c4g`（4 GB RAM）**：`year_pivot_out` 跨批次滞留全市场当年切片会额外抬高 `200~400 MiB` 峰值（若跨年同时重建 2+ 个年份可达 `400~800 MiB`），4 GB 机器容易触发 Swap。通过内存门控（总物理内存 `< 6 GiB` 自动关闭内存直传）可完全规避风险，同时仍享受原子写防护。
* **`2c8g` / `4c8g`**：常规单活跃年轮次开启内存直传安全且省去全部临时 Pivot 写-读-删 I/O；跨年多年度重建自动退回临时文件。
* **强依赖 PKL 消费端**：撤回 `fadvise` + 增加原子写后，本 PR 对 PKL 消费端是 **100% 纯正向收益**（既保热页缓存、又消 `EOFError`）。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/storage/candle_manager.py` — `save_data_file` 原子写（防 PKL 读半成品）

```python
    def save_data_file(self, symbol, df: pd.DataFrame):
        df_path = self.format_data_file_path(symbol)
        tmp_path = f'{df_path}.{os.getpid()}.{threading.get_ident()}.tmp'
        try:
            if self.save_type == 'feather':
                df = df.reset_index(drop=True)
                df.to_feather(tmp_path)
            elif self.save_type == 'parquet':
                df.to_parquet(tmp_path)
            elif self.save_type == 'pickle':
                if isinstance(df, pd.DataFrame):
                    df.to_pickle(tmp_path)
                else:
                    pd.to_pickle(df, tmp_path)
            elif self.save_type == 'compressed_pickle':
                if isinstance(df, pd.DataFrame):
                    df.to_pickle(tmp_path)
                else:
                    raise ValueError(f'Symbol {symbol} if of type {type(df)}, cannot save as compressed_pickle')
            os.replace(tmp_path, df_path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
```

*(注：Windows 若遇并发读 `PermissionError`，可复用 PR-02 的短退避重试循环。)*

### 6.2 `core/strategy/preprocess.py` — 内存门控的年度 Pivot 切片直传

1. 新增系统内存探测辅助函数（不新增强制依赖，支持无 `psutil` 时读 `/proc/meminfo` 回退）：

```python
_MIN_IN_MEMORY_PIVOT_BYTES = 6 * 1024 * 1024 * 1024  # 6 GiB

def _should_use_in_memory_pivot(handle: BmacHandle) -> bool:
    explicit = getattr(handle, 'in_memory_pivot', None)
    if explicit is not None:
        return bool(explicit)
    try:
        import psutil
        return psutil.virtual_memory().total >= _MIN_IN_MEMORY_PIVOT_BYTES
    except Exception:
        pass
    try:
        with open('/proc/meminfo', 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('MemTotal:'):
                    kb = int(line.split()[1])
                    return kb * 1024 >= _MIN_IN_MEMORY_PIVOT_BYTES
    except OSError:
        pass
    return False
```

2. `gen_batch_data` 新增可选参数 `year_pivot_out=None`：仅当传入 `year_pivot_out` 且 `year == cur_year`（单活跃年）时直传内存；历史未冻结年份仍走临时文件，防冷启跨年内存爆炸：

```python
        market_dict_for_pivot = {
            sym: df.loc[df['candle_begin_time'].between(start_ts, end_ts), PIVOT_COLUMNS]
            for sym, df in market_dict.items()
        }
        if year_pivot_out is not None and year == cur_year:
            year_pivot_out.setdefault(year, {}).update(market_dict_for_pivot)
        else:
            preprocess_mgr.set_candle(pivot_batch_name, run_time, market_dict_for_pivot)
            year_pivot_name[year] = pivot_batch_name
```

3. `gen_pivot` 新增可选参数 `year_pivot_data=None`，优先从内存消费并立刻 `pop(year)` 释放引用：

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

4. `preprocess_type_data` 根据 `_should_use_in_memory_pivot(handle)` 决定是否传入 `year_pivot_data` 字典。

## 7. 测试与验收清单

参考 `alpha/tests/test_module10_preprocess_pipeline.py` 并补充低内存门控与原子写测试：
1. `test_pivot_skips_temp_for_pivot_batch_disk_io_and_matches_oracle`：开启 `in_memory_pivot=True` 时，spy 断言当年 `*_for_pivot_*` 读写为 0，产出 `market_pivot` 与磁盘版逐位一致。
2. `test_pivot_falls_back_to_temp_files_on_low_memory_or_historical_year`：模拟 `total_mem < 6 GiB`（`2c4g`）或历史未冻结年份，验证自动走原临时文件落盘路径。
3. `test_save_data_file_atomic_write_preserves_old_file_on_write_failure`：模拟写入 `.tmp` 抛异常时旧 `.pkl` 完整不受损且无 `.tmp` 残留。
