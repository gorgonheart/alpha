# PR-01: `CandleFileManager` 引入 `O(1)` `scandir` 就绪文件索引缓存（消除每币 `glob` 风暴）

* **建议分支**：`perf/candle-manager-scandir-cache`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（完全独立，第 1 批优先合入）
* **参考源提交（`alpha`）**：`f44d7ee`（初始实现）、`18dadf77`（跨进程 pickle 泄漏与 TTL 根治修复）

---

## 1. 场景

在 `use_websocket=True` 模式下，`Writer` 子进程在每个 5 分钟边界集中处理 1,287+ 个交易对（Binance 约 520 spot + 767 swap），每个交易对落盘后需连续调用 `save_ready(symbol, run_time)` 与 `save_ws_ready(symbol, run_time)` 清理旧时间戳标记并写入新标记；同时在 `resample_1h` 与 `preprocess` 目录，每轮也会高频调用 `save_ready` / `remove_symbol`。

## 2. 目标

1. 将单次 `save_ready` / `save_ws_ready` / `remove_symbol` 的目录扫描复杂度从 $O(N)$ 降为 $O(1)$，彻底消除每 5 分钟数千次全目录 `glob()` 系统调用。
2. 保证在**多进程 `ProcessPoolExecutor` / `spawn` 复用 `BmacHandle`（携带已初始化的 `CandleFileManager`）**场景下，绝不发生跨进程缓存陈旧导致的 `.ready` / `.ws_ready` 历史文件泄漏。
3. 零外部接口变化，对 Binance 与 OKX 透明生效。

## 3. 问题（v2.0.2 现状与根因）

定位：`coin-realtime-data_v2.0.2/core/storage/candle_manager.py`（L155–L192, L222–L232）

```python
def save_ready(self, symbol, run_time):
    old_ready_file_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ready'))
    for p in old_ready_file_paths:
        os.remove(p)
...
def save_ws_ready(self, symbol, run_time):
    old_ws_ready_paths = glob(os.path.join(self.base_dir, f'{symbol}_*.ws_ready'))
    for p in old_ws_ready_paths:
        os.remove(p)
```

* `base_5m` 目录下包含约 `3,000~4,000` 个文件（`.pkl` + `.ready` + `.ws_ready` + 子目录）。
* Python `glob.glob()` 每次调用都会执行一次 `os.scandir()` 遍历目录内全部目录项并做 fnmatch 匹配。
* 每 5 分钟 1,287 个 symbol × 2 次 `glob` = **2,574 次全目录扫描**（约 **800 万~1,000 万次目录项遍历/轮**），直接争抢 Writer I/O 吞吐与内核 dentry 锁。

## 4. 项目文档中的分析、设计与踩坑记录

* **设计来源**：`docs/superpowers/plans/2026-11-25-producer-compute-io-overlap.md`（Task 1）。
* **实测收益**：随计算-IO 重叠二期合入后，配合 WAL 直通与 Writer 优化，测试机 `barrier` 从 `6.51s / 13.21s` 压至 `1.12s / 2.31s`（正式机 `0.13s / 1.39s`）。
* **关键避坑（必须一并合入）**：
  * `alpha` 首版（`f44d7ee`）仅用布尔标记 `_ready_indexed=True` 永久缓存。上线实测发现：当父进程持有的 `CandleFileManager` 在首轮索引后，被 `ProcessPoolExecutor` pickle 序列化传给多轮子进程写入 `batch` / `1h` 文件时，父进程与后续子进程会继承过期缓存，导致旧轮次 `.ready` 文件未被删除（目录中累积数十个过期 `.ready`）。
  * 提交 `18dadf77` 引入两道护栏根治该问题：
    1. 实现 `__getstate__()`：序列化跨进程传递时强制清空 `_ready_cache`、`_ws_ready_cache` 并重置 `_ready_indexed = False`，子进程按当前磁盘态懒建索引；
    2. 引入 `_READY_INDEX_TTL_SEC = 2.0`：在 2 秒突发落盘窗内命中 $O(1)$ 缓存（覆盖整轮 1,287 币写入），超过 2 秒进入下个周期或被外部进程改写后自动用单次 `os.scandir()` 刷新。
  * 修复上线验证（`2026-11-25`）：跨进程 `.ready` 文件数量长期恒定在 14 个，零泄漏。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

* **硬件规格**：**全规格强正收益，无低配副作用**。进程内索引缓存仅存路径字符串（1,287 币约 `~200 KB` 内存），在 `2c4g` / `2c8g` 弱 CPU 与低突发 IOPS 云盘上，省去数百万次目录项遍历的边际收益比 `4c8g` 更加显著。
* **强依赖 PKL 消费端**：**完全成立**。本改动不改变 `.ready` 文件命名、内容或写入时机，下游 `check_ready()` / `pd.read_pickle()` 契约零变化。
* **联动建议**：针对 PKL 消费端慢读易踩非原子写竞态的问题，建议配合 PR-05 中的 `save_data_file` 原子写（`.tmp` + `os.replace`）一并合入。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 修改 `core/storage/candle_manager.py`

1. 顶部增加 `import time` 及模块常量 `_READY_INDEX_TTL_SEC = 2.0`。
2. 在 `CandleFileReader.__init__` 初始化缓存字段，并新增 `__getstate__` 与 `_ensure_ready_indexed`：

```python
_READY_INDEX_TTL_SEC = 2.0

class CandleFileReader:
    def __init__(self, base_dir, save_type):
        ...
        self._ready_cache: dict[str, list[str]] = {}
        self._ws_ready_cache: dict[str, list[str]] = {}
        self._ready_indexed: bool = False
        self._ready_indexed_at: float = 0.0

    def __getstate__(self):
        state = self.__dict__.copy()
        state['_ready_cache'] = {}
        state['_ws_ready_cache'] = {}
        state['_ready_indexed'] = False
        state['_ready_indexed_at'] = 0.0
        return state

    def _ensure_ready_indexed(self) -> None:
        now_mono = time.monotonic()
        if (
            getattr(self, '_ready_indexed', False)
            and (now_mono - getattr(self, '_ready_indexed_at', 0.0)) <= _READY_INDEX_TTL_SEC
        ):
            return
        r_cache: dict[str, list[str]] = {}
        w_cache: dict[str, list[str]] = {}
        if os.path.isdir(self.base_dir):
            try:
                with os.scandir(self.base_dir) as it:
                    for entry in it:
                        name = entry.name
                        if name.endswith('.ws_ready'):
                            stem = name[:-9]
                            sep = stem.rfind('_')
                            if sep > 0:
                                w_cache.setdefault(stem[:sep], []).append(entry.path)
                        elif name.endswith('.ready'):
                            stem = name[:-6]
                            sep = stem.rfind('_')
                            if sep > 0:
                                r_cache.setdefault(stem[:sep], []).append(entry.path)
            except OSError:
                pass
        self._ready_cache = r_cache
        self._ws_ready_cache = w_cache
        self._ready_indexed = True
        self._ready_indexed_at = now_mono
```

3. 更新 `CandleFileManager.clear_all`、`save_ready`、`save_ws_ready`、`remove_symbol`：

```python
    def clear_all(self):
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)
        self._ready_cache = {}
        self._ws_ready_cache = {}
        self._ready_indexed = True
        self._ready_indexed_at = time.monotonic()

    def save_ready(self, symbol, run_time):
        self._ensure_ready_indexed()
        for p in self._ready_cache.pop(symbol, ()):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

        if run_time is not None:
            ready_file_path = self.format_ready_file_path(symbol, run_time)
            with open(ready_file_path, 'w') as fout:
                fout.write(str(datetime.now().timestamp()))
            self._ready_cache[symbol] = [ready_file_path]

    def save_ws_ready(self, symbol, run_time):
        self._ensure_ready_indexed()
        for p in self._ws_ready_cache.pop(symbol, ()):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

        if run_time is not None:
            ws_ready_file_path = self.format_ws_ready_file_path(symbol, run_time)
            with open(ws_ready_file_path, 'w') as fout:
                fout.write(str(datetime.now().timestamp()))
            self._ws_ready_cache[symbol] = [ws_ready_file_path]

    def remove_symbol(self, symbol):
        self._ensure_ready_indexed()
        for p in self._ready_cache.pop(symbol, ()):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        for p in self._ws_ready_cache.pop(symbol, ()):
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
        df_path = self.format_data_file_path(symbol)
        if os.path.exists(df_path):
            os.remove(df_path)
```

## 7. 测试与验收清单

参考 `alpha/tests/test_compute_io_overlap_phase2.py` 中以下两个用例直接迁移至 `v2.0.2`：
1. `test_candle_file_manager_save_ready_eliminates_per_call_glob`：预置历史 `.ready`/`.ws_ready` 文件，连续 15 次调用 `save_ready`/`save_ws_ready`，monkeypatch `glob` 断言调用次数为 **0**，且磁盘仅保留最新时间戳标记。
2. `test_candle_file_manager_pickle_and_cross_process_writes_never_leak_ready_files`：模拟父进程索引后经 `pickle.loads(pickle.dumps(parent_mgr))` 跨多轮子进程写入，及 TTL 过期后父进程写入，断言无任何过期 `.ready`/`.ws_ready` 残留。
