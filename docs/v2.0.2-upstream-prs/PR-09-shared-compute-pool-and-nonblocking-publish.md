# PR-09: 规格自适应共享 `compute_pool`（预热 + `.ready` 后立即释放）与内存门控非阻塞 `data_api` 发布

* **建议分支**：`perf/shared-compute-pool-and-nonblocking-publish`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：建议在 PR-04、PR-06 之后合入
* **参考源提交（`alpha`）**：`33d0854`、`ddd81eb`、`baa6e94`

---

## 1. 场景

在 `v2.0.2` 的 WS 更新周期中：
1. `br_exe` 并发对 `spot` 和 `swap` 执行 `_barrier_and_resample()`，两者各自在 `update_resample_from_base()` 内创建临时 `ProcessPoolExecutor(max_workers=4, mp_context="spawn")`，瞬间同时拉起 **8 个 spawn Python 子进程**；
2. 当同时开启 `use_api['kline'] = True`（Data API 兜底）时，主流程必须死等慢速的 `task_data_api.result()` 完成后才启动 `preprocess`，拖慢整轮发布。

## 2. 目标

1. **彻底消灭 8-spawn 内存峰并按硬件规格分级调度**：由 `RuntimeSupervisor` 统一托管周期内 `compute_pool`：
   * **`4c8g+`**：启用 4-worker 共享进程池，`T-22s` 调用 `warm_compute_pool()` 提前预导入 `numpy`/`pandas` 隐藏冷启动，`.ready` 发布后立刻 `release_compute_pool(wait=False)` 归还全部子进程匿名堆页；
   * **`2c8g`**：自动降为 2-worker 共享进程池（避免 4 进程在 2 核上互抢），发布后立即释放；
   * **`2c4g`（总物理内存 `< 6 GiB`）**：自动关闭多进程 `compute_pool` 与 `T-22s` 提前预热（`get_compute_pool()` 返回 `None`，`update_resample_from_base` 平滑走进程内 `ThreadPoolExecutor(max_workers=2)`），直接省去 `300~500 MB` spawn 空壳内存。
2. **内存门控的非阻塞 `data_api` 发布**：引入无状态的 `choose_publication_inputs()`，在内存充裕（`>= 6 GiB`）且 Binance 100% 覆盖当轮全市场时，立即启动 `preprocess` 不再苦等慢速 `data_api`；在 `2c4g` 上等待 `task_data_api` 完成后再启动 `preprocess`，避免双子进程并发压穿 4 GB 内存。

## 3. 问题（v2.0.2 现状与根因）

1. **双市场 8-spawn 峰值爆炸**（`v2.0.2/core/pipeline/update_cycle.py:265` + `core/pipeline/resample.py:322`）：
   * `update_cycle.py` 用 2 线程并发调用 `update_resample_from_base('spot')` 与 `update_resample_from_base('swap')`；
   * `resample.py` 每次都执行 `with ProcessPoolExecutor(max_workers=4, mp_context=mp.get_context("spawn"))`，瞬间并行 spawn 8 个重型 Python 进程，冷启动导入 `pandas` 耗时 ~3–4s，在 `4c8g` 触发 Swap，在 `2c4g` 直接 OOM。
2. **串行阻塞在 `task_data_api`**（`v2.0.2/core/pipeline/update_cycle.py:283-292`）：
   * 即便 Binance WS 已经 100% 拿齐并重采样完所有交易对，仍必须等 `data_api` 轮询下载完成后才能开始 `preprocess`。

## 4. 项目文档中的分析与实测证据

* **预热消除 spawn 冷启动**：`alpha/docs/2026-10-31-dispatch-prewarm.md` 在真实 Windows spawn 环境下实测，冷启动 3 worker 晚启动 `3.957 / 3.967 / 3.977 s`，提前在元数据空窗期预热后 3 个独立 PID 晚启动降至 **`0.002256 s`（2.3 ms）**。
* **发布后释放计算池的内存收益**：`alphaonline/docs/2026-11-25-prod-vs-test-comparison.md`（§5）与 `2026-11-27-latest-round-resource-profile.md`（§3.2）对比：
  * 发布后立即销毁计算池并释放内存池的测试机，空闲常驻内存迅速回落至 **`254.6 MB`**（计算期 `2.0 GB` → 空闲 `254.6 MB`），轮内 `MemAvailable` 地板保持 **`4,218 MB`**；
  * 对比长驻未回收的老旧生产者进程（22 天未重启），单进程/子进程堆碎片逐批膨胀至 **`4.3 → 5.1 GB`**。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

### 5.1 硬件规格局限与分级策略
* **`4c8g+`**：全套收益最高——消灭 8-spawn，4 worker 吃满 4 核，`T-22s` 隐藏冷启动，`.ready` 后瞬间归还堆页，非阻塞 `data_api` 显著缩短发布延迟。
* **`2c8g`**：`compute_workers` 必须降为 `2`（多开只会上下文切换）；非阻塞 `data_api` 在 2 核上若 `data_api` 与 `preprocess` 同时满载会有轻度抢核，可保留配置开关。
* **`2c4g`（关键避坑）**：
  * 每个导入 `numpy`/`pandas` 的 `spawn` worker 空壳占 `100–160 MiB`，计算期达 `300–500 MiB`。提前 22 秒拉起 2~4 个 worker 并叠加 `2 Writer + 2 ws_runner + main`，再并发跑 `task_data_api` + `preprocess`，会直接把 4 GB 机器打入重度 Swap 或 OOM。
  * 因此在 `total_mem < 6 GiB` 时自动返回 `compute_pool = None`（退回零额外进程开销的 `ThreadPoolExecutor(max_workers=2)`，并关闭 `T-22s` 预热与非阻塞 `data_api` 并发）。

### 5.2 强依赖 PKL 消费端评估（价值显著放大）
* **`.ready` 发布后立刻 `release_compute_pool` 对 PKL 消费端比 Arrow 场景更关键**：
  * Arrow mmap 消费端与生产端共享页缓存、私有堆增长温和；
  * **PKL 消费端**在看到 `.ready` 标志的下一秒，会立刻把数百 MB~数 GB 的 `*_dict_batch*.pkl` 反序列化到自身私有堆（瞬时膨胀 `1.5–3.0 GB`）。
  * 生产端在 `.ready` 发出的瞬间调用 `release_compute_pool(wait=False)` 销毁计算子进程，让 Linux 立刻回收 `1–2 GB` 匿名堆页，恰好错开双端内存尖峰，大幅降低同机 PKL 消费端 Swap/OOM 概率。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/pipeline/runtime_supervisor.py`
* 新增内存探测辅助 `_total_memory_bytes()` 与顶层预热函数 `_warm_compute_worker()`（预导入 `numpy`/`pandas`）。
* `RuntimeSupervisor` 新增规格自适应 `compute_workers`（`total_mem < 6 GiB` 时为 `0`，即禁用多进程池；否则为 `min(4, max(1, os.cpu_count() or 2))`，支持显式传参覆盖）：

```python
_MIN_COMPUTE_POOL_MEM_BYTES = 6 * 1024 * 1024 * 1024  # 6 GiB

def _total_memory_bytes() -> Optional[int]:
    try:
        import psutil
        return int(psutil.virtual_memory().total)
    except Exception:
        pass
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    return None

def _warm_compute_worker() -> int:
    import numpy  # noqa: F401
    import pandas  # noqa: F401
    return os.getpid()
```

* `get_compute_pool()` / `warm_compute_pool()` 在 `self.compute_workers <= 0`（`2c4g`）时直接返回 `None`（不 spawn 任何计算子进程）；在 `>= 1` 时创建并复用池，异常 `_broken=True` 时自动重建；`release_compute_pool(wait=False)` 在 `.ready` 发布后立即销毁回收；`shutdown()` 兜底 `release_compute_pool(wait=True)`。

### 6.2 `core/pipeline/resample.py`
* `update_resample_from_base(handle, trade_type, run_time, *, executor=None)`：
  * 传入共享 `executor`（`4c8g`/`2c8g`）时，按 `max(4, pool_workers * 2)` 切块提交到共享池真多核并行；
  * `executor is None`（`2c4g` 或单测）时回退 `ThreadPoolExecutor(max_workers=2)`，彻底移除 `v2.0.2` 原有的临时 `ProcessPoolExecutor(max_workers=4)`（消灭 8-spawn）。

### 6.3 `core/pipeline/publication_sources.py`（新增纯函数模块）
* 提供 `choose_publication_inputs(handle, run_time, binance_result, data_api_result, *, binance_done, data_api_done, confirmed_empty=None)`：
  * 检查每个 symbol 的 `(first, last)` 边界是否覆盖 `expected = run_time - resample_delta`；
  * Binance 优先，仅缺失币种才回退 `data_api`；
  * 仅允许“明确返回空 DataFrame（含 WS `confirmed_empty`）且本地无历史文件”的新币跳过，绝不掩盖成熟币抓取失败。

### 6.4 `core/app.py` & `core/pipeline/update_cycle.py`
* `core/app.py`：启用 offset 在 `T-22s` 唤醒后、调用 `update_meta` 之前调用 `supervisor.warm_compute_pool()`（`2c4g` 下为纯 no-op）。
* `core/pipeline/update_cycle.py`：
  * `run_ws_cycle` 获取 `compute_pool = supervisor.get_compute_pool()` 传给 `_barrier_and_resample`；
  * 当系统内存 `>= 6 GiB`（且允许并发）时，使用 `choose_publication_inputs()` 在 Binance 100% 覆盖时立刻触发 `_run_preprocess`（不等待 `task_data_api`）；在 `2c4g`（`< 6 GiB`）上若启用了 `task_data_api`，先 `task_data_api.result()` 收尾释放其子进程内存，再运行 `_run_preprocess`；
  * `finally` 中立刻调用 `supervisor.release_compute_pool(wait=False)` 归还匿名堆页。

## 7. 测试与验收清单

参考 `alpha/tests/test_producer_memory_hygiene.py`、`tests/test_2b_ws_cycle_and_app.py`、`tests/test_parallel_pipeline_phase1.py` 并补充低配退回单测：
1. `test_supervisor_release_and_warm_compute_pool_lifecycle`：验证 `warm_compute_pool` 幂等预热、`release_compute_pool` 销毁清空引用、下次 `get_compute_pool` 重建新池及 `_broken=True` 自愈。
2. `test_supervisor_disables_compute_pool_on_low_memory_4gb`：模拟 `total_mem = 4 GiB`，验证默认 `compute_workers == 0`、`warm_compute_pool()` 与 `get_compute_pool()` 均返回 `None` 且不 spawn 子进程。
3. `test_ws_cycle_releases_compute_pool_and_app_warms_at_t_minus_15s`：验证执行顺序 `warm_pool < update_meta < run_ws_cycle` 且 `preprocess_done < release_pool`。
4. `test_ws_cycle_launches_preprocess_without_blocking_on_slow_data_api`：在 `>= 6 GiB` 条件下模拟慢速 `data_api` 阻塞在事件门控上，断言 Binance 全覆盖时 `preprocess` 立即启动不被阻塞；在 `4 GiB` 条件下先收尾 `data_api` 再启动 `preprocess`。
5. `test_publication_sources_confirmed_empty_allows_unopened_listing_and_rejects_rest_failure`：验证未开盘新币允许跳过、REST 失败或成熟币缺数据严格抛 `ValueError` 阻断半成品发布。
