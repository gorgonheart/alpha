# PR-09: 周期内共享 `compute_pool` 生命周期（预热 + 发布后释放）与非阻塞 `data_api` 发布

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

1. **消灭每轮 8-spawn 内存峰与冷启动延迟**：由 `RuntimeSupervisor` 统一托管周期内复用的 `compute_pool`（默认 `min(4, cpu_count)` 个 worker）：
   * 启用 offset 在 `T-22s` 空窗期调用 `warm_compute_pool()` 提前 spawn 并预导入 `numpy`/`pandas`，将冷启动完全隐藏在整点之前；
   * `update_resample_from_base` 复用该 `compute_pool` 并发分块重采样，不再各自临时创建进程池；
   * 预处理完成（`.ready` 发布）后立即调用 `release_compute_pool(wait=False)` 销毁子进程，让 OS 100% 回收子进程 pymalloc/glibc 匿名堆页，避免跨轮堆碎片常驻；
   * 支持 worker 异常崩溃（`_broken=True`）自动重建自愈。
2. **非阻塞 `data_api` 裁决发布**：引入无状态的 `choose_publication_inputs()`，在 Binance 100% 覆盖当轮全市场时（含已确认未开盘空新币），立即锁定每币数据源启动 `preprocess`，不再苦等慢速的 `data_api`；仅当 Binance 覆盖不全且启用了 `data_api` 时才等待 `data_api` 做二次裁决。

## 3. 问题（v2.0.2 现状与根因）

1. **双市场 8-spawn 峰值爆炸**（`v2.0.2/core/pipeline/update_cycle.py:265` + `core/pipeline/resample.py:322`）：
   * `update_cycle.py` 用 2 线程并发调用 `update_resample_from_base('spot')` 与 `update_resample_from_base('swap')`；
   * `resample.py` 每次都执行 `with ProcessPoolExecutor(max_workers=4, mp_context=mp.get_context("spawn"))`，在 4 核 8 GB 机器上瞬间并行 spawn 8 个重型 Python 进程，冷启动导入 `pandas` 耗时 ~3–4s，且内存叠加诱发 OOM/Swap。
2. **串行阻塞在 `task_data_api`**（`v2.0.2/core/pipeline/update_cycle.py:283-292`）：
   ```python
   if task_data_api is not None:
       if task_data_api.result() is not None:
           result.is_data_api_success = True
   ...
   if result.is_binance_success or result.is_data_api_success:
       _run_preprocess(...)
   ```
   即便 Binance WS 已经 100% 拿齐并重采样完所有交易对，仍必须等 `data_api` 轮询下载完成后才能开始 `preprocess`。

## 4. 项目文档中的分析与实测证据

* **预热消除 spawn 冷启动**：`alpha/docs/2026-10-31-dispatch-prewarm.md` 在真实 Windows spawn 环境下实测，冷启动 3 worker 晚启动 `3.957 / 3.967 / 3.977 s`，提前在元数据空窗期预热后 3 个独立 PID 晚启动降至 **`0.002256 s`（2.3 ms）**。
* **发布后释放计算池的内存收益**：`alphaonline/docs/2026-11-25-prod-vs-test-comparison.md`（§5）与 `2026-11-27-latest-round-resource-profile.md`（§3.2）对比：
  * 发布后立即销毁计算池并释放内存池的测试机，空闲常驻内存迅速回落至 **`254.6 MB`**（计算期 `2.0 GB` → 空闲 `254.6 MB`），轮内 `MemAvailable` 地板保持 **`4,218 MB`**；
  * 对比长驻未回收的老旧生产者进程（22 天未重启），单进程/子进程堆碎片逐批膨胀至 **`4.3 → 5.1 GB`**。

## 5. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 5.1 `core/pipeline/runtime_supervisor.py`
* 新增顶层预热函数 `_warm_compute_worker()`（预导入 `numpy`/`pandas`）。
* `RuntimeSupervisor` 新增 `compute_workers` 配置与 3 个生命周期方法（并在 `shutdown()` 中调用 `release_compute_pool(wait=True)`）：

```python
def _warm_compute_worker() -> int:
    import numpy  # noqa: F401
    import pandas  # noqa: F401
    return os.getpid()

class RuntimeSupervisor:
    COMPUTE_WORKERS = 4
    ...
    def get_compute_pool(self) -> ProcessPoolExecutor:
        with self._compute_pool_lock:
            broken = getattr(self._compute_pool, "_broken", False) if self._compute_pool is not None else False
            if self._compute_pool is not None and broken:
                try:
                    self._compute_pool.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                self._compute_pool = None
                self._compute_pool_warmed = False
            if self._compute_pool is None:
                self._compute_pool = ProcessPoolExecutor(
                    max_workers=self.compute_workers,
                    mp_context=self.ctx,
                )
                self._compute_pool_warmed = False
            return self._compute_pool

    def warm_compute_pool(self) -> ProcessPoolExecutor:
        with self._compute_pool_lock:
            pool = self._ensure_compute_pool_locked()
            if not self._compute_pool_warmed:
                for _ in range(self.compute_workers):
                    pool.submit(_warm_compute_worker)
                self._compute_pool_warmed = True
            return pool

    def release_compute_pool(self, *, wait: bool = False) -> bool:
        with self._compute_pool_lock:
            pool = self._compute_pool
            if pool is None:
                return False
            self._compute_pool = None
            self._compute_pool_warmed = False
        try:
            pool.shutdown(wait=wait, cancel_futures=True)
        except Exception:
            pass
        return True
```

### 5.2 `core/pipeline/resample.py`
* `update_resample_from_base(handle, trade_type, run_time, *, executor=None)`：
  * 传入共享 `executor`（即 `supervisor.get_compute_pool()`）时，按 `max(4, pool_workers * 2)` 切块提交到共享池真多核并行；
  * 未传 `executor` 时回退 `ThreadPoolExecutor(max_workers=2)`（不再擅自 spawn 4 进程，确保单测与无 supervisor 调用零额外进程开销）。

### 5.3 `core/pipeline/publication_sources.py`（新增纯函数模块）
* 提供 `choose_publication_inputs(handle, run_time, binance_result, data_api_result, *, binance_done, data_api_done, confirmed_empty=None)`：
  * 检查每个 symbol 的 `(first, last)` 边界是否覆盖 `expected = run_time - resample_delta`；
  * Binance 优先，仅缺失币种才回退 `data_api`；
  * 仅允许“明确返回空 DataFrame（含 WS `confirmed_empty`）且本地无历史文件”的新币跳过，绝不掩盖成熟币抓取失败。

### 5.4 `core/app.py` & `core/pipeline/update_cycle.py`
* `core/app.py`：启用 offset 在 `T-22s` 唤醒后、调用 `update_meta` 之前先调用 `supervisor.warm_compute_pool()`，使进程 spawn/import 与网络 I/O 并行重叠。
* `core/pipeline/update_cycle.py`：
  * `run_ws_cycle` 获取 `compute_pool = supervisor.get_compute_pool()` 传给 `_barrier_and_resample`；
  * 用 `choose_publication_inputs()` 实现 Binance 100% 覆盖时立刻触发 `_run_preprocess`（不等待 `task_data_api`），并在 `finally` 中立刻调用 `supervisor.release_compute_pool(wait=False)`，随后再收尾等待后台 `task_data_api` 完成。

## 6. 测试与验收清单

参考 `alpha/tests/test_producer_memory_hygiene.py`、`tests/test_2b_ws_cycle_and_app.py`、`tests/test_parallel_pipeline_phase1.py`：
1. `test_supervisor_release_and_warm_compute_pool_lifecycle`：验证 `warm_compute_pool` 幂等预热、`release_compute_pool` 销毁清空引用、下次 `get_compute_pool` 重建新池及 `_broken=True` 自愈。
2. `test_ws_cycle_releases_compute_pool_and_app_warms_at_t_minus_15s`：验证执行顺序 `warm_pool < update_meta < run_ws_cycle` 且 `preprocess_done < release_pool`。
3. `test_ws_cycle_launches_preprocess_without_blocking_on_slow_data_api`：模拟慢速 `data_api` 阻塞在事件门控上，断言 Binance 全覆盖时 `preprocess` 立即启动不被阻塞。
4. `test_publication_sources_confirmed_empty_allows_unopened_listing_and_rejects_rest_failure`：验证未开盘新币允许跳过、REST 失败或成熟币缺数据严格抛 `ValueError` 阻断半成品发布。
