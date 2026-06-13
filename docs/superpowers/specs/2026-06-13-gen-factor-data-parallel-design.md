# gen_factor_data 并行重构 — 设计文档

**Date:** 2026-06-13

## Goal

重构 `core/preprocess.py` 中的 `gen_factor_data`，消除两个串行瓶颈：

1. **batch 间串行**：当前每个 batch 必须等上一个 batch 完成（compute + write）才能开始
2. **因子间串行**：同一 symbol 的三个因子（动量、ILLQ、CCI）顺序调用，而它们完全独立

同步调整 `config.py` 中的 `preprocess_num_batch`，适配 4-core 8GB Linux 服务器。

回归测试用真实数据（7200 行 BTCUSDT candle fixture）确保重构前后输出逐值一致。

---

## 生产环境数据规模

| 指标 | Spot | Swap |
|---|---|---|
| Symbol 数量 | 422 | 625 |
| 当前 batch 大小 | 128 | 128 |
| 当前 batch 数 | 4 | 5 |
| 推荐 batch 大小 | 256 | 256 |
| 推荐 batch 数 | 2 | 3 |
| 因子数 | 3（动量/ILLQ/CCI）| 同左 |

---

## 当前并行拓扑（重构前）

```
preprocess_offset_data
├─ [Thread-spot]  gen_factor_data         ← ThreadPoolExecutor(max_workers=2) 已存在
│     while batch1..batch4 (sequential):
│         read_candle  →  ThreadPoolExecutor(symbols, 4 workers)
│                              _calc_symbol_factors:
│                                  for 动量, ILLQ, CCI  ← 串行
│         set_candle (blocking I/O)
└─ [Thread-swap]  gen_factor_data
      while batch1..batch5 (sequential): ...
```

**瓶颈：**
- Spot 4 次 batch 串行循环 × (compute + write)
- Swap 5 次 batch 串行循环 × (compute + write)
- 每个 symbol 内 3 个因子串行，工作单元粒度粗
- 每次 batch 创建/销毁 ThreadPoolExecutor

---

## 重构后并行拓扑

```
preprocess_offset_data
├─ [Thread-spot]  gen_factor_data (new)
│     load all batches into memory  ← sequential, fast I/O
│     flatten to (symbol, factor) work units: 422×3 = 1266 units
│     ThreadPoolExecutor(max_workers=FACTOR_WORKERS)  ← single pool
│         worker: _calc_one_factor(symbol, df, module, factor_name, param_list)
│     merge results per symbol, write all batches  ← sequential
└─ [Thread-swap]  gen_factor_data (new)
      flatten: 625×3 = 1875 units
      ThreadPoolExecutor(max_workers=FACTOR_WORKERS)
```

**核心变化：**

| 维度 | 重构前 | 重构后 |
|---|---|---|
| batch 间 | 串行（compute+IO 交替等待）| 一次性加载，一次性写回 |
| 因子间 | 每 symbol 内串行 | 独立 (symbol, factor) 工作单元，跨 symbol 和因子并行 |
| 线程池 | 每 batch 创建一次（最多 5 次）| 单一池，全程复用 |
| 工作单元粒度 | 1 symbol × 3 factors = 粗 | 1 symbol × 1 factor = 细 |

---

## 4-core 8GB Linux 参数推荐

### `preprocess_num_batch` 改为 256

`num_batch` 控制 `gen_batch_data_incremental` 的 K 线加载批次大小，对重构后的因子计算阶段无直接影响（因子阶段一次性加载所有 batch）。

| num_batch | spot batch 数 | swap batch 数 | 总循环次数（加载阶段）| 每 batch 内存峰值 |
|---|---|---|---|---|
| 128（当前）| 4 | 5 | 9 | ~140 MB |
| **256（推荐）**| **2** | **3** | **5** | ~280 MB |

减少 44% 的加载循环迭代，内存峰值 280 MB 远低于 8GB 限制。

### `FACTOR_WORKERS` = `os.cpu_count() or 4` = 4

Spot 和 Swap 各自在独立线程中运行（`preprocess_offset_data` 的 2-worker 池），两者并发时各用 4 workers = 共 8 个 GIL-released 线程在 4 core 上。所有三个因子均为 cffi C 扩展，GIL 在 C 调用期间释放，Linux 内核可真正并发调度。

**内存峰值估算（因子计算阶段）：**

| 项目 | 估算 |
|---|---|
| 每 symbol DataFrame（7200行×~26列×8B）| ~1.5 MB |
| Spot 全量（422×1.5MB）| ~633 MB |
| Swap 全量（625×1.5MB）| ~938 MB |
| Spot+Swap 并发驻留 | ~1.57 GB |
| OS + Python + 其他 | ~1.5 GB |
| **合计峰值** | **~3.1 GB** ✅ 8GB 安全 |

---

## 文件修改范围

| 文件 | 变更 | 范围 |
|---|---|---|
| `core/preprocess.py` | 新增 `_calc_one_factor`；重构 `gen_factor_data`；`_calc_symbol_factors` 可保留（已被 module5 测试引用） | ~40 行 |
| `config.py` | `preprocess_num_batch: 128 → 256` | 1 行 |
| `tests/test_module7_gen_factor_parallel.py` | 新增测试文件 | 新建 |
| `tests/support/module7_harness.py` | 多 symbol、多 batch、多因子的 harness | 新建 |

`realtime_data.py`、`factors/` 目录、所有因子文件：**不需要改动**。

---

## 新接口

### `_calc_one_factor`（新增）

```python
def _calc_one_factor(
    item: tuple[str, pd.DataFrame],
    module,
    factor_name: str,
    param_list: list,
) -> tuple[str, str, dict[str, pd.Series]]:
    """Worker: compute one factor for one symbol. Returns (symbol, factor_name, {param_key: Series})."""
    symbol, df = item
    raw = module.signal_multi_params(df, param_list)
    return symbol, factor_name, raw
```

### `gen_factor_data`（重构后逻辑）

```python
def gen_factor_data(handle, run_time, trade_type):
    # 1. 读 config（不变）
    # 2. 一次性加载所有 batch 到 all_batches: dict[batch_key, market_dict]
    # 3. 构建扁平工作单元: [(symbol, df, module, factor_name, param_list), ...]
    # 4. 单一 ThreadPoolExecutor(max_workers=FACTOR_WORKERS) 处理所有单元
    # 5. 按 (batch_key, symbol) 归并结果，df.assign(**factor_cols)
    # 6. 顺序 set_candle 写回各 batch
```

---

## 测试策略

### 现有测试（不改动）

`tests/test_module5_gen_factor_data.py` 的 5 个测试全部继续通过——它们已经测试了 `gen_factor_data` 的功能正确性（单 symbol、单 batch、golden 比对）。重构后这些测试仍然是回归保护。

### 新增测试：`tests/test_module7_gen_factor_parallel.py`

测试目标：在**多 symbol + 多 batch + 多因子**场景下，重构后的输出与重构前逐值一致。

**真实数据基础：** 复用 `tests/fixtures/module6/input_btcusdt_candles.pkl`（7200 行 BTCUSDT candle）。构造多 symbol 场景时，对同一 DataFrame 做轻微扰动（不同 offset）创造 N 个不同 symbol 的数据，确保各 symbol 结果可独立比对。

**测试清单（4 个）：**

| 测试 | 场景 | 断言 |
|---|---|---|
| `test_parallel_single_batch_multi_symbol` | 1 batch，8 symbols，3 factors（动量+ILLQ+CCI）| 每个 symbol 每个因子列值与串行参考实现逐值一致（`assert_array_equal`） |
| `test_parallel_multi_batch_multi_symbol` | 3 batches，各 8/8/4 symbols，3 factors | 同上，验证跨 batch 结果正确合并写回 |
| `test_parallel_column_completeness` | 2 batches，4 symbols，3 factors，2+1+1=4 params | 每个 symbol 的 DataFrame 包含所有 `{factor}_{param}` 列，无遗漏无重复 |
| `test_parallel_idempotent` | 2 batches，4 symbols，运行两次 | 第二次结果与第一次完全相同（幂等性） |

**参考实现（串行基准）：** harness 中内置 `reference_gen_factor_data` 函数，使用原始 `_calc_symbol_factors` 逻辑（不依赖 `gen_factor_data` 实现），作为独立基准对比 C 扩展的并行输出。

**Golden 文件：** 不需要新增 `.npy` golden——直接用串行参考实现在运行时计算基准，与并行结果比对。这样测试不依赖预提交的 golden，也不受 golden 过期问题影响。

---

## 不改动的内容

- `_calc_symbol_factors`：保留原函数（module5 测试 harness 未直接引用，但保留避免破坏潜在引用）
- `preprocess_type_data` 的外层 ThreadPoolExecutor（spot/swap 并发）：不变
- 所有 `factors/` 模块
- `realtime_data.py`
