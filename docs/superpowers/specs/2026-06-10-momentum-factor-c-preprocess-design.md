# 动量因子 C 扩展 + Preprocess 预计算设计文档

**日期**：2026-06-10  
**范围**：`factors/动量因子210401修正.py` → C 扩展加速 + 纳入 preprocess 预计算流程  
**目标**：全市场 × 多参数场景下，将因子计算从实盘/回测按需计算改为 preprocess 阶段预计算，结果存入 market_dict，同时通过 C 扩展大幅降低计算耗时。

---

## 1. 背景与需求

- **规模**：300+ 标的 × 5+ params，当前 Python 实现在每次回测/实盘启动时按需计算，延迟高
- **痛点**：`np.split` 产生大量小 DataFrame、pandas `ewm`/`rolling` 的对象开销、多次内存分配
- **目标**：① C 扩展提供最大计算速度；② 将结果预存入 market_dict，实盘直接读取

---

## 2. 整体架构

三层结构：

```
┌─────────────────────────────────────────────────────┐
│  C 共享库  factors/c_ext/momentum_core.c (.dll/.so) │
│  纯数值运算：EWM、rolling min/max、anomaly split     │
│  输入/输出：double* 指针，无 pandas 依赖              │
└───────────────────┬─────────────────────────────────┘
                    │ ctypes.CDLL（自动释放 GIL）
┌───────────────────▼─────────────────────────────────┐
│  Python 包装层  factors/c_ext/momentum_wrapper.py   │
│  ctypes 类型转换（ndarray → c_double*）              │
│  保持与现有 signal_multi_params 接口完全兼容          │
└───────────────────┬─────────────────────────────────┘
                    │
┌───────────────────▼─────────────────────────────────┐
│  Preprocess 集成  core/preprocess.py                │
│  新增 gen_factor_data()：market_dict 写完后运行      │
│  因子列（如 动量因子210401修正_168）写入同一 DF      │
│  PIVOT_COLUMNS 不含因子列，下游 pivot 不受影响        │
└─────────────────────────────────────────────────────┘
```

**文件结构**：

```
factors/
  c_ext/
    momentum_core.c        ← 算法实现
    momentum_core.h        ← 函数声明
    momentum_wrapper.py    ← ctypes 封装
    build.bat              ← Windows 一键编译
    momentum_core.dll      ← 预编译产物（git 跟踪）
    validate.py            ← Python/C 输出一致性验证
  动量因子210401修正.py    ← 改为调用 wrapper，signal_multi_params 接口不变
```

---

## 3. C 库 API 与算法

### 3.1 函数签名

```c
// factors/c_ext/momentum_core.h
void compute_momentum(
    const double* close,   // [len] 收盘价
    const double* high,    // [len] 最高价
    const double* low,     // [len] 最低价
    int    n,              // span 参数（如 168）
    int    len,            // 输入数组总长度
    int    trunc_len,      // 截取末尾条数（52560）
    double* out            // [len] 输出，前端 NaN 填充
);
```

### 3.2 算法：两阶段段落处理

**原则**：不实际切分数组，只记录 segment 边界索引，在预分配 output buffer 上原地操作，保证与 Python 逻辑一致、输出一致。

**Phase 1 — 预计算 + 异常检测**（对末尾 trunc_len 个点）：

| 步骤 | 操作 | C 实现 |
|------|------|--------|
| 计算 tp | `(close+high+low)/3`，tp[0]=tp[1] | 线性扫描 |
| high4min | `rolling(4).min` of high | 滑动窗口 |
| low4max | `rolling(4).max` of low | 滑动窗口 |
| pctchange | `(high4min - low4max) / low4max` | 逐元素 |
| rolling mean/std | Welford 在线算法，窗口 n | O(n) |
| is_anomaly | `abs(zscore)>25 & (pct>0.2 or pct<-0.5)` | 逐元素 |
| 收集 segments | anomaly 位置设 tp=NaN，记录边界 | O(n) |

**Phase 2 — 按 segment 处理**（per param n）：

```
for each segment [start, end):
  if length <= 168:
      out[start..end] = NaN
  else:
      reset: ewm_state, min_deque, max_deque, valid_count
      for i in [start, end):
          if tp[i] == NaN:
              out[i] = NaN            // anomaly 末位，EWM(NaN)=NaN
          else:
              ewm_val = α*tp[i] + (1-α)*ewm_val   // α = 2/(n+1)
              diff = tp[i] - ewm_val
              push diff → 单调双端队列（窗口 n）
              if valid_count >= 167:
                  out[i] = (diff - min) / (max - min)
              else:
                  out[i] = NaN        // min_periods=167 未满足
```

**Phase 3 — 叠加长窗口分量**（全量 trunc_len，不分段）：

```
max_high = rolling(4368, min_periods=1).max of high4min   // 单调队列
min_low  = rolling(4368, min_periods=1).min of low4max    // 单调队列
out[i] += (tp[i] - min_low[i]) / (max_high[i] - min_low[i])
// tp[i]=NaN 处自动 NaN，与 Python 一致
```

**关键优化**：
- 所有 rolling min/max 使用单调双端队列，O(n) 时间复杂度
- 整个 symbol 仅 2 次数组遍历（Phase 1 + Phase 2+3 合并）
- 无堆内存分配（output buffer 由 Python 预分配传入）

### 3.3 一致性验证

`validate.py` 对相同输入分别运行 Python 原版和 C 版：

```python
assert np.allclose(py_out, c_out, atol=1e-12, equal_nan=True)
```

---

## 4. Preprocess 集成

### 4.1 新增配置（`config.py`）

```python
# 预计算因子配置：{因子模块名: [param 列表]}
precompute_factor_configs = {
    '动量因子210401修正': [168, 336]
}
```

### 4.2 集成点

```
preprocess_type_data():
    gen_batch_data_incremental × N batches   ← 现有
    gen_factor_data()                        ← 新增（本设计）
    gen_pivot_incremental()                  ← 现有，不受影响
```

### 4.3 并行策略

ctypes 调用 C 函数自动释放 GIL，`ThreadPoolExecutor` 实现真正 CPU 并行。

```python
def gen_factor_data(handle, run_time, trade_type):
    for batch_name in all_factor_batch_names(trade_type):
        market_dict = preprocess_mgr.read_candle(batch_name)

        def _calc(item):
            symbol, df = item
            for factor_name, param_list in precompute_factor_configs.items():
                cols = call_c_wrapper(factor_name, df, param_list)
                df = pd.concat([df, cols], axis=1)
            return symbol, df

        max_workers = min(len(market_dict), 16)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            updated = dict(pool.map(_calc, market_dict.items()))

        preprocess_mgr.set_candle(batch_name, run_time, updated)
```

**两级并行**：
- Level 1（主）：symbol 级，ThreadPoolExecutor，GIL 在 C 层释放
- Level 2（次）：param 级，同一 symbol 多 param 在 C 内顺序处理（Phase 1 pctchange 复用，Phase 2 per param）

### 4.4 增量行为

- EWM 有状态，必须全量重算（无法仅追加新 bar）
- `gen_batch_data_incremental` 已跳过无更新 symbol，`gen_factor_data` 每次处理所有 batch（EWM 必须全量重算，无法判断哪些 batch "有变化"而跳过）
- 单个 symbol 因子计算耗时约 1ms（C 版），全市场 300 标的 × 并行 ≈ 可接受

### 4.5 实盘降级

- 实盘/回测读取 market_dict 时因子列已存在，直接使用
- 若列缺失，`factor_calc.py` 的 `MultiFactorCalculator` 照常调 `signal_multi_params` 降级重算
- `动量因子210401修正.py` 的 `signal_multi_params` 接口不变，内部改为调用 C wrapper

---

## 5. 构建系统

### 5.1 编译（Windows，conda alpha311 环境）

**`factors/c_ext/build.bat`**：

```bat
@echo off
conda run -n alpha311 gcc -O3 -march=native -shared -fPIC ^
    -o factors/c_ext/momentum_core.dll ^
    factors/c_ext/momentum_core.c
echo Build done.
```

MSVC 回退：

```bat
cl /O2 /LD factors\c_ext\momentum_core.c /Fe:factors\c_ext\momentum_core.dll
```

### 5.2 预编译产物

`momentum_core.dll` 提交至 git，免去他人安装编译器。需要重新编译时运行 `build.bat`。

---

## 6. 实施顺序

1. 实现 `momentum_core.c` + `momentum_core.h`
2. 编译验证，运行 `validate.py` 确认输出一致
3. 实现 `momentum_wrapper.py`，更新 `动量因子210401修正.py` 调用 wrapper
4. 在 `config.py` 添加 `precompute_factor_configs`
5. 在 `core/preprocess.py` 实现 `gen_factor_data()`，接入 `preprocess_type_data`
6. 端到端测试：运行一次完整 preprocess，确认因子列写入 market_dict，实盘读取正确

---

## 7. 边界条件与风险

| 风险 | 处理 |
|------|------|
| C/Python 浮点输出不一致 | validate.py `atol=1e-12` 强制验证，不通过不接入 |
| .dll 平台差异（Linux 部署） | build.bat 对应提供 build.sh，输出 .so |
| preprocess 耗时增加 | 并行抵消，监控日志记录 gen_factor_data 耗时 |
| 新增因子列使 market_dict 文件变大 | 可接受；PIVOT_COLUMNS 不含因子列，pivot 不受影响 |
| EWM 全量重算 | C 版 ~1ms/symbol，可接受 |
