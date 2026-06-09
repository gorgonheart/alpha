# 增量计算改造实施计划

## Context

BMAC 当前在每 5 分钟的更新周期内，对 resample K 线合并、资金费率更新、pivot 矩阵生成、market dict 生成等操作进行全量重算（14,050 行），而实际每周期只新增 1-19 行数据。目标是将这四个模块改为增量计算，只处理新增数据，在保证正确性的前提下大幅降低每周期 CPU 和 I/O 开销。

## 改造模块一览

| # | 模块 | 文件 | 核心函数 | 改动量 | 节省比 |
|---|------|------|---------|:---:|:---:|
| 1 | Resample K 线合并 | `core/resample_candle.py` | `update_resample_candle()` | ~60行 | ~470x |
| 2 | 资金费率更新 | `core/candle_manager.py` | `update_candle()` | ~25行 | ~10,000x |
| 3 | Market Dict 生成 | `core/preprocess.py` | `gen_batch_data()` | ~50行 | ~14,050x |
| 4 | Pivot 滚动窗口 | `core/preprocess.py` | `gen_pivot()` | ~70行 | ~14,050x |

总计改动 3 个文件，~205 行，无新增外部依赖。

## 统一验证前置（所有模块强制执行）

1. **冻结旧代码基线** — 在修改每个模块前，先保存旧实现快照，形成 `baseline_*` callable。由于当前目录不是 Git 仓库，**不能将 Git cache 作为唯一旧代码来源**；基线必须在本次改造开始前就地冻结，供后续 benchmark 直接调用。

2. **固定真实输入样本** — 所有验证均从 `data/` 目录读取历史真实输入，构造 `old_state + incremental_input`：
   - 模块 1：`binance_*_1h_resample` / `data_api_*_1h_resample` 对应 offset 的真实 resample 输入
   - 模块 2：`funding/`、`coin_cap/` 与其他 `update_candle()` 调用方真实文件
   - 模块 3：`preprocess_1h_resample/*/spot_dict_batch*.pkl`、`swap_dict_batch*.pkl` 上游真实输入
   - 模块 4：`preprocess_1h_resample/*/market_pivot_*` 与 batch market_dict 真实输入

3. **统一 old-vs-new 验证契约** — 对每个模块都执行：
   - 调用 `baseline_X(old_state, incremental_input)` 得到 `old_output`
   - 调用 `new_X(old_state, incremental_input)` 得到 `new_output`
   - 断言输出类型、结构、排序、索引、列集合、key 集合完全一致
   - DataFrame 输出逐列逐值比较；`dict[str, DataFrame]` / 嵌套 dict 递归比较
   - 首个 diff 必须打印具体位置（symbol / key / row / column）

4. **统一 benchmark 契约** — 所有 benchmark 使用 `time.perf_counter()`：
   - 预热 1-3 次，不计入结果
   - 正式重复运行 10 次，记录 old/new 的 mean 与 p95
   - **硬门槛：`new_mean <= old_mean`，否则失败**
   - 如遇明显 I/O 抖动，需在同机、同输入、无并发干扰环境复测，不允许直接放宽标准

5. **统一输出格式** — 每个模块都产出一份验证记录：样本名称、输入窗口、旧/新耗时、是否一致、首个 diff（如有）、结论。

---

## 模块 1：Resample K 线合并 (`update_resample_candle`)

**文件：** `core/resample_candle.py`
**改动量：** ~60 行
**依赖顺序：** 独立

### 实施步骤

1. **读取已有数据** — `candle_mgr.has_symbol(symbol)` 判断是否新币。新币或无旧数据 → 调用 `_full_update()`（现有逻辑）。

2. **拆分新数据** — 将 `df_resample` 按 `last_old_time` 拆分为重叠区（`<= last_old_time`）和纯新增区（`> last_old_time`）。

3. **OHLCV 修正检测** (`_detect_corrections()`) — 取旧数据中与重叠区时间戳交集的行，逐值对比 `['open','high','low','close','volume']`。任何差异 → 降级 `_full_update()` 并记录日志。

4. **增量路径** (`_incremental_update()`)：
   - **overlap_window 不作为写死常量**。默认实现可从旧数据尾部截取一段工作区，但窗口大小必须由 benchmark 在真实样本上验证后确定；`15` 只能作为初始候选值，不能作为计划中的正确性前提。
   - 工作区 = `df_old.tail(overlap_window)` + 纯新数据，只在该工作区上执行 `drop_duplicates` + `sort_values`
   - `pre_close` 从工作区首条之前一根旧 K 线的 `close` 获取，传给 `incremental_fill_gaps()` 作为 ffill 起点
   - `incremental_fill_gaps()` 只检查工作区内 gap，并保证新旧交界处填充连续
   - 拼接：`df_old.iloc[:-overlap_window]` + 处理后的工作区 → `filter_gaps()` → `tail(num_candles)` 后写盘
   - 如 benchmark 发现某类真实样本需要更大窗口，直接上调窗口，不追求最小常数

5. **保持 `resample_history()` 不变** — 启动时全量 resample 走原逻辑，不受影响。

6. **保持函数签名不变** — 调用方 `binance_update.py` 和 `data_api_update.py` 无需修改。

### 关键边界处理
- **新币首次出现：** 走 `_full_update()`
- **Binance 修正检测开销：** <0.1ms（重叠区通常 3-5 行）
- **fill_gaps 边界：** `pre_close` 从旧数据获取，保证 ffill 连续性
- **Benchmark 必测场景：** 无 gap、小 gap 需 ffill、大 gap 触发 `filter_gaps()`、重叠区完全一致、重叠区存在修正后自动降级全量
- **断言粒度：** 不只检查关键列，必须对最终 DataFrame 的 columns、dtypes、index、`candle_begin_time` 排序与数值内容做完整一致性比较

---

## 模块 2：资金费率更新 (`update_candle`)

**文件：** `core/candle_manager.py`
**改动量：** ~25 行
**依赖顺序：** 独立，建议最先实施（最简单、风险最低）

### 实施步骤

1. **判断新旧关系** — 取 `df_old['candle_begin_time'].max()` 和 `df_new` 的最值：
   - 任何新行 `<= last_old_time` → 时间戳重叠 = 历史修正 → 降级全量（`pd.concat` + 去重 + 排序）
   - 全部 `> last_old_time` → 纯新增 → 增量路径

2. **增量路径** — `df_old` 有序无重复：
   - 计算需丢弃行数：`drop_count = len(df_old) + len(df_new) - num_candles`
   - 如 `drop_count > 0`：`df_old = df_old.iloc[drop_count:]`
   - `pd.concat([df_old, df_new])` → 写盘

3. **⚠️ 这是基类方法** — 需确认所有调用方语义兼容。当前全量重算后结果与增量追加等价，无行为差异。

### 适用范围
- 资金费率实时更新（`binance_update.py` 中的 `update_binance_funding_rates()`）
- CoinCap 更新（`coin_cap.py` 中的 `update_daily_coin_cap()`）
- 任何其他通过 `update_candle()` 写入的数据

---

## 模块 3：Market Dict 增量生成 (`gen_batch_data`)

**文件：** `core/preprocess.py` — `gen_batch_data()`
**改动量：** ~50 行
**依赖顺序：** 需在模块 4 之前实施

### 实施步骤

1. **读取旧 market_dict** — 通过 `preprocess_mgr.has_symbol(full_batch_name)` 检查上周期缓存是否存在。

2. **逐 symbol 处理** — 对每个 symbol 分三种情况：
   - **旧 dict 不存在** → 全量：`add_additional_cols` + spot-swap 匹配（原逻辑）
   - **旧 dict 存在且 symbol 在其中** → 增量：
     - 取 `df_old['candle_begin_time'].max()` 作为 `last_ts`
     - `df_new = df_candle[df_candle['candle_begin_time'] > last_ts]` 取纯新增行
     - 只对新行调用 `add_additional_cols()` 和 `_apply_spot_swap_match()`
     - **`first_candle_time` 必须继承旧值**：`df_new['first_candle_time'] = df_old['first_candle_time'].iloc[0]`
     - **`last_candle_time` 不能保留新增段局部值**：拼接后统一回写为整表最大 `candle_begin_time`
     - `pd.concat([df_old, df_new])` + `drop_duplicates` → 写回
   - **旧 dict 存在但 symbol 不在其中**（新上市币）→ 全量处理

3. **磁盘输出保持全量** — `preprocess_mgr.set_candle(full_batch_name, run_time, new_market_dict)` 的内容仍是完整的 dict of DataFrames，下游兼容。

4. **年份 time_range 逻辑不变** — `start_year` / `end_year` / `year_pivot_name` 生成逻辑保持原样。

### 辅助函数
- `_apply_spot_swap_match()` — 从原 `gen_batch_data` 循环体中抽取 spot-swap 匹配赋值逻辑，减少函数体复杂度。

### 关键边界处理
- **字段不变量：** 增量路径产物必须与全量路径在 `first_candle_time` / `last_candle_time` / `funding_fee` / `symbol_spot` / `symbol_swap` 上完全一致
- **Benchmark 必测样本：** 有 funding 的 swap、无 funding 的 swap、spot、存在 match 裁剪区间的 symbol、无 match row 的 symbol
- **断言粒度：** `market_dict` 不只检查 symbol 集合，必须递归比较每个 symbol DataFrame 的结构、排序与逐值内容

---

## 模块 4：Pivot 滚动窗口 (`gen_pivot`)

**文件：** `core/preprocess.py` — `gen_pivot()`
**改动量：** ~70 行
**依赖顺序：** 需在模块 3 之后实施（依赖 `gen_batch_data` 输出的 market_dict）

### 实施步骤

1. **提取新行** (`_extract_new_row()`) — 从模块 3 生成的多个 batch 的 market_dict 中：
   - **以旧 pivot 最后一个时间戳 `last_pivot_ts` 为增量边界**，而不是依赖“全局第二大时间戳”启发式
   - 遍历所有 batch，提取 `candle_begin_time > last_pivot_ts` 的行
   - 将所有新增行按 `candle_begin_time` 排序后做 pivot → 得到有序的新行 DataFrame
   - 如发现任何 batch 中存在 `candle_begin_time <= last_pivot_ts` 的重叠修正，直接触发 `_full_rebuild_pivot()`

2. **Symbol 变更检测** — 对比 `old_pivot['open'].columns` 和 `new_row['open'].columns`：
   - 有增删 → 触发 `_full_rebuild_pivot()` + 记录日志
   - 无变化 → 进入滚动窗口路径

3. **滚动窗口路径** — 对 `open` / `close` / `vwap1m` / `funding_rate` 四个 DataFrame：
   - `drop_count = len(new_row)`（新行数，通常为 1）
   - `pd.concat([old_df.iloc[drop_count:], new_row[key]])` 删除最旧 N 行、追加 N 行新行
   - `new_row[key]` 的 index 已是排序后的时间戳

4. **24 小时全量校验** (`_should_full_rebuild()`)：
   - 用 `_last_full_rebuild` 字典记录每个 pivot_key 上次全量重建时间
   - **使用 `datetime.now(tz=timezone.utc)`（实际当前时间），不是 `run_time`**
   - 距上次超过 86400 秒 → 触发全量重建，**并将 `_last_full_rebuild` 更新为实际当前时间**
   - 首次运行（无缓存）→ 必定全量

5. **历史年份跳过逻辑保留** — `int(year) < cur_year - 1` 且 pivot 文件已存在 → 跳过。

### 辅助函数
- `_extract_new_row(pivot_mgr, pivot_names, year, last_pivot_ts)` — 从多个 batch 的 market_dict 中提取 `candle_begin_time > last_pivot_ts` 的新增行，排序后 pivot 返回有序 DataFrame
- `_full_rebuild_pivot(preprocess_mgr, pivot_name, market_dict, market_type)` — 调用 `make_market_pivot()` 做全量重建
- `_should_full_rebuild(pivot_key, interval_seconds=86400)` — 使用 `datetime.now(tz=timezone.utc)` 判断，触发后更新缓存时间为实际当前时间

### 关键边界处理
- **symbol 集合变化：** 与旧 pivot 列集合不一致时，一律全量重建，不做局部修补
- **增量边界：** 只信任 `old_pivot` 的最后时间戳，不信任跨 batch 的次新时间启发式
- **断言粒度：** `market_pivot` 必须比较 key 集合（spot: `open/close/vwap1m`；swap: `open/close/vwap1m/funding_rate`）、每个 DataFrame 的 index/columns/逐值内容

---

## 风险矩阵与回滚策略

| 模块 | 风险 | 回滚方式 |
|------|:---:|------|
| 模块 2 | ⭐ 低 — 最简单，逻辑等价 | 还原 `update_candle()` 即可 |
| 模块 1 | ⭐⭐ 中 — fill_gaps 边界需谨慎 | 修正检测自动降级全量，最差情况性能不变 |
| 模块 3 | ⭐⭐ 中 — 依赖旧数据缓存 | 首次无缓存自动全量，无正确性风险 |
| 模块 4 | ⭐⭐⭐ 较高 — pivot 滚动窗口 + symbol 变更 | 24h 全量校验兜底 + symbol 变更自动降级 |

**推荐实施顺序：** 模块 2 → 模块 1 → 模块 3 → 模块 4（由简到难、由独立到依赖）

---

## 验证方法

### 模块级强制验证协议（每个模块都必须单独执行）

1. **读取真实历史输入** — 从 `data/` 目录选择固定样本，构造 `old_state + incremental_input`，禁止用手写玩具数据替代主验证。
2. **调用旧/新两套实现** — 旧实现来自改造前冻结的 `baseline_*` snapshot；新实现为当前修改后的函数。
3. **一致性断言**：
   - 输出类型一致
   - 输出结构一致（DataFrame columns / dtypes / index / 排序；dict key 集合；嵌套 DataFrame 结构）
   - 关键字段与数值内容 100% 一致
   - 若不一致，打印首个 diff 的 symbol / key / row / column
4. **Benchmark**：
   - 预热 1-3 次
   - 正式运行 10 次
   - 用 `time.perf_counter()` 记录 old/new mean 与 p95
   - **若 `new_mean > old_mean`，测试失败**
5. **验证记录** — 每个模块都保存：样本名、输入窗口、旧/新耗时、是否一致、失败原因。

### 模块 1：`update_resample_candle()`
- 样本覆盖：无 gap、小 gap 需 ffill、大 gap 触发 `filter_gaps()`、重叠区被交易所修正
- 额外要求：对不同 `overlap_window` 候选值跑 benchmark，选出第一个在全部真实样本上正确且不退化的窗口；不能先验假设 `15` 永远足够

### 模块 2：`update_candle()`
- 样本覆盖：纯新增、单条重叠、多条重叠、空旧表、`num_candles` 截断
- 额外要求：验证写盘后再读回结果仍与旧逻辑一致；性能统计包含 read + update + write 端到端耗时

### 模块 3：`gen_batch_data()`
- 样本覆盖：有 funding 的 swap、无 funding 的 swap、spot、存在 match 裁剪区间、无 match row
- 额外要求：重点断言 `first_candle_time`、`last_candle_time`、`funding_fee`、`symbol_spot`、`symbol_swap` 与旧逻辑完全一致

### 模块 4：`gen_pivot()`
- 样本覆盖：spot/swap 各至少一个年份；多 batch 新增时间不齐；symbol 新增/移除；存在重叠修正
- 额外要求：重点断言 `market_pivot` key 集合与每个 pivot DataFrame 的 index/columns/逐值内容完全一致；任何 overlap 修正或 symbol 集合变化都应触发全量重建

### 额外回归验证
1. **修正检测验证** — 手动构造重叠区数据不一致的场景，确认降级日志输出
2. **Symbol 变更验证** — 新增/移除 symbol 后确认全量重建触发
3. **24h 校验验证** — 启动后等待 >24h，确认全量重建日志出现且结果一致
4. **跨 batch 排序验证** — 确认多 batch 有新行且时间戳分散时，新增提取与排序结果正确
