# 数据中心（d:/alpha）预处理 IO 优化 + Arrow 发布 设计文档

- 日期: 2026-06-27
- 项目: `d:/alpha`（BMAC 数据中心 / 生产者）
- 配套文档: `d:/alphaonline/docs/superpowers/specs/2026-06-27-consumer-mmap-arrow-read-design.md`（消费者）
- 状态: 待评审

## 1. 背景与目标

`realtime_data.py` 每 5 分钟更新一次行情，并在 `preprocess` 阶段把
`{symbol: DataFrame}` 批次写入 `preprocess_1h_resample/{minute}m/`，供下游
`d:/alphaonline` 读取。线上日志（2026-06-24）显示单轮 `preprocess` 墙钟约 **49s**：

```
00:10:30  开始预处理 spot + swap (ThreadPoolExecutor max_workers=2)
00:10:54  Market Dict spot  完成  24.7s
00:11:04  Market Dict swap  完成  34.3s   ← 关键路径
00:11:07  预计算因子 spot    完成  12.5s
00:11:17  预计算因子 swap    完成  12.6s
00:11:12  Pivot spot        完成   4.1s
00:11:19  Pivot swap        完成   1.9s
总墙钟 00:10:30 → 00:11:19 ≈ 49s
```

经代码核对，这 49s **几乎全部是生产者内部的 pickle 重写**，与跨进程交接无关。
目标：把这一阶段从 ~49s 压到 **10–20s**，并顺带修复一个发布顺序竞态。

**关键约束（来自代码，不可违反）：**
- 预处理数据是**增量状态**：`gen_batch_data_incremental` 每轮读取上一轮的批次
  pkl，只追加新行（[preprocess.py:276-364](../../../core/preprocess.py)）。
  因此生产者必须**持续保留完整历史文件**，本设计只能是**加法**，不能用共享内存
  替换文件持久化。
- 因子参数最大 13608（`config.py -> precompute_factor_configs`），需要 14060 根
  历史 K 线才能算对，所以生产者**自身文件不可裁切到 4050**。

## 2. 现状问题（三处，按影响排序）

### 问题 1：为追加几根 K 线，每轮重写全量历史（最大头）
`gen_batch_data_incremental` 在内存中只追加新行，但 `set_candle` →
`save_data_file` 会把**整个批次**（约 180 symbol × 14060 行）作为一个 pickle
**整体重写**（[candle_manager.py:103-119](../../../core/candle_manager.py)）。
即"写 GB 级文件只为加几分钟数据"。这是 24–34s "Market Dict" 阶段的主因。

### 问题 2：批次被写两次（且引入竞态）
`preprocess_type_data` 先 `gen_batch_data_incremental` 写出**不含因子**的批次，
随后 `save_ready` 打 ready 标志，**再** `gen_factor_data` 把所有批次**读回、算
因子、再全量重写**（[preprocess.py:507-548](../../../core/preprocess.py),
[preprocess.py:605-688](../../../core/preprocess.py)）。
- 一次完整的"读回 + 重写"全部批次，约 12.5s 因子阶段大半是 IO 而非计算。
- **竞态**：`save_ready`（539 行）在 `gen_factor_data`（543 行）**之前**。消费者
  `check_bmac_update_flag` 等的就是这个标志，因此消费者可能读到 (a) 尚无因子列
  的批次，或 (b) `gen_factor_data` 用 `os.remove`+`to_pickle` 非原子重写过程中的
  半成品 → `EOFError: Ran out of input`。今天只是因为消费者其它启动步骤通常更慢
  才碰巧没暴雷，**并无保证**。

### 问题 3：pickle 格式本身慢
所有 `CandleFileManager` 用 `save_type="pickle"`
（[incremental_update.patch:1043](../../../incremental_update.patch) 及全部测试
harness 确认）。Arrow/Feather 对数值型 DataFrame 的读写通常比 pickle 快 2–4×，
文件更小。`CandleFileManager` 已内建 `feather` 分支，调用点零改动
（[candle_manager.py:107-109](../../../core/candle_manager.py)）。

## 3. 设计

### 3.1 Lever 1 — Base/Recent 分层存储，停止全量重写

把每个批次从"单一全量文件"改为两层：

- **base 文件**：历史不可变部分，仅在跨周期合并（compaction）时重写。
- **recent 文件**：最近 `N` 根（复用消费者侧 `preprocess_num_recent` 语义，建议
  `N = 一周左右的小时数 + 余量`），每轮只重写这个小文件。

每轮 `gen_batch_data_incremental`：
1. 读 base + recent（base 可走 mmap 零拷贝，见 3.4）。
2. 追加新行到 recent。
3. 若 recent 超过阈值 → 把溢出部分并入 base（**周期性** compaction，非每轮）。
4. 只写 recent（小文件）；base 多数周期**不写**。

效果：把"每轮写 GB"降为"每轮写 MB + 周期性写 GB"。这是把 24–34s 压下来的主要杠杆。

> 备选（更简单但收益小）：保留单文件，仅切到 feather（Lever 3）。若 Lever 1 实现
> 风险过高，可先只做 Lever 2+3，留 Lever 1 作第二期。实现计划阶段再定取舍。

> **与消费者 fallback 的耦合（重要）**：消费者的 fallback 路径 glob
> `{type}_dict_batch*` 直接读生产者内部批次文件。Lever 1 把单批次文件拆成 base/recent
> 后，这个 glob 假设会失效。因此 **Arrow 发布文件（§3.4）是唯一稳定的跨项目接口**；
> base/recent 属生产者内部存储，消费者不应直接依赖。上线 Lever 1 时，必须确保消费者
> 已切到 Arrow 快路径为主，且其 fallback 不再依赖 `batch*` 布局（见消费者文档 §6
> 上线顺序与 §5 契约）。

### 3.2 Lever 2 — 一次写入即带因子（并修复竞态）

把因子计算并入批次生成流程，使批次**写一次就含因子列**：
- 合并 `gen_batch_data_incremental` 与 `gen_factor_data`：在追加新行、组装好
  `market_dict` 后，**先**对（新行或全量，取决于因子是否可增量）算因子列，
  `df.assign(**factor_cols)`，**再** `set_candle` 落盘。
- 移除 `gen_factor_data` 的"读回全部批次 + 全量重写"往返。
- **发布顺序修正**：`save_ready(f'{trade_type}_dict')` 必须在因子列写盘**之后**
  调用。这样消费者看到 ready 标志时，批次一定已含因子。

> 注意：部分因子可能无法仅对"新行"增量计算（依赖滚动窗口）。实现计划需逐因子确认
> `signal_multi_params` 是否支持只算尾部。无法增量的因子，退化为对 recent 层全量
> 重算（recent 远小于全量，仍比现状快）。

### 3.3 Lever 3 — 批次存储 pickle → feather

把 `preprocess_mgr` 的 `save_type` 由 `"pickle"` 改为 `"feather"`。
- 调用点零改动（manager 已支持）。
- **校验项**：feather 要求列可被 Arrow 序列化。已知列：OHLCV/因子为 float64（OK），
  `symbol/symbol_spot/symbol_swap` 为字符串（feather 支持 OK），时间列为
  datetime（OK）。实现阶段需跑一次全列 dtype 校验，确认无 object 杂类型。
- 文件扩展名自动变 `.fea`。**这会改变消费者读取路径**，见 §5 契约。

### 3.4 Arrow 发布步骤（跨进程交接的生产者侧）

在因子写盘、ready 标志之后，新增**发布**步骤，产出消费者 mmap 读取的文件：
1. 对每个 trade_type，取各 symbol 的 `tail(publish_kline_num)`（新增配置，默认
   ~5000，需 ≥ 消费者 `get_kline_num` + run_time 过滤余量）。
2. 组装为**单个 Arrow IPC 文件**：**每 symbol 一个 record batch** + 一个 manifest
   （batch index → symbol），使消费者可零拷贝重建 `{symbol: DataFrame}`。
3. 原子发布：写 `*.arrow.tmp` → `fsync` → **原子 rename** 为 `*.arrow` → 写
   `.ready` 标志。原子 rename 保证消费者要么看到完整的"含因子"快照，要么看到上一
   轮，**绝不**看到半成品。
4. 路径：
   - `preprocess_1h_resample/{minute}m/spot_dict_{ts}.arrow`
   - `preprocess_1h_resample/{minute}m/swap_dict_{ts}.arrow`

> Windows 说明：本方案用**内存映射文件**而非 `multiprocessing.shared_memory`，
> 故无命名共享内存的生命周期/属主问题——文件天然持久，页缓存由 OS 管理，周期间
> 不占用常驻进程内存。

### 3.5 publish_kline_num 契约

新增生产者配置 `publish_kline_num`（默认 5000）。生产者只在 **Arrow 发布文件**里
裁到这个深度；自身 base/recent 文件仍保留 14060 全历史（因子正确性 + 增量）。
消费者拿到后再 `tail(get_kline_num=4050)`。约束：`publish_kline_num ≥ 4050 + run_time
过滤余量`。两项目通过本文档约定该常量，生产者**不需要**知道消费者的 `get_kline_num`。

## 4. 不在本设计范围内

- exginfo 交接：维持现有 `snapshot_bmac_exginfo` 拷贝到 cache 的机制，体量小、已
  race-safe，不并入本次。
- pivot 文件（`market_pivot_*`）：保持现状 pkl + 独立 ready 标志。Arrow 发布只覆盖
  `{spot,swap}_dict`。pivot 是否同样优化留待第二期。
- 消费者侧读取改造：见配套消费者设计文档。

## 5. 跨项目契约（生产者承诺 / 消费者依赖）

两项目独立实现，但必须在以下契约上逐项一致：

| 项 | 现状 | 本设计 |
| --- | --- | --- |
| 数据 ready 标志 | `{spot,swap}_dict_{ts}.ready` | 不变（仍是消费者的 gate），但**改为因子写盘后才打** |
| 批次文件 | `{type}_dict_batch*.pkl` | 兼容保留；新增 `{type}_dict_{ts}.arrow` 快路径 |
| 批次格式 | pickle | feather（`.fea`）— 消费者 fallback 路径需同步改 |
| 新增发布文件 | 无 | `{type}_dict_{ts}.arrow`（每 symbol 一 batch + manifest） |
| 发布深度 | 全量 14060 | `tail(publish_kline_num≈5000)` |
| 每 symbol 列 | symbol, candle_begin_time, first/last_candle_time, symbol_spot/swap, OHLCV, funding_fee, avg_price_1m/5m, 因子列 | **不变** |
| 原子性 | 无（os.remove+写） | tmp → fsync → rename → ready |

**消费者最严苛的依赖**：它先按 `candle_begin_time + utc_offset < run_time` 过滤，
**再** `tail(get_kline_num)`（[d:/alphaonline datatools.py:173-182]）。故发布深度必须
留出 run_time 过滤余量，5000 安全。

## 6. 风险与回滚

- **Lever 1 风险最高**（改存储布局）。回滚：保留单文件 + feather 即可获得部分收益。
- **Lever 3 (feather) 改了文件扩展名**，必须与消费者同步上线，否则消费者按 `.pkl`
  glob 读不到。上线顺序：消费者先支持双格式读取（先发 fallback），生产者再切 feather。
- 因子增量正确性：无法增量的因子退化为 recent 层全量重算，需回归测试逐因子对账
  （复用 `tests/support/*_harness.py`）。
- 全程**加法**，Arrow 发布失败不影响既有 pkl 路径，消费者自动 fallback。

## 7. 验收

- 单轮 `preprocess` 墙钟从 ~49s 降到 ≤20s（线上日志对比）。
- 因子列在 ready 标志之前已落盘（竞态修复，构造并发读测试验证无 `EOFError`）。
- 生产者自身增量更新仍正确（回归 harness 对账 base/recent 合并结果 == 旧全量结果）。
- Arrow 发布文件可被消费者零拷贝重建出等价 `{symbol: DataFrame}`。
