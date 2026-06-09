# Split kline_count_1h spot/swap Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 `kline_count_1h` 拆成 `kline_count_1h_spot=3000` 和 `kline_count_1h_swap=14060`，同时影响数据保留长度和历史回溯窗口，减少 spot 预处理数据量约 79%。

**Architecture:** 在 `config.py` 中新增两个字段替换原字段；在 `BmacHandle` 中新增两对字段和两个 dispatch 方法；所有 9 处调用点均已在 `trade_type` 上下文中，替换为方法调用即可。

**Tech Stack:** Python, dataclasses (BmacHandle), conda env alpha311

---

## Files Modified

| 文件 | 改动 |
|---|---|
| `config.py` | 替换 `kline_count_1h` → `kline_count_1h_spot` / `kline_count_1h_swap`；更新 `allowed_attrs` |
| `core/handle.py` | BmacHandle 替换字段，新增 dispatch 方法；`create_bmac_handle` 计算两个 `num_base_candles`；更新 `debug_handle` |
| `core/binance_update.py` | 3 处调用点替换 |
| `core/data_api_update.py` | 1 处调用点替换 |
| `core/resample_candle.py` | 1 处调用点替换 |
| `core/history_data/candle.py` | 2 处调用点替换 |
| `core/history_data/funding.py` | 1 处调用点替换（含日志） |

---

## Task 1: 更新 config.py

**Files:**
- Modify: `config.py:12`

- [ ] **Step 1: 替换 config.py 中的 kline_count_1h**

将 `config.py` 第 11-12 行：
```python
# Resample(1h) 周期 K 线数量，默认: 2000 小时
kline_count_1h = 14060
```
改为：
```python
# Resample(1h) 周期 K 线数量
kline_count_1h_spot = 3000
kline_count_1h_swap = 14060
```

- [ ] **Step 2: 更新 allowed_attrs**

将 `config.py` 第 82 行：
```python
allowed_attrs = (
    'kline_count_1h', 'error_webhook_url', 'use_api', 'data_api_uuid', 'data_api_key', 'enabled_hour_offsets',
    'funding_rate')
```
改为：
```python
allowed_attrs = (
    'kline_count_1h_spot', 'kline_count_1h_swap', 'error_webhook_url', 'use_api', 'data_api_uuid', 'data_api_key',
    'enabled_hour_offsets', 'funding_rate')
```

---

## Task 2: 更新 BmacHandle 数据类和工厂函数（core/handle.py）

**Files:**
- Modify: `core/handle.py:132-135, 172-191, 389, 429-454`

- [ ] **Step 1: 替换 BmacHandle 的字段声明**

将 `core/handle.py` 第 131-135 行：
```python
    # 重采样周期 K 线数量
    kline_count_1h: int

    # 基础周期 K 线的数量
    num_base_candles: int
```
改为：
```python
    # 重采样周期 K 线数量（spot/swap 分别配置）
    kline_count_1h_spot: int
    kline_count_1h_swap: int

    # 基础周期 K 线的数量（spot/swap 分别配置）
    num_base_candles_spot: int
    num_base_candles_swap: int
```

- [ ] **Step 2: 在 get_binance_handle 方法之后新增两个 dispatch 方法**

在 `core/handle.py` 第 190 行（`raise ValueError` 行）之后，在 `get_binance_handle` 方法结束后插入：
```python
    def get_kline_count_1h(self, trade_type: str) -> int:
        if trade_type == 'spot':
            return self.kline_count_1h_spot
        return self.kline_count_1h_swap

    def get_num_base_candles(self, trade_type: str) -> int:
        if trade_type == 'spot':
            return self.num_base_candles_spot
        return self.num_base_candles_swap
```

- [ ] **Step 3: 更新 create_bmac_handle 中的 num_base_candles 计算**

将 `core/handle.py` 第 388-389 行：
```python
    # 计算基础周期 K 线数量，比配置要求的重采样周期多 3 个周期，防止数据不足
    num_base_candles = num_offsets * (bmac_config.kline_count_1h + 3)
```
改为：
```python
    # 计算基础周期 K 线数量，比配置要求的重采样周期多 3 个周期，防止数据不足
    num_base_candles_spot = num_offsets * (bmac_config.kline_count_1h_spot + 3)
    num_base_candles_swap = num_offsets * (bmac_config.kline_count_1h_swap + 3)
```

- [ ] **Step 4: 更新 create_bmac_handle 的 BmacHandle 构造调用**

将 `core/handle.py` 第 432-434 行：
```python
        resample_interval=bmac_config.resample_interval,
        kline_count_1h=bmac_config.kline_count_1h,
        num_base_candles=num_base_candles,
```
改为：
```python
        resample_interval=bmac_config.resample_interval,
        kline_count_1h_spot=bmac_config.kline_count_1h_spot,
        kline_count_1h_swap=bmac_config.kline_count_1h_swap,
        num_base_candles_spot=num_base_candles_spot,
        num_base_candles_swap=num_base_candles_swap,
```

- [ ] **Step 5: 更新 debug_handle 日志**

将 `core/handle.py` 第 452-454 行：
```python
    logger.debug((f'{handle.resample_interval} 数量={handle.kline_count_1h:,d}, '
                  f'Offset={handle.enabled_hour_offsets}, '
                  f'{handle.base_interval} 数量={handle.num_base_candles:,d}'))
```
改为：
```python
    logger.debug((f'spot {handle.resample_interval} 数量={handle.kline_count_1h_spot:,d}, '
                  f'swap {handle.resample_interval} 数量={handle.kline_count_1h_swap:,d}, '
                  f'Offset={handle.enabled_hour_offsets}'))
```

---

## Task 3: 更新 binance_update.py 的 3 处调用点

**Files:**
- Modify: `core/binance_update.py:39, 92, 103, 156`

- [ ] **Step 1: funding rate 保留条数（行 39，swap-only）**

将 `core/binance_update.py` 第 39 行：
```python
        funding_mgr.update_candle(symbol, run_time, g, handle.kline_count_1h)
```
改为：
```python
        funding_mgr.update_candle(symbol, run_time, g, handle.get_kline_count_1h('swap'))
```

- [ ] **Step 2: resample 回溯窗口（行 92）**

将 `core/binance_update.py` 第 92 行：
```python
        theoretical_start = run_time - handle.kline_count_1h * resample_delta
```
改为：
```python
        theoretical_start = run_time - handle.get_kline_count_1h(trade_type) * resample_delta
```

- [ ] **Step 3: resample tail-trim（行 103）**

将 `core/binance_update.py` 第 102-103 行：
```python
        first_begin_time, last_begin_time = update_resample_candle(resample_mgr, symbol, df_resample,
                                                                   handle.kline_count_1h, run_time, resample_delta)
```
改为：
```python
        first_begin_time, last_begin_time = update_resample_candle(resample_mgr, symbol, df_resample,
                                                                   handle.get_kline_count_1h(trade_type), run_time, resample_delta)
```

- [ ] **Step 4: 5m base candle tail-trim（行 156）**

将 `core/binance_update.py` 第 156 行：
```python
        required_first_begin = run_time - handle.num_base_candles * base_delta
```
改为：
```python
        required_first_begin = run_time - handle.get_num_base_candles(trade_type) * base_delta
```

---

## Task 4: 更新 data_api_update.py（行 79）

**Files:**
- Modify: `core/data_api_update.py:79`

- [ ] **Step 1: Data API resample tail-trim**

将 `core/data_api_update.py` 第 78-79 行：
```python
            first_begin_time, last_begin_time = update_resample_candle(candle_mgr, symbol, df_symbol,
                                                                       handle.kline_count_1h, run_time, resample_delta)
```
改为：
```python
            first_begin_time, last_begin_time = update_resample_candle(candle_mgr, symbol, df_symbol,
                                                                       handle.get_kline_count_1h(trade_type), run_time, resample_delta)
```

---

## Task 5: 更新 resample_candle.py（行 61）

**Files:**
- Modify: `core/resample_candle.py:61`

- [ ] **Step 1: resample_history 回溯窗口**

将 `core/resample_candle.py` 第 61 行：
```python
        theoretical_start = run_time - handle.kline_count_1h * resample_delta
```
改为：
```python
        theoretical_start = run_time - handle.get_kline_count_1h(trade_type) * resample_delta
```

---

## Task 6: 更新 history_data/candle.py（行 220, 330）

**Files:**
- Modify: `core/history_data/candle.py:220, 330`

- [ ] **Step 1: HistoryDownloader.check_finish（行 220）**

`HistoryDownloader` 继承自 `Downloader`，`self.trade_type` 已在基类中存储。

将 `core/history_data/candle.py` 第 220 行：
```python
        required_first_begin = self.run_time - base_delta * self.handle.num_base_candles
```
改为：
```python
        required_first_begin = self.run_time - base_delta * self.handle.get_num_base_candles(self.trade_type)
```

- [ ] **Step 2: verify_exist_history（行 330）**

`trade_type` 是该函数的参数，直接可用。

将 `core/history_data/candle.py` 第 330 行：
```python
    required_first_begin = run_time - base_delta * handle.num_base_candles
```
改为：
```python
    required_first_begin = run_time - base_delta * handle.get_num_base_candles(trade_type)
```

---

## Task 7: 更新 history_data/funding.py（行 67, 88）

**Files:**
- Modify: `core/history_data/funding.py:67, 88`

- [ ] **Step 1: funding rate 历史回溯窗口（行 67）**

将 `core/history_data/funding.py` 第 66-67 行：
```python
    # 1. 计算全局 start time = now - handle.kline_count_1h 小时，然后 floor 取整
    global_start_time = run_time - timedelta(hours=handle.kline_count_1h)
```
改为：
```python
    # 1. 计算全局 start time = now - swap kline_count_1h 小时，然后 floor 取整
    global_start_time = run_time - timedelta(hours=handle.get_kline_count_1h('swap'))
```

- [ ] **Step 2: 更新 funding.py 日志（行 88）**

将 `core/history_data/funding.py` 第 88 行：
```python
        f"1h K线数量: {handle.kline_count_1h}，"
```
改为：
```python
        f"1h K线数量(swap): {handle.get_kline_count_1h('swap')}，"
```

---

## Task 8: 验证启动并记录结果

- [ ] **Step 1: 使用 conda env alpha311 验证 Python 语法**

```bash
conda run -n alpha311 python -c "import config; print(config.kline_count_1h_spot, config.kline_count_1h_swap)"
```
Expected output: `3000 14060`

- [ ] **Step 2: 验证 handle 可以正常构建**

```bash
conda run -n alpha311 python -c "
from config import *
from core.handle import create_bmac_handle
import config as cfg
h = create_bmac_handle(cfg)
print('spot kline_count_1h:', h.get_kline_count_1h('spot'))
print('swap kline_count_1h:', h.get_kline_count_1h('swap'))
print('spot num_base_candles:', h.get_num_base_candles('spot'))
print('swap num_base_candles:', h.get_num_base_candles('swap'))
"
```
Expected output:
```
spot kline_count_1h: 3000
swap kline_count_1h: 14060
spot num_base_candles: 36036
swap num_base_candles: 168756
```

- [ ] **Step 3: 运行一个完整 update cycle，记录 spot/swap 预处理耗时**

观察日志中 `预处理 Market Dict spot`、`预处理 Pivot Table spot`、`预处理 Market Dict swap` 的耗时，填入 design doc 的"修改结果"章节：`docs/superpowers/specs/2026-06-09-split-kline-count-spot-swap-design.md`

- [ ] **Step 4: Commit**

```bash
git add config.py core/handle.py core/binance_update.py core/data_api_update.py core/resample_candle.py core/history_data/candle.py core/history_data/funding.py docs/superpowers/specs/2026-06-09-split-kline-count-spot-swap-design.md docs/superpowers/plans/2026-06-09-split-kline-count-spot-swap.md
git commit -m "feat: 分离 spot/swap 的 kline_count_1h，spot=3000，swap=14060"
```
