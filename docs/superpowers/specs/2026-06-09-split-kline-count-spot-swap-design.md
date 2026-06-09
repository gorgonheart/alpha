# Design: 分离 spot/swap 的 kline_count_1h

**日期**: 2026-06-09  
**背景**: spot 只需保留 3000 根 1h K 线，swap 需要 14060 根。目前两者共用一个值，导致 spot 处理了不必要的数据量，拖慢预处理速度。

---

## 问题分析

### 当前状态

`config.py` 定义单一常量：

```python
kline_count_1h = 14060
```

`BmacHandle` 持有该值，并在以下 6 处调用点统一使用，不区分 spot/swap：

| 文件 | 行 | 用途 |
|---|---|---|
| `core/handle.py` | 389 | 计算 5m base candle 数量 `num_base_candles` |
| `core/binance_update.py` | 39 | funding rate 保留条数（swap-only） |
| `core/binance_update.py` | 92, 103 | resample 回溯窗口 + tail-trim |
| `core/binance_update.py` | 156 | 5m base candle tail-trim |
| `core/data_api_update.py` | 79 | Data API resample tail-trim |
| `core/resample_candle.py` | 61 | resample_history 回溯窗口 |
| `core/history_data/candle.py` | 220, 330 | 历史 5m K 线下载终止条件 |
| `core/history_data/funding.py` | 67 | funding rate 历史回溯窗口（swap-only） |

### 性能基准（修改前）

```
预处理 Market Dict spot: 22.6 秒
预处理 Pivot Table spot: 9.4 秒
预处理 Market Dict swap: 28.5 秒
```

spot 使用 14060 条数据，实际只需 3000 条。理论上 spot 数据量降至 21%，
预处理 spot 总耗时预期从 ~32s 降至 ~7s。

---

## 方案选择

选择**方案 A**：两个有名字的 config 字段 + BmacHandle dispatch 方法。

- 改动局部，向后兼容（config.json 可独立 override）
- 语义清晰，不改变字段类型
- 所有调用点已经在 trade_type 上下文中，接入成本低

---

## 设计

### 1. config.py

移除 `kline_count_1h`，替换为：

```python
kline_count_1h_spot = 3000
kline_count_1h_swap = 14060
```

`allowed_attrs` 中同步替换，支持 config.json 独立 override。

### 2. BmacHandle（core/handle.py）

**字段变更：**

```python
# 移除
kline_count_1h: int
num_base_candles: int

# 新增
kline_count_1h_spot: int
kline_count_1h_swap: int
num_base_candles_spot: int
num_base_candles_swap: int
```

**新增 dispatch 方法：**

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

**create_bmac_handle() 计算逻辑：**

```python
num_base_candles_spot = num_offsets * (bmac_config.kline_count_1h_spot + 3)
num_base_candles_swap = num_offsets * (bmac_config.kline_count_1h_swap + 3)
```

### 3. 调用点变更

| 文件 | 行 | 原写法 | 改后 |
|---|---|---|---|
| `core/binance_update.py` | 39 | `handle.kline_count_1h` | `handle.get_kline_count_1h('swap')` |
| `core/binance_update.py` | 92 | `handle.kline_count_1h` | `handle.get_kline_count_1h(trade_type)` |
| `core/binance_update.py` | 103 | `handle.kline_count_1h` | `handle.get_kline_count_1h(trade_type)` |
| `core/binance_update.py` | 156 | `handle.num_base_candles` | `handle.get_num_base_candles(trade_type)` |
| `core/data_api_update.py` | 79 | `handle.kline_count_1h` | `handle.get_kline_count_1h(trade_type)` |
| `core/resample_candle.py` | 61 | `handle.kline_count_1h` | `handle.get_kline_count_1h(trade_type)` |
| `core/history_data/candle.py` | 220 | `handle.num_base_candles` | `handle.get_num_base_candles(trade_type)` |
| `core/history_data/candle.py` | 330 | `handle.num_base_candles` | `handle.get_num_base_candles(trade_type)` |
| `core/history_data/funding.py` | 67 | `handle.kline_count_1h` | `handle.get_kline_count_1h('swap')` |

### 4. debug_handle() 日志更新

```python
logger.debug((f'spot {handle.resample_interval} 数量={handle.kline_count_1h_spot:,d}, '
              f'swap {handle.resample_interval} 数量={handle.kline_count_1h_swap:,d}, '
              f'Offset={handle.enabled_hour_offsets}'))
```

---

## 预期效果

| 指标 | 修改前 | 修改后（预期） |
|---|---|---|
| spot Market Dict 预处理 | ~22.6s | ~5s |
| spot Pivot Table 预处理 | ~9.4s | ~2s |
| swap 不变 | ~28.5s | ~28.5s |
| spot 存储空间 | ~14060 行/symbol | ~3000 行/symbol |

---

## 修改结果（实施后填写）

> 待实施后记录实际性能数据
