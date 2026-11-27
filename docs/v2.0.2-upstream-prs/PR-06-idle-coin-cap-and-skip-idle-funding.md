# PR-06: `coin_cap` 日期缓存与空档冷却、按需创建 `aux_exe`、非启用轮跳过实时资金费写盘与 `T-22s` 唤醒

* **建议分支**：`perf/idle-coincap-and-skip-idle-funding`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（第 1 批独立合入）
* **参考源提交（`alpha`）**：`baa6e94`、`5694033`

---

## 1. 场景

1. 当用户开启市值数据（`use_api['coin_cap'] = True`）但关闭 K 线 Data API（`use_api['kline'] = False`）时，`coin_cap` 是**日频数据**（每天仅更新 1 次），却在每个启用 offset 的关键发布窗口内被拉起独立子进程并全盘扫描所有市值 pkl。
2. 当 `enabled_hour_offsets` 未开启全部 12 个偏移（如 6 户错峰 `5m/15m/25m/35m/45m/55m` 或单偏移 `25m`）时，每小时存在 6~11 个“非启用 5m 空档”。
3. 启用 offset 轮次在 `T-15s` 唤醒执行 `update_meta`（拉取 `exginfo_spot/swap` + 767 币实时资金费并写盘），高负载下偶发逼近整点。

## 2. 目标

1. **`coin_cap` 缓存进程内最新日期 + 移至空档按 30 分钟冷却检查**：启动首次扫描一次磁盘后缓存 `_LAST_COIN_CAP_DATE`，避免每轮全盘读几百个市值 pkl；仅当启用全部 12 个 offset（无空档）时才回退在周期内检查。
2. **按需创建 `aux_exe` 子进程池**：`run_ws_cycle` 中仅在 `handle.data_api is not None`（或无空档需内联市值更新）时才 spawn `ProcessPoolExecutor`，消除关键发布期内无谓的 2-worker spawn 子进程内存占用。
3. **非启用 5m 空档跳过实时预测资金费写盘**：给 `ExchangeAdapter.update_meta` / `update_exchange_meta` 增加 `include_realtime_funding: bool = True` 参数（对 Binance 与 OKX 统一生效），非启用轮传 `False`，避免白白重写 767 个合约的预测资金费 pkl。
4. **启用 offset 唤醒提前量由 `T-15s` 扩宽至 `T-22s`**（可配置 `enabled_meta_lead_sec`，默认 22s），为元数据拉取与计算池预热留足安全余量，防止跨过整点。

## 3. 问题（v2.0.2 现状与根因）

1. **`coin_cap_update_to_recent` 每轮全盘读 pkl**（`v2.0.2/core/ingest/coin_cap.py:33-40`）：
   ```python
   symbols = coin_cap_mgr.get_all_symbols()
   max_candle_begin_time = max(coin_cap_mgr.read_candle(s)['candle_begin_time'].max() for s in symbols)
   ```
   每个启用轮次都在关键发布窗口内 spawn 独立子进程，把 `coin_cap/` 目录下全部历史 pkl 反序列化一遍只为求一个日频 `max()`。
2. **`run_ws_cycle` 无条件创建 2-worker `aux_exe`**（`v2.0.2/core/pipeline/update_cycle.py:258-262`）：
   即使 `data_api` 与 `coin_cap` 均为 `None`，也仍进入 `with ProcessPoolExecutor(max_workers=2, mp_context=_SPAWN_CTX) as aux_exe:` 上下文；启用 `coin_cap` 时会 spawn 一个陪跑子进程在发布峰值抢占内存。
3. **非启用 5m 轮次处理粗糙**（`v2.0.2/core/app.py:42-46`）：
   * 当前 `v2.0.2` 非启用 offset 仅 `time.sleep(60); return`，未维护 WS 动态订阅与健康检查，也未利用空档承接日频市值更新；
   * 若在非启用轮直接调用 `update_meta()` 而不区分 `include_realtime_funding`，则会每 5 分钟重写全部 767 个合约的预测资金费 pkl。

## 4. 项目文档中的分析与实测证据

* **实测数据（`alphaonline/docs/kb/performance-analysis.md` §2、2026-11-27 过夜 4 轮实测）**：
  * 启用偏移 `update_meta` 在 `T-22s` 启动后，均于 `:45~:49` 完成，相对整点留足 **`11–15s` 安全余量**，彻底消除跨整点风险；
  * 非启用 5m 空档资金费写盘耗时降至 **`0.000s`**（0 次无谓 pkl 重写）；
  * `coin_cap` 移至空档并缓存最大日期后，`run_ws_cycle` 在 `data_api=None` 时不再 spawn 任何 `aux_exe` 子进程，节省关键发布窗内存与进程创建开销。

## 5. 硬件规格（`2c4g`/`2c8g`/`4c8g`）与消费端（PKL vs Arrow）局限评估

* **硬件规格**：**全规格强正收益，越低配收益越显著**。在 `2c4g` 机器上，省去 `run_ws_cycle` 关键发布期的无谓 `aux_exe` spawn 子进程、省去全盘反序列化几百个 `coin_cap` pkl 以及 767 币资金费 pkl 重写，可直接压低 CPU 峰值与 150~300 MiB 内存占用。
* **强依赖 PKL 消费端与外部脚本边界**：
  * 常规消费端（仅在 `enabled_hour_offsets` 触发读取 `funding/*.pkl` 与 `coin_cap/*.pkl`）：**完全成立**。
  * **边界前提**：若存在第三方监控或自定义策略在**非启用 5m 边界**直接读取磁盘 `funding/{symbol}.pkl` 获取实时预测资金费率，跳过写盘会使其看到上一启用轮的资金费。可通过 `cfg.skip_idle_realtime_funding = True`（默认 `True`，允许用户设为 `False` 关闭跳过）保留兼容性。

## 6. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 6.1 `core/ingest/coin_cap.py`
* 新增进程内日期缓存 `_LAST_COIN_CAP_DATE`、冷却状态 `_LAST_COIN_CAP_CHECK_MONO`、`reset_coin_cap_idle_state()` 与 `maybe_update_coin_cap_idle(handle, run_time, *, min_interval_sec=1800.0)`：

```python
_LAST_COIN_CAP_DATE: dict[str, datetime] = {}
_LAST_COIN_CAP_CHECK_MONO: dict[str, float] = {}

def reset_coin_cap_idle_state() -> None:
    _LAST_COIN_CAP_DATE.clear()
    _LAST_COIN_CAP_CHECK_MONO.clear()

def _coin_cap_cache_key(coin_cap_mgr) -> Optional[str]:
    base_dir = getattr(coin_cap_mgr, 'base_dir', None)
    return str(base_dir) if base_dir is not None else None

async def coin_cap_update_to_recent(handle: BmacHandle, run_time: datetime):
    coin_cap_mgr = handle.coin_cap_handle.coin_cap_mgr
    cache_key = _coin_cap_cache_key(coin_cap_mgr)

    max_candle_begin_time = _LAST_COIN_CAP_DATE.get(cache_key) if cache_key is not None else None
    if max_candle_begin_time is None:
        symbols = coin_cap_mgr.get_all_symbols()
        if not symbols:
            raise RuntimeError('历史市场数据不存在，请重新上传')
        max_candle_begin_time = max(coin_cap_mgr.read_candle(s)['candle_begin_time'].max() for s in symbols)
        if cache_key is not None:
            _LAST_COIN_CAP_DATE[cache_key] = max_candle_begin_time
    ...
    latest_written = max_candle_begin_time
    for dt in date_range:
        await update_daily_coin_cap(handle, dt, run_time)
        if dt.date() > latest_written.date():
            latest_written = dt
    if cache_key is not None:
        _LAST_COIN_CAP_DATE[cache_key] = latest_written

def maybe_update_coin_cap_idle(
    handle: BmacHandle,
    run_time: datetime,
    *,
    min_interval_sec: float = 1800.0,
) -> Optional[bool]:
    coin_cap_handle = getattr(handle, 'coin_cap_handle', None)
    if coin_cap_handle is None:
        return None
    cache_key = _coin_cap_cache_key(coin_cap_handle.coin_cap_mgr) or str(id(coin_cap_handle.coin_cap_mgr))
    now_mono = time.monotonic()
    last_mono = _LAST_COIN_CAP_CHECK_MONO.get(cache_key)
    if last_mono is not None and (now_mono - last_mono) < min_interval_sec:
        return None
    _LAST_COIN_CAP_CHECK_MONO[cache_key] = now_mono
    return run_coin_cap_update_to_recent(handle, run_time)
```

### 6.2 `core/exchange/adapter.py` & `core/pipeline/data_update.py`
* 为 `ExchangeAdapter.update_meta`、`update_exchange_meta`、`update_meta` 增加向后兼容关键字参数 `include_realtime_funding: bool = True`：

```python
async def update_exchange_meta(handle: BmacHandle, run_time, *, include_realtime_funding: bool = True):
    async with create_aiohttp_session(handle.http_timeout_sec) as session:
        exginfo_spot = update_exginfo(handle, session, 'spot', run_time)
        exginfo_swap = update_exginfo(handle, session, 'swap', run_time)
        tasks = [exginfo_spot, exginfo_swap]
        if handle.fetch_funding_rate and include_realtime_funding:
            tasks.append(update_funding_rates(handle, session, run_time))
        await asyncio.gather(*tasks)
```

### 6.3 `core/pipeline/update_cycle.py`
* 在 `run_ws_cycle` 中，仅在 `handle.data_api is not None`（或全 12 offset 无空档且开启 `coin_cap`）时创建 `aux_exe`，否则使用 `contextlib.nullcontext(None)` 零 spawn 开销：

```python
has_idle_slots = len(set(getattr(handle, "enabled_hour_offsets", ()))) < 12
need_inline_coin_cap = handle.coin_cap_handle is not None and not has_idle_slots
aux_workers = int(handle.data_api is not None) + int(need_inline_coin_cap)
aux_ctx = (
    ProcessPoolExecutor(max_workers=aux_workers, mp_context=_SPAWN_CTX)
    if aux_workers > 0
    else nullcontext(None)
)
```

### 6.4 `core/app.py`
* 新增常量 `ENABLED_META_LEAD_SEC = 22`（支持 `cfg.enabled_meta_lead_sec` 覆盖）。
* 启用 offset 在 `run_time - timedelta(seconds=lead_sec)` 唤醒并传 `include_realtime_funding=True`。
* 非启用 offset 在 `run_time - timedelta(seconds=15)` 唤醒，按 `not getattr(cfg, "skip_idle_realtime_funding", True)` 决定是否刷新实时预测资金费（默认 `False`），并维护 WS 动态订阅与健康检查；到达 `run_time` 后调用 `maybe_update_coin_cap_idle(handle, run_time)`。

## 7. 测试与验收清单

参考 `alpha/tests/test_producer_memory_hygiene.py` 与 `tests/test_producer_post1126_optimizations.py`：
1. `test_coin_cap_caches_latest_date_and_skips_aux_exe_in_ws_cycle`：验证首次扫描后缓存日期、冷却期内跳过、冷却后复用缓存零重读盘，且 `data_api=None` 时 `run_ws_cycle` 绝不创建 `ProcessPoolExecutor`。
2. `test_enabled_offset_wakes_at_t_minus_22s_and_disabled_offset_skips_realtime_funding_writes`：断言启用轮在 `T-22s` 唤醒且 `include_realtime_funding=True`，非启用轮在 `T-15s` 唤醒且 `include_realtime_funding=False`（0 次实时资金费 pkl 写盘）。
