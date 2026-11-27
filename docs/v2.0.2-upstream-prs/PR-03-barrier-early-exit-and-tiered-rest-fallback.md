# PR-03: Barrier 竞态安全早退、可取消 14s 分级 REST 兜底与按需启动补齐

* **建议分支**：`perf/barrier-early-exit-and-tiered-rest`
* **目标仓库 / 基线**：`coin-realtime-data_v2.0.2`
* **前置依赖**：无（建议在 PR-02 之后合入）
* **参考源提交（`alpha`）**：`ddd81eb`、`3e8972b`、`0d64e9d`

---

## 1. 场景

WS 模式下每个启用周期，`update_cycle.py` 会并发启动：
1. `wait_for_ready_barrier` 轮询全市场（1,287+ 币种）就绪状态；
2. 后台 `_rest_fallback_worker` 线程在等待 grace 窗口后，向交易所 REST 拉取 WS 漏网币种；
3. 进程启动后进主循环前的 `run_startup_catchup` 启动补齐。

在出现刚上线未开盘新币、偶发单根 WS 漏推或 Writer 正常落盘尾段时，原有逻辑存在三类明显的性能退化。

## 2. 目标

1. **消除未开盘新币导致的 45s 软超时假阻塞**：当 REST 确认某币无任何数据或请求已结束且无待写盘项时，Barrier 竞态安全地立即提前退出。
2. **消除假缺口 REST 抢跑与队列反压**：将 `ws_wait_sec` 显式配置化并上调至 `14.0s`（可配置覆盖）；当双市场 Barrier 100% 完成且无 `.gap` 时通过 `cancel` 事件立即终止后台 REST 线程。
3. **分级 REST `limit` 削减 95% 冗余传输**：无 `.gap` 历史断档、仅本轮单根 WS 漏推时仅请求 `limit=5`（而非全量 `limit=99`）；仅带 `.gap` 断档标记才拉 `once_update_candles=99`。
4. **启动补齐按需化**：`run_startup_catchup` 仅拉取本地缺失的 symbol（不再无条件全量拉取全市场 1,287 币），滚动重启且本地已覆盖时实现 **0 次 REST 请求**。

## 3. 问题（v2.0.2 现状与根因）

1. **Barrier 缺少早退机制，遇未开盘新币必等满 45s**（`v2.0.2/core/pipeline/barrier.py:85-118`）：
   * 只有超过 `timeout_sec=45.0` 才检查 `rest_done.is_set() and not (pending & submitted)`。只要交易所 `exginfo` 中存在 1 个未开盘新币（WS 不推、REST 返回空），Barrier 每次都干等满 **45 秒**。
2. **`ws_wait_sec=10.0` 隐式耦合且压在 Writer 排空中段**（`v2.0.2/core/pipeline/update_cycle.py:36-38, 157-179`）：
   * `_REST_FALLBACK_GRACE_SEC` 用 `inspect.signature` 隐式耦合 `barrier` 默认值 `10.0s`，且用不可取消的 `time.sleep(10.0)`。
   * 即使 Barrier 早已 100% 放行，REST 线程仍会睡满 10s 再扫描目录；而若 Writer 排空耗时 12~15s，10s 判定点会误判 200~350 个在途币种为“缺失”，触发全量 `limit=99` REST 请求，往同一 Writer 队列再塞 ~25,000 行重复数据，造成**自我加重拥塞**。
3. **REST 兜底“一刀切”拉 99 根**（`v2.0.2/core/pipeline/data_update.py:126`）：
   * 单根漏推与多小时断档均发 `limit=99`，载荷与 Writer 合并成本高出约 20 倍。
4. **`run_startup_catchup` 盲目全量 REST**（`v2.0.2/core/pipeline/update_cycle.py:126-140`）：
   * 重启后无视本地 5m/WS 缓存已有数据，强行对全市场 1,287 币各拉 99 根 5m K 线（OKX 实测 40–54s，Binance ~25s）。

## 4. 项目文档中的分析与实测证据

* **83 轮生产级统计（`alphaonline/docs/2026-11-24-grace-tuning-analysis.md`）**：
  * 逐 symbol `.ws_ready` mtime 曲线（3 轮 1,287 币）：`+12s`（2+10）时 swap 覆盖率仅 `66.0% / 78.7% / 86.8%`，`+15s` 时三轮全部 **100%**（max `14.92s`）。
  * 83 轮中位假缺口 **`290` 个/轮**（swap 中位 `245`，占全域 `32.1%`，**0/83 轮为零缺口**）。
  * 24,114 次兜底请求中至少 **42.5%（10,243 次）** 可严格证明纯属冗余；随机 120 币 × 300 根的 `_source` 溯源显示 spot `100% ws`、swap `99.82% ws`。
  * 每次误触 REST 额外耗费 API 时间 `5.4s`（中位）+ Writer 队列延长 `2~4s`，导致慢轮 barrier 被拖到 `21~22s`。
  * 参数扫描证伪“缩短 grace 到 3s”（会把 55–77% swap 推给 REST，请求涨至 ~1,050），确认 **`ws_wait_sec=14.0`（判定点 `+16s`）** 为兼顾 0 假缺口与掉线兜底的最优解。
* **重启按需补齐实测（`alphaonline/docs/kb/performance-analysis.md` §2）**：上线 `0d64e9d` 后，滚动重启阶段 2 实现 **0 REST 覆盖**，首轮零修订。

## 5. 代码改动（面向 `coin-realtime-data_v2.0.2`）

### 5.1 `config_binance.py` / `config_okx.py` & `core/handle.py`
* 新增可配置项 `ws_wait_sec = 14.0`（带非负数值校验，写入 `BmacHandle.ws_wait_sec: float = 14.0`）。

### 5.2 `core/pipeline/barrier.py`
* 新增可选参数 `rest_confirmed_empty=None`，并在 `while True` 顶部竞态安全采样 `rest_done_was_set`：

```python
    while True:
        # 必须在扫描 pending 之前采样 rest_done，确保 rest_done.set() 之前写入的 .ready
        # 至少被完整扫描一轮，绝不会因竞态被误判为 skipped。
        rest_done_was_set = rest_done is not None and rest_done.is_set()
        ws_grace_passed = (time.monotonic() - start) >= ws_wait_sec
        submitted_set = set(rest_submitted or ())

        waiting_gap: set[str] = set()
        if rest_done is not None:
            gap_syms = reader.list_gap_marker_symbols() & pending
            if gap_syms and rest_done_was_set:
                gap_syms &= submitted_set
            waiting_gap = gap_syms

        for sym in list(pending):
            if sym in waiting_gap:
                continue
            if reader.check_ws_ready(sym, run_time) or (
                ws_grace_passed and reader.check_ready(sym, run_time)
            ):
                pending.discard(sym)
                success.add(sym)

        if not pending:
            break

        # 竞态安全提前退出：完整扫描完 rest_done 后的全部 .ready，且剩余 pending 均不在待落盘集合中
        if ws_grace_passed and rest_done_was_set and not (pending & submitted_set):
            break
```

### 5.3 `core/pipeline/data_update.py`
* 为 `fetch_update_resample`、`async_fetch_update_resample_candles`、`fetch_update_binance_candles` 增加可选参数 `symbol_limits: Optional[dict[str, int]] = None`（及空集合短路）：

```python
    if num_symbols == 0:
        return dict(), dict()

    limits_map = symbol_limits or {}
    params_list = [
        {'symbol': s, 'interval': base_interval, 'limit': int(limits_map.get(s, once_update_candles))}
        for s in symbols_trading
    ]
    ...
    for symbol, df_base in zip(symbols_trading, base_klines_list):
        req_limit = int(limits_map.get(symbol, once_update_candles))
        window_truncated = len(df_base) >= req_limit
        if not df_base.empty and 'candle_begin_time' in df_base.columns:
            df_base = df_base[df_base['candle_begin_time'] < run_time]
```

### 5.4 `core/pipeline/update_cycle.py`
1. 拆分 `_compute_missed_and_gaps()` 返回 `(missed, gap_symbols)`（保留 `_compute_missed_symbols` 兼容包装）。
2. `_start_rest_fallback` 的 `state` 新增 `"cancel": threading.Event()`、`"confirmed_empty"` 与 `"failed"`。
3. `_rest_fallback_worker` 使用 `cancel_ev.wait(timeout=grace_sec)` 替代 `time.sleep`；屏障双市场 `success == total` 且 `not has_gaps` 时，主流程立即调用 `rest_state["cancel"].set()`。
4. 真正需要 REST 兜底时按是否有 `.gap` 构造 `symbol_limits`：

```python
_SINGLE_BAR_FALLBACK_LIMIT: int = 5

symbol_limits: dict[str, dict[str, int]] = {"spot": {}, "swap": {}}
for tt, m_set, g_set in (("spot", spot_missed, spot_gaps), ("swap", swap_missed, swap_gaps)):
    mh = handle.get_market_handle(tt)
    full_lim = int(getattr(mh, "once_update_candles", 99))
    single_lim = min(_SINGLE_BAR_FALLBACK_LIMIT, full_lim)
    symbol_limits[tt] = {sym: (full_lim if sym in g_set else single_lim) for sym in m_set}
```

5. `run_startup_catchup()` 改为先调用 `_compute_missed_and_gaps()`，仅当 `spot_missed` 或 `swap_missed` 非空时才按 `symbols_subset` + `symbol_limits` 拉取缺失子集。

## 6. 测试与验收清单

参考 `alpha/tests/test_2b_ws_cycle_and_app.py`、`tests/test_parallel_pipeline_phase1.py`、`tests/test_compute_io_overlap_phase2.py`：
1. `test_barrier_exits_early_after_rest_done_for_unopened_listing_without_race`：验证未开盘新币在 `rest_done` 后 `<1s` 早退，不等满 `timeout_sec`。
2. `test_barrier_does_not_skip_ready_written_mid_scan_before_rest_done`：构造扫描中途落盘 `.ready` 并立刻 `rest_done.set()` 的极端竞态，断言绝不误判为 `skipped`。
3. `test_rest_fallback_cancels_immediately_when_barriers_complete_with_zero_misses`：配置 `ws_wait_sec=5.0`，屏障就绪后验证整轮 `<1.0s` 退出且 0 REST 调用。
4. `test_rest_fallback_uses_small_limit_for_single_bar_miss_and_full_limit_for_gap_symbol`：断言单根漏推 `limit=5`、带 `.gap` 标记 `limit=99`。
5. `test_run_startup_catchup_skips_covered_ws_symbols_and_fetches_only_missed`：验证已覆盖 symbol 被跳过、仅缺失 symbol 触发 REST。
