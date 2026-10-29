# Binance fundingRate 接口参数与采集端修复（2026-10-29）

采集端 `core/history_data/funding.py` 依赖 `GET /fapi/v1/fundingRate` 拉取全量/增量资金费。历史日志里出现过 `403 Forbidden` 和"资金费率一直没有结果"，同时存在"这个接口只给 200 条 / 换代理才能拿到 1000 条"的说法。本文记录 2026-10-29 对官方文档、接口实测和代码的核对结论。

## 1. 官方文档写明的内容

`GET /fapi/v1/fundingRate` 参数：`symbol`、`startTime`、`endTime`、`limit`。

> `limit` — Default 100; max 1000
> If `startTime` and `endTime` are not sent, the most recent **200** records are returned.
> If the number of data between `startTime` and `endTime` is larger than `limit`, return as `startTime` + `limit`. In ascending order.

请求权重：与 `GET /fapi/v1/fundingInfo` 共享 **500/5min/IP**。现版文档与 legacy-docs 表述一致。

## 2. 实测行为（与文档有出入）

直连与 `http://127.0.0.1:7897` 代理返回完全一致，因此与代理无关。

| 请求 | 实测返回 |
|---|---|
| 不传任何时间参数，`limit` 省略 | 100 |
| 不传 startTime，`limit=200/300/450/500` | 按 limit 返回 |
| 不传 startTime，`limit=501/600/800/1000` | **一律 500，静默截断，不报错** |
| 只有 endTime，`limit=1000` | 500（endTime 救不了，只有 startTime 能突破） |
| **带 startTime**，`limit=1000` | **1000，可翻页** |
| `limit=1001` / `limit=0` | HTTP 200 + `{"status":"ERROR","type":"GENERAL","code":"99099990","errorData":"illegal params."}` |
| `symbol` 不存在 | HTTP 200 + `[]`（旧版是 HTTP 400 / `-1121`） |

带 startTime 翻页实测：BTCUSDT 从 2019-09-01 起连续 6 页、每页 1000 条，时间首尾相接无缺口，说明只要带 startTime 就能一直翻到上市首条。

## 3. 关于"什么时候改成 500 的"

- 官方 changelog 中 `fundingRate` 只有两条实质变更：**2023-11-01 新增 `markPrice`**、**2026-07-23 新增 `rateType`**。没有任何一条提到 limit 或返回条数。
- 文档至今仍写"最近 200 条"，实测已是 500，属于**未公告的实现变更**，无法从官方渠道拿到准确日期。
- 该端点已迁移到币安新后端服务，三条证据：非法参数走新的 `99099990` 信封且 HTTP 仍为 200；响应**不再返回 `x-mbx-used-weight-1m`** 头（`klines` 仍有）；未知 symbol 返回空数组而非 `-1121`。参数行为以实测为准，不能依赖文档。
- **"换代理后可以 limit 1000"与 limit 政策无关**。`logs/realtime_data.log.2026-10-15` 中请求一直是 `?symbol=xxx&limit=1000&startTime=...`，返回的是 nginx 的 `403 Forbidden` HTML（WAF 拦截）；换代理后 IP 不再被拦，同样参数即回 1000 条。

## 4. 代码改动

`core/history_data/funding.py`，保持请求语义不变，只改三处：

| 改动 | 原因 |
|---|---|
| 新增 `FUNDING_MAX_CONCURRENCY = 4`，用 `asyncio.Semaphore` 限制在途请求 | 原来把剩余 756 个 symbol 一次性 `gather` 出去（≈150 次/秒），是 WAF 403 的直接诱因 |
| 翻页游标 `+timedelta(hours=1)` → `+timedelta(milliseconds=1)` | `startTime` 是闭区间，`+1h` 会跳过 1h 结算与 `Special` 资金费 |
| 配套 `int(ts.timestamp()) * 1000` → `int(ts.timestamp() * 1000)` | 原写法把毫秒截成整秒，`+1ms` 会退化成重复取同一秒，必须一起改 |
| 进度日志 `logger.debug` → `msg` + 30 秒节流的 `logger.info` | 与 `candle.py` 一致；原来全量下载期间日志一条不输出，表现为"卡死" |

同批改动另见统一文档 2026-10-29 节：`core/utils/log_kit.py` 多进程日志轮转修复、`core/history_data/candle.py` 阶段 3 进度改 INFO。

## 5. 验证（alpha311，未重启线上进程）

- `py_compile` 通过。
- 用改动后的游标逻辑离线复刻翻页：BTCUSDT/ETHUSDT/BTCUSDC/SOLUSDC 各 1757 条、2 页、`重复=0`、间隔全为 8.0h、覆盖 2025-03-23~2026-10-29。
- WMTUSDT/JPMUSDT 为 498/503 条，间隔里出现 0.0、1.0、4.0、8.0 小时——正是 `Special` 资金费与 1h/4h 结算的情况，证明 `+1h` 游标会漏数据。
- 并发 4 与并发 8 的吞吐都是约 5 次/秒（瓶颈在代理而非并发数），全量 1512 次请求约 5 分钟。

## 6. 运维注意

- 取历史一律用 `limit=1000` + **必带 `startTime`**；不要用"不传 startTime + limit=1000"，会被静默砍成 500 且不报错。
- 完成判定 `len(df_new) < FUNDING_RATE_BATCH_LIMIT` 仅在带 startTime 时成立。
- 新服务不回 `x-mbx-used-weight-*` 头，限流只能靠 429/403 观察；出现时把 `FUNDING_MAX_CONCURRENCY` 降到 2。
- `symbol` 不存在现在返回 `[]`，代码会走 `df_new.empty` 分支标记完成，不抛异常，下架/拼写错误会被静默吞掉。
