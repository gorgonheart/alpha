# 最新共享 exchangeInfo：简化实现与复核

后续用户确认的例外（2026-10-31）：缺失或不完整的共享exchangeInfo不再只等待，backfill_one_offset主动调用一次原metadata更新流程，浅拷贝handle关闭本次资金费请求，只获取现货/合约exchangeInfo。实际获取时刻作为共享ready时间，历史补算仍按原K线截止。成功后继续补算，后续复用；失败走原错误/冷却，不生成半成品。获取后预算不足240秒只保存metadata、推迟重计算。本次仅backfill缺失分支及导入修改，realtime_data/binance_update零新增修改。TDD6项失败后修复，共享/调度/恢复专项32项通过（2.36秒）；日志alphaonline/research/exg_bootstrap_red.log、exg_bootstrap_green.log。未请求真实接口或重启服务。本段替代下文缺失时只等待的历史说明。

按用户确认，exchangeInfo采用当前交易池口径：每个正常offset开始时沿用原流程更新一份现货和一份合约共享文件，其后采集、预处理读取它们。空档补算复用最新完整共享数据，不另外联网，不要求数据来自该offset或上一小时。K线和历史资金费仍按原时间轴处理，不使用未来K线。

## 本轮代码范围

| 文件 | 结果 | 复核重点 |
|---|---|---|
| core/binance_update.py | 完整恢复Git HEAD原版，本文件零差异 | 移除此前新增22行：独立快照helper、导入、保存、补建及同轮缓存分支；正常offset本来只调用一次metadata更新，无需另加缓存层 |
| core/backfill.py | 修改backfill_one_offset的metadata段、移除独立快照导入、更新说明 | 用最新共享exginfo，检查现货/合约文件及其共同ready轮次；该轮次不与历史K线run_time比较。防止一次更新仅完成一个市场时混用半更新数据 |
| realtime_data.py | 本轮零修改 | 保留已确认的每5分钟补算调度、更新失败保留产物和首次初始化逻辑；未增加与独立快照有关的调用，故无须回退这些独立功能 |
| core/preprocess.py、初始化/文件系统、账户及alphaonline业务代码 | 本轮零修改 | 保留已有并行、框架ready及K线守卫，不扩大core修改范围 |

补算仍使用小型临时exg_mgr和浅拷贝handle，将共享币种数据用于本次历史预热的映射和ready，不修改真实共享文件的run_time，也不删除真实ready。这是适配框架现有精确ready检查所需的隔离，不是每offset持久快照。

缺少完整共享数据时返回stale，等待下个正常offset获取，不发请求、不发布缺少metadata的半成品。首次启动的原初始化流程会清共享exginfo，因此首次正常offset获取之前可能等待；本轮不修改初始化流程。运行更新失败保留历史产物的既有修复继续生效。

旧exchange_info_snapshots目录不再读写；没有删除已存在的生产目录。因子和预处理按当前币种池预热，不承诺复现历史时点的上下架状态。此次未改变历史K线截断、基础数据新鲜度或资金费计算。

## 验证

TDD红阶段3 failed / 4 passed：旧实现仍创建独立目录，且拒绝用00:10共享数据补23:20/23:40。修改后20项专项通过（2.30秒）：共享exchangeInfo7项、调度/异常恢复8项、实际预处理发布5项。验证正常offset共享覆盖、无独立目录、跨offset/历史时间复用、缺失/半更新等待、无网络补算、不改写共享文件/ready，以及每5分钟调度和更新失败不再初始化。

日志：alphaonline/research/shared_exg_red.log、shared_exg_green.log。core/binance_update.py已对照HEAD确认无差异，源码中不再引用exchange_info_snapshot_manager或exchange_info_snapshots。未重启、部署、发送通知、调用真实接口或删除生产数据。
