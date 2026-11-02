# 五分钟调度、最近offset补算与采集预热

用户确认实施：更新、补算或空检查完成后，均转入完成时刻之后的下一个5分钟调度点；每次补最近即将到来的一个缺失offset；采集进程在目标时刻前预热。正常采集仍需metadata成功，且不能提前请求未闭合K线。

## 最小修改与人工复核

| 文件/位置 | 本轮修改 | 审核点 |
|---|---|---|
| realtime_data.py / update_all | 将既有metadata准备移入提前创建的进程池范围；提交等待目标时刻的预热任务；任务就绪后才提交真实采集 | 在run_time前15秒开始，进程导入与metadata I/O重叠；metadata失败或预热异常不取数；每轮结束按原上下文释放进程 |
| core/backfill.py | MAX_OFFSETS_PER_WINDOW由2改1，冷却过滤后按下次到达分钟排序；同步注释 | 08:25优先40m，08:45优先55m，08:58优先下一小时20m；只对缺ready且不在失败冷却中的offset排序 |
| realtime_data.py / main、非offset分支 | 沿用前轮已实现代码，本轮无新增修改 | 每次完成后next_run_time(base_interval)重新调度；非offset分支等待该点再检查一次，不每分钟探测 |
| 其他core、因子、C、账户、alphaonline业务代码 | 本轮零修改 | 共享exchangeInfo、K线/资金费时间轴、ready/pivot契约、因子线程数4保持 |

当前base_interval=5m。程序在08:22:08完成正常更新，则08:25检查补算；若补算于08:27结束，下一次08:30；若跨过08:30到08:31才结束，则下一次08:35，不补跑已经错过的调度点。空检查及被捕获的补算错误同样转下个边界。更新异常仍保留产物并使用既有恢复路径，其后也由基础周期调度，不回到完整初始化。

补算“下一个offset”指下一次即将到来的缺失分钟偏移，历史run_time仍使用最近已到达边界。例如08:25选40m，补算截止07:40，绝不伪造08:40 ready。共享最新exchangeInfo按当前交易池预热，价格仍只用历史已闭合K线；240秒预算、数据新鲜度、未来数据截断和失败冷却保留。已完成/冷却中的近offset不阻止选择后续缺失offset。

预热不使用常驻池。按已启用任务计算1–3个worker（币安必需，Data API/市值可选），分别提交sleep_until_run_time任务占住worker，确保通常能在目标前并行完成spawn/import。主进程同时准备metadata及映射，等到目标时刻并消费全部预热结果，再提交币安/Data API/市值采集。metadata晚到或准备窗口不足仍可能晚启动，不能承诺网络请求毫秒级准点。只把with前移不能确保worker启动，此实现实际提交了预热任务。

## 验证

TDD正确红阶段5项失败（research/dispatch_prewarm_red2.log）。最终30项专项通过，2.09秒：补算选择及预热11项、调度/恢复12项、共享exchangeInfo7项。覆盖最近offset、跨小时、metadata失败不取数、提前创建池并提交预热、所有任务不早于目标、四种Data API/市值开关组合、0/128/380秒补算后的下一边界、错误恢复与共享信息。日志research/dispatch_prewarm_final.log。

另以真实Windows spawn执行实际update_all，替换交易所/市值/metadata为离线模拟函数，不调用网络、不写生产行情。冷启动3个任务分别晚3.956745/3.966506/3.976500秒，预热后三个独立PID均晚0.002256秒，全部不早于目标。采样一次，仅证明接线和可消除启动等待，不是线上延迟保证；实际BmacHandle传输、网络和机器负载尚需运行日志验证。结果research/dispatch_prewarm_verify_20261031/result.json，脚本research/verify_dispatch_prewarm.py，日志research/dispatch_prewarm_verify.log。

未重启服务、部署、发送通知或删除生产数据。人工审核重点为update_all的准备顺序/进程生命周期及backfill的单目标排序；需重新加载realtime_data进程后才生效。旧历史文档的“每空档补两个、配置顺序优先、整点后创建采集池”由本说明替代。
