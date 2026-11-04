# Arrow 单轮保留与正式部署

## 行为与最小修改范围

2026-11-05用户确认：先清理正式alpha2旧轮，只保留各offset/各市场最后一轮，再发布配置化单轮保留修复。保留完整历史深度，不改变因子、K线、PKL、Pivot或交易数学。

- `config.py`增加`arrow_snapshot_keep=1`，支持既有私有`config.json`覆盖，必须是正整数。每轮只有一次整数检查，无文件内容扫描。
- core仅`core/preprocess.py`接入该配置，`core/arrow_publish.py`负责失败临时文件回收及保留策略；realtime_data、startup和alphaonline业务代码零修改。
- 每轮写自己的`{spot|swap}_dict_{epoch}.arrow.tmp`，写完fsync、原子replace后才清旧轮。相同轮次重跑原子覆盖；相邻轮次成功后删除旧轮，目录最终只留一份。失败保留上次完整快照，删除本次半成品。
- 保留时间戳文件名、schema、full_history、symbols及原ready发布顺序；不采用固定文件名覆盖，alphaonline仍精确按run_time取同轮。补算旧轮时优先保留刚发布的current_epoch，不能按最大时间戳误删该轮。
- 单轮只描述稳定占用，写新轮仍需要临时空间。Linux已打开的mmap在旧路径unlink后仍可读，尚未打开的旧轮路径不会再保留；消费者需在下一小时同offset发布前完成读取。如有跨小时读取需求，可将配置调至2。

## TDD与接口验证

新增行为测试首次9失败/3通过，确认旧版不支持单轮配置且写满会遗留临时文件。修复后本地alpha311部署专项148通过（11.82秒），包括所有11个5分钟offset、配置覆盖/非法值、同轮原子替换、旧轮补算、ENOSPC/replace失败、完整/裁尾数值及ready顺序。未改golden或数值容差。

独立离线脚本调用实际alphaonline的Arrow读取模块：11个offset×2市场×完整/裁尾共44组逐值一致。Linux发布后需再次调用服务器实际alphatrade读取模块，并验证旧mmap在unlink后的读取；验证不下单、不请求外部API、不触碰业务快照。

## 正式清理记录

目标`ubuntu@43.167.235.164:/home/ubuntu/alpha2`。清理前确认PM2列表已经为空，alpha2无写入进程。逐一打开每组最新Arrow元数据确认有效后，删除旧轮Arrow、对应旧ready及残留Arrow临时文件；保留12个完整文件。删除14个文件，逻辑大小合计21680197904字节（20.191GiB），磁盘可用从0恢复约16GiB。没有删除其他PKL、行情、Pivot、snapshot或改变其他服务。

当前PM2没有alpha2条目，因此本轮只部署和离线验证，不创建/启动新生产者，不执行pm2 resurrect。服务器配置为5/15/25/35/45/55m，保留现有凭据、路径和代理；原config.py的相同offset/代理改动应移到config.json，源码回归干净。

## PM2恢复边界

ubuntu的dump.pm2与dump.pm2.bak均不存在，日志06:14起ENOSPC，06:27的pm2.pid是0字节，当前daemon列表为空。不能直接pm2 resurrect恢复原列表；应从既有startup.json/历史命令逐项重建，确认无同程序存活，再pm2 save。不能为了恢复alpha2同时自动启动所有交易实例。此次检查不承诺EOFError已修复；空间不足后PKL读取错误需要另行定位。

Linux验收、提交号和日志在发布后追加。
