# 每轮 exchangeInfo 复用与空档补算

已被后续简化方案替代：用户确认补算使用最新共享exchangeInfo，无需各offset独立快照或历史轮次匹配。此前core/binance_update.py新增代码已全部恢复原版。下文仅保留历史说明；当前实现和验证以[共享exchangeInfo复核](2026-10-31-shared-exchange-info.md)为准。

调度补充（2026-10-31）：非offset分支现先等待基础周期调度点，5m配置下只在08:25/08:30等边界检查补算，不再在08:22/23/24反复检查未来08:25。仅realtime_data分支改动，恢复及快照专项13项通过，未重启。共享exchangeInfo每轮覆盖，每offset独立快照只保留最近一轮，不累积小时历史；无对应快照时仍等待。仅重采样K线可不依赖exchangeInfo，完整预处理还依赖币种列表和现货/合约映射，本轮未新增“仅重采样预热”流程。

后续修正（2026-10-31用户确认）：realtime_data.main已增加初始化成功状态。成功后的更新/调度异常保留产物，等待60秒后重新调度，不再自动清目录初始化；首次初始化未成功仍按原流程重试。缺失offset补算入口保留。恢复与快照专项11项通过（1.90秒），未重启服务。本段替代文末“旧异常行为未改”的历史状态；下方最小范围表记录前一轮快照改动，本次另增加main的状态及日志接线，不新增core修改。

用户要求后续步骤复用每个分钟偏移开始时获取的 exchangeInfo。原实现的非offset分支不会在触发补算前更新metadata，而是在每个backfill_one_offset内部重新调用update_binance_meta；两个补算目标会各自再请求现货、合约exchangeInfo及资金费。

现改为正常轮次获取后保存小型快照，补算只读取对应offset和精确run_time的快照。新增目录位于exg_mgr.base_dir的同级exchange_info_snapshots/<offset>/，每个offset保留最近一轮的现货/合约DataFrame和ready。只保存已过滤的交易所信息，不复制行情、Arrow或MODEL，不增加哈希/manifest或逐币验证。目录独立于init_dirs清理的exginfo和预处理目录，因此现有自动初始化不会删除这些快照。

## 最小源码改动及人工复核

| 文件 | 改动 | 审核点 |
|---|---|---|
| core/binance_update.py | +22行，两个导入、一个小型路径helper、update_binance_exginfo两处接线 | 同市场/同run_time已经ready则复用；新轮次获取新数据；获取后保存对应offset快照；已有该轮共享数据也能无网络补建快照 |
| core/backfill.py（既有未跟踪文件） | 仅backfill_one_offset的metadata段及所需导入、模块说明 | 移除update_binance_meta网络调用；精确检查现货/合约两份ready及文件；用临时exg_mgr和浅拷贝handle生成补算映射，避免改变共享exginfo的run_time |
| realtime_data.py | 本轮零新增修改 | 保留此前backfill接入；首次正常metadata请求失败后的原异常重初始化行为本轮未改 |
| history_data/history.py、history_data/utils.py | 本轮最终零修改 | 初始化下载流程保持原样 |
| alphaonline/core、startup、账户、因子、线程数 | 本轮零修改 | 不扩展到交易或因子语义 |

没有对应快照、跨offset、现货/合约轮次不一致时返回stale，等待后续正常轮次；不追加接口请求、不使用新快照冒充旧轮次、不提前写预处理ready。初次安装该功能时，已被旧初始化删除的历史exchangeInfo无法恢复；须等对应正常offset获取后才有可用快照。首次冷启动也可能因此等到正常offset，不再为提前补算额外获取exchangeInfo。

原始K线新鲜度、未来K线截断和offset往返守卫保留。历史资金费沿用已有磁盘数据，补算不再次刷新；正常offset资金费更新保持原流程。同轮重复调用update_binance_meta时仅exchangeInfo复用，资金费不纳入此次缓存。

## 验证与状态

TDD先复现四项失败，补充“已有该轮共享数据但尚无独立快照”的失败用例后修复。最终16项专项通过（3.99秒），包含5项exchangeInfo测试、6项共享因子池测试、5项预处理发布测试。覆盖同轮请求一次/下一轮刷新、snapshot跨offset隔离、共享exginfo被清理后快照仍在、无网络补建快照、补算缺失/混轮/错offset等待、补算不改变原共享文件和ready。日志alphaonline/research/exg_reuse_red.log、exg_reuse_seed_red.log及exg_reuse_delivery.log。

仅离线测试，未请求真实交易所、未重启或部署、未删除生产数据。首次正常exchangeInfo请求本身失败仍可能触发旧全量初始化，这与取消补算重复请求是两件事；尚未改变用户未确认的失败处理选择。独立快照可在此初始化过程中保留，但不能避免行情预处理目录按旧逻辑被清理。
