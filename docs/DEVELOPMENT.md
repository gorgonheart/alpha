# 两项目统一开发文档入口

当前统一说明维护在 [alphaonline/docs/DEVELOPMENT.md](../../alphaonline/docs/DEVELOPMENT.md)，本机绝对路径为 `D:/alphaonline/docs/DEVELOPMENT.md`。新对话先读该文档及本项目 AGENTS.md，再读相关专题。

此处只维护入口，避免生产者和消费者各写一套互相矛盾的当前状态。如果单独迁移 alpha 仓库，请同时携带配套 alphaonline 文档；缺少它时需从代码与历史测试核对约定，不能凭旧 CLAUDE.md 猜测。

2026-10-29 采集端修复（alpha 侧专题，已随主干提交）：

- [Binance fundingRate 接口参数与采集端修复](2026-10-29-funding-rate-api.md)：`limit=1000` 仅在带 `startTime` 时生效，不传 startTime 会被静默截断到 500（文档仍写 200，属未公告变更）；`funding.py` 改为信号量限流、游标 `+1ms`、进度改 INFO。
- 同批：`core/utils/log_kit.py` 多进程日志轮转修复（子进程写 `logs/<name>-worker-<日期>.log`）、`core/history_data/candle.py` 阶段 3 进度日志改 INFO。

本次 2026-10-14 修正：预计算动量使用 `动量210401_双项异常隔离V2` 及独立 `momentum_v2_build.py`；CciV3/ILLQ 不变。111 与异常 CCI 是消费端因子，不能误接到 CciV3 扩展。
