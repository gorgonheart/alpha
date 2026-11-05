# 市值请求可配置代理

`config.py`新增`coin_cap_use_proxy=False`，默认延续直连；机器`config.json`可覆盖：

```json
{
  "proxy": "http://127.0.0.1:7897",
  "coin_cap_use_proxy": true
}
```

开启后，市值产品状态主/备地址、受鉴权的每日下载链接接口及CSV下载均复用`proxy`。HTTP和HTTPS目标使用同一HTTP代理地址。开关必须是布尔值，开启时必须配置代理；连接失败沿用既有重试与失败处理，不自动改为直连。

运行代码仅修改两个core文件：`coin_cap.py`在初始化补历史与周期更新入口传入所选代理；`quantclass_async.py`增加兼容原三参数构造的可选`coin_cap_proxy`，仅应用到市值三类请求。`handle.py`、DataAPI K线、Binance、通知、请求重试、CSV解析/合并及因子数学不变。开关在启动时读取，修改后需重启alpha生效。

TDD新增9项首次全部失败；实现后市值/配置11项通过，关联调度/恢复/交易所信息共43项通过。覆盖默认直连、JSON开启、非法配置、主地址失败仍代理访问备用地址、鉴权参数保留、实际三步更新及合并结果、旧构造与K线不改变路由。独立复核无阻塞问题。

本轮在独立worktree/分支`codex/coin-cap-proxy-20261105`开发，未修改另一工作区的Arrow需求发布候选。测试部署只替换两处core并向现有配置增量添加开关，不覆盖其他候选代码；测试代码、临时数据和日志放在alpha/test下，可人工删除，运行源码与配置不依赖它。原生扩展无需重新构建。

目标仅`43.173.122.28:/home/ubuntu/alpha`。验证先在隔离副本运行11项测试，再通过实际7897代理读取产品状态、获取一天已完成市值数据的下载链接并下载解析CSV，不写业务缓存、不输出密钥或签名URL。通过后备份运行文件和机器JSON，开启测试机开关，仅重启PM2 realtime_data；不改动交易消费者、Mihomo或正式服务器。

## 测试服发布结果

功能提交`f7f4bf48b3ead0e612bebd6aec1e8541324d0e0b`保存在隔离分支，未合并或修改另一工作区的未提交Arrow候选。测试机不是Git安装，本轮只增量发布`config.py`、`config.json`和上述两个core文件；其他代码、原生扩展、策略与机器差异保持。

Ubuntu隔离副本11项通过（1.68秒）。真实代理验证取得2026-11-04市值CSV，832行/830币，产品状态、鉴权下载链接、CSV三次请求均断言传入代理：候选2.794秒；发布后直接加载已安装Quantclass客户端再验1.876秒，均成功，不写业务市值缓存、不输出API密钥或签名URL。

16:33（UTC+8）仅重启测试PM2 realtime_data，PID753634→981057；交易端alphatrade-dryrun PID850132不变。新启动日志16:33:37确认“初始化市值数据成功”；Mihomo未重启，正式服务器未操作。测试机私有JSON已设置`coin_cap_use_proxy=true`，代理`http://127.0.0.1:7897`，权限600。

- 测试及日志：`/home/ubuntu/alpha/test/coin-cap-proxy-6doauf9e/`，含offline.log、online.log、installed_online.log及隔离副本，验证结束后可整体人工删除。
- 运行文件/机器配置备份：`/home/ubuntu/alpha-deploy-backups/coin-cap-proxy-20261105T083324Z`，UTC命名、目录700/文件600；不依赖test目录。回退需恢复四个备份文件并仅重启测试alpha。
- 发布前16:25日志另有DataAPI K线下载链接超时；本轮该通道保持原直连行为，不能把市值验证通过描述为DataAPI全部请求恢复。
