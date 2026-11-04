# 正式服务器可删除测试目录

2026-11-05按用户要求，本轮alpha2测试统一收纳在`/home/ubuntu/alpha2/test/`。人工可在离线测试结束后删除整个test目录；业务运行、机器配置、行情、原生扩展、PM2及Git主工作副本不依赖该目录。

## 目录与接口

- 开发仓库继续维护`tests/`，不重命名开发测试或修改测试夹具路径。
- 正式alpha2使用本机Git sparse checkout排除根`tests/`，正常Git fast-forward后不会重新散落测试源码。原`tests/`迁入`test/archive/tests/`；本轮独立对拍脚本和历史测试/构建报告也迁入archive，原始文件保留等待人工删除。
- `bash tools/deploy_alpha.sh verify`通过`tools/run_deployment_tests.py`从当前已提交Git树生成`test/verify-*/source/`完整独立副本，不复制私有config.json。当前机器C扩展仅复制到测试副本，没有业务代码指向测试副本的反向引用。
- 测试副本使用空API凭据、关闭Data API和隔离storage_path；pytest临时目录、系统临时目录、Numba缓存、Python缓存和verify.log均位于同一verify-*目录。
- 保留`tools/run_deployment_tests.py`和`tools/deploy_alpha.sh`作为运维入口，两者都不在程序启动时加载。删除test不影响业务；下次人工运行verify自动重新生成。
- 业务data、logs、config.json、factors/c_ext本机扩展和.git均不在test；不要删除这些目录或用test下的代码启动业务。部署备份及Git发布包属于运维资产，没有混入测试目录。

## 最小修改与验证

业务源码、core、config、realtime_data和startup零修改。本轮仅部署工具、Git忽略项、工具专项测试和维护文档。4项工具测试覆盖私有配置隔离、sparse checkout恢复夹具、删除后业务独立/可再次生成、测试报告和临时产物路径。

正式迁移保持现有alpha及alpha2的PM2主进程PID不变，不重启、不下单、不改行情。验证完成后暂时把test改名隐藏，使用生产路径导入运行入口及相关模块，并检查PM2 PID与运行状态；随后恢复目录，留待人工整体删除。该检查不调用main或真实计算流程。

## 人工维护

离线验证命令：

```bash
cd /home/ubuntu/alpha2
bash tools/deploy_alpha.sh verify
```

完成后核对输出的测试结果和`test/verify-*/verify.log`，即可人工删除`/home/ubuntu/alpha2/test`整个目录。不要在verify仍运行时删除。即便目录已经删除，未来Git更新及业务启动也不要求恢复；只有再次运行verify时才重建。

需要改变生产排除规则时可查看`git sparse-checkout list`；不要执行`git sparse-checkout disable`，否则Git会重新检出根tests目录。其他机器开发工作副本不自动启用该规则。

## 正式验收记录

部署代码提交`ef4af1134afc1097be767faa62a7fb9d1980c2df`，Git主干和正式alpha2已同步。core、config、factors、realtime_data及startup均无本轮业务差异，没有重编译或替换运行中的C扩展。

工具专项4通过；本地原入口152通过（23.85秒），新隔离入口152通过（36.60秒）；Ubuntu新隔离入口152通过（10.88秒）。报告为`/home/ubuntu/alpha2/test/verify-a_ps_rxc/verify.log`。

正式已将原`tests/`和9个旧测试/构建报告、独立对拍脚本迁入`test/archive/`。旧报告原先位于`/home/ubuntu/alpha-deploy-logs/`及`/home/ubuntu/alpha2-*.log`，历史文档里的旧路径只代表迁移前位置。部署备份、业务日志和原生扩展保持原位。测试目录最终899个文件，全部保留等待人工删除，没有代用户删除整个test目录。

Git sparse checkout排除根tests/后工作区干净。生产实际执行了可逆缺目录验证：临时将test改名隐藏，新的Python进程从生产路径成功导入config、core.arrow_publish、core.preprocess、realtime_data，确认模块和storage_path均不指向test；随后恢复test。测试fixture另验证过实际删除test后业务仍可运行且verify可重新生成。

迁移前后PM2状态一致：alpha为stopped、PID0（与更早只读审计时online不同，是本轮执行前已有状态）；alpha2为online、PID3855266。config.json逐字节保持，未启动/停止/重启任何服务，未下单，未清业务数据。检查结束可用空间约79GiB；该容量变化不能归因于本次测试迁移。
