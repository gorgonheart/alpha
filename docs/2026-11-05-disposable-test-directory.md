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

服务器验收结果与实际迁移清单在发布后记录。
