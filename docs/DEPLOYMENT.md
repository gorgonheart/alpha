# alpha Linux部署与维护

## 当前布局

- 工作副本：`/home/ubuntu/alpha2`，分支`main`。
- SSH私有Git远程：`/home/ubuntu/repos/alpha.git`；部署端origin指向此仓库。
- 独立解释器：`/home/ubuntu/miniconda3/envs/alpha311/bin/python`，Python3.11。
- 机器差异和凭据：工作副本内`config.json`，权限600，Git忽略。策略参数默认维护config.py一份；JSON只放服务器差异及凭据。
- 数据：由JSON的storage_path指定，默认`/home/ubuntu/alpha2/data`，日志为工作副本logs。两者不受Git更新影响。

首次部署只安装、编译并离线验证，不启动/注册第二份服务，不改变旧`/home/ubuntu/alpha`及其运行进程。不要同时启动两份生产者写同一目录。首次启动会清理所配置storage_path下派生数据；确认数据迁移、磁盘容量和消费者路径后才能人工切换。

config.example.json只有占位值。实际config.json在机器上维护，不提交Git，不把本地代理地址带到Linux。Git历史早期已有机器配置，因此此仓库保持SSH私有访问，不当作公共源码发布。Windows.pyd和Linux.so、缓存不入Git；切换版本后必须在目标机器重编译四个扩展。requirements-deploy.txt记录本次验证的直接依赖，勿因更新代码自动升级当前环境。

## 日常更新

1. 开发机在功能分支修改和验证，提交后fast-forward合并main。
2. 把main推到服务器私有仓库（本机production remote）。SSH密钥通过本机ssh-agent或临时GIT_SSH_COMMAND指定，勿写进仓库。

```powershell
git -C D:/alpha push production main
```

3. 服务器先确认目标alpha2调度已经停止且子进程全部退出；不要误停仍在运行的旧alpha。

```bash
cd /home/ubuntu/alpha2
bash tools/deploy_alpha.sh status
bash tools/deploy_alpha.sh update
```

update只允许干净的受跟踪文件和fast-forward更新，构建四个本机C扩展，然后执行无需生产数据/网络的专项测试；不会自动启动。检测到本目录realtime_data进程时拒绝更新。config.json保持不动；参数/依赖有变化时单独人工检查，不自动安装依赖或删除数据。

若未配置开发机SSH远程，也可用Git bundle传递完整版本，避免逐文件复制或`tar --keep-old-files`留下旧代码：

```powershell
git -C D:/alpha bundle create alpha-update.bundle main
scp alpha-update.bundle ubuntu@SERVER:/home/ubuntu/alpha-update.bundle
```

```bash
git --git-dir=/home/ubuntu/repos/alpha.git fetch /home/ubuntu/alpha-update.bundle main:refs/heads/main
cd /home/ubuntu/alpha2
bash tools/deploy_alpha.sh update
```

## 回滚

保持服务停止，先用git log/reflog找出此前通过验收的提交。不要reset或清理config/data。

```bash
cd /home/ubuntu/alpha2
git log --oneline -10
git switch --detach PREVIOUS_GOOD_COMMIT
bash tools/deploy_alpha.sh build
bash tools/deploy_alpha.sh verify
```

确认代码/因子语义与现有数据兼容后再人工启动。代码回滚不等于数据回滚；有因子语义变化时需先完成对应状态/产物验收。回到主干可用git switch main后重新build/verify。

## 运行方式与首次启用

本次保留旧服务运行，不新增或启动systemd/PM2任务。人工切换时沿用单一进程管理器，设置cwd为alpha2、解释器为alpha311绝对路径，stdout/stderr单独写控制台日志，不能重定向到程序自身轮转的realtime_data.log。原进程未停止、数据路径未确认时不要启动。

本次审计旧data约87GB、磁盘可用约43GB，无法据此支持另建同规模完整数据副本；alpha2仅部署代码，未复制数据。未来需先解决容量或制定旧数据目录复用方案，不能直接执行两份初始化。

## 离线验证

Linux CCI构建显式关闭FMA contraction，保持GCC/MSVC的标量运算顺序；既有Windows golden须逐值完全一致，不因部署放宽容差或重建golden。IDE机器设置与缓存不跟踪，生产者代码保持干净。

```bash
cd /home/ubuntu/alpha2
bash tools/deploy_alpha.sh build
bash tools/deploy_alpha.sh verify
```

测试使用版本管理的原始BTC输入和独立Python oracle，不需要D盘、alphaonline项目或生产data。测试不运行realtime_data入口，不请求交易所、不通知、不下单。build/verify不会启动服务。
