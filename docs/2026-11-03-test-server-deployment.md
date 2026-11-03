# 新测试服务器部署与人工维护

## 范围与当前状态

目标：`ubuntu@43.173.122.28`，工作目录 `/home/ubuntu/alpha`，私有 Git 仓库 `/home/ubuntu/repos/alpha.git`。本轮仅发布 alpha 生产端、创建环境、本机编译并离线验证；不部署交易端、不启动 realtime_data、不注册自启动、不复制行情或业务 snapshot，不操作原生产服务器。

首次审计：Ubuntu 24.04.4 LTS、x86_64、4 CPU、约7.5 GiB内存、1.9 GiB swap、根盘约161 GiB可用；目标安装目录不存在且无 alpha 生产者进程。已有 GCC 13.3、Git、make、curl。本轮安装官方 conda-forge Miniforge，前缀沿用 `/home/ubuntu/miniconda3`，创建独立 `alpha311`，实际 Python 3.11.18。默认 shell 不依赖 conda init，维护命令使用绝对解释器。

## 本轮最小代码范围

- `config.py`：`publish_kline_num=0` 默认发布完整保留历史；JSON仍可覆盖为正数，仅发布末N行。
- `core/arrow_publish.py`：0发布全部行并写full_history标记；逐币转换和写入，避免同时保留全市场第二份Arrow缓冲；正数裁尾、币种顺序、原子替换和ready接口保持。
- `realtime_data.py`、其他core及因子公式没有新增修改。现有线程池、五分钟调度、共享exchangeInfo及更新异常恢复沿用主干。
- 补充Arrow专项：完整/裁尾数据逐值一致、标记、非默认索引与输入不变、负参数不发布；部署verify纳入原有及新增Arrow测试。没有改变golden或数值容差。

## 首次手工部署步骤

1. 开发机审核本次diff、用alpha311验证后提交功能分支，fast-forward合并main。仅提交本次源码、测试和文档；保留其他诊断文件，不复制Windows二进制、缓存、私有JSON和data。
2. 创建并上传Git bundle（完整历史含旧配置，只允许私有传输/存放）：

```powershell
git -C D:/alpha bundle create D:/alpha-update.bundle main
scp -i E:/siteup/env/visibility-deploy.pem D:/alpha-update.bundle ubuntu@43.173.122.28:/home/ubuntu/
```

SSH若拒绝原密钥ACL，使用本机ssh-agent，或在本机临时复制密钥并仅授予当前用户权限；不要上传私钥或修改已有密钥内容。

3. 新服务器创建环境（已经安装的前缀不得覆盖）：

```bash
curl -fL --retry 3 https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -o /home/ubuntu/Miniforge3-Linux-x86_64.sh
bash /home/ubuntu/Miniforge3-Linux-x86_64.sh -b -p /home/ubuntu/miniconda3
/home/ubuntu/miniconda3/bin/conda create -y -n alpha311 --override-channels -c conda-forge python=3.11 pip
```

4. 安装Git副本与固定直接依赖：

```bash
umask 077
mkdir -p /home/ubuntu/repos /home/ubuntu/alpha-deploy-logs
git clone --bare --branch main /home/ubuntu/alpha-update.bundle /home/ubuntu/repos/alpha.git
git clone --branch main /home/ubuntu/repos/alpha.git /home/ubuntu/alpha
cd /home/ubuntu/alpha
git config pull.ff only
git config fetch.prune true
/home/ubuntu/miniconda3/envs/alpha311/bin/python -m pip install -r requirements-deploy.txt
/home/ubuntu/miniconda3/envs/alpha311/bin/python -m pip check
cp config.example.json config.json
chmod 600 config.json
```

5. 编辑config.json：proxy为null、storage_path为`/home/ubuntu/alpha/data`、本机Data API凭据、通知URL按需配置。公共offset/历史长度/因子参数继承Git的config.py；仅测试部分offset时通过JSON的enabled_hour_offsets覆盖。部署所用JSON不写入Git，不打印密钥。默认4个因子线程、完整Arrow发布。
6. 在目标机编译四个扩展并离线验证：

```bash
cd /home/ubuntu/alpha
bash tools/deploy_alpha.sh build
bash tools/deploy_alpha.sh verify
```

## 人工启用前

默认开启coin_cap，首次运行前需要将近期历史市值CSV放到`/home/ubuntu/alpha/data/coin_cap/`；也可在确认一致性后安装有效市值历史。代码/离线测试不会生成它。Data API凭据的联网有效性也不属于离线验收。

确认容量、offset和完整历史消费者兼容后，人工前台启动观察初始化及至少一轮增量。首次启动会清理本安装storage_path下派生数据，不能指向生产目录：

```bash
cd /home/ubuntu/alpha
/home/ubuntu/miniconda3/envs/alpha311/bin/python -u /home/ubuntu/alpha/realtime_data.py
```

本轮不执行该命令。验收后选择一个进程管理器，固定cwd和绝对解释器；控制台日志不要覆盖程序自身轮转日志。测试端交易消费者应独立部署和dry_run，不能管理同一线上账户。

## 后续更新与回滚

只更新本安装：先停止并等待其全部子进程退出，保留config.json与业务data。上传新bundle后：

```bash
git --git-dir=/home/ubuntu/repos/alpha.git fetch /home/ubuntu/alpha-update.bundle main:refs/heads/main
cd /home/ubuntu/alpha
bash tools/deploy_alpha.sh status
bash tools/deploy_alpha.sh update
```

update只fast-forward并重新build/verify，不自动安装依赖或启动服务。依赖变化时人工安装requirements-deploy.txt并pip check。回滚保持停止，`git switch --detach PREVIOUS_GOOD_COMMIT`后build/verify；不要reset --hard、git clean或删data。代码回滚不代表数据协议可回滚。

## 验收记录

本地Arrow/配置快速验证12项通过；完整部署专项111项通过（11.31秒）。目标Linux构建/测试结果在部署完成后补记。构建、依赖、验证和环境日志保存在`/home/ubuntu/alpha-deploy-logs/`。未完成行情初始化或真实增量运行验收。
