# 两项目统一开发文档入口

2026-11-03测试服务器发布：审核并提交完整历史Arrow发布（0表示全部、逐币写入及full_history标记），新增完整/裁尾/输入不变/非法深度测试，纳入部署verify。新增服务器43.173.122.28，独立alpha311(Python3.11.18)，Linux四扩展构建及111项专项通过；本地同组111项通过。新环境Requests间接依赖chardet固定5.2.0消除兼容警告。干净发布包不含旧Git历史，--previous维护路径已验证；本轮不启动、不传生产密钥。流程和验收记录见[测试服务器部署](2026-11-03-test-server-deployment.md)。唯一新增core范围为core/arrow_publish.py，无realtime_data或因子公式变化。

2026-11-02验收完成：本地101项通过，服务器独立alpha311(Python3.11.14)四个C扩展构建成功、Linux专项100项通过。CCI在GCC上关闭FMA contraction后与既有Windows golden逐值相同（最大差0），未改公式/golden/容差。候选运行代码dc12fdc，之后仅部署记录更新；源码提交并合并main，alpha2以Git跟踪服务器SSH私有仓库，配置为忽略文件config.json且权限600。旧alpha继续运行，本次不切换；详见[部署维护入口](DEPLOYMENT.md)。

2026-11-02部署入口：[Linux部署与维护](DEPLOYMENT.md)。源码/测试入Git，机器凭据和平台差异迁至忽略的config.json（新增proxy/storage_path/publish_kline_num覆盖），四个C扩展由目标机器编译，缓存/二进制不跟踪。专项原始输入及动量Python oracle随仓库携带，移除对D盘/另一项目的测试依赖。alpha2仅部署验证，旧alpha保持运行；具体提交和Linux验收结果见统一开发说明本次记录。

2026-10-31后续：空档补算缺失/不完整共享exchangeInfo时主动更新一次现货/合约信息，成功后继续补算，后续复用；关闭本次资金费刷新，失败沿用补算错误处理，获取耗时挤占预算则推迟重计算。仅core/backfill.py分支变化，realtime_data和binance_update本轮零修改；32项专项通过，未重启或联网。替代此前共享数据缺失只等待的行为。

2026-10-31：[五分钟调度与预热复核](2026-10-31-dispatch-prewarm.md)。每次完成后转下一基础周期点；backfill每次只补最近即将到来的一个缺失offset，realtime_data提前实际启动采集进程，与metadata I/O重叠，到点后才取数，保留每轮释放。30项专项通过；真实Windows spawn模拟采集晚启动约3.96秒降至约2.3ms，非网络实盘保证。未重启，其他core/因子本轮不改。

2026-10-31最新：[共享exchangeInfo简化与复核](2026-10-31-shared-exchange-info.md)。正常offset更新共享数据，补算复用最新完整现货/合约信息，无独立offset快照、无历史时间限制、无补算metadata请求。core/binance_update.py完全恢复HEAD原版；backfill改读共享数据，realtime_data本轮零修改，既有并行/调度/异常恢复保留。20项专项通过，未重启，旧独立快照目录未删但不再使用。本条替代下方历史独立快照方案。

2026-10-31补算调度：realtime_data非offset分支先等待基础周期调度点再检查一次补算，移除每60秒重复检查。例如08:22完成正常轮次后，08:25再检查，随后08:30；core本轮零修改，调度恢复/快照专项13项通过，未重启。exchangeInfo共享文件及每offset独立快照均覆盖更新；完整预处理仍依赖其币种筛选和映射，单独重采样K线才可不依赖。详见统一开发说明最新记录。

2026-10-31后续修复：realtime_data.main初始化成功后不再因更新/调度异常回到完整初始化；等待60秒、保留产物、重新计算下一run_time继续更新，首次初始化失败仍重试。缺失offset补算入口保留。仅main接线及日志变化，11项离线专项通过，未重启；本条替代下文“首次正常请求失败后重初始化尚未修改”。详见统一开发说明最新记录。

2026-10-31：[每轮exchangeInfo复用与补算审核](2026-10-31-exchange-info-reuse.md)。正常offset获取后保存独立小型快照；补算读取对应offset/精确run_time，无快照等待，不再重新请求metadata。binance_update新增22行，backfill仅改metadata接线；realtime_data及初始化下载本轮零新增修改。16项专项通过，未重启，首次正常请求失败后的旧重初始化行为尚未修改。

2026-10-30后续确认：用户已指定本地使用4个共享因子线程，config.py现为precompute_factor_workers=4，无config.json覆盖；preprocess.py与已验证的审核补丁一致。未重启服务，下文默认0为首次交付记录。

2026-10-30共享因子池：仅修改preprocess两个调度函数（+24/-8），新增precompute_factor_workers及JSON覆盖；默认0，人工启用后现货/合约共用2或4个因子线程。realtime_data本轮零修改，原始PKL/Arrow/pivot和完整历史计算保持。17项专项通过，360币种真实增量预处理4线程20.35秒对原版22.92秒，产物精确一致；全币池/Linux仍待目标机验证。详见[人工复核与实测](../../alphaonline/docs/2026-10-30-shared-factor-pool-review.md)。

2026-10-30构建入口补充：修复CFFI以GBK读取已有UTF-8生成C文件的失败，illq_build CLI在Windows自动以`-X utf8`重执行，MSVC增加`/utf-8`。已有生成文件连续隔离编译两次成功，编码回归及数值oracle共3项通过；未替换生产pyd或重启服务，详见统一说明最新记录。

2026-10-30补充：alphaonline服务器初始化审计发现ILLQ C的Inf累加与方差消减误差，源码及独立oracle已修正，构建脚本显式UTF-8。40m全币池6345组ILLQ在隔离重编译后与PKL重算一致；旧生产二进制/Arrow未替换，需人工审核与目标平台编译。详情见[实现复核](../../alphaonline/docs/2026-10-30-server-initialization-review.md)。

当前统一说明维护在 [alphaonline/docs/DEVELOPMENT.md](../../alphaonline/docs/DEVELOPMENT.md)，本机绝对路径为 `D:/alphaonline/docs/DEVELOPMENT.md`。新对话先读该文档及本项目 AGENTS.md，再读相关专题。

此处只维护入口，避免生产者和消费者各写一套互相矛盾的当前状态。如果单独迁移 alpha 仓库，请同时携带配套 alphaonline 文档；缺少它时需从代码与历史测试核对约定，不能凭旧 CLAUDE.md 猜测。

2026-10-29 采集端修复（alpha 侧专题，已随主干提交）：

- [Binance fundingRate 接口参数与采集端修复](2026-10-29-funding-rate-api.md)：`limit=1000` 仅在带 `startTime` 时生效，不传 startTime 会被静默截断到 500（文档仍写 200，属未公告变更）；`funding.py` 改为信号量限流、游标 `+1ms`、进度改 INFO。
- 同批：`core/utils/log_kit.py` 多进程日志轮转修复（子进程写 `logs/<name>-worker-<日期>.log`）、`core/history_data/candle.py` 阶段 3 进度日志改 INFO。

2026-10-30 采集端新增功能（alpha 侧专题）：

- [首次启动「空档补算」](2026-10-30-backfill.md)：重启后除 init 那一分钟外，其余 offset 的 preprocess 目录是空的，进程空转。`core/backfill.py` 在「休息 60s 后跳过」前用磁盘 5m 数据本地重采样 + 预处理，先补前两个缺 ready 的 offset。计算口径与到点更新共用 `calc_resample` / `update_resample_candle` / `preprocess_offset_data`，差异只在重采样输入窗口。
- 同批三道守卫：旧 resample 目录含更晚 run_time 的 K 线时先截断（否则引入未来函数）；磁盘 5m 未覆盖到 run_time 时放弃不写 ready（否则静默偏短）；offset 往返校验（配置不一致直接跳过）。`stale`/`skip` 不吃 30 分钟冷却。

本次 2026-10-14 修正：预计算动量使用 `动量210401_双项异常隔离V2` 及独立 `momentum_v2_build.py`；CciV3/ILLQ 不变。111 与异常 CCI 是消费端因子，不能误接到 CciV3 扩展。
