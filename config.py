import json

from core.utils.path_kit import get_folder_path, get_file_path

# ====================================================================================================
# ** 基础配置 **
# - K线数量
# - 全局报错机器人通知
# - 本地存储路径
# ====================================================================================================
# Resample(1h) 周期 K 线数量
kline_count_1h_spot = 15000
kline_count_1h_swap = 15000
# 全局报错机器人通知
# - 创建企业微信机器人 参考帖子: https://bbs.quantclass.cn/thread/10975
# - 配置案例  https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxxxxxxxxxxxxx
error_webhook_url = ""  # Machine-specific value comes from ignored config.json.
# 数据存放路径，默认在项目目录的data文件夹
storage_path = get_folder_path('data')

# ====================================================================================================
# ** 彩虹API服务配置 **
# - 目前支持使用API来更新`K线数据`，`CoinCap数据`
# - True表示开启False表示关闭
# - 只要有一个选项为True，就必须配置data_api的uuid和key
# ====================================================================================================
# 只要有一个配置为true，就必须配置data_api的uuid和key
use_api = {
    # **使用API来更新K线**
    # 会使用DATA API服务来更新K线
    'kline': True,

    # **使用API来更新市值数据**
    # 使用DATA API服务来更新CoinCap数据
    # 1. 需要手动历史市值数据数据，下载地址：https://www.quantclass.cn/data/coin/coin-cap
    # 2. 然后上传历史数据到 `storage_path -> coin_cap`之后，
    # 3. 再启动BMAC
    'coin_cap': True,
}
# 个人中心里面的葫芦id。（网址：https://www.quantclass.cn/login）
data_api_uuid = ""  # Machine-specific value comes from ignored config.json.
# 个人中心里面的api_key配置。（网址：https://www.quantclass.cn/login）
data_api_key = ""  # Machine-specific value comes from ignored config.json.

# ====================================================================================================
# ** 其他配置 **
# 系统参数，别动就别碰，不了解也没关系，也不需要了解，动手之前务必请教林奇
# ====================================================================================================
# 如果使用代理 注意替换IP和Port
# proxy = None
proxy = 'http://127.0.0.1:7897'  # 如果你用clash的话
# Market-cap requests only; the existing machine JSON may opt in.
coin_cap_use_proxy = False

# 只更新部分分钟偏移（hour_offset），可有效节省你的硬盘，
# 当然如果你配置了新的hour_offset策略，需要同步更新数据中心这边的配置
# 默认所有的offset都需要更新，kline_count_1h=25000，大概占用68GB存储
enabled_hour_offsets = ( '20m','40m','55m','25m','45m','5m','5m','15m',)

# Base 周期，通常设为 5m，与 Data API 一致
base_interval = '5m'

# Resample 周期，通常设为 1h，与 Data API 一致
resample_interval = '1h'

# 是否使用资金费
funding_rate = True

# 是否生成预处理数据
preprocess = True

# 预计算因子配置：{因子模块名（不含 factors. 前缀）: [param 列表]}
# 留空 dict 则跳过因子预计算步骤
precompute_factor_configs = {
    '动量210401_双项异常隔离V2': [13608,5421,3509,12600],
    'ILLQStdBbi45208':   [2184, 2320, 1104, 264, 96],  # 与当前选币/动量过滤窗口一致
    'CciV3':             [1176, 10248, 5208],
}
# 因子共享线程数：0 保持各市场逐币串行；2/4 按币种并行，现货与合约共用额度。
# 本地已确认使用 4；设为 0 可恢复逐币串行，目标机器需单独测量。
precompute_factor_workers = 4
# 预处理数据拆分 Batch 大小
preprocess_num_batch = 180
# 预处理数据近期 K 线条数（仓位管理框架）
preprocess_num_recent = 3
# Arrow 发布深度（设计文档 §3.5）：生产者只在 Arrow 发布文件里裁到这个深度；
# 自身 base/recent pkl 文件仍保留全历史（因子正确性 + 增量）。
# 正数深度需覆盖消费者窗口及过滤余量；0表示不裁切。
# 0：发布全部保留历史，初始化直接复用长因子；普通消费者需先裁行再转 Pandas。
publish_kline_num = 0
# 每个 offset、每个市场保留的完整 Arrow 轮数；新轮原子发布后再清旧轮。
# 1 表示只保留当前发布轮，写入时仍需要一份临时文件空间。
arrow_snapshot_keep = 1
# Opt-in shared consumer demand; capture once for both markets, no raw-history trim.
arrow_demand_enabled = False
# None preserves publish_kline_num for unregistered consumers (0 = full).
# Set e.g. 5000 only after confirming ALL non-participating readers fit it.
arrow_demand_min_rows = None
# REST 实时采集相对 run_time 的延后秒数（避开币安跨 5m 边界瞬间未定稿的 K 线，
# 减少下游交易端 preflight 每小时 2h 伪修订 replay）。0 表示准点立即抓取。
rest_fetch_delay_seconds = 3.0
# v2.0.2 / 2B WebSocket 与存储保留配置
use_websocket = True
num_writers = 4
ws_wait_sec = 14.0
ws_flat_pkl_bars = 240
base_5m_retention_days = 30
min_history_start_date = '2025-03-25'
history_trim_date = None

# 如果有 json 配置，则使用 json 配置覆盖掉 config.py 中的配置
json_file_config = get_file_path('config.json', as_path_type=True)
if json_file_config.exists():
    with open(json_file_config, 'r', encoding='utf-8') as f:
        config_data = json.load(f)

        # 目前只支持配置这些
        allowed_attrs = (
            'kline_count_1h', 'kline_count_1h_spot', 'kline_count_1h_swap',
            'error_webhook_url', 'use_api', 'data_api_uuid', 'data_api_key',
            'enabled_hour_offsets', 'funding_rate', 'precompute_factor_configs', 'precompute_factor_workers',
            'coin_cap_use_proxy', 'proxy', 'storage_path', 'publish_kline_num', 'arrow_snapshot_keep',
            'arrow_demand_enabled', 'arrow_demand_min_rows', 'rest_fetch_delay_seconds',
            'use_websocket', 'num_writers', 'ws_wait_sec', 'ws_flat_pkl_bars', 'base_5m_retention_days',
            'min_history_start_date', 'history_trim_date',
        )

        for _attr in allowed_attrs:
            if _attr in config_data:
                globals()[_attr] = config_data[_attr]  # 自动转换为全局变量

        if 'kline_count_1h' in config_data:
            if 'kline_count_1h_spot' not in config_data:
                kline_count_1h_spot = config_data['kline_count_1h']
            if 'kline_count_1h_swap' not in config_data:
                kline_count_1h_swap = config_data['kline_count_1h']

kline_count_1h = max(kline_count_1h_spot, kline_count_1h_swap)

if type(arrow_snapshot_keep) is not int or arrow_snapshot_keep < 1:
    raise ValueError('arrow_snapshot_keep must be a positive integer')

if type(coin_cap_use_proxy) is not bool:
    raise ValueError('coin_cap_use_proxy must be a boolean')

if type(use_websocket) is not bool:
    raise ValueError('use_websocket must be a boolean')

if type(num_writers) is not int or num_writers < 1:
    raise ValueError('num_writers must be an integer >= 1')

if type(ws_flat_pkl_bars) is not int or ws_flat_pkl_bars < 1:
    raise ValueError('ws_flat_pkl_bars must be an integer >= 1')

if type(base_5m_retention_days) is not int or base_5m_retention_days < 7:
    raise ValueError('base_5m_retention_days must be an integer >= 7')

if isinstance(rest_fetch_delay_seconds, bool) or not isinstance(rest_fetch_delay_seconds, (int, float)) or rest_fetch_delay_seconds < 0:
    raise ValueError('rest_fetch_delay_seconds must be a non-negative number')
if isinstance(ws_wait_sec, bool) or not isinstance(ws_wait_sec, (int, float)) or ws_wait_sec < 0:
    raise ValueError('ws_wait_sec must be a non-negative number')
if coin_cap_use_proxy and not proxy:
    raise ValueError('coin_cap_use_proxy requires a configured proxy')
