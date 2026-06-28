import json

from core.utils.path_kit import get_folder_path, get_file_path

# ====================================================================================================
# ** 基础配置 **
# - K线数量
# - 全局报错机器人通知
# - 本地存储路径
# ====================================================================================================
# Resample(1h) 周期 K 线数量
kline_count_1h_spot = 14060
kline_count_1h_swap = 14060
# 全局报错机器人通知
# - 创建企业微信机器人 参考帖子: https://bbs.quantclass.cn/thread/10975
# - 配置案例  https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxxxxxxxxxxxxx
error_webhook_url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=c338c9be-1b53-457a-9510-44d52913e046'
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
data_api_uuid = 'b96311ed0215388b9f490c541156b2e6'
# 个人中心里面的api_key配置。（网址：https://www.quantclass.cn/login）
data_api_key = 'RCPT17XM78IVALDJTEFXAPANVMP7ZIZI'

# ====================================================================================================
# ** 其他配置 **
# 系统参数，别动就别碰，不了解也没关系，也不需要了解，动手之前务必请教林奇
# ====================================================================================================
# 如果使用代理 注意替换IP和Port
# proxy = None
proxy = 'http://127.0.0.1:7897'  # 如果你用clash的话

# 只更新部分分钟偏移（hour_offset），可有效节省你的硬盘，
# 当然如果你配置了新的hour_offset策略，需要同步更新数据中心这边的配置
# 默认所有的offset都需要更新，kline_count_1h=25000，大概占用68GB存储
enabled_hour_offsets = ( '10m','40m','30m',)

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
    '动量因子210401修正': [10248, 8568, 13608, 13608],
    'ILLQStdBbi45208':   [2856, 1344, 1512, 672, 168],
    'CciV3':             [1176,10248,10248,5208],
}
# 预处理数据拆分 Batch 大小
preprocess_num_batch = 180
# 预处理数据近期 K 线条数（仓位管理框架）
preprocess_num_recent = 3
# Arrow 发布深度（设计文档 §3.5）：生产者只在 Arrow 发布文件里裁到这个深度；
# 自身 base/recent pkl 文件仍保留全历史（因子正确性 + 增量）。
# 约束：publish_kline_num >= 消费者 get_kline_num(4050) + run_time 过滤余量，5000 安全。
publish_kline_num = 5000

# 如果有 json 配置，则使用 json 配置覆盖掉 config.py 中的配置
json_file_config = get_file_path('config.json', as_path_type=True)
if json_file_config.exists():
    with open(json_file_config, 'r', encoding='utf-8') as f:
        config_data = json.load(f)

        # 目前只支持配置这些
        allowed_attrs = (
            'kline_count_1h_spot', 'kline_count_1h_swap', 'error_webhook_url', 'use_api', 'data_api_uuid', 'data_api_key',
            'enabled_hour_offsets', 'funding_rate', 'precompute_factor_configs')

        for _attr in allowed_attrs:
            if _attr in config_data:
                globals()[_attr] = config_data[_attr]  # 自动转换为全局变量
