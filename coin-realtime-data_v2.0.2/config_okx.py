import json

from core.utils.path_kit import get_folder_path, get_file_path

# ====================================================================================================
# ** 基础配置 **
# - K线数量
# - 全局报错机器人通知
# - 本地存储路径
# ====================================================================================================
# Resample(1h) 周期 K 线数量，默认: 2000 小时
kline_count_1h = 100
# 全局报错机器人通知
# - 创建企业微信机器人 参考帖子: https://bbs.quantclass.cn/thread/10975
# - 配置案例  https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxxxxxxxxxxxxx
error_webhook_url = ''
# OKX 数据存放路径，默认在项目目录的 data_okx 文件夹（与 Binance 的 data 隔离）
storage_path = get_folder_path('data_okx')

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
    'kline': False,

    # **使用API来更新市值数据**
    # 使用DATA API服务来更新CoinCap数据
    # 1. 需要手动历史市值数据数据，下载地址：https://www.quantclass.cn/data/coin/coin-cap
    # 2. 然后上传历史数据到 `storage_path -> coin_cap`之后，
    # 3. 再启动BMAC
    'coin_cap': False,

    # **冷启动快照恢复**
    # 使用DATA API服务获取快照下载链接，冷启动时把数据中心打包好的 base-5m 月分区快照恢复进本地，
    # 让随后的三段式下载只需补缺口。仅当本地 5m 数据缺失时才生效（正常重启不重复拉取）；
    # 恢复失败会软退化为正常全量下载，不阻断冷启动。
    'snapshot_restore': False,
}
# 个人中心里面的葫芦id。（网址：https://www.quantclass.cn/login）
data_api_uuid = ''
# 个人中心里面的api_key配置。（网址：https://www.quantclass.cn/login）
data_api_key = ''

# ====================================================================================================
# ** 其他配置 **
# 系统参数，别动就别碰，不了解也没关系，也不需要了解，动手之前务必请教林奇
# ====================================================================================================
# 如果使用代理 注意替换IP和Port
proxy = None
# proxy = 'http://127.0.0.1:7897'  # 如果你用clash的话

# 只更新部分分钟偏移（hour_offset），可有效节省你的硬盘，
# 当然如果你配置了新的hour_offset策略，需要同步更新数据中心这边的配置
# 默认所有的offset都需要更新，kline_count_1h=25000，大概占用68GB存储
enabled_hour_offsets = ('0m', '5m', '10m', '15m', '20m', '25m', '30m', '35m', '40m', '45m', '50m', '55m')

# Base 周期，通常设为 5m，与 Data API 一致
base_interval = '5m'

# 是否启用 WS+REST 并行架构（False = 退化为 v5 之前的纯 REST 模式）
use_websocket = True

# Resample 周期，通常设为 1h，与 Data API 一致
resample_interval = '1h'

# 是否使用资金费
funding_rate = True

# 是否生成预处理数据
preprocess = True

# 预处理数据拆分 Batch 大小
preprocess_num_batch = 64


# 如果有 json 配置，则使用 json 配置覆盖掉 config.py 中的配置
json_file_config = get_file_path('config_okx.json', as_path_type=True)
if json_file_config.exists():
    with open(json_file_config, 'r', encoding='utf-8') as f:
        config_data = json.load(f)

        # 目前只支持配置这些
        allowed_attrs = (
            'kline_count_1h', 'error_webhook_url', 'use_api', 'data_api_uuid', 'data_api_key', 'enabled_hour_offsets',
            'funding_rate')

        for _attr in allowed_attrs:
            if _attr in config_data:
                globals()[_attr] = config_data[_attr]  # 自动转换为全局变量
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
