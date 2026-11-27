import time
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional

import aiohttp
import pandas as pd
import py7zr
import pytz

from core.utils.log_kit import logger

# 快照 (bmac-snapshot) 固定地址: manifest 国内/国外双源 + 「按月取下载链接」鉴权端点。
SNAPSHOT_MANIFEST_URLS = [
    'https://quantclass.cn-sh2.ufileos.com/sapi/data/bmac_snapshot_data.json',
    'https://qtcls.jpn-tky.ufileos.com/sapi/data/bmac_snapshot_data.json',
]
SNAPSHOT_LINK_URL_TMPL = 'https://api.quantclass.cn/api/data/client/bmac-snapshot/data/{product_name}'
SNAPSHOT_PRODUCT_TMPL = 'bmac-snapshot-{exchange}-{market}-data'


class QuantclassDataApi:

    def __init__(self, session: aiohttp.ClientSession, api_key, uuid, proxy: Optional[str] = None):
        self.api_key = api_key
        self.uuid = uuid
        self.session = session
        self.proxy = proxy

    async def aioreq_data_api(self, offset) -> dict:
        url = 'https://api.quantclass.cn/api/data/realtime/coin/binance-1h'
        params = {'offset': offset, 'uuid': self.uuid}
        headers = {'api-key': self.api_key}

        async with self.session.get(url, params=params, headers=headers, proxy=self.proxy) as resp:
            data = await resp.json()
            data = data['data']
            ts = datetime.strptime(data['ts'], '%Y%m%d%H%M')
            data['ts'] = pytz.timezone('Asia/Shanghai').localize(ts)

            return data

    async def aioreq_coincap_url(self, run_time: str | datetime):
        if isinstance(run_time, datetime):
            run_time = run_time.strftime("%Y-%m-%d")
        url = f'https://api.quantclass.cn/api/data/get-download-link/coin-cap-daily/{run_time}'

        params = {'uuid': self.uuid}
        headers = {'api-key': self.api_key}

        async with self.session.get(url, params=params, headers=headers, proxy=self.proxy) as resp:
            data = await resp.text()
            return data

    async def aioreq_coincap_df(self, url) -> Optional[pd.DataFrame]:
        async with self.session.get(url, proxy=self.proxy) as resp:
            if not str(resp.status).startswith('2'):
                return None
            raw_data = BytesIO(await resp.read())
        df = pd.read_csv(raw_data, encoding='gbk', skiprows=1, parse_dates=['candle_begin_time', 'date_added'],
                         dtype={
                             'usd_price': float,
                             'max_supply': float,
                             'circulating_supply': float,
                             'total_supply': float,
                             'max_mcap': float,
                             'circulating_mcap': float,
                             'total_mcap': float,
                         })
        df['symbol'] = df['symbol'].str.replace('-', '')
        return df

    async def aioreq_candle_raw(self, url) -> BytesIO:
        async with self.session.get(url, proxy=self.proxy) as resp:
            archive_data = BytesIO(await resp.read())
        return archive_data

    async def aioreq_candle_df(self, url) -> pd.DataFrame:
        archive_data = await self.aioreq_candle_raw(url)

        with py7zr.SevenZipFile(archive_data, mode='r') as archive:
            for fname, bio in archive.readall().items():
                if fname.endswith('.csv'):
                    df = pd.read_csv(bio, parse_dates=['candle_begin_time'], encoding='gbk')
                    df['candle_begin_time'] = df['candle_begin_time'].dt.tz_localize('UTC')

                    # Data API 一定为 1 小时线
                    df['candle_end_time'] = df['candle_begin_time'] + timedelta(hours=1)

                    columns = [
                        'symbol', 'candle_begin_time', 'candle_end_time', 'open', 'high', 'low', 'close', 'volume',
                        'quote_volume', 'trade_num', 'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
                    ]
                    return df[columns]

    async def aioreq_product_infos(self):
        urls = [
            'https://qtcls.jpn-tky.ufileos.com/sapi/data/products-status.json',
            'https://quantclass.cn-sh2.ufileos.com/sapi/data/products-status.json',
        ]

        last_error = None
        for url in urls:
            try:
                async with self.session.get(url, proxy=self.proxy) as resp:
                    if not str(resp.status).startswith('2'):
                        raise ValueError(f'HTTP {resp.status}')
                    data = (await resp.json(content_type=None))['data']

                df = pd.DataFrame.from_dict(data, orient='index', columns=['name_en', 'name_zh', 'update_time', 'data_range'])
                if df.empty:
                    raise ValueError('返回数据为空')
                df['update_time'] = pd.to_datetime(df['update_time']).dt.tz_localize('Asia/Shanghai').dt.tz_convert('utc')

                return df
            except Exception as e:
                last_error = e
                if url != urls[-1]:
                    logger.warning(f'请求 product_infos 失败: {url}, 错误: {repr(e)}, 尝试备用地址')
                else:
                    logger.warning(f'请求 product_infos 失败: {url}, 错误: {repr(e)}, 无更多备用地址')

        raise ValueError(f'所有 product_infos 地址均请求失败, 最后错误: {repr(last_error)}')

    async def aioreq_snapshot_manifest(self) -> dict:
        """拉取快照 manifest: 国内/国外双源依次尝试 (同 product_infos 容错), 附时间戳防 CDN 缓存。"""
        last_error = None
        for url in SNAPSHOT_MANIFEST_URLS:
            try:
                params = {'t': int(time.time())}  # 防 CDN 缓存
                async with self.session.get(url, params=params, proxy=self.proxy) as resp:
                    if not str(resp.status).startswith('2'):
                        raise ValueError(f'HTTP {resp.status}')
                    manifest = await resp.json(content_type=None)
                if not isinstance(manifest, dict):
                    raise ValueError('manifest 格式不符')
                return manifest
            except Exception as e:
                last_error = e
                if url != SNAPSHOT_MANIFEST_URLS[-1]:
                    logger.warning(f'请求快照 manifest 失败: {url}, 错误: {repr(e)}, 尝试备用地址')
                else:
                    logger.warning(f'请求快照 manifest 失败: {url}, 错误: {repr(e)}, 无更多备用地址')
        raise ValueError(f'所有快照 manifest 地址均请求失败, 最后错误: {repr(last_error)}')

    async def aioreq_snapshot_data_url(self, exchange: str, market: str, month: str) -> str:
        """取某月快照 parquet 的签名下载链接 (鉴权同 coin-cap: uuid 参数 + api-key 头)。

        端点响应形如 {"code": 200, "data": "<下载链接>"}; code != 200 或 data 为空时抛错。
        """
        product_name = SNAPSHOT_PRODUCT_TMPL.format(exchange=exchange, market=market)
        url = SNAPSHOT_LINK_URL_TMPL.format(product_name=product_name)
        params = {'month': month, 'from_client': 'True', 'uuid': self.uuid}  # from_client: 实盘标记
        headers = {'api-key': self.api_key}

        async with self.session.get(url, params=params, headers=headers, proxy=self.proxy) as resp:
            data = await resp.json(content_type=None)
        if not isinstance(data, dict) or data.get('code') != 200 or not data.get('data'):
            raise ValueError(f'取 {product_name}/{month} 下载链接失败: {data}')
        return data['data']

    async def aioreq_snapshot_bytes(self, url) -> bytes:
        """从签名链接下载快照 parquet 字节 (链接本身免鉴权)。"""
        async with self.session.get(url, proxy=self.proxy) as resp:
            if not str(resp.status).startswith('2'):
                raise ValueError(f'HTTP {resp.status}')
            return await resp.read()
#󠀂󠁥󠀹󠁢󠁥󠀹󠁡󠁥󠀵󠀸󠁤󠀹󠁡󠁥󠀸󠀸󠀷󠁢󠀴󠀲󠀰󠀳󠀳󠀳󠀲󠀳󠀲󠀳󠀹󠀳󠀷󠁿
