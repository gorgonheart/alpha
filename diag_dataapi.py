"""
诊断 DataAPI 实际返回内容。
运行: conda run -n alpha311 python diag_dataapi.py
"""
import asyncio
from io import BytesIO

import aiohttp
import config as cfg
from core.utils.network import create_aiohttp_session


async def main():
    async with create_aiohttp_session(30) as session:
        from core.quantclass_async import QuantclassDataApi
        api = QuantclassDataApi(session, cfg.data_api_key, cfg.data_api_uuid)

        # 1. 获取 URL
        from core.utils.time import get_offset_from_runtime
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.timezone('Asia/Shanghai')).replace(minute=0, second=0, microsecond=0)
        offset_str = get_offset_from_runtime(now, '1h')
        print(f'offset: {offset_str}, time: {now}')

        try:
            info = await api.aioreq_data_api(offset=offset_str)
            print(f'API info: ts={info["ts"]}, spot_url={info.get("spot", "N/A")[:80]}...')
            spot_url = info.get('spot')
        except Exception as e:
            print(f'获取 URL 失败: {e}')
            return

        if not spot_url:
            print('无 spot URL')
            return

        # 2. 下载原始内容，检查实际格式
        print(f'\n下载 spot 数据: {spot_url[:80]}...')
        raw: BytesIO = await api.aioreq_candle_raw(url=spot_url)
        data = raw.getvalue()
        print(f'响应大小: {len(data)} bytes')
        print(f'前 16 字节 (hex): {data[:16].hex()}')
        print(f'前 200 字节 (text): {data[:200]}')

        # 7z 魔数: 37 7a bc af 27 1c
        if data[:6] == b'7z\xbc\xaf\x27\x1c':
            print('\n[OK] 是有效的 7z 文件')
        elif data[:5] == b'<?xml' or data[:1] == b'<':
            print('\n[ERR] 返回了 HTML/XML (可能是错误页)')
        elif data[:1] == b'{':
            print('\n[ERR] 返回了 JSON (可能是错误响应)')
        elif len(data) == 0:
            print('\n[ERR] 空响应')
        else:
            print('\n[?] 未知格式')


import sys
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
