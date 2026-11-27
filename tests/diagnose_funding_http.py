"""Bounded public-endpoint comparison; never imports production config or writes data."""
import ast
import asyncio
import json
import time
import sys
from pathlib import Path

import aiohttp


async def main():
    tree = ast.parse((Path(__file__).resolve().parents[1] / 'config.py').read_text(encoding='utf-8-sig'))
    proxy = next(ast.literal_eval(n.value) for n in tree.body
                 if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == 'proxy' for t in n.targets))
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
        async def request(label, symbol='HDUSDT', route=proxy, endpoint='fundingRate', minimal=False, query=None):
            params = dict(symbol=symbol, limit=1000, startTime=1741417200000) if endpoint == 'fundingRate' else {}
            if minimal:
                params = dict(symbol=symbol, limit=1)
            if query is not None:
                params = dict(symbol=symbol, **query)
            started = time.monotonic()
            result = dict(case=label, symbol=symbol, endpoint=endpoint, limit=params.get('limit'), at=time.strftime('%H:%M:%S'))
            try:
                async with session.get('https://fapi.binance.com/fapi/v1/' + endpoint, params=params, proxy=route) as response:
                    body = await response.text()
                    result.update(status=response.status, server=response.headers.get('Server'), content_type=response.headers.get('Content-Type'))
                    try:
                        data = json.loads(body)
                        result['rows'] = len(data) if isinstance(data, list) else None
                        if isinstance(data, dict):
                            result.update(code=data.get('code'), message=data.get('msg'))
                    except ValueError:
                        result['body_preview'] = body[:150]
            except Exception as exc:
                result['error'] = type(exc).__name__
            result['seconds'] = round(time.monotonic() - started, 3)
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if result.get('status') == 200 and isinstance(data, list):
                return data
            return None
        if '--limits' in sys.argv:
            limits = [int(x) for x in sys.argv[sys.argv.index('--limits') + 1].split(',')]
            for limit in limits:
                if not 1 <= limit <= 1000:
                    raise ValueError('limit must be 1..1000')
                for symbol in ['BTCUSDT', 'HDUSDT', 'NEOUSDT']:
                    await request('limit_probe', symbol, query=dict(limit=limit, startTime=1741417200000))
                    await asyncio.sleep(2)
            return
        if '--verify-limit' in sys.argv:
            limit = int(sys.argv[sys.argv.index('--verify-limit') + 1])
            if not 1 <= limit <= 1000:
                raise ValueError('limit must be 1..1000')
            for symbol in ['BTCUSDT', 'HDUSDT', 'NEOUSDT']:
                reference = await request('candidate', symbol, query=dict(limit=limit, startTime=1741417200000))
                if reference is None:
                    raise RuntimeError('Candidate request failed')
                pages, start = [], 1741417200000
                while len(pages) < limit:
                    await asyncio.sleep(2)
                    size = min(100, limit-len(pages))
                    page = await request('oracle_page', symbol, query=dict(limit=size, startTime=start))
                    if page is None:
                        raise RuntimeError('Oracle request failed')
                    pages.extend(page)
                    if len(page) < size:
                        break
                    start = page[-1]['fundingTime'] + 1
                assert pages == reference, 'Pagination mismatch'
                assert len({x['fundingTime'] for x in pages}) == len(pages), 'Duplicate time'
                print(json.dumps(dict(symbol=symbol, limit=limit, pagination_equal=True, rows=len(pages))), flush=True)
                await asyncio.sleep(2)
            return
        if '--verify-pagination' in sys.argv:
            for symbol in ['BTCUSDT', 'HDUSDT', 'NEOUSDT']:
                start = 1741417200000
                pages = []
                for _ in range(2):
                    page = await request('solution_page_100', symbol, query=dict(limit=100, startTime=start))
                    if page is None:
                        raise RuntimeError('Pagination HTTP failure')
                    pages.extend(page)
                    if len(page) < 100:
                        break
                    start = page[-1]['fundingTime'] + 1
                    await asyncio.sleep(2)
                reference = await request('reference_200', symbol, query=dict(limit=200, startTime=1741417200000))
                if reference is None:
                    raise RuntimeError('Reference HTTP failure')
                assert pages == reference, 'Pagination mismatch'
                assert len({x['fundingTime'] for x in pages}) == len(pages), 'Duplicate time'
                print(json.dumps(dict(symbol=symbol, pagination_equal=True, rows=len(pages))), flush=True)
                await asyncio.sleep(2)
            return
        if '--matrix' in sys.argv:
            for label, query in [
                ('latest_1000', dict(limit=1000)),
                ('old_start_1', dict(limit=1, startTime=1741417200000)),
                ('old_start_100', dict(limit=100, startTime=1741417200000)),
                ('old_start_1000', dict(limit=1000, startTime=1741417200000)),
                ('old_bounded_1000', dict(limit=1000, startTime=1741417200000, endTime=1742022000000)),
                ('recent_start_1000', dict(limit=1000, startTime=int(time.time()*1000)-86400000)),
                ('latest_1_repeat', dict(limit=1)),
            ]:
                await request(label, 'BTCUSDT', query=query)
                await asyncio.sleep(2)
            return
        if '--minimal-only' in sys.argv:
            await request('proxy_latest_only', 'BTCUSDT', minimal=True)
            await request('direct_latest_only', 'BTCUSDT', route=None, minimal=True)
            return
        await request('proxy_single')
        await request('proxy_control_time', endpoint='time')
        await asyncio.gather(*(request('proxy_burst_3', s) for s in ['HDUSDT', 'NEOUSDT', 'BTCUSDT']))
        for symbol in ['HDUSDT', 'NEOUSDT', 'BTCUSDT']:
            await asyncio.sleep(2)
            await request('proxy_paced_2s', symbol)
        await request('direct_single', route=None)
        await request('proxy_latest_only', 'BTCUSDT', minimal=True)
        await request('direct_latest_only', 'BTCUSDT', route=None, minimal=True)


if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
