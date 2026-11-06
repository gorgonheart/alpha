"""Coin-cap proxy opt-in must cover metadata, URL and CSV without changing klines."""
import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core import coin_cap
from core.quantclass_async import QuantclassDataApi
from core.utils import path_kit

PROXY = 'http://127.0.0.1:7897'
PRODUCT = {'data': {'coin-cap-daily': ['coin-cap-daily', 'cap', '2026-11-04 08:00:00', 'daily']}}
CSV = ('metadata\ncandle_begin_time,date_added,symbol,market_cap\n'
       '2026-11-03,2020-01-01,BTC-USDT,123\n').encode('gbk')


class Response:
    def __init__(self, *, status=200, data=None, text='', raw=b''):
        self.status, self.data, self.body, self.raw = status, data, text, raw

    async def __aenter__(self): return self
    async def __aexit__(self, *args): return False
    async def json(self, **kwargs): return self.data
    async def text(self): return self.body
    async def read(self): return self.raw


class Session:
    def __init__(self, replies, calls): self.replies, self.calls = replies, calls
    async def __aenter__(self): return self
    async def __aexit__(self, *args): return False
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return next(self.replies)


@pytest.mark.parametrize('proxy', [None, PROXY])
def test_cap_requests_all_use_selected_route_including_backup(proxy):
    calls = []
    replies = iter([Response(status=503), Response(data=PRODUCT),
                    Response(text='https://download.example/cap.csv'), Response(raw=CSV)])
    api = QuantclassDataApi(Session(replies, calls), 'test-key', 'test-id', coin_cap_proxy=proxy)
    async def run():
        products = await api.aioreq_product_infos()
        assert 'coin-cap-daily' in products.index
        url = await api.aioreq_coincap_url(pd.Timestamp('2026-11-03'))
        return await api.aioreq_coincap_df(url)
    result = asyncio.run(run())
    assert result['symbol'].tolist() == ['BTCUSDT']
    assert len(calls) == 4
    assert all(kwargs['proxy'] == proxy for _, kwargs in calls)
    assert calls[2][1]['headers'] == {'api-key': 'test-key'}
    assert calls[2][1]['params'] == {'uuid': 'test-id'}


def test_legacy_constructor_and_kline_routes_remain_direct():
    calls = []
    api = QuantclassDataApi(Session(iter([Response(data=PRODUCT)]), calls), 'key', 'id')
    asyncio.run(api.aioreq_product_infos())
    assert calls[0][1].get('proxy') is None
    calls.clear()
    session = Session(iter([Response(data={'data': {'ts': '202611030025'}}), Response(raw=b'archive')]), calls)
    api = QuantclassDataApi(session, 'key', 'id', coin_cap_proxy=PROXY)
    asyncio.run(api.aioreq_data_api('25m'))
    assert asyncio.run(api.aioreq_candle_raw('https://download.example/kline')) is not None
    assert all('proxy' not in kwargs for _, kwargs in calls)


@pytest.mark.parametrize('enabled', [False, True])
def test_cap_update_routes_all_three_steps_and_preserves_merge(monkeypatch, enabled):
    calls = []
    replies = iter([Response(data=PRODUCT), Response(text='https://download.example/cap.csv'), Response(raw=CSV)])
    monkeypatch.setattr(coin_cap, 'bmac_config', SimpleNamespace(coin_cap_use_proxy=enabled))
    monkeypatch.setattr(coin_cap, 'create_aiohttp_session', lambda timeout: Session(replies, calls))
    existing = pd.DataFrame({'candle_begin_time': pd.to_datetime(['2026-11-02']),
                             'date_added': pd.to_datetime(['2020-01-01']),
                             'symbol': ['BTCUSDT'], 'market_cap': [100]})
    stored = {'BTCUSDT': existing}
    manager = SimpleNamespace(get_all_symbols=lambda: list(stored), read_candle=lambda s: stored[s],
                              has_symbol=lambda s: s in stored,
                              set_candle=lambda s, t, f: stored.update({s: f}))
    handle = SimpleNamespace(http_timeout_sec=8, proxy=PROXY,
        coin_cap_handle=SimpleNamespace(coin_cap_mgr=manager,
            cfg=SimpleNamespace(data_api_key='key', data_api_uuid='id')))
    asyncio.run(coin_cap.coin_cap_update_to_recent(handle, pd.Timestamp('2026-11-05')))
    assert len(calls) == 3
    expected_proxy = PROXY if enabled else None
    assert all(kwargs['proxy'] == expected_proxy for _, kwargs in calls)
    assert stored['BTCUSDT']['market_cap'].tolist() == [100, 123]
    assert stored['BTCUSDT']['candle_begin_time'].tolist() == list(pd.to_datetime(['2026-11-02', '2026-11-03']))


def load_config(tmp_path, monkeypatch, settings=None):
    monkeypatch.setattr(path_kit, 'get_file_path', lambda *a, **k: tmp_path/a[0])
    monkeypatch.setattr(path_kit, 'get_folder_path', lambda *a, **k: str(tmp_path/'data'))
    if settings is not None: (tmp_path/'config.json').write_text(json.dumps(settings))
    spec = importlib.util.spec_from_file_location('coin_cap_config', Path(__file__).resolve().parents[1]/'config.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_default_direct_and_private_json_can_enable_proxy(tmp_path, monkeypatch):
    assert load_config(tmp_path, monkeypatch).coin_cap_use_proxy is False
    config = load_config(tmp_path, monkeypatch, {'coin_cap_use_proxy': True, 'proxy': PROXY})
    assert config.coin_cap_use_proxy is True and config.proxy == PROXY


@pytest.mark.parametrize('settings', [{'coin_cap_use_proxy': 'false'},
    {'coin_cap_use_proxy': 1}, {'coin_cap_use_proxy': True, 'proxy': None}])
def test_invalid_proxy_switch_fails_instead_of_silently_using_direct(tmp_path, monkeypatch, settings):
    with pytest.raises(ValueError, match='coin_cap_use_proxy'):
        load_config(tmp_path, monkeypatch, settings)
