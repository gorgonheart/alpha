import asyncio
from types import SimpleNamespace

import pandas as pd
import pytest

from core import backfill, binance_update
from core.candle_manager import CandleFileManager


def manager(path):
    path.mkdir()
    return CandleFileManager(path, save_type='pickle')


def test_normal_round_refreshes_shared_data_without_offset_snapshots(tmp_path, monkeypatch):
    calls = []
    class Fetcher:
        def __init__(self, *args): pass
        async def get_exchange_info(self):
            calls.append(1)
            return {'BTCUSDT': {'symbol': 'BTCUSDT', 'version': len(calls)}}
    monkeypatch.setattr(binance_update, 'BinanceFetcher', Fetcher)
    mgr = manager(tmp_path/'exg')
    handle = SimpleNamespace(exg_mgr=mgr, proxy=None, resample_interval='1h',
        get_binance_handle=lambda kind: SimpleNamespace(api_type=kind, symbol_filter=lambda rows: list(rows)))
    first = pd.Timestamp('2026-10-31 00:20', tz='Asia/Hong_Kong').to_pydatetime()
    for stamp in (first, first + pd.Timedelta(minutes=20)):
        asyncio.run(binance_update.update_binance_exginfo(handle, None, 'spot', stamp))
    assert len(calls) == 2
    assert mgr.read_candle('exginfo_spot').version.tolist() == [2]
    assert not (tmp_path/'exchange_info_snapshots').exists()


@pytest.mark.parametrize('mode', ['complete', 'missing', 'mixed'])
@pytest.mark.parametrize('offset', ['20m', '40m'])
def test_backfill_reuses_shared_pair_and_refreshes_only_when_incomplete(tmp_path, monkeypatch, mode, offset):
    mgr = manager(tmp_path/'exg')
    source = pd.Timestamp('2026-10-31 00:10', tz='Asia/Hong_Kong').to_pydatetime()
    target = (source - pd.Timedelta(hours=1)).replace(minute=int(offset[:-1]))
    spot = pd.DataFrame({'symbol': ['BTCUSDT'], 'lot_size': [0.001]})
    swap = pd.DataFrame({'symbol': ['ETHUSDT'], 'lot_size': [0.01]})
    if mode != 'missing':
        mgr.set_candle('exginfo_spot', source, spot)
        mgr.set_candle('exginfo_swap', source if mode == 'complete' else target, swap)
    handle = SimpleNamespace(exg_mgr=mgr, resample_interval='1h', fetch_funding_rate=True)
    refreshes = []
    async def refresh(received, stamp):
        refreshes.append(stamp)
        assert received.fetch_funding_rate is False
        for market, df in [('spot',spot),('swap',swap)]:
            received.exg_mgr.set_candle('exginfo_'+market, stamp, df)
    def network_forbidden(*args):
        raise AssertionError('backfill must not request exchangeInfo')
    monkeypatch.setattr(backfill, 'update_binance_meta', refresh, raising=False)
    monkeypatch.setattr(backfill, 'now_time', lambda: source)
    monkeypatch.setattr(backfill, 'seconds_to_next_offset', lambda *args: 600)
    monkeypatch.setattr(binance_update, 'BinanceFetcher', network_forbidden)
    monkeypatch.setattr(backfill, 'is_base_data_fresh', lambda *args: (True, target))
    seen = []
    def resample(local, market, stamp, offset):
        assert local.exg_mgr.check_ready('exginfo_'+market, target)
        assert local.exg_mgr is not mgr
        pd.testing.assert_frame_equal(local.exg_mgr.read_candle('exginfo_'+market), spot if market == 'spot' else swap)
        return {market: 'unchanged-candle-range'}
    monkeypatch.setattr(backfill, 'local_resample_offset', resample)
    monkeypatch.setattr(backfill, 'preprocess_offset_data', lambda *args: seen.append(args[2:]))
    state = backfill.backfill_one_offset(handle, offset, target)
    assert state == 'ok'
    assert len(seen) == 1
    assert len(refreshes) == (0 if mode == 'complete' else 1)
    assert handle.fetch_funding_rate is True
    assert mgr.check_ready('exginfo_spot', source)
    assert mgr.check_ready('exginfo_swap', source)
    assert not mgr.check_ready('exginfo_spot', target)
    assert not mgr.has_symbol('spot_swap_matches')
    # A later backfill reuses this completed pair without another refresh.
    assert backfill.backfill_one_offset(handle, offset, target) == 'ok'
    assert len(refreshes) == (0 if mode == 'complete' else 1)
    assert not (tmp_path/'exchange_info_snapshots').exists()


@pytest.mark.parametrize('outcome', ['failure', 'budget'])
def test_bootstrap_metadata_does_not_publish_if_refresh_fails_or_budget_runs_out(tmp_path, monkeypatch, outcome):
    mgr = manager(tmp_path/'exg')
    stamp = pd.Timestamp('2026-10-31 09:10', tz='Asia/Hong_Kong').to_pydatetime()
    handle = SimpleNamespace(exg_mgr=mgr, fetch_funding_rate=True)
    async def refresh(received, current):
        if outcome == 'failure': raise TimeoutError('exchangeInfo unavailable')
        for market in ('spot','swap'):
            mgr.set_candle('exginfo_'+market, current, pd.DataFrame({'symbol':['BTCUSDT']}))
    monkeypatch.setattr(backfill, 'update_binance_meta', refresh, raising=False)
    monkeypatch.setattr(backfill, 'is_base_data_fresh', lambda *args: (True,stamp))
    monkeypatch.setattr(backfill, 'now_time', lambda: stamp)
    monkeypatch.setattr(backfill, 'seconds_to_next_offset', lambda *args: 30)
    def forbidden(*args): raise AssertionError('must not resample/publish')
    monkeypatch.setattr(backfill, 'local_resample_offset', forbidden)
    monkeypatch.setattr(backfill, 'preprocess_offset_data', forbidden)
    if outcome == 'failure':
        with pytest.raises(TimeoutError): backfill.backfill_one_offset(handle,'20m',stamp)
    else:
        assert backfill.backfill_one_offset(handle,'20m',stamp) == 'stale'
        assert mgr.check_ready('exginfo_spot',stamp)
