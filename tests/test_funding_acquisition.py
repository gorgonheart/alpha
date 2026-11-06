"""Offline API pages, on-disk funding files and base tick scheduling."""
import asyncio
import os
import shutil
import stat
import threading
import time
from contextlib import AsyncExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pandas as pd
import pytest

import realtime_data as rt
from core import binance_update as update
from core.binance_fetcher_async import BinanceFetcher
from core.binance_market_async import BinanceAPIException
from core.candle_manager import CandleFileManager
from core.history_data import funding as startup


HOUR = pd.Timestamp('2026-10-01 08:00:00Z')
MS = HOUR.value // 1_000_000


def raw(offset, rate, symbol='A'):
    return dict(symbol=symbol, fundingTime=MS + offset, fundingRate=rate)


def fake_market(monkeypatch, rows):
    calls = []
    async def history(**params):
        calls.append(params)
        if isinstance(rows, BaseException):
            raise rows
        return [row for row in rows if params['startTime'] <= row['fundingTime'] <= params['endTime']][:params['limit']]
    market = SimpleNamespace(aioreq_funding_rate=history)
    monkeypatch.setattr('core.binance_fetcher_async.create_binance_market_api', lambda *a: market)
    return calls


def handle(folder):
    return SimpleNamespace(binance_swap=SimpleNamespace(api_type='usdt_futures'), proxy=None,
                           http_timeout_sec=3, fetch_funding_rate=True,
                           get_kline_count_1h=lambda kind: {'swap': 36}[kind],
                           funding_mgr=CandleFileManager(folder, 'pickle'))


def test_startup_replaces_old_estimate_with_complete_hour_across_pages(tmp_path, monkeypatch):
    h = handle(tmp_path)
    h.funding_mgr.set_candle('A', HOUR, pd.DataFrame(dict(
        symbol=['A', 'A'], candle_begin_time=[HOUR-pd.Timedelta(hours=2), HOUR], funding_rate=[.7, .9])))
    # Equal raw milliseconds represent one event. Different milliseconds in the
    # same hour are distinct settlements. The second page begins within that hour.
    rows = [raw(5, .1), raw(5, .1), raw(3_000_001, -.03), raw(3_600_009, .2)]
    calls = fake_market(monkeypatch, rows)
    monkeypatch.setattr(startup, 'create_aiohttp_session', lambda *a: AsyncExitStack())
    monkeypatch.setattr(startup, 'FUNDING_RATE_BATCH_LIMIT', 2)
    monkeypatch.setattr(startup.asyncio, 'sleep', AsyncMock())
    for _ in range(2):
        asyncio.run(startup.async_download_funding_rates(h, HOUR + pd.Timedelta(hours=3), ['A']))
        observed = h.funding_mgr.read_candle('A').set_index('candle_begin_time')
        assert observed.index.tolist() == [HOUR-pd.Timedelta(hours=2), HOUR, HOUR+pd.Timedelta(hours=1)]
        assert observed.funding_rate.tolist() == pytest.approx([.7, .07, .2])
    assert calls[1]['startTime'] == MS + 6


@pytest.mark.parametrize('depth,local_hours,settled_hour,expected_start', [
    (72, [-20, -2, 4], -20, -24),  # A future forecast must not shorten initial reconciliation.
    (72, [-32, 4], -30, -40),      # Resume an older outage despite a future-dated estimate.
    (12, [-20, -2, 4], -10, -12),  # Only request retained history.
])
def test_upgrade_reconciles_funding_before_first_ready(tmp_path, monkeypatch, depth, local_hours,
                                                      settled_hour, expected_start):
    h = handle(tmp_path)
    h.get_kline_count_1h = lambda kind: depth
    h.funding_mgr.set_candle('A', HOUR, pd.DataFrame(dict(
        symbol='A', candle_begin_time=[HOUR+pd.Timedelta(hours=i) for i in local_hours],
        funding_rate=[.9]*len(local_hours))))
    calls = fake_market(monkeypatch, [raw(settled_hour*3_600_000+1, .2)])
    monkeypatch.setattr(startup, 'create_aiohttp_session', lambda *a: AsyncExitStack())
    stamp = HOUR + pd.Timedelta(minutes=25)
    asyncio.run(startup.async_download_funding_rates(h, stamp, ['A']))
    assert calls[0]['startTime'] == MS + expected_start*3_600_000
    assert h.funding_mgr.read_candle('A').set_index('candle_begin_time').loc[
        HOUR+pd.Timedelta(hours=settled_hour), 'funding_rate'] == .2


def test_replacement_preserves_existing_reader_permissions(tmp_path, monkeypatch):
    from core.funding_backfill import save_funding_hours
    h = handle(tmp_path)
    frame = pd.DataFrame(dict(symbol=['A'], candle_begin_time=[HOUR], funding_rate=[.2]))
    h.funding_mgr.set_candle('A', HOUR, frame)
    existing = tmp_path/'A.pkl'
    existing.chmod(0o640)
    copied_modes = []
    original = shutil.copymode
    def copy_mode(source, target):
        copied_modes.append(source)
        return original(source, target)
    monkeypatch.setattr(shutil, 'copymode', copy_mode)
    save_funding_hours(h.funding_mgr, 'A', HOUR, frame.assign(funding_rate=.3), None)
    assert copied_modes == [existing]
    if os.name == 'posix':
        assert stat.S_IMODE(existing.stat().st_mode) == 0o640


def test_disabled_offset_fetches_metadata_then_checks_backfill(monkeypatch):
    sequence = []
    async def meta(*args): sequence.append('meta')
    monkeypatch.setattr(rt, 'update_binance_meta', meta)
    monkeypatch.setattr(rt, 'sleep_until_run_time', sequence.append)
    monkeypatch.setattr(rt, 'maybe_backfill', lambda h: sequence.append('backfill'))
    rt.update_all(SimpleNamespace(enabled_hour_offsets=('5m',)), HOUR)
    assert sequence == [HOUR - pd.Timedelta(seconds=15), 'meta', HOUR, 'backfill']


@pytest.mark.parametrize('bad_row', [raw(3, 'NaN'), raw(3, 'inf'), raw(-1, .1), raw(3, .1, 'B')])
def test_history_rejects_corrupt_or_mixed_response(monkeypatch, bad_row):
    market = SimpleNamespace(aioreq_funding_rate=AsyncMock(return_value=[raw(2, .2), bad_row]))
    monkeypatch.setattr('core.binance_fetcher_async.create_binance_market_api', lambda *args: market)
    with pytest.raises(ValueError):
        asyncio.run(BinanceFetcher('usdt_futures', None).get_hist_funding_rate(
            'A', startTime=MS, endTime=MS+4, limit=1000, _max_times=0))


def test_expired_or_invalid_premium_index_never_overwrites_settled(tmp_path, monkeypatch):
    h = handle(tmp_path)
    h.get_binance_handle = lambda _: h.binance_swap
    rows = [dict(symbol=s, nextFundingTime=t, lastFundingRate=r)
            for s, t, r in [('A', MS-1, .9), ('B', None, .5), ('C', MS, 'NaN'), ('D', MS+3600000, -.2)]]
    market = SimpleNamespace(aioreq_premium_index=AsyncMock(return_value=rows))
    monkeypatch.setattr('core.binance_fetcher_async.create_binance_market_api', lambda *args: market)
    frame = asyncio.run(update.update_binance_funding_rates(h, None, HOUR))
    assert frame[['symbol', 'funding_rate']].values.tolist() == [['D', -.2]]


def recent_setup(monkeypatch, tmp_path, rows):
    from core import funding_backfill as recent
    monkeypatch.setattr(recent, '_cooldown_until', 0)
    monkeypatch.setattr(recent, '_history_stopped', False)
    monkeypatch.setattr(recent, '_round_hour', None)
    monkeypatch.setattr(recent, '_next_run_time', None)
    monkeypatch.setattr(recent, '_last_symbol', None)
    monkeypatch.setattr(recent, '_request_times', recent.deque())
    calls = fake_market(monkeypatch, rows)
    h = handle(tmp_path)
    return recent, calls, h


def run_recent(recent, h, stamp, symbols=('A',)):
    return asyncio.run(recent.backfill_recent_funding_rates(
        h, None, stamp, symbols, deadline=time.monotonic()+2))


def test_recent_includes_settled_open_hour_and_confirms_after_save(tmp_path, monkeypatch):
    rows = [raw(1,.1), raw(1,.1), raw(1_100_000,-.03), raw(1_900_000,.02)]
    recent, calls, h = recent_setup(monkeypatch, tmp_path, rows)
    stamp = HOUR + pd.Timedelta(minutes=25)
    frames, stats = run_recent(recent, h, stamp)
    assert calls[0]['endTime'] == stamp.value // 1_000_000 - 1
    assert calls[0]['startTime'] == MS - 24*3600000
    assert frames['A'].funding_rate.tolist() == pytest.approx([.07])
    assert stats['completed'] == 0
    update.save_funding_updates(h, stamp, pd.DataFrame(), frames, stats, time.monotonic()+2)
    assert stats['completed'] == 1
    assert h.funding_mgr.read_candle('A').funding_rate.tolist() == pytest.approx([.07])
    assert run_recent(recent, h, stamp)[1]['reason'] == 'hour_complete'
    later = stamp + pd.Timedelta(hours=1)
    frames, stats = run_recent(recent, h, later)
    update.save_funding_updates(h, later, pd.DataFrame(), frames, stats, time.monotonic()+2)
    assert h.funding_mgr.read_candle('A').funding_rate.tolist() == pytest.approx([.09])


@pytest.mark.parametrize('status', [403, 418, 429])
def test_optional_http_errors_stop_and_defer_requests(tmp_path, monkeypatch, status):
    err = BinanceAPIException(SimpleNamespace(headers={}), status, '{}')
    recent, calls, h = recent_setup(monkeypatch, tmp_path, err)
    stamp = HOUR + pd.Timedelta(minutes=83)
    assert run_recent(recent, h, stamp)[1]['reason'] == f'http_{status}'
    assert run_recent(recent, h, stamp+pd.Timedelta(minutes=20))[1]['reason'] in {'cooldown', 'disabled'}
    assert len(calls) == 1


def test_recent_skips_exhausted_budget_and_quota(tmp_path, monkeypatch):
    recent, calls, h = recent_setup(monkeypatch, tmp_path, [])
    stamp = HOUR + pd.Timedelta(minutes=83)
    assert asyncio.run(recent.backfill_recent_funding_rates(h, None, stamp, ['A'], deadline=0))[1]['reason'] == 'deadline'
    monkeypatch.setattr(recent, 'REQUEST_LIMIT', 1)
    recent._request_times.append(time.monotonic())
    assert run_recent(recent, h, stamp)[1]['reason'] == 'rolling_limit'
    assert not calls


def test_serialization_failure_keeps_previous_funding_and_ready(tmp_path, monkeypatch):
    recent, _, h = recent_setup(monkeypatch, tmp_path, [raw(4, .1)])
    previous = pd.DataFrame(dict(symbol=['A'], candle_begin_time=[HOUR], funding_rate=[.9]))
    h.funding_mgr.set_candle('A', HOUR, previous)
    stamp = HOUR + pd.Timedelta(minutes=25)
    frames, stats = run_recent(recent, h, stamp)
    def fail_write(*args, **kwargs):
        raise OSError('cannot write pickle')
    monkeypatch.setattr(pd.DataFrame, 'to_pickle', fail_write)
    update.save_funding_updates(h, stamp, pd.DataFrame(), frames, stats, time.monotonic()+2)
    pd.testing.assert_frame_equal(h.funding_mgr.read_candle('A'), previous)
    assert h.funding_mgr.check_ready('A', HOUR) and stats['completed'] == 0


def test_corrected_hour_replaces_legacy_nonhour_estimate(tmp_path, monkeypatch):
    recent, _, h = recent_setup(monkeypatch, tmp_path, [raw(4, .1)])
    previous = pd.DataFrame(dict(symbol=['A'], candle_begin_time=[HOUR+pd.Timedelta(minutes=10)], funding_rate=[.9]))
    h.funding_mgr.set_candle('A', HOUR, previous)
    stamp = HOUR + pd.Timedelta(minutes=25)
    frames, stats = run_recent(recent, h, stamp)
    update.save_funding_updates(h, stamp, pd.DataFrame(), frames, stats, time.monotonic()+2)
    observed = h.funding_mgr.read_candle('A')
    assert list(zip(observed.candle_begin_time, observed.funding_rate)) == [(HOUR, .1)]


def test_metadata_timeout_cancels_only_optional_io(tmp_path, monkeypatch):
    recent, _, h = recent_setup(monkeypatch, tmp_path, [])
    stamp = pd.Timestamp.now('UTC').ceil('h') + pd.Timedelta(hours=1)
    h.get_binance_handle = lambda _: h.binance_swap
    markets, cancellations = [], []
    async def exchange(handle, session, market, stamp):
        markets.append(market)
        return ['A']
    async def slow(*args, **kwargs):
        try:
            await asyncio.sleep(60)
        finally:
            cancellations.append(1)
    monkeypatch.setattr(BinanceFetcher, 'get_hist_funding_rate', slow)
    realtime = pd.DataFrame(dict(symbol=['A'], next_funding_time=[stamp], funding_rate=[.2]))
    monkeypatch.setattr(BinanceFetcher, 'get_realtime_funding_rate', AsyncMock(return_value=realtime))
    monkeypatch.setattr(update, 'update_binance_exginfo', exchange)
    monkeypatch.setattr(update, 'create_aiohttp_session', lambda *a: AsyncExitStack())
    monkeypatch.setattr(update, 'NETWORK_SECONDS', .05)
    asyncio.run(asyncio.wait_for(update.update_binance_meta(h, stamp), 2))
    assert markets == ['spot', 'swap'] and cancellations == [1]
    assert h.funding_mgr.read_candle('A').funding_rate.tolist() == [.2]


def test_startup_reserves_quota_again_for_http_retry(tmp_path, monkeypatch):
    h = handle(tmp_path)
    now, attempts = [0.], []
    async def http(**params):
        attempts.append(now[0])
        if len(attempts) == 1:
            raise ConnectionError('one failed request')
        return []
    async def sleep(seconds): now[0] += seconds
    monkeypatch.setattr(startup, 'REQUEST_LIMIT', 1)
    monkeypatch.setattr(startup, 'time', SimpleNamespace(monotonic=lambda: now[0], perf_counter=lambda: now[0]))
    monkeypatch.setattr(startup.asyncio, 'sleep', sleep)
    monkeypatch.setattr(startup, 'create_aiohttp_session', lambda *a: AsyncExitStack())
    monkeypatch.setattr('core.binance_fetcher_async.create_binance_market_api',
                        lambda *a: SimpleNamespace(aioreq_funding_rate=http))
    asyncio.run(startup.async_download_funding_rates(h, HOUR, ['A']))
    assert attempts == [0., 300.]


def test_required_saves_overlap_slow_optional_history(tmp_path, monkeypatch):
    h = handle(tmp_path)
    stamp = pd.Timestamp.now('UTC').ceil('h') + pd.Timedelta(hours=1)
    writing, history_done = threading.Event(), threading.Event()
    in_flight = []
    async def history(*args, **kwargs):
        in_flight.append(await asyncio.to_thread(writing.wait, 1))
        history_done.set()
        return {}, dict(requests=1, success=0, failed=0, elapsed=0, reason='complete')
    original = update.save_funding_hours
    def write(*args):
        writing.set()
        assert history_done.wait(2)
        return original(*args)
    frame = pd.DataFrame(dict(symbol=['A'], candle_begin_time=[stamp], funding_rate=[.2]))
    monkeypatch.setattr(update, 'update_binance_exginfo', AsyncMock(return_value=['A']))
    monkeypatch.setattr(update, 'create_aiohttp_session', lambda *a: AsyncExitStack())
    monkeypatch.setattr(update, 'backfill_recent_funding_rates', history)
    monkeypatch.setattr(update, 'update_binance_funding_rates', AsyncMock(return_value=frame))
    monkeypatch.setattr(update, 'save_funding_hours', write)
    asyncio.run(update.update_binance_meta(h, stamp))
    assert in_flight == [True] and h.funding_mgr.check_ready('A', stamp)


def test_late_already_persisted_history_does_not_rewrite_same_tick(tmp_path, monkeypatch):
    from core.funding_backfill import save_funding_hours
    h = handle(tmp_path)
    stamp = HOUR + pd.Timedelta(minutes=25)
    frame = pd.DataFrame(dict(symbol=['A'], candle_begin_time=[HOUR], funding_rate=[.2]))
    save_funding_hours(h.funding_mgr, 'A', stamp, frame, None)
    def fail_rewrite(*args, **kwargs): pytest.fail('redundant same-tick pickle write')
    monkeypatch.setattr(pd.DataFrame, 'to_pickle', fail_rewrite)
    save_funding_hours(h.funding_mgr, 'A', stamp, frame, None)
