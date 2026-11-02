from types import SimpleNamespace

import pandas as pd
import pytest

import realtime_data as rt
from core import backfill as bf


@pytest.mark.parametrize('minute,expected', [(25,'40m'), (45,'55m'), (58,'20m')])
def test_backfill_selects_only_nearest_upcoming_missing_offset(monkeypatch, minute, expected):
    now = pd.Timestamp(f'2026-10-31 08:{minute}', tz='Asia/Hong_Kong').to_pydatetime()
    monkeypatch.setattr(bf, 'now_time', lambda: now)
    monkeypatch.setattr(bf, 'pending_offsets', lambda h: ['20m','40m','55m'])
    monkeypatch.setattr(bf, 'seconds_to_next_offset', lambda *args: 600)
    monkeypatch.setattr(bf, '_last_attempt', {})
    selected = []
    class Pool:
        def __init__(self, **kwargs): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def submit(self, fn, handle, targets):
            selected.extend(targets)
            return SimpleNamespace(result=lambda: {s:'stale' for s in targets})
    monkeypatch.setattr(bf, 'ProcessPoolExecutor', Pool)
    bf.maybe_backfill(SimpleNamespace(enabled_hour_offsets=('20m','40m','55m')))
    assert selected == [expected]
    historical = bf.latest_offset_run_time(expected, '5m', now)
    assert historical <= now
    assert historical.minute == int(expected[:-1])


@pytest.mark.parametrize('metadata_fails', [False, True])
@pytest.mark.parametrize('data_api,coin_cap', [(True,True),(True,False),(False,True),(False,False)])
def test_pool_is_started_before_metadata_and_fetch_waits_for_target(monkeypatch, metadata_fails, data_api, coin_cap):
    stamp = pd.Timestamp('2026-10-31 08:40', tz='Asia/Hong_Kong').to_pydatetime()
    now = [stamp-pd.Timedelta(seconds=20)]
    events, submissions = [], []
    handle = SimpleNamespace(base_interval='5m', enabled_hour_offsets=('40m',), data_api=object() if data_api else None,
        coin_cap_handle=object() if coin_cap else None, preprocess_handle=None)
    workers = 1+int(data_api)+int(coin_cap)
    def wait(target): now[0] = max(now[0], target)
    monkeypatch.setattr(rt, 'sleep_until_run_time', wait)
    monkeypatch.setattr(rt, 'divider', lambda *a, **kw: None)
    class Job:
        def __init__(self, fn, args): self.fn, self.args = fn, args
        def result(self): return self.fn(*self.args)
    class Pool:
        def __init__(self, **kwargs):
            assert kwargs['max_workers'] == workers
            events.append(('create',now[0]))
        def __enter__(self): return self
        def __exit__(self, *args): events.append(('close',now[0]))
        def submit(self, fn, *args):
            submissions.append(fn)
            events.append(('submit',now[0]))
            return Job(fn,args)
    monkeypatch.setattr(rt, 'ProcessPoolExecutor', Pool)
    monkeypatch.setattr(rt, 'as_completed', lambda jobs: iter(jobs))
    async def meta(*args):
        events.append(('metadata',now[0]))
        if metadata_fails: raise TimeoutError()
        now[0] += pd.Timedelta(seconds=5)
    monkeypatch.setattr(rt, 'update_binance_meta', meta)
    monkeypatch.setattr(rt, 'update_symbol_match', lambda *a: None)
    def fetch(*args):
        assert now[0] >= stamp
        events.append(('fetch',now[0]))
        return {},{},{},{}
    monkeypatch.setattr(rt, 'fetch_update_binance_candles', fetch)
    monkeypatch.setattr(rt, 'update_data_api_candles', lambda *a: ({},{}))
    monkeypatch.setattr(rt, 'run_coin_cap_update_to_recent', lambda *a: True)
    monkeypatch.setattr(rt, 'update_binance_base_candles', lambda *a: None)
    if metadata_fails:
        with pytest.raises(TimeoutError): rt.update_all(handle,stamp)
        assert not any(name=='fetch' for name,_ in events)
    else:
        rt.update_all(handle,stamp)
    assert events[0][0] == 'create' and events[0][1] < stamp
    meta_index = next(i for i,e in enumerate(events) if e[0]=='metadata')
    assert sum(e[0]=='submit' for e in events[:meta_index]) == workers
    assert submissions[:workers] == [wait]*workers
    assert events[-1][0] == 'close'
