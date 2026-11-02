"""Run only the scheduler with fake initialization/I/O, never the live service."""
from types import SimpleNamespace

import pandas as pd
import pytest

import realtime_data as rt


class EndLoop(BaseException):
    pass


def setup_loop(monkeypatch):
    handle = SimpleNamespace(base_interval='5m', enabled_hour_offsets=('20m', '40m', '55m'))
    monkeypatch.setattr(rt, 'create_bmac_handle_from_config', lambda: handle)
    monkeypatch.setattr(rt, 'debug_handle', lambda *args: None)
    monkeypatch.setattr(rt, 'divider', lambda *args, **kwargs: None)
    monkeypatch.setattr(rt, 'sleep_until_run_time', lambda *args: None)
    alerts, sleeps = [], []
    monkeypatch.setattr(rt, 'send_wechat_work_msg', lambda message, *args: alerts.append(message))
    monkeypatch.setattr(rt.time, 'sleep', sleeps.append)
    return handle, alerts, sleeps


@pytest.mark.parametrize('failure', [TimeoutError('late'), ConnectionError('offline'), ValueError('invalid input')])
def test_update_failure_keeps_initialized_products_and_recomputes_runtime(tmp_path, monkeypatch, failure):
    handle, alerts, sleeps = setup_loop(monkeypatch)
    initializations, updates, generated_times = [], [], []
    product = tmp_path/'completed.arrow'
    def initialize(*args):
        initializations.append(1)
        product.write_text(str(len(initializations)))
    monkeypatch.setattr(rt, 'init_history', initialize)
    def clock(*args):
        stamp = pd.Timestamp('2026-10-31 00:20', tz='Asia/Hong_Kong') + pd.Timedelta(minutes=20*len(generated_times))
        generated_times.append(stamp)
        return stamp
    monkeypatch.setattr(rt, 'next_run_time', clock)
    def update(received, stamp):
        updates.append((received, stamp))
        if len(updates) == 1:
            raise failure
        if len(updates) == 3:
            raise EndLoop()
    monkeypatch.setattr(rt, 'update_all', update)
    with pytest.raises(EndLoop):
        rt.main()
    assert len(initializations) == 1
    assert product.read_text() == '1'
    assert [stamp for _, stamp in updates] == generated_times
    assert len(set(generated_times)) == 3
    assert sleeps == [60]
    assert len(alerts) == 1
    assert '保留' in alerts[0] and '重启' not in alerts[0]


def test_initialization_failure_retries_until_first_success(monkeypatch):
    _, alerts, sleeps = setup_loop(monkeypatch)
    attempts = []
    def initialize(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise TimeoutError('initial download')
    monkeypatch.setattr(rt, 'init_history', initialize)
    monkeypatch.setattr(rt, 'next_run_time', lambda *args: None)
    monkeypatch.setattr(rt, 'update_all', lambda *args: (_ for _ in ()).throw(EndLoop()))
    with pytest.raises(EndLoop):
        rt.main()
    assert len(attempts) == 2
    assert sleeps == [60]
    assert len(alerts) == 1 and '初始化' in alerts[0]


def test_scheduler_failure_before_first_update_does_not_reinitialize(monkeypatch):
    _, alerts, sleeps = setup_loop(monkeypatch)
    initializations, ticks = [], []
    monkeypatch.setattr(rt, 'init_history', lambda *args: initializations.append(1))
    def clock(*args):
        ticks.append(1)
        if len(ticks) == 1:
            raise ValueError('scheduler failure')
        raise EndLoop()
    monkeypatch.setattr(rt, 'next_run_time', clock)
    with pytest.raises(EndLoop):
        rt.main()
    assert initializations == [1]
    assert sleeps == [60]
    assert len(alerts) == 1 and '保留' in alerts[0]


def test_missing_offset_backfill_remains_available_after_update_failure(monkeypatch):
    handle, _, _ = setup_loop(monkeypatch)
    initializations, backfills, calls = [], [], []
    monkeypatch.setattr(rt, 'init_history', lambda *args: initializations.append(1))
    monkeypatch.setattr(rt, 'maybe_backfill', lambda received: backfills.append(received))
    monkeypatch.setattr(rt, 'next_run_time', lambda *args: pd.Timestamp('2026-10-31 00:15', tz='Asia/Hong_Kong'))
    original = rt.update_all
    def update(*args):
        calls.append(1)
        if len(calls) == 1:
            raise TimeoutError()
        if len(calls) == 3:
            raise EndLoop()
        return original(*args)
    monkeypatch.setattr(rt, 'update_all', update)
    with pytest.raises(EndLoop):
        rt.main()
    assert initializations == [1]
    assert backfills == [handle]


@pytest.mark.parametrize('failed_check', [False, True])
@pytest.mark.parametrize('work_seconds', [0, 128, 380])
def test_backfill_waits_for_five_minute_boundaries_after_offset_finishes(monkeypatch, failed_check, work_seconds):
    setup_loop(monkeypatch)
    now = [pd.Timestamp('2026-10-31 08:19', tz='Asia/Hong_Kong')]
    checks, waits, updates = [], [], []
    monkeypatch.setattr(rt, 'init_history', lambda *args: None)
    def next_tick(*args):
        return now[0].floor('5min') + pd.Timedelta(minutes=5)
    monkeypatch.setattr(rt, 'next_run_time', next_tick)
    monkeypatch.setattr(rt.time, 'sleep', lambda seconds: now.__setitem__(0, now[0]+pd.Timedelta(seconds=seconds)))
    def wait_until(stamp):
        waits.append(stamp)
        now[0] = max(now[0], stamp)
    monkeypatch.setattr(rt, 'sleep_until_run_time', wait_until)
    def backfill(*args):
        checks.append(now[0])
        if len(checks) == 2:
            raise EndLoop()
        now[0] += pd.Timedelta(seconds=work_seconds)
        if failed_check:
            raise RuntimeError('missing snapshot')
    monkeypatch.setattr(rt, 'maybe_backfill', backfill)
    original = rt.update_all
    def update(handle, stamp):
        updates.append(stamp)
        if stamp.minute == 20:
            # Simulate a normal offset finishing at +128 seconds.
            now[0] = stamp + pd.Timedelta(seconds=128)
            return
        return original(handle, stamp)
    monkeypatch.setattr(rt, 'update_all', update)
    with pytest.raises(EndLoop):
        rt.main()
    next_check = (checks[0] + pd.Timedelta(seconds=work_seconds)).floor('5min') + pd.Timedelta(minutes=5)
    assert checks == [pd.Timestamp('2026-10-31 08:25', tz='Asia/Hong_Kong'), next_check]
    assert waits == updates[1:]
