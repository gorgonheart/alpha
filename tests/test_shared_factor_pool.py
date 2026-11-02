"""Offline contracts for shared symbol scheduling, including real disk products."""
from types import SimpleNamespace
from threading import Barrier, Lock, get_ident

import numpy as np
import pandas as pd
import pytest

from core import preprocess as pp
from core.arrow_publish import read_arrow_snapshot
from tests.test_module10_preprocess_pipeline import _make_full_handle


def _input(rows=73):
    x = np.arange(rows, dtype=float)
    return pd.DataFrame({
        'candle_begin_time': pd.date_range('2026-01-01 00:20', periods=rows, freq='h', tz='UTC'),
        'open': 10 + x, 'high': 12 + x, 'low': 9 + x,
        'close': 11 + x, 'quote_volume': 100 + x,
    })


def _feed(handle, frame, symbols=('AAAUSDT', 'BBBUSDT', 'CCCUSDT')):
    stamp = frame.candle_begin_time.iloc[-1].to_pydatetime()
    mgr = handle._binance.resample_mgr_map['20m']
    for i, symbol in enumerate(symbols):
        data = frame.copy()
        data['close'] += i * 10
        mgr.save_data_file(symbol, data)
        mgr.save_ready(symbol, stamp)
    for market in ('spot', 'swap'):
        handle.exg_mgr.save_data_file(f'exginfo_{market}', pd.DataFrame({'symbol': list(reversed(symbols))}))
        handle.exg_mgr.save_ready(f'exginfo_{market}', stamp)
    handle.exg_mgr.save_data_file('spot_swap_matches', pd.DataFrame({'spot': symbols, 'swap': symbols}))
    handle.exg_mgr.save_ready('spot_swap_matches', stamp)
    return stamp, {s: (frame.candle_begin_time.min(), frame.candle_begin_time.max()) for s in symbols}


@pytest.fixture
def oracle_factor(monkeypatch):
    # Independent simple formula makes lost tasks, wrong-market data and truncation visible.
    def signal(df, params):
        return {str(n): df.close.rolling(n).mean() + df.is_spot for n in params}
    specs = [(SimpleNamespace(signal_multi_params=signal), 'Oracle', [3, 5])]
    monkeypatch.setattr(pp, '_load_factor_modules_params', lambda: specs)
    return specs


@pytest.mark.parametrize('workers', [0, 2, 4])
def test_cold_and_incremental_products(tmp_path, monkeypatch, oracle_factor, workers):
    monkeypatch.setattr(pp, 'bmac_config', SimpleNamespace(precompute_factor_workers=workers, publish_kline_num=20))
    frame = _input()
    handle, mgr, _ = _make_full_handle(tmp_path, frame.iloc[:-1], frame.candle_begin_time.iloc[-2])
    handle.preprocess_handle.num_batch = 2  # crosses a batch boundary
    seen = []
    original = pp.preprocess_type_data

    def observe(*args, **kwargs):
        seen.append(kwargs.get('factor_executor'))
        return original(*args, **kwargs)
    monkeypatch.setattr(pp, 'preprocess_type_data', observe)
    for current in (frame.iloc[:-1], frame):
        stamp, begins = _feed(handle, current)
        pp.preprocess_offset_data(handle, stamp, begins, begins)
        for market in ('spot', 'swap'):
            assert mgr.check_ready(f'{market}_dict', stamp)
            assert mgr.check_ready(f'market_pivot_{market}', stamp)
            published = read_arrow_snapshot(tmp_path / 'preprocess' / '20m' / f'{market}_dict_{int(stamp.timestamp())}.arrow')
            assert list(published) == ['AAAUSDT', 'BBBUSDT', 'CCCUSDT']
            raw = mgr.read_candle(f'{market}_dict_batch1') | mgr.read_candle(f'{market}_dict_batch2')
            for symbol, df in raw.items():
                assert not any(c.startswith('Oracle_') for c in df)
                expected = df.assign(**{f'Oracle_{n}': df.close.rolling(n).mean() + (market == 'spot') for n in (3, 5)})
                pd.testing.assert_frame_equal(published[symbol], expected.tail(20).reset_index(drop=True), check_exact=True)
            pivot = mgr.read_candle(f'market_pivot_{market}_2026')
            assert len(pivot['close']) == len(current)
            for symbol in raw:
                np.testing.assert_array_equal(pivot['close'][symbol], raw[symbol].close)
    if workers:
        assert all(pool is not None for pool in seen)
        assert seen[0] is seen[1] and seen[2] is seen[3]
        assert seen[0] is not seen[2]  # per invocation lifetime
    else:
        assert seen == [None] * 4


def test_factor_failure_does_not_publish_failed_market(tmp_path, monkeypatch, oracle_factor):
    monkeypatch.setattr(pp, 'bmac_config', SimpleNamespace(precompute_factor_workers=4, publish_kline_num=20))
    frame = _input()
    handle, mgr, _ = _make_full_handle(tmp_path, frame, frame.candle_begin_time.iloc[-1])
    stamp, begins = _feed(handle, frame)
    def fail(df, params):
        if df.is_spot.iloc[0] and df.symbol.iloc[0] == 'BBBUSDT':
            raise RuntimeError('factor failed')
        return {'3': df.close}
    oracle_factor[0][0].signal_multi_params = fail
    with pytest.raises(RuntimeError, match='factor failed'):
        pp.preprocess_offset_data(handle, stamp, begins, begins)
    assert not mgr.check_ready('spot_dict', stamp)
    assert not mgr.check_ready('market_pivot_spot', stamp)
    assert not list((tmp_path / 'preprocess' / '20m').glob('spot_dict_*.arrow'))


def test_no_factors_does_not_create_factor_pool(tmp_path, monkeypatch):
    monkeypatch.setattr(pp, 'bmac_config', SimpleNamespace(precompute_factor_workers=4))
    monkeypatch.setattr(pp, '_load_factor_modules_params', lambda: [])
    monkeypatch.setattr(pp, 'preprocess_match_symbols', lambda *args: pd.DataFrame())
    seen = []
    monkeypatch.setattr(pp, 'preprocess_type_data', lambda *args, **kwargs: seen.append(kwargs.get('factor_executor')))
    pp.preprocess_offset_data(None, None, None, None)
    assert seen == [None, None]


def test_four_factor_workers_progress_across_both_markets(tmp_path, monkeypatch, oracle_factor):
    monkeypatch.setattr(pp, 'bmac_config', SimpleNamespace(precompute_factor_workers=4, publish_kline_num=20))
    frame = _input()
    handle, _, _ = _make_full_handle(tmp_path, frame, frame.candle_begin_time.iloc[-1])
    handle.preprocess_handle.num_batch = 2
    stamp, begins = _feed(handle, frame, symbols=('AAAUSDT', 'BBBUSDT'))
    barrier, lock, threads = Barrier(4), Lock(), set()
    original = oracle_factor[0][0].signal_multi_params
    def synchronized(df, params):
        with lock:
            threads.add(get_ident())
        barrier.wait(timeout=10)
        return original(df, params)
    oracle_factor[0][0].signal_multi_params = synchronized
    pp.preprocess_offset_data(handle, stamp, begins, begins)
    assert len(threads) == 4
