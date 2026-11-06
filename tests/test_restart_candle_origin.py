"""Regular restart of a continuously publishing producer, with rolled-off source rows."""
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from core.arrow_publish import read_arrow_snapshot
from core.candle_manager import CandleFileManager
from core.preprocess import preprocess_type_data
from core.utils.file_system import init_dirs
from tests.test_module10_preprocess_pipeline import _make_full_handle


def publish(handle, stamp, kind, frames):
    bars = handle.get_binance_handle(kind).resample_mgr_map[f'{stamp.minute}m']
    for symbol, frame in frames.items():
        bars.set_candle(symbol, stamp, frame)
    handle.exg_mgr.set_candle(f'exginfo_{kind}', stamp, pd.DataFrame({'symbol': list(frames)}))
    match = pd.DataFrame([dict(spot=s, swap=s, first_begin_time=f.candle_begin_time.min(),
                               last_begin_time=f.candle_begin_time.max()) for s, f in frames.items()])
    preprocess_type_data(handle, stamp, kind, match, factor_modules_params=[], publish_kline_num=170)
    mgr = handle.preprocess_handle.preprocess_mgr_map[f'{stamp.minute}m']
    assert mgr.check_ready(f'{kind}_dict', stamp) and mgr.check_ready(f'market_pivot_{kind}', stamp)
    arrow = mgr.base_dir / f'{kind}_dict_{int(stamp.timestamp())}.arrow'
    return read_arrow_snapshot(arrow), mgr.read_candle(f'market_pivot_{kind}_{stamp.year}')


def restart_case(root, legacy_pkl=False, correction=None, kind='spot', add_listing=True, between_rounds=False):
    times = pd.date_range('2026-01-01 00:25', periods=24*180+1, freq='h', tz='UTC')
    bars = pd.DataFrame({'candle_begin_time': times, 'open': 1., 'close': 2.})
    for column in ('high', 'low', 'volume', 'quote_volume', 'trade_num',
                   'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'):
        bars[column] = 3.
    stamp = times[-1]
    handle, mgr, off = _make_full_handle(root, bars.iloc[:-1], stamp)
    handle.binance_spot = handle._binance
    handle.binance_spot.resample_dir = root / 'binance'
    handle.binance_spot.base_candle_dir = root / 'base_spot'
    handle.binance_swap = SimpleNamespace(base_candle_dir=root / 'base_swap', resample_dir=root / 'swap',
                                          resample_mgr_map={})
    handle.preprocess_handle.preprocess_dir = mgr.base_dir.parent
    handle.coin_cap_handle = None
    if kind == 'swap':
        handle.fetch_funding_rate = True
        handle.funding_mgr = CandleFileManager(root / 'funding', 'pickle')
        handle.funding_mgr.base_dir.mkdir()
        rate = pd.DataFrame({'candle_begin_time': [times[-5].floor('h')], 'funding_rate': [.001]})
        handle.funding_mgr.save_data_file('BTCUSDT', rate)
    old = publish(handle, stamp, kind, {'BTCUSDT': bars.iloc[:-1].copy()})
    if between_rounds:
        # A newer Arrow has just evicted the old gate but has not released its own.
        (mgr.base_dir / f'{kind}_dict_{int(stamp.timestamp())}.ready').unlink()
    else:
        # First upgrade from the old producer: no independent origin record yet.
        (root / 'preprocess_origins' / off / f'{kind}.pkl').unlink(missing_ok=True)
    if legacy_pkl:
        (mgr.base_dir / f'{kind}_dict_{int(stamp.timestamp())}.arrow').unlink()
    init_dirs(handle)
    init_dirs(handle)  # restart again before any rebuilt output is ready
    recent = bars.tail(170).copy()
    if correction == 'close':
        recent.loc[recent.index[-5], 'close'] = 3.
    if correction == 'funding':
        handle.funding_mgr.save_data_file('BTCUSDT', rate.assign(funding_rate=-.002))
    frames = {'BTCUSDT': recent}
    if add_listing:
        frames['NEW'] = recent.tail(1).copy()
    current = publish(handle, stamp + pd.Timedelta(hours=1), kind, frames)
    return mgr, times, old, current


@pytest.mark.parametrize('legacy_pkl', [False, True])
def test_regular_restart_keeps_published_age_without_restoring_old_bars(tmp_path, legacy_pkl):
    mgr, times, (before, _), (after, _pivot) = restart_case(tmp_path, legacy_pkl)
    assert before['BTCUSDT'].first_candle_time.eq(times[0]).all()
    for frames in (after, mgr.read_candle('spot_dict_batch1')):
        assert frames['BTCUSDT'].candle_begin_time.tolist() == list(times[-170:])
        assert frames['BTCUSDT'].first_candle_time.eq(times[0]).all()
        assert frames['NEW'].first_candle_time.tolist() == [times[-1]]


@pytest.mark.parametrize(('kind', 'correction'), [('spot', None), ('swap', None),
                                                  ('spot', 'close'), ('swap', 'funding')])
def test_alphatrade_model_compares_actual_ready_products(tmp_path, monkeypatch, kind, correction):
    consumer = Path(__file__).resolve().parents[2] / 'alphaonline'
    if not (consumer / 'tools/robust40/handoff.py').is_file():
        pytest.skip('requires the sibling alphatrade checkout')
    monkeypatch.syspath_prepend(str(consumer))
    from tools.robust40.handoff import first_revision, input_frames

    _, times, before, after = restart_case(tmp_path, correction=correction, kind=kind, add_listing=False)
    # Last 168 closed candles of the account snapshot; include the next hour only on the new side.
    def model_inputs(pair, end):
        raw, pivot = pair
        return input_frames({kind: raw}, {kind: pivot}, end, start=times[-169])
    revision = first_revision(model_inputs(before, times[-2]), model_inputs(after, times[-1]))
    assert revision == (times[-5].tz_localize(None) if correction else None)


def test_published_origins_are_offset_and_market_local(tmp_path):
    from core.candle_origins import load_origins, preserve_published_origins

    for minute in (5, 25):
        for kind, days in [('spot', 0), ('swap', 20)]:
            times = pd.date_range('2026-01-01', periods=4, freq='h', tz='UTC') + pd.Timedelta(days=days, minutes=minute)
            bars = pd.DataFrame({'candle_begin_time': times, 'open': 1., 'close': 2.})
            h, mgr, _ = _make_full_handle(tmp_path, bars, times[-1])
            publish(h, times[-1], kind, {'BTCUSDT': bars.iloc[:-1]})
        preserve_published_origins(mgr)
    for minute in (5, 25):
        for kind, days in [('spot', 0), ('swap', 20)]:
            expected = pd.Timestamp('2026-01-01', tz='UTC') + pd.Timedelta(days=days, minutes=minute)
            assert load_origins(tmp_path / 'preprocess' / f'{minute}m', kind)['BTCUSDT'][0] == expected


def test_restart_between_gates_uses_previous_completed_publication(tmp_path):
    _, times, _, (after, _) = restart_case(tmp_path, between_rounds=True)
    assert after['BTCUSDT'].first_candle_time.eq(times[0]).all()


def test_legacy_pkl_excludes_obsolete_batches_after_market_shrinks(tmp_path):
    import os
    from core.candle_origins import load_origins, preserve_published_origins

    first = pd.Timestamp('2026-01-01 00:25', tz='UTC')
    mgr = CandleFileManager(tmp_path / 'preprocess' / '25m', 'pickle')
    mgr.base_dir.mkdir(parents=True)
    frame = pd.DataFrame({'candle_begin_time': [first], 'first_candle_time': [first]})
    mgr.save_data_file('spot_dict_batch2', {'BTCUSDT': frame, 'DELISTED': frame})
    os.utime(mgr.format_data_file_path('spot_dict_batch2'), (100, 100))
    mgr.set_candle('spot_dict_batch1', first, {'BTCUSDT': frame.assign(candle_begin_time=first + pd.Timedelta('1h'))})
    mgr.save_ready('market_pivot_spot', first)
    mgr.save_ready('spot_dict', first)
    preserve_published_origins(mgr)
    assert load_origins(mgr.base_dir, 'spot') == {'BTCUSDT': (first, first + pd.Timedelta('1h'))}


def test_unverifiable_old_release_stops_before_any_directory_is_cleared(tmp_path):
    stamp = pd.Timestamp('2026-01-01 00:25', tz='UTC')
    bars = pd.DataFrame({'candle_begin_time': [stamp], 'open': [1.], 'close': [2.]})
    h, mgr, _ = _make_full_handle(tmp_path, bars, stamp)
    mgr.save_ready('market_pivot_spot', stamp)
    with pytest.raises(ValueError, match='No completed spot dict gate'):
        init_dirs(h)
    assert h.exg_mgr.check_ready('exginfo_spot', stamp)
    assert mgr.check_ready('market_pivot_spot', stamp)
