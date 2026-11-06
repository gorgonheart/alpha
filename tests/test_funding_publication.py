"""Offline, offset-local funding publication through the normal producer entry point."""
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import pytest

from core.arrow_publish import read_arrow_snapshot
from core.candle_manager import CandleFileManager
from core.preprocess import preprocess_type_data
from tests.test_module10_preprocess_pipeline import _make_full_handle


SYMBOL = 'BTCUSDT'


def setup_offset(root, minute, funding):
    times = pd.date_range('2025-12-31 22:00', periods=2, freq='h', tz='UTC') + pd.Timedelta(minutes=minute)
    bars = pd.DataFrame({'candle_begin_time': times, 'open': [1., 2.], 'close': [3., 4.]})
    first = times[-1] + pd.Timedelta(hours=1)
    handle, mgr, offset = _make_full_handle(root, bars, first)
    handle.fetch_funding_rate = True
    handle.funding_mgr = funding
    match = pd.DataFrame([dict(spot=SYMBOL, swap=SYMBOL, first_begin_time=times[0], last_begin_time=times[-1])])
    run_round(handle, first, match)
    return handle, mgr, offset, bars, match


def run_round(handle, stamp, match):
    handle.exg_mgr.save_ready('exginfo_swap', stamp)
    preprocess_type_data(handle, stamp, 'swap', match, factor_modules_params=[])


def write_rates(funding, hours, values):
    funding.save_data_file(SYMBOL, pd.DataFrame({
        'candle_begin_time': pd.to_datetime(hours, utc=True), 'funding_rate': values,
    }))


def check_products(mgr, stamp, expected):
    assert mgr.check_ready('swap_dict', stamp)
    assert mgr.check_ready('market_pivot_swap', stamp)
    batch = mgr.read_candle('swap_dict_batch1')[SYMBOL]
    arrow = read_arrow_snapshot(mgr.base_dir / f'swap_dict_{int(stamp.timestamp())}.arrow')[SYMBOL]
    assert batch.funding_fee.tolist() == expected
    assert arrow.funding_fee.tolist() == expected
    pivot = mgr.read_candle('market_pivot_swap_2025')
    assert pivot['funding_rate'][SYMBOL].tolist() == expected
    assert pivot['close'][SYMBOL].tolist() == [3., 4.]


@pytest.mark.parametrize('bars_available', [True, False])
def test_late_settlement_corrects_own_ready_products_and_survives_retry(tmp_path, monkeypatch, bars_available):
    # A partial collection retains the earlier local 22:00 rate; the 23:00 rate arrives late.
    funding = CandleFileManager(tmp_path, 'pickle')
    write_rates(funding, ['2025-12-31 22:00'], [.125])
    handle, mgr, offset, bars, match = setup_offset(tmp_path, 20, funding)
    current = pd.Timestamp('2026-01-10 01:20', tz='UTC')  # offset offline past the frozen-year cutoff
    check_products(mgr, pd.Timestamp('2026-01-01 00:20', tz='UTC'), [.125, 0.])
    write_rates(funding, ['2025-12-31 23:00'], [-.25])
    if bars_available:
        handle._binance.resample_mgr_map[offset].save_ready(SYMBOL, current)
    # Raw batch can be durable before factors/pivot finish. A retry still repairs the old pivot.
    with monkeypatch.context() as failure:
        def fail(*args, **kwargs):
            raise RuntimeError('interrupted before publication')
        failure.setattr('core.preprocess.publish_arrow_snapshot_for_type', fail)
        with pytest.raises(RuntimeError, match='interrupted before publication'):
            run_round(handle, current, match)
    assert mgr.read_candle('swap_dict_batch1')[SYMBOL].funding_fee.tolist() == [.125, -.25]
    run_round(handle, current, match)
    check_products(mgr, current, [.125, -.25])


def test_offsets_apply_shared_settlement_only_when_they_publish(tmp_path):
    funding = CandleFileManager(tmp_path, 'pickle')
    write_rates(funding, ['2025-12-31 22:00'], [.125])
    offsets = [setup_offset(tmp_path / str(m), m, funding) for m in (0, 20, 40)]
    write_rates(funding, ['2025-12-31 23:00'], [.375])

    def publish(slot):
        handle, mgr, _, bars, match = slot
        stamp = bars.candle_begin_time.iloc[-1] + pd.Timedelta(hours=2)
        run_round(handle, stamp, match)
        check_products(mgr, stamp, [.125, .375])

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(publish, offsets[:2]))
    _, last_mgr, _, last_bars, _ = offsets[-1]
    check_products(last_mgr, last_bars.candle_begin_time.iloc[-1] + pd.Timedelta(hours=1), [.125, 0.])
    publish(offsets[-1])


def test_dict_gate_observes_pivot_and_duplicate_run_cannot_amend_release(tmp_path, monkeypatch):
    funding = CandleFileManager(tmp_path, 'pickle')
    write_rates(funding, ['2025-12-31 23:00'], [0.1])
    handle, mgr, _, bars, match = setup_offset(tmp_path, 20, funding)
    current = bars.candle_begin_time.iloc[-1] + pd.Timedelta(hours=2)
    write_rates(funding, ['2025-12-31 23:00'], [0.2])
    save_ready = mgr.save_ready
    def inspect_gate(name, stamp):
        if name == 'swap_dict':
            assert mgr.check_ready('market_pivot_swap', stamp)
            assert mgr.read_candle('market_pivot_swap_2025')['funding_rate'][SYMBOL].iloc[-1] == 0.2
        save_ready(name, stamp)
    with monkeypatch.context() as gate:
        gate.setattr(mgr, 'save_ready', inspect_gate)
        run_round(handle, current, match)
    write_rates(funding, ['2025-12-31 23:00'], [0.3])
    run_round(handle, current, match)
    check_products(mgr, current, [0., 0.2])


def test_swap_arrow_with_partial_funding_sources(tmp_path):
    times = pd.date_range('2026-01-01 08:20', periods=2, freq='h', tz='UTC')
    bars = pd.DataFrame({'candle_begin_time': times, 'open': [1., 2.], 'close': [2., 3.]})
    stamp = times[-1] + pd.Timedelta(hours=1)
    handle, mgr, off = _make_full_handle(tmp_path, bars, stamp)
    handle.fetch_funding_rate = True
    handle.funding_mgr = CandleFileManager(tmp_path, 'pickle')
    write_rates(handle.funding_mgr, ['2026-01-01 08:00'], [.25])
    handle._binance.resample_mgr_map[off].set_candle('AAAUSDT', stamp, bars)
    symbols = ['AAAUSDT', SYMBOL]
    handle.exg_mgr.set_candle('exginfo_swap', stamp, pd.DataFrame({'symbol': symbols}))
    match = pd.DataFrame(dict(spot=symbols, swap=symbols, first_begin_time=times[0], last_begin_time=times[-1]))
    run_round(handle, stamp, match)
    published = read_arrow_snapshot(mgr.base_dir / f'swap_dict_{int(stamp.timestamp())}.arrow')
    assert published['AAAUSDT'].funding_fee.tolist() == [0., 0.]
    assert published[SYMBOL].funding_fee.tolist() == [.25, 0.]


def test_untouched_legacy_funding_uses_last_valid_exact_hour_before_candle_join(tmp_path):
    funding = CandleFileManager(tmp_path, 'pickle')
    source = pd.DataFrame({
        'candle_begin_time': ['2025-12-31 23:00', 'invalid', '2025-12-31 22:05', '2025-12-31 22:55',
                              '2025-12-31 23:00', '2025-12-31 23:17', '2025-12-31 23:00'],
        'funding_rate': ['0.125', '7', '0.105', '0.1', '-0.35', '.875', 'inf'],
    })
    funding.save_data_file(SYMBOL, source)
    handle, mgr, _, _, match = setup_offset(tmp_path, 20, funding)
    first = pd.Timestamp('2026-01-01 00:20', tz='UTC')
    check_products(mgr, first, [.1, -.35])
    pd.testing.assert_frame_equal(funding.read_candle(SYMBOL), source)

    # Only invalid indications are available for 23:00 on the following pass.
    source = source.iloc[[4, 6]].assign(funding_rate=['corrupt', '-inf'])
    funding.save_data_file(SYMBOL, source)
    run_round(handle, first + pd.Timedelta(hours=1), match)
    check_products(mgr, first + pd.Timedelta(hours=1), [.1, -.35])
