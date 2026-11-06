"""Independent expected mapping, including retained historical rows."""
import pandas as pd
import pytest
from core.symbol_match import get_spot_swap_matches
from core.preprocess import gen_batch_data, gen_batch_data_incremental
from tests.test_module10_preprocess_pipeline import _make_full_handle


def pairs(spots, swaps):
    df = get_spot_swap_matches(spots, swaps)
    return {(None if pd.isna(a) else a, None if pd.isna(b) else b) for a,b in df.itertuples(index=False,name=None)}


@pytest.mark.parametrize('spots,swaps,expected', [
    (['1000CATUSDT'], ['CATUSDT','1000CATUSDT'], {('1000CATUSDT','1000CATUSDT'),(None,'CATUSDT')}),
    (['CATUSDT'], ['1000CATUSDT'], {('CATUSDT','1000CATUSDT')}),
    (['LUNAUSDT'], ['LUNA2USDT','1000LUNAUSDT'], {('LUNAUSDT',None),(None,'LUNA2USDT'),(None,'1000LUNAUSDT')}),
    (['CATUSDT','1000CATUSDT'], [], {('CATUSDT',None),('1000CATUSDT',None)}),
    ([], [], set()),
])
def test_exact_unique_or_unmatched(spots, swaps, expected):
    assert pairs(spots,swaps) == expected
    assert pairs(spots[::-1],swaps[::-1]) == expected


def test_duplicate_symbols_cannot_duplicate_mapping_rows():
    result=get_spot_swap_matches(['AAA','AAA'], ['AAA','AAA'])
    assert result.to_dict('records') == [{'spot':'AAA','swap':'AAA'}]


@pytest.mark.parametrize('replacement', ['CORRECT', None])
def test_incremental_replaces_mapping_over_retained_history(tmp_path, replacement):
    times=pd.date_range('2026-01-01 00:25',periods=4,freq='h',tz='UTC')
    bars=pd.DataFrame({'candle_begin_time':times,'open':1.,'close':2.})
    stamp=times[-1]+pd.Timedelta(hours=1)
    handle,mgr,off=_make_full_handle(tmp_path,bars,stamp)
    old=pd.DataFrame([dict(spot='BTCUSDT',swap='WRONG',first_begin_time=times[0],last_begin_time=times[-1])])
    gen_batch_data(handle,stamp,'spot',old,['BTCUSDT'],'batch1')
    new=old.copy()
    new['swap']=replacement
    new['first_begin_time']=times[1]
    new['last_begin_time']=times[2]
    if replacement is None:
        new['first_begin_time']=pd.NaT
        new['last_begin_time']=pd.NaT
    _,_,result=gen_batch_data_incremental(handle,stamp,'spot',new,['BTCUSDT'],'batch1')
    frame=result['BTCUSDT']
    assert frame['symbol_swap'].fillna('').tolist() == (['', 'CORRECT','CORRECT',''] if replacement else ['']*4)
    assert frame['symbol_spot'].tolist() == ['BTCUSDT']*4
    assert frame['first_candle_time'].eq(times[0]).all()


@pytest.mark.parametrize('kind',['spot','swap'])
def test_one_sided_history_does_not_invent_partner(kind):
    from core.preprocess import calc_match,apply_symbol_match
    from types import SimpleNamespace
    times=pd.date_range('2026-01-01',periods=3,freq='h',tz='UTC')
    bounds={'AAA':(times[0],times[-1])}
    match=calc_match('AAA','AAA',bounds if kind=='spot' else {},bounds if kind=='swap' else {})
    frame=pd.DataFrame({'candle_begin_time':times})
    apply_symbol_match(frame,kind,'AAA',SimpleNamespace(**match))
    other='swap' if kind=='spot' else 'spot'
    assert frame['symbol_'+kind].eq('AAA').all()
    assert frame['symbol_'+other].eq('').all()


def test_rolling_source_window_keeps_verified_retained_pairing(tmp_path):
    from core.preprocess import calc_match
    times = pd.date_range('2026-01-01 00:25', periods=5, freq='h', tz='UTC')
    bars = pd.DataFrame({'candle_begin_time': times[:4], 'open': 1., 'close': 2.})
    stamp = times[4]
    handle, mgr, off = _make_full_handle(tmp_path, bars, stamp)
    bounds = {'BTCUSDT': (times[0], times[3])}
    matches = pd.DataFrame([calc_match('BTCUSDT', 'BTCUSDT', bounds, bounds)])
    gen_batch_data(handle, stamp, 'spot', matches, ['BTCUSDT'], 'batch1')
    source = handle.get_binance_handle('spot').resample_mgr_map[off]
    source.set_candle('BTCUSDT', stamp + pd.Timedelta(hours=1), pd.DataFrame({
        'candle_begin_time': times[1:], 'open': 1., 'close': 2.}))
    rolled = {'BTCUSDT': (times[1], times[4])}
    matches = pd.DataFrame([calc_match('BTCUSDT', 'BTCUSDT', rolled, rolled)])
    _, _, result = gen_batch_data_incremental(
        handle, stamp + pd.Timedelta(hours=1), 'spot', matches, ['BTCUSDT'], 'batch1')
    assert result['BTCUSDT'].candle_begin_time.tolist() == times.tolist()
    assert result['BTCUSDT'].symbol_swap.tolist() == ['BTCUSDT'] * 5


def test_retained_mapping_never_invents_older_pair_or_keeps_after_end():
    from core.preprocess import apply_symbol_match
    from types import SimpleNamespace
    times = pd.date_range('2026-01-01', periods=5, freq='h', tz='UTC')
    frame = pd.DataFrame({'candle_begin_time': times,
                          'symbol_swap': ['', 'AAA', 'AAA', 'AAA', 'AAA']})
    row = SimpleNamespace(spot='AAA', swap='AAA', first_begin_time=times[2],
                          last_begin_time=times[3])
    apply_symbol_match(frame, 'spot', 'AAA', row)
    assert frame.symbol_swap.tolist() == ['', 'AAA', 'AAA', 'AAA', '']
