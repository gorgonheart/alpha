import pandas as pd
import pytest
from core import preprocess as pp
from core.arrow_publish import read_arrow_snapshot
from tests.test_module10_preprocess_pipeline import _make_full_handle


@pytest.mark.parametrize('minute',[0,25,55])
@pytest.mark.parametrize('interrupt',[False,True])
def test_overlap_price_correction_reaches_all_products(tmp_path,monkeypatch,minute,interrupt):
    times=pd.date_range('2025-12-31 22:00',periods=4,freq='h',tz='UTC')+pd.Timedelta(minutes=minute)
    bars=pd.DataFrame({'candle_begin_time':times,'open':[1.,2.,3.,4.],'close':[2.,3.,4.,5.],'volume':[10.,20.,30.,40.]})
    stamp=times[-1]+pd.Timedelta(hours=1)
    h,mgr,off=_make_full_handle(tmp_path,bars,stamp)
    match=pd.DataFrame([dict(spot='BTCUSDT',swap='BTCUSDT',first_begin_time=times[0],last_begin_time=times[-1])])
    pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=[])
    revised=bars.copy()
    revised.loc[[0,3],'open']=[11.,44.]
    revised.loc[[0,3],'close']=[22.,55.]
    revised.loc[[0,3],'volume']=[100.,400.]
    newer=stamp+pd.Timedelta(days=9)
    h._binance.resample_mgr_map[off].set_candle('BTCUSDT',newer,revised)
    h.exg_mgr.save_ready('exginfo_spot',newer)
    if interrupt:
        original=pp.gen_pivot_incremental
        def fail(*a,**kw): raise OSError('injected after batch')
        monkeypatch.setattr(pp,'gen_pivot_incremental',fail)
        with pytest.raises(OSError,match='injected'):
            pp.preprocess_type_data(h,newer,'spot',match,factor_modules_params=[])
        assert not mgr.check_ready('spot_dict',newer)
        monkeypatch.setattr(pp,'gen_pivot_incremental',original)
    pp.preprocess_type_data(h,newer,'spot',match,factor_modules_params=[])
    raw=mgr.read_candle('spot_dict_batch1')['BTCUSDT']
    arrow=read_arrow_snapshot(mgr.base_dir/f'spot_dict_{int(newer.timestamp())}.arrow')['BTCUSDT']
    for actual in [raw,arrow]:
        for col in ['open','close','volume']:
            assert actual[col].tolist()==revised[col].tolist()
        assert actual.avg_price_1m.tolist()==revised.open.tolist()
        assert actual.first_candle_time.eq(times[0]).all()
    for year in [2025,2026]:
        pivot=mgr.read_candle(f'market_pivot_spot_{year}')
        source=revised.set_index('candle_begin_time').loc[lambda d:d.index.year==year]
        for dst,src in [('open','open'),('close','close'),('vwap1m','open')]:
            pd.testing.assert_series_equal(pivot[dst]['BTCUSDT'],source[src],check_names=False,check_freq=False)


def test_unchanged_current_year_does_not_rewrite_pivot(tmp_path,monkeypatch):
    times=pd.date_range('2026-01-01 00:25',periods=3,freq='h',tz='UTC')
    bars=pd.DataFrame({'candle_begin_time':times,'open':1.,'close':2.})
    stamp=times[-1]+pd.Timedelta(hours=1)
    h,mgr,off=_make_full_handle(tmp_path,bars,stamp)
    match=pd.DataFrame([dict(spot='BTCUSDT',swap='BTCUSDT',first_begin_time=times[0],last_begin_time=times[-1])])
    pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=[])
    later=stamp+pd.Timedelta(hours=1)
    h.exg_mgr.save_ready('exginfo_spot',later)
    h._binance.resample_mgr_map[off].save_ready('BTCUSDT',later)
    writes=[]
    original=mgr.set_candle
    def record(name,*a,**kw):
        writes.append(name)
        return original(name,*a,**kw)
    monkeypatch.setattr(mgr,'set_candle',record)
    pp.preprocess_type_data(h,later,'spot',match,factor_modules_params=[])
    assert 'market_pivot_spot_2026' not in writes
    assert mgr.check_ready('spot_dict',later)


def test_revised_full_history_long_factors_equal_fresh_rebuild(tmp_path):
    from tests.support.module3_harness import load_btcusdt_spot
    bars=load_btcusdt_spot().tail(14060).reset_index(drop=True)
    assert len(bars)>=14000
    stamp=bars.candle_begin_time.iloc[-1]+pd.Timedelta(hours=1)
    match=pd.DataFrame([dict(spot='BTCUSDT',swap='BTCUSDT',first_begin_time=bars.candle_begin_time.iloc[0],last_begin_time=bars.candle_begin_time.iloc[-1])])
    modules=pp._load_factor_modules_params()
    assert len(modules)==3
    h,mgr,offset=_make_full_handle(tmp_path/'continued',bars,stamp)
    pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=modules,publish_kline_num=14060)
    revised=bars.copy()
    revised.loc[[len(bars)-100,len(bars)-1],'close']*=1.03
    revised.loc[[len(bars)-100,len(bars)-1],'quote_volume']*=2
    newer=stamp+pd.Timedelta(hours=1)
    h._binance.resample_mgr_map[offset].set_candle('BTCUSDT',newer,revised)
    h.exg_mgr.save_ready('exginfo_spot',newer)
    pp.preprocess_type_data(h,newer,'spot',match,factor_modules_params=modules,publish_kline_num=14060)
    fresh,fresh_mgr,_=_make_full_handle(tmp_path/'fresh',revised,newer)
    pp.preprocess_type_data(fresh,newer,'spot',match,factor_modules_params=modules,publish_kline_num=14060)
    actual=read_arrow_snapshot(mgr.base_dir/f'spot_dict_{int(newer.timestamp())}.arrow')['BTCUSDT']
    expected=read_arrow_snapshot(fresh_mgr.base_dir/f'spot_dict_{int(newer.timestamp())}.arrow')['BTCUSDT']
    pd.testing.assert_frame_equal(actual,expected,check_exact=True)
    for year in bars.candle_begin_time.dt.year.unique():
        for field,table in mgr.read_candle(f'market_pivot_spot_{year}').items():
            pd.testing.assert_frame_equal(table,fresh_mgr.read_candle(f'market_pivot_spot_{year}')[field],check_exact=True)
