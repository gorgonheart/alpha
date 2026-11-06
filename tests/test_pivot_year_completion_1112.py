import pandas as pd
import pytest
from core import preprocess as pp
from tests.test_module10_preprocess_pipeline import _make_full_handle


@pytest.mark.parametrize('minute',range(0,60,5))
def test_closed_year_marker_requires_final_grid_bar_and_existing_pivot(tmp_path,minute):
    stamp=pd.Timestamp('2026-01-01 01:00',tz='UTC')+pd.Timedelta(minutes=minute)
    final=pd.Timestamp('2025-12-31 23:00',tz='UTC')+pd.Timedelta(minutes=minute)
    bars=pd.DataFrame({'candle_begin_time':[final-pd.Timedelta(hours=2),final],'open':[1.,2.],'close':[2.,3.]})
    h,mgr,_=_make_full_handle(tmp_path,bars,stamp)
    match=pd.DataFrame([dict(spot='BTCUSDT',swap='BTCUSDT',first_begin_time=bars.candle_begin_time.min(),last_begin_time=final)])
    pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=[])
    assert pp.should_skip_pivot_year(mgr,'spot',2025,2026)
    pivot=mgr.read_candle('market_pivot_spot_2025')
    flag=pp.get_pivot_complete_flag_path(mgr,'spot',2025)
    flag.unlink()
    truncated={k:v.iloc[:-1] for k,v in pivot.items()}
    pp.mark_pivot_complete_if_closed(mgr,'spot',2025,stamp,truncated,h.resample_interval)
    assert not flag.exists()
    pp.mark_pivot_complete_if_closed(mgr,'spot',2025,stamp,pivot,h.resample_interval)
    assert flag.exists()
    mgr.remove_symbol('market_pivot_spot_2025')
    assert not pp.should_skip_pivot_year(mgr,'spot',2025,2026)


def test_unchanged_closed_year_skips_staging_and_write(tmp_path,monkeypatch):
    times=pd.date_range('2025-12-31 22:25',periods=2,freq='h',tz='UTC')
    bars=pd.DataFrame({'candle_begin_time':times,'open':1.,'close':2.})
    stamp=times[-1]+pd.Timedelta(hours=1)
    h,mgr,off=_make_full_handle(tmp_path,bars,stamp)
    match=pd.DataFrame([dict(spot='BTCUSDT',swap='BTCUSDT',first_begin_time=times[0],last_begin_time=times[-1])])
    pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=[])
    later=stamp+pd.Timedelta(days=3)
    h.exg_mgr.save_ready('exginfo_spot',later)
    h._binance.resample_mgr_map[off].save_ready('BTCUSDT',later)
    writes=[];original=mgr.set_candle
    def record(name,*a,**kw):
        writes.append(name);return original(name,*a,**kw)
    monkeypatch.setattr(mgr,'set_candle',record)
    pp.preprocess_type_data(h,later,'spot',match,factor_modules_params=[])
    assert not any('2025' in name for name in writes)
    assert mgr.check_ready('market_pivot_spot',later)


def test_configured_two_hour_grid_uses_epoch_offset_not_only_minute(tmp_path):
    from core.candle_manager import CandleFileManager
    mgr=CandleFileManager(base_dir=tmp_path,save_type='pickle')
    stamp=pd.Timestamp('2026-01-01 01:25',tz='UTC')
    final=pd.Timestamp('2025-12-31 23:25',tz='UTC')
    pivot={'close':pd.DataFrame({'AAA':[1.]},index=pd.DatetimeIndex([final]))}
    pp.mark_pivot_complete_if_closed(mgr,'swap',2025,stamp,pivot,'2h')
    assert pp.get_pivot_complete_flag_path(mgr,'swap',2025).exists()
