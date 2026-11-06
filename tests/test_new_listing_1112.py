import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
import pandas as pd
import pytest
from core import binance_update as bu, preprocess as pp
from tests.test_module10_preprocess_pipeline import _make_full_handle


@pytest.mark.parametrize('limit,expected_rows,first_volume', [(99,2,11.),(24,1,12.)])
def test_new_short_history_preserves_first_closed_bar(monkeypatch,limit,expected_rows,first_volume):
    times=pd.date_range('2026-01-01 00:30',periods=24,freq='5min',tz='UTC')
    base=pd.DataFrame({'candle_begin_time':times})
    for col in ['open','high','low','close','volume','quote_volume','trade_num','taker_buy_base_asset_volume','taker_buy_quote_asset_volume']:
        base[col]=1.
    stamp=times[-1]
    saved=[]
    def save(mgr,symbol,df,*args):
        saved.append(df)
        return df.candle_begin_time.min(),df.candle_begin_time.max()
    monkeypatch.setattr(bu,'update_resample_candle',save)
    h=SimpleNamespace(base_interval='5m',resample_interval='1h',get_kline_count_1h=lambda _:14050,
        exg_mgr=SimpleNamespace(read_candle=lambda _:pd.DataFrame({'symbol':['NEW']})))
    b=SimpleNamespace(once_update_candles=limit,resample_mgr_map={'25m':SimpleNamespace(has_symbol=lambda _:True)})
    asyncio.run(bu.fetch_update_resample(h,b,SimpleNamespace(batch_get_candle=AsyncMock(return_value=[base])),'spot',stamp))
    assert len(saved[0])==expected_rows
    assert saved[0].volume.iloc[0]==first_volume
    assert saved[0].candle_begin_time.max()<stamp


def test_empty_base_partitions_are_not_indexed(monkeypatch,tmp_path):
    async def noop(*a): pass
    monkeypatch.setattr(bu,'async_sleep_until_run_time',noop)
    monkeypatch.setattr(bu,'TSManager',lambda _:SimpleNamespace(update=lambda _:None,list_partitions=lambda:[],trim_partition=lambda *a:pytest.fail('empty partition')))
    h=SimpleNamespace(base_interval='5m',get_num_base_candles=lambda _:10,get_binance_handle=lambda _:SimpleNamespace(base_candle_dir=tmp_path))
    bu.update_base_candles(h,pd.Timestamp('2026-01-01',tz='UTC'),'spot',{'NEW':pd.DataFrame()})


def test_all_empty_market_does_not_publish_ready(tmp_path):
    bars=pd.DataFrame({'candle_begin_time':pd.Series([],dtype='datetime64[ns, UTC]'),'open':pd.Series([],dtype=float),'close':pd.Series([],dtype=float)})
    stamp=pd.Timestamp('2026-01-01 00:25',tz='UTC')
    h,mgr,off=_make_full_handle(tmp_path,bars,stamp)
    match=pd.DataFrame(columns=['spot','swap','first_begin_time','last_begin_time'])
    result=pp.gen_batch_data(h,stamp,'spot',match,['BTCUSDT'],'batch1')
    assert result[1]=={} and result[2]=={}
    with pytest.raises(ValueError,match='No current market candles'):
        pp.preprocess_type_data(h,stamp,'spot',match,factor_modules_params=[])
    assert not mgr.check_ready('spot_dict',stamp)


def test_unmatched_valid_coin_still_generates_pivot_year(tmp_path):
    stamp=pd.Timestamp('2026-01-01 02:25',tz='UTC')
    bars=pd.DataFrame({'candle_begin_time':[stamp-pd.Timedelta(hours=1)],'open':[1.],'close':[2.]})
    h,mgr,_=_make_full_handle(tmp_path,bars,stamp)
    result=pp.gen_batch_data(h,stamp,'spot',pd.DataFrame(columns=['spot','swap','first_begin_time','last_begin_time']),['BTCUSDT'],'batch1')
    assert 2026 in result[1]
    assert result[2]['BTCUSDT'].symbol_spot.tolist()==['BTCUSDT']
