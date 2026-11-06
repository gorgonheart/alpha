from types import SimpleNamespace
import pandas as pd
import pytest
from core import preprocess as pp

STAMP=pd.Timestamp('2026-01-01 01:25',tz='UTC')
BOUNDS=(STAMP-pd.Timedelta(hours=3),STAMP-pd.Timedelta(hours=1))


def handle():
    mgr=SimpleNamespace(has_symbol=lambda s:s!='NEW')
    return SimpleNamespace(resample_interval='1h',exg_mgr=SimpleNamespace(read_candle=lambda _:pd.DataFrame({'symbol':['OLD','NEW']})),
        get_binance_handle=lambda _:SimpleNamespace(resample_mgr_map={'25m':mgr}))


def choose(bn,api=None,bn_done=True,api_done=True):
    from core.publication_sources import choose_publication_inputs
    return choose_publication_inputs(handle(),STAMP,bn,api,binance_done=bn_done,data_api_done=api_done)


def bn_result():
    frames={'OLD':pd.DataFrame({'x':[1]}),'NEW':pd.DataFrame()}
    return frames,frames,{'OLD':BOUNDS},{'OLD':BOUNDS}


def test_dataapi_partial_cannot_publish_before_binance_completion():
    assert choose(None,({'OLD':BOUNDS},{'OLD':BOUNDS}),bn_done=False) is None


def test_binance_complete_does_not_wait_dataapi_and_excludes_empty_listing():
    sources,spot,swap=choose(bn_result(),api_done=False)
    assert sources=={'spot':{'OLD':'binance'},'swap':{'OLD':'binance'}}
    assert spot==swap=={'OLD':BOUNDS}


def test_failed_binance_uses_only_complete_dataapi():
    api=({'OLD':BOUNDS,'NEW':BOUNDS},{'OLD':BOUNDS,'NEW':BOUNDS})
    sources,*_=choose(None,api)
    assert all(v=='data_api' for market in sources.values() for v in market.values())
    with pytest.raises(ValueError,match='Incomplete'):
        choose(None,({'OLD':BOUNDS},{'OLD':BOUNDS}))


def test_stale_binance_falls_back_or_waits_without_publishing_stale():
    bn=bn_result()
    stale=(BOUNDS[0],BOUNDS[1]-pd.Timedelta(hours=1))
    bn=(bn[0],bn[1],{'OLD':stale},bn[3])
    assert choose(bn,api_done=False) is None
    sources,*_=choose(bn,({'OLD':BOUNDS},{'OLD':BOUNDS}))
    assert sources['spot']['OLD']=='data_api'
    assert sources['swap']['OLD']=='binance'


def test_selected_source_cannot_switch_when_other_source_arrives():
    bn=SimpleNamespace(check_ready=lambda *a:True,read_candle=lambda *a:'late-binance')
    api=SimpleNamespace(check_ready=lambda *a:True,read_candle=lambda *a:'chosen-dataapi')
    assert pp.read_candle_data(bn,api,'OLD',STAMP,source='data_api')==('chosen-dataapi','data_api')
    assert pp.read_candle_data(bn,api,'OLD',STAMP,source='absent') is None


@pytest.mark.parametrize('first',['api','binance'])
@pytest.mark.parametrize('failure',[None,'api','binance'])
def test_scheduler_dispatches_once_and_source_failure_does_not_lose_other_source(monkeypatch,first,failure):
    import realtime_data as rt
    h=handle();h.enabled_hour_offsets=('25m',);h.base_interval='5m'
    h.data_api=object();h.coin_cap_handle=None;h.preprocess_handle=object()
    events=[];jobs={}
    class Job:
        def __init__(self,fn,args):self.fn,self.args=fn,args
        def result(self):return self.fn(*self.args)
    class Pool:
        def __init__(self,**kw):pass
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def submit(self,fn,*args):
            job=Job(fn,args);jobs[fn.__name__]=job
            if fn is publish:
                # Source map already fixed when dispatched, before later future resolves.
                events.append(('submit',args[0].publication_sources))
            return job
    def binance(*a):
        events.append(('binance',None))
        if failure=='binance':raise TimeoutError('offline source failure')
        return bn_result()
    def api(*a):
        events.append(('api',None))
        if failure=='api':raise TimeoutError('offline source failure')
        return ({'OLD':BOUNDS,'NEW':BOUNDS},{'OLD':BOUNDS,'NEW':BOUNDS})
    def publish(*a):events.append(('publish',a[0].publication_sources))
    async def meta(*a):pass
    monkeypatch.setattr(rt,'ProcessPoolExecutor',Pool)
    monkeypatch.setattr(rt,'sleep_until_run_time',lambda *a:None)
    monkeypatch.setattr(rt,'update_binance_meta',meta)
    monkeypatch.setattr(rt,'update_symbol_match',lambda *a:None)
    monkeypatch.setattr(rt,'fetch_update_binance_candles',binance)
    monkeypatch.setattr(rt,'update_data_api_candles',api)
    monkeypatch.setattr(rt,'preprocess_offset_data',publish)
    monkeypatch.setattr(rt,'as_completed',lambda _:iter([jobs[first],jobs['api' if first=='binance' else 'binance']]))
    monkeypatch.setattr(rt,'update_binance_base_candles',lambda *a:events.append(('base',None)))
    monkeypatch.setattr(rt,'copy_data_api_candles',lambda *a:None)
    monkeypatch.setattr(rt,'send_wechat_work_msg',lambda *a:None)
    rt.update_all(h,STAMP.to_pydatetime())
    assert sum(name=='submit' for name,_ in events)==1
    assert sum(name=='publish' for name,_ in events)==1
    selected=next(v for n,v in events if n=='submit')
    assert selected['spot']['OLD']==('data_api' if failure=='binance' else 'binance')
    if first=='binance' and failure!='binance':
        assert [n for n,_ in events].index('submit')<[n for n,_ in events].index('api')
    assert ('base',None) in events or failure=='binance'
    assert not hasattr(h,'publication_sources')


@pytest.mark.parametrize('mature',[False,True])
def test_actual_fetch_empty_file_is_new_listing_only_without_real_history(tmp_path,mature):
    import asyncio
    from unittest.mock import AsyncMock
    from core.binance_update import fetch_update_resample
    from tests.test_module10_preprocess_pipeline import _make_full_handle
    from core.publication_sources import choose_publication_inputs
    times=pd.date_range(end=STAMP-pd.Timedelta(minutes=5),periods=24,freq='5min')
    base=pd.DataFrame({'candle_begin_time':times})
    for col in ['open','high','low','close','volume','quote_volume','trade_num','taker_buy_base_asset_volume','taker_buy_quote_asset_volume']:base[col]=1.
    h,_,off=_make_full_handle(tmp_path,base,STAMP,symbol='OLD')
    h.base_interval='5m';h.get_kline_count_1h=lambda _:14050
    h._binance.once_update_candles=99
    for kind in ['spot','swap']:h.exg_mgr.set_candle('exginfo_'+kind,STAMP,pd.DataFrame({'symbol':['OLD','NEW']}))
    if mature:h._binance.resample_mgr_map[off].set_candle('NEW',STAMP,base.iloc[:1].assign(candle_begin_time=STAMP-pd.Timedelta(days=1)))
    frames,bounds=asyncio.run(fetch_update_resample(h,h._binance,SimpleNamespace(batch_get_candle=AsyncMock(return_value=[base,base.iloc[:0]])),'spot',STAMP))
    assert h._binance.resample_mgr_map[off].has_symbol('NEW')
    result=(frames,frames,bounds,bounds)
    if mature:
        with pytest.raises(ValueError,match='Incomplete'):
            choose_publication_inputs(h,STAMP,result,None,binance_done=True,data_api_done=True)
    else:
        sources,*_=choose_publication_inputs(h,STAMP,result,None,binance_done=True,data_api_done=True)
        assert sources=={'spot':{'OLD':'binance'},'swap':{'OLD':'binance'}}
