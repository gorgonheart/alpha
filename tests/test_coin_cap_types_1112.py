import asyncio
from types import SimpleNamespace
import pandas as pd
import pytest
from core.coin_cap import convert_coin_cap_csv
from core.quantclass_async import QuantclassDataApi
from tests.test_coin_cap_proxy import Session, Response

FIELDS=['usd_price','max_supply','circulating_supply','total_supply','max_mcap','circulating_mcap','total_mcap']

def csv(value):
    return ('metadata\ncandle_begin_time,date_added,symbol,'+','.join(FIELDS)+'\n2026-11-01,2020-01-01,BTC-USDT,'+','.join([value]*7)+'\n').encode('gbk')

@pytest.mark.parametrize('value',['1','','1.5e10'])
def test_download_explicit_float64(value):
    api=QuantclassDataApi(Session(iter([Response(raw=csv(value))]),[]),'key','id')
    df=asyncio.run(api.aioreq_coincap_df('https://example.invalid/cap'))
    assert all(str(df[c].dtype)=='float64' for c in FIELDS)

@pytest.mark.parametrize('value',['1','','1.5e10','broken'])
def test_import_same_numeric_contract_and_failure_preserves_input(tmp_path,value):
    file=tmp_path/'BTC-USDT.csv';file.write_bytes(csv(value))
    writes=[]
    mgr=SimpleNamespace(base_dir=tmp_path,has_symbol=lambda _:False,set_candle=lambda *args:writes.append(args[-1]))
    h=SimpleNamespace(coin_cap_mgr=mgr)
    if value=='broken':
        with pytest.raises(ValueError):convert_coin_cap_csv(h,pd.Timestamp('2026-11-02'))
        assert file.exists() and not writes
    else:
        convert_coin_cap_csv(h,pd.Timestamp('2026-11-02'))
        assert all(str(writes[0][c].dtype)=='float64' for c in FIELDS)
        assert not file.exists()

def test_download_rejects_invalid_text():
    api=QuantclassDataApi(Session(iter([Response(raw=csv('broken'))]),[]),'key','id')
    with pytest.raises(ValueError):asyncio.run(api.aioreq_coincap_df('https://example.invalid/cap'))
