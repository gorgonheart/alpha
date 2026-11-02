import numpy as np
import pandas as pd
from factors.c_ext.illq_wrapper import compute_illq_c
from tests.support.illq_harness import signal_multi_params_py


def test_positive_volume_flat_candle_does_not_poison_future_windows():
    rng = np.random.default_rng(42)
    df = pd.DataFrame(dict(open=np.full(600,10.), high=11.+rng.random(600),
        low=9.-rng.random(600),close=10.+rng.random(600),quote_volume=rng.uniform(100,1000,600)))
    df.loc[50,['open','high','low','close']] = 10.
    for n in (24,96):
        expected = signal_multi_params_py(df,[n])[str(n)]
        actual = compute_illq_c(*(df[c].to_numpy() for c in ('open','high','low','close','quote_volume')),n=n)
        np.testing.assert_allclose(actual,expected,rtol=1e-10,atol=1e-10,equal_nan=True)


def test_large_level_small_variance_matches_pandas_oracle():
    rng=np.random.default_rng(7)
    df=pd.DataFrame(dict(open=np.full(1000,10.),high=11.,low=9.,close=10.,
                        quote_volume=1e10+rng.normal(0,20,1000)))
    for n in (24,96,264):
        expected=signal_multi_params_py(df,[n])[str(n)]
        actual=compute_illq_c(*(df[c].to_numpy() for c in ('open','high','low','close','quote_volume')),n=n)
        np.testing.assert_allclose(actual,expected,rtol=1e-10,atol=1e-10,equal_nan=True)
