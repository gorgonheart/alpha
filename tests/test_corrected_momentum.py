"""用户修正的 Python 因子是独立 oracle；禁止用 C 输出重建预期值。"""
import importlib
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

NAME = '动量210401_双项异常隔离V2'
spec = importlib.util.spec_from_file_location('momentum_v2_oracle',
    Path(__file__).resolve().parent / 'support' / 'momentum_v2_oracle.py')
oracle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(oracle)


def candles(length=2400, events=True):
    x = np.arange(length)
    close = 100 + .001 * x + .2 * np.sin(x / 19)
    df = pd.DataFrame({'high': close * 1.001, 'low': close * .999, 'close': close,
                       'candle_begin_time': pd.date_range('2025-01-01', periods=length, freq='h')})
    if events and length > 2000:
        # Four consecutive wide bars trigger the four-bar detector; two separate events.
        for start in [900, 1100]:
            df.loc[start:start + 3, 'high'] *= 3
            df.loc[start:start + 3, 'low'] *= .1
        df.loc[1500, 'close'] = np.nan
        df.loc[1800, 'low'] = 0
    return df


@pytest.mark.parametrize('param', [168, 8568, 10248, 13608, (800, 37, 12, 6)])
@pytest.mark.parametrize('clock', ['hourly', 'gapped', 'no_time'])
def test_native_matches_all_oracle_states(param, clock):
    native = importlib.import_module(f'factors.{NAME}')
    df = candles()
    if clock == 'gapped':
        df.loc[1000:, 'candle_begin_time'] += pd.Timedelta(hours=240)
    elif clock == 'no_time':
        df = df.drop(columns='candle_begin_time')
    before = df.copy(deep=True)
    expected = oracle.diagnostics(df, param)
    actual = native.diagnostics(df, param)
    pd.testing.assert_frame_equal(actual, expected, rtol=2e-11, atol=2e-11)
    pd.testing.assert_frame_equal(df, before, check_exact=True)
    if param != 168:
        assert expected.anomaly.any()
    assert expected.invalid.any()


@pytest.mark.parametrize('length', [0, 1, 2, 166, 167, 168, 169, 600])
def test_short_flat_and_index_inputs(length):
    native = importlib.import_module(f'factors.{NAME}')
    for flat in [False, True]:
        df = candles(length, False)
        if flat:
            df[['high', 'low', 'close']] = 100.
        df.index = pd.RangeIndex(20, 20 + length)
        pd.testing.assert_frame_equal(native.diagnostics(df, 8568), oracle.diagnostics(df, 8568),
                                      rtol=2e-11, atol=2e-11)


def test_causal_parameter_order_and_cooldown_boundaries():
    native = importlib.import_module(f'factors.{NAME}')
    df = candles()
    params = [8568, 10248, (800, 37, 12, 6)]
    full = native.signal_multi_params(df, params)
    reverse = native.signal_multi_params(df, params[::-1])
    for end in [168, 900, 904, 1072, 1104, 1501, 1801, 2200]:
        prefix = native.signal_multi_params(df.iloc[:end], params)
        for p in params:
            pd.testing.assert_series_equal(prefix[str(p)], full[str(p)].iloc[:end], check_exact=True)
            pd.testing.assert_series_equal(full[str(p)], reverse[str(p)], check_exact=True)
    diag = native.diagnostics(df, 8568)
    assert diag.blocked.iloc[1500:1669].all()
    assert not diag.blocked.iloc[1669]
    assert diag.normal_count.iloc[1501] == 1


@pytest.mark.parametrize('param', [0, 166, (200, 0, 2, 0), (200, 4, 1, 0), (200, 4, 2, -1), 168.5])
def test_invalid_parameters(param):
    native = importlib.import_module(f'factors.{NAME}')
    with pytest.raises((ValueError, TypeError)):
        native.signal_multi_params(candles(10), [param])


def test_invalid_times():
    native = importlib.import_module(f'factors.{NAME}')
    for times in [[pd.NaT, pd.Timestamp('2025-01-01')],
                  pd.to_datetime(['2025-01-01', '2025-01-01']),
                  pd.to_datetime(['2025-01-02', '2025-01-01'])]:
        df = candles(2)
        df['candle_begin_time'] = times
        with pytest.raises(ValueError):
            native.signal_multi_params(df, [168])


def test_production_path_does_not_build_diagnostic_frames(monkeypatch):
    from factors.c_ext import momentum_v2_wrapper as bridge
    native = importlib.import_module(f'factors.{NAME}')
    df = candles()
    expected = oracle.signal_multi_params(df, [3509, 5421])
    def reject(*args, **kwargs):
        raise AssertionError('production must not allocate a diagnostic DataFrame')
    monkeypatch.setattr(bridge.pd, 'DataFrame', reject)
    actual = native.signal_multi_params(df, [3509, 5421])
    for p in actual:
        np.testing.assert_allclose(actual[p], expected[p], rtol=2e-11, atol=2e-11)


def test_typed_timestamps_do_not_reparse(monkeypatch):
    from factors.c_ext import momentum_v2_wrapper as bridge
    df = candles()
    def reject(*args, **kwargs):
        raise AssertionError('datetime columns must not be reparsed')
    monkeypatch.setattr(bridge.pd, 'to_datetime', reject)
    bridge.inputs(df)


@pytest.mark.parametrize('kind', ['string', 'timezone', 'microseconds', 'readonly', 'no_time'])
def test_signal_path_matches_diagnostics_and_oracle(kind):
    native = importlib.import_module(f'factors.{NAME}')
    df = candles()
    if kind == 'string':
        df['candle_begin_time'] = df.candle_begin_time.astype(str)
    elif kind == 'timezone':
        df['candle_begin_time'] = df.candle_begin_time.dt.tz_localize('UTC').dt.tz_convert('Asia/Hong_Kong')
    elif kind == 'microseconds':
        df['candle_begin_time'] = df.candle_begin_time.astype('datetime64[us]')
    elif kind == 'no_time':
        df = df.drop(columns='candle_begin_time')
    else:
        for col in ['high', 'low', 'close']:
            df[col].values.flags.writeable = False
    for p in [3509, (800, 37, 12, 6)]:
        actual = native.signal_multi_params(df, [p])[str(p)]
        pd.testing.assert_series_equal(actual, native.diagnostics(df, p)['signal'], check_exact=True)
        pd.testing.assert_series_equal(actual, oracle.signal_multi_params(df, [p])[str(p)],
                                       rtol=2e-11, atol=2e-11)


def test_real_history_and_thread_safety():
    from concurrent.futures import ThreadPoolExecutor
    from tests.support.module6_harness import load_btcusdt_candles
    native = importlib.import_module(f'factors.{NAME}')
    df = load_btcusdt_candles()
    before = df.copy(deep=True)
    params = [8568, 10248, 13608]
    with ThreadPoolExecutor(max_workers=3) as pool:
        actuals = list(pool.map(lambda p: native.diagnostics(df, p), params))
    for p, actual in zip(params, actuals):
        pd.testing.assert_frame_equal(actual, oracle.diagnostics(df, p), rtol=2e-11, atol=2e-11)
    pd.testing.assert_frame_equal(df, before, check_exact=True)


def test_configured_momentum_parameters_match_python_history():
    from core.preprocess import _load_factor_modules_params, apply_precomputed_factors
    from tests.support.module6_harness import load_btcusdt_candles
    modules = [item for item in _load_factor_modules_params() if item[1] == NAME]
    assert len(modules) == 1
    assert modules[0][2] == [13608, 5421, 3509, 12600]
    df = load_btcusdt_candles()
    before = df.copy(deep=True)
    actual = apply_precomputed_factors(df, modules)
    expected = oracle.signal_multi_params(df, modules[0][2])
    for param, values in expected.items():
        np.testing.assert_allclose(actual[f'{NAME}_{param}'], values, rtol=2e-11, atol=2e-11)
    pd.testing.assert_frame_equal(df, before, check_exact=True)


def test_isolated_incremental_embedding_and_raw_cache(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from core.candle_manager import CandleFileManager
    from core import preprocess
    from core.utils.time import get_offset_from_runtime
    df = candles()
    df['candle_begin_time'] = df.candle_begin_time.dt.tz_localize('UTC')
    df['open'] = df.close
    params = [13608, 5421, 3509, 12600]
    monkeypatch.setattr(preprocess, 'bmac_config', SimpleNamespace(precompute_factor_configs={NAME: params}))
    source_dir, cache_dir = tmp_path / 'source', tmp_path / 'cache'
    source_dir.mkdir(); cache_dir.mkdir()
    source = CandleFileManager(base_dir=source_dir, save_type='pickle')
    cache = CandleFileManager(base_dir=cache_dir, save_type='pickle')
    offset = get_offset_from_runtime(df.candle_begin_time.iloc[-1].to_pydatetime(), '1h')
    market = SimpleNamespace(resample_mgr_map={offset: source})
    handle = SimpleNamespace(resample_interval='1h', data_api=None, fetch_funding_rate=False,
                             get_binance_handle=lambda _: market,
                             preprocess_handle=SimpleNamespace(preprocess_mgr_map={offset: cache}))
    match = pd.DataFrame([dict(spot='BTCUSDT', swap='BTCUSDT',
                               first_begin_time=df.candle_begin_time.iloc[0],
                               last_begin_time=df.candle_begin_time.iloc[-1])])
    for end in [2100, 2400]:
        prefix = df.iloc[:end].copy()
        rt = prefix.candle_begin_time.iloc[-1].to_pydatetime()
        source.save_data_file('BTCUSDT', prefix)
        source.save_ready('BTCUSDT', rt)
        name, _, actual = preprocess.gen_batch_data_incremental(
            handle, rt, 'spot', match, ['BTCUSDT'], 'batch1', with_factors=True)
        expected = oracle.signal_multi_params(prefix, params)
        for p in params:
            np.testing.assert_allclose(actual['BTCUSDT'][f'{NAME}_{p}'], expected[str(p)],
                                       rtol=2e-11, atol=2e-11)
        assert not any(c.startswith(NAME) for c in cache.read_candle(name)['BTCUSDT'].columns)


@pytest.mark.parametrize('seed', [0, 7, 42])
def test_long_random_history_and_invalid_resets(seed):
    native = importlib.import_module(f'factors.{NAME}')
    rng = np.random.default_rng(seed)
    df = candles(18000, False)
    prices = np.exp(rng.normal(0, .004, len(df)).cumsum())
    df['high'], df['low'], df['close'] = prices*1.01, prices*.99, prices
    for row, column, value in [(900, 'high', np.inf), (1200, 'low', -1), (1600, 'close', 0),
                                (2200, 'high', -1), (2300, 'low', np.nan)]:
        df.loc[row, column] = value
    for p in [168, 8568, (800, 37, 12, 6)]:
        pd.testing.assert_frame_equal(native.diagnostics(df, p), oracle.diagnostics(df, p),
                                      rtol=2e-11, atol=2e-11)
