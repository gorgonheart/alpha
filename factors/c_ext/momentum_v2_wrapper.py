"""校验输入并桥接版本化 C 核心；不修改调用者数据。"""
from numbers import Integral
import numpy as np
import pandas as pd
try:
    from . import _momentum_v2_core as native
except ImportError as exc:
    raise ImportError('Build with alpha311: python factors/c_ext/momentum_v2_build.py') from exc
if not hasattr(native.lib, 'compute_momentum_v2_fast'):
    raise ImportError('Rebuild optimized extension: python factors/c_ext/momentum_v2_build.py')


def params(param):
    values = (param, 4368, 167, 168) if isinstance(param, Integral) else tuple(param)
    if len(values) != 4 or any(not isinstance(x, Integral) for x in values):
        raise ValueError('Use integer n or (n, location_window, warmup_bars, cooldown_hours)')
    n, location, warmup, cooldown = map(int, values)
    if n < warmup or warmup < 2 or location < 1 or cooldown < 0:
        raise ValueError('Require n >= warmup >= 2, location_window >= 1, cooldown >= 0')
    if max(n, location, warmup, cooldown) > 2147483647 or cooldown > 2562047:
        raise ValueError('Parameter exceeds native integer/timedelta range')
    return n, location, warmup, cooldown


def inputs(df):
    arrays = [np.ascontiguousarray(df[col].to_numpy(dtype=float)) for col in ['high', 'low', 'close']]
    times = np.empty(0, dtype=np.int64)
    if 'candle_begin_time' in df:
        t = df['candle_begin_time']
        if not isinstance(t.dtype, (np.dtypes.DateTime64DType, pd.DatetimeTZDtype)):
            t = pd.to_datetime(t)
        times = np.ascontiguousarray(t.array.as_unit('ns').asi8)
        if np.any(times == np.iinfo(np.int64).min) or np.any(times[1:] <= times[:-1]):
            raise ValueError('Momentum requires unique increasing timestamps')
    if len(df) > 2147483647:
        raise ValueError('Input exceeds native index range')
    return arrays, times, int('candle_begin_time' in df)


def calculate(df, param, prepared, diagnostic=True):
    n, location, warmup, cooldown = params(param)
    arrays, times, timed = prepared
    ffi = native.ffi
    signal = np.empty(len(df), dtype=float)
    if diagnostic:
        result = {k: np.empty(len(df), dtype=float) for k in ['trend', 'location']}
        flags = {k: np.empty(len(df), dtype=np.uint8) for k in ['anomaly', 'invalid', 'blocked']}
        counts = np.empty(len(df), dtype=np.int64)
        outputs = [*(ffi.from_buffer('double[]', result[k]) for k in ['trend', 'location']),
                   ffi.from_buffer('double[]', signal),
                   *(ffi.from_buffer('unsigned char[]', flags[k]) for k in ['anomaly', 'invalid', 'blocked']),
                   ffi.from_buffer('long long[]', counts)]
    else:
        outputs = [ffi.NULL, ffi.NULL, ffi.from_buffer('double[]', signal),
                   ffi.NULL, ffi.NULL, ffi.NULL, ffi.NULL]
    code = native.lib.compute_momentum_v2_fast(
        *(ffi.from_buffer('double[]', a) for a in arrays), ffi.from_buffer('long long[]', times),
        timed, len(df), n, location, warmup, cooldown,
        *outputs)
    if code:
        raise MemoryError('momentum V2 native allocation failed')
    if not diagnostic:
        return pd.Series(signal, index=df.index, name='signal', copy=False)
    return pd.DataFrame({'trend': result['trend'], 'location': result['location'],
                         **{k: v.astype(bool) for k, v in flags.items()},
                         'normal_count': counts, 'signal': signal}, index=df.index)
