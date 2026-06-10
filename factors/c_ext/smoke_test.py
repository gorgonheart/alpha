import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
import _momentum_core as m
import numpy as np

n = 200
rng = np.random.default_rng(42)
close = rng.random(300).astype(np.float64) + 1.0
high  = close + 0.01
low   = close - 0.01
out   = np.empty(300, dtype=np.float64)

m.lib.compute_momentum(
    m.ffi.cast('double*', close.ctypes.data),
    m.ffi.cast('double*', high.ctypes.data),
    m.ffi.cast('double*', low.ctypes.data),
    n, 300, 300,
    m.ffi.cast('double*', out.ctypes.data),
)
print('smoke ok, non-nan count:', np.sum(~np.isnan(out)))
assert np.sum(~np.isnan(out)) > 0, "All NaN - something is wrong"
