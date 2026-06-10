"""
numpy bridge to C extension.
Loads _momentum_core*.pyd from the same directory.
If not found, raise ImportError with build instructions.
"""
import os
import sys
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _momentum_core as _mod
except ImportError as e:
    raise ImportError(
        f"C extension not found: {e}\n"
        f"Run:  conda run -n alpha311 python {os.path.join(_HERE, 'build.py')}"
    ) from e

_lib = _mod.lib
_ffi = _mod.ffi


def _ptr(arr: np.ndarray):
    return _ffi.cast("double *", arr.ctypes.data)


def compute_momentum_c(
    close: np.ndarray,
    high: np.ndarray,
    low: np.ndarray,
    n: int,
    trunc_len: int = 52560,
) -> np.ndarray:
    """
    Compute momentum factor (C implementation).

    Parameters
    ----------
    close, high, low : 1-D float64 ndarray, same length
    n                : span parameter
    trunc_len        : use last trunc_len bars (default 52560)

    Returns
    -------
    out : 1-D float64 ndarray, same length as input, NaN-padded at front
    """
    close = np.ascontiguousarray(close, dtype=np.float64)
    high  = np.ascontiguousarray(high,  dtype=np.float64)
    low   = np.ascontiguousarray(low,   dtype=np.float64)
    length = len(close)
    out = np.empty(length, dtype=np.float64)
    _lib.compute_momentum(
        _ptr(close), _ptr(high), _ptr(low),
        int(n), int(length), int(trunc_len),
        _ptr(out),
    )
    return out
