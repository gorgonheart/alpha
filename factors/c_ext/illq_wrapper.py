# factors/c_ext/illq_wrapper.py
"""numpy bridge to _illq_core C extension."""
import os
import sys
import numpy as np

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    import _illq_core as _mod
except ImportError as e:
    raise ImportError(
        f"C extension not found: {e}\n"
        f"Run:  conda run -n alpha311 python {os.path.join(_HERE, 'illq_build.py')}"
    ) from e

_lib = _mod.lib
_ffi = _mod.ffi


def _ptr(arr: np.ndarray):
    return _ffi.cast("double *", arr.ctypes.data)


def compute_illq_c(
    open_: np.ndarray,
    high:  np.ndarray,
    low:   np.ndarray,
    close: np.ndarray,
    qvol:  np.ndarray,
    n: int,
) -> np.ndarray:
    """
    Compute ILLQStdBbi45208 factor (C implementation).

    Parameters
    ----------
    open_, high, low, close, qvol : 1-D float64 ndarray, same length
    n : rolling window (param)

    Returns
    -------
    out : 1-D float64 ndarray, same length as input
    """
    open_ = np.ascontiguousarray(open_, dtype=np.float64)
    high  = np.ascontiguousarray(high,  dtype=np.float64)
    low   = np.ascontiguousarray(low,   dtype=np.float64)
    close = np.ascontiguousarray(close, dtype=np.float64)
    qvol  = np.ascontiguousarray(qvol,  dtype=np.float64)
    length = len(open_)
    out = np.empty(length, dtype=np.float64)
    _lib.compute_illq(
        _ptr(open_), _ptr(high), _ptr(low), _ptr(close), _ptr(qvol),
        int(n), int(length),
        _ptr(out),
    )
    return out
