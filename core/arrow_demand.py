"""Thin forwarding shim to core.strategy.arrow_demand."""
import importlib.util
from pathlib import Path
import sys

try:
    import core.strategy.arrow_demand as _impl
except ModuleNotFoundError:
    _target = Path(__file__).resolve().parent / 'strategy' / 'arrow_demand.py'
    _spec = importlib.util.spec_from_file_location('_alpha_arrow_demand_impl', _target)
    _impl = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_impl)

globals().update({k: v for k, v in _impl.__dict__.items() if not k.startswith('__')})
if __name__ in sys.modules:
    sys.modules[__name__] = _impl
