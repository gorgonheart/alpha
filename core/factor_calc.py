"""Thin forwarding shim to core.strategy.factor_calc."""
import sys
import core.strategy.factor_calc as _impl

sys.modules[__name__] = _impl
