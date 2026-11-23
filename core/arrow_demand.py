"""Thin forwarding shim to core.strategy.arrow_demand."""
import sys
import core.strategy.arrow_demand as _impl

sys.modules[__name__] = _impl
