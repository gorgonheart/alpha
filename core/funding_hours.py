"""Thin forwarding shim to core.strategy.funding_hours."""
import sys
import core.strategy.funding_hours as _impl

sys.modules[__name__] = _impl
