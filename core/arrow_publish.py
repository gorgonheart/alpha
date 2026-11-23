"""Thin forwarding shim to core.strategy.arrow_publish."""
import sys
import core.strategy.arrow_publish as _impl

sys.modules[__name__] = _impl
