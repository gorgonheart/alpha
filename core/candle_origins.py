"""Thin forwarding shim to core.strategy.candle_origins."""
import sys
import core.strategy.candle_origins as _impl

sys.modules[__name__] = _impl
