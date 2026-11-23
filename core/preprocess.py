"""Thin forwarding shim to core.strategy.preprocess."""
import sys
import core.strategy.preprocess as _impl

sys.modules[__name__] = _impl
