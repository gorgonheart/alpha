"""Thin forwarding shim to core.strategy.funding_publication."""
import sys
import core.strategy.funding_publication as _impl

sys.modules[__name__] = _impl
