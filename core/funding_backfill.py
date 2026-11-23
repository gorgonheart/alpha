"""Thin forwarding shim to core.strategy.funding_backfill."""
import sys
import core.strategy.funding_backfill as _impl

sys.modules[__name__] = _impl
