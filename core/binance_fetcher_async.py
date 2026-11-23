"""Thin forwarding shim to core.exchange.rest.binance_fetcher."""
import sys
import core.exchange.rest.binance_fetcher as _impl

sys.modules[__name__] = _impl
