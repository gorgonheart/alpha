"""Thin forwarding shim to core.exchange.rest.binance_market."""
import sys
import core.exchange.rest.binance_market as _impl

sys.modules[__name__] = _impl
