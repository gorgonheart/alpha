"""Thin forwarding shim to core.ingest.coin_cap."""
import sys
import core.ingest.coin_cap as _impl

sys.modules[__name__] = _impl
