"""Thin forwarding shim to core.ingest.quantclass."""
import sys
import core.ingest.quantclass as _impl

sys.modules[__name__] = _impl
