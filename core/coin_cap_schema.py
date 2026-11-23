"""Thin forwarding shim to core.ingest.coin_cap_schema."""
import sys
import core.ingest.coin_cap_schema as _impl

sys.modules[__name__] = _impl
