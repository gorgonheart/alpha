"""Thin forwarding shim to core.ingest.data_api_update."""
import sys
import core.ingest.data_api_update as _impl

sys.modules[__name__] = _impl
