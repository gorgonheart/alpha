"""Thin forwarding shim to core.pipeline.data_update."""
import sys
import core.pipeline.data_update as _impl

sys.modules[__name__] = _impl
