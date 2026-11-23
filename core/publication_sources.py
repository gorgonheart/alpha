"""Thin forwarding shim to core.pipeline.publication_sources."""
import sys
import core.pipeline.publication_sources as _impl

sys.modules[__name__] = _impl
