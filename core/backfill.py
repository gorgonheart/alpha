"""Thin forwarding shim to core.pipeline.backfill."""
import sys
import core.pipeline.backfill as _impl

sys.modules[__name__] = _impl
