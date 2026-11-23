"""薄转发兼容层：主实现已迁移至 core.pipeline.resample。"""

from core.pipeline.resample import (  # noqa: F401
    _read_base_tail,
    _resample_chunk_worker,
    _resample_one_symbol,
    calc_resample,
    fill_gaps,
    filter_gaps,
    resample_history,
    update_resample_candle,
    update_resample_candle_incremental,
    update_resample_from_base,
)

__all__ = [
    'calc_resample',
    'fill_gaps',
    'filter_gaps',
    'resample_history',
    'update_resample_candle',
    'update_resample_candle_incremental',
    'update_resample_from_base',
]
