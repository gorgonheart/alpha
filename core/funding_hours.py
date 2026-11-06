"""Normalize one symbol's stored funding history for reading or merging."""
import numpy as np
import pandas as pd


def normalize_funding_hours(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or not {'candle_begin_time', 'funding_rate'}.issubset(frame.columns):
        return frame.iloc[:0]

    raw_times = frame['candle_begin_time']
    utc = isinstance(raw_times.dtype, pd.DatetimeTZDtype) and str(raw_times.dt.tz) == 'UTC'
    times = raw_times if utc else pd.to_datetime(raw_times, format='mixed', utc=True, errors='coerce')
    hours = times.dt.floor('h')
    rates = pd.to_numeric(frame['funding_rate'], errors='coerce')
    valid = (times.notna() & np.isfinite(rates)).fillna(False)
    if (utc and valid.all() and rates.dtype == frame['funding_rate'].dtype and times.equals(hours)
            and times.is_monotonic_increasing and times.is_unique):
        return frame

    # The old premiumIndex writer could keep a forecast and an exact-hour
    # settlement together. Prefer the last valid exact-hour row, falling
    # back to the last valid off-hour indication in original storage order.
    normalized = frame.loc[valid].assign(candle_begin_time=hours[valid], funding_rate=rates[valid])
    exact = times[valid].eq(hours[valid])
    normalized = pd.concat([normalized.loc[~exact], normalized.loc[exact]])
    return normalized.drop_duplicates('candle_begin_time', keep='last').sort_values('candle_begin_time')
