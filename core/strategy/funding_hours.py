"""Normalize and atomically persist one symbol's funding history."""
import os
import shutil
from pathlib import Path
from tempfile import NamedTemporaryFile

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


def save_funding_hours(manager, symbol, run_time_or_incoming, incoming=None, depth=None, *, min_ts=None):
    """Keep one canonical value per UTC hour and publish the funding PKL atomically.

    Supports both signatures:
    - save_funding_hours(manager, symbol, run_time, incoming, depth=None, *, min_ts=None)
    - save_funding_hours(manager, symbol, incoming, *, depth=None, min_ts=None)
    """
    if isinstance(run_time_or_incoming, pd.DataFrame) and incoming is None:
        run_time = None
        incoming_df = run_time_or_incoming
    else:
        run_time = run_time_or_incoming
        incoming_df = incoming

    stored = manager.read_candle(symbol) if manager.has_symbol(symbol) else None
    if stored is not None:
        previous = normalize_funding_hours(stored)
        if not previous.empty:
            incoming_df = pd.concat([previous, incoming_df], ignore_index=True)

    merged = incoming_df.dropna(subset=['candle_begin_time']).drop_duplicates('candle_begin_time', keep='last')
    merged = merged.sort_values('candle_begin_time')
    if min_ts is not None:
        merged = merged[merged['candle_begin_time'] >= min_ts]
    elif depth is not None:
        merged = merged.tail(depth)
    merged = merged.reset_index(drop=True)

    if stored is not None and stored.equals(merged) and (run_time is None or manager.check_ready(symbol, run_time)):
        return

    destination = Path(manager.format_data_file_path(symbol))
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = None
    try:
        with NamedTemporaryFile(dir=destination.parent, suffix='.tmp', delete=False) as output:
            temp_path = Path(output.name)
            merged.to_pickle(output)
        if stored is not None:
            shutil.copymode(destination, temp_path)
        os.replace(temp_path, destination)
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)

    if run_time is not None:
        manager.save_ready(symbol, run_time)
