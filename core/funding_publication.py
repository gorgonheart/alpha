"""Reconcile a single offset's retained swap candles before its next publication."""
from collections import defaultdict
from pathlib import Path

import pandas as pd

from .funding_backfill import BACKFILL_HOURS


class FundingPublication:
    def __init__(self, folder, run_time):
        # The pivot gate is renewed only after every year was written. A failed
        # batch/factor/pivot attempt therefore replays the same settlement window.
        prefix = 'market_pivot_swap_'
        epochs = [int(path.stem[len(prefix):]) for path in Path(folder).glob(prefix + '*.ready')
                  if path.stem[len(prefix):].isdecimal()]
        self.since = None
        if epochs:
            previous = pd.Timestamp(min(max(epochs), int(run_time.timestamp())), unit='s', tz='UTC')
            self.since = previous.floor('h') - pd.Timedelta(hours=BACKFILL_HOURS)
        self.by_year = defaultdict(dict)

    def reconcile(self, symbol, frame, funding):
        times = frame['candle_begin_time']
        # Frames enriched by the existing hour join have a UTC key_0 column
        # and floating-point fees. Older/no-source rows need that same Arrow
        # schema, even when this symbol is the first batch in the snapshot.
        if not pd.api.types.is_float_dtype(frame['funding_fee']):
            frame['funding_fee'] = frame['funding_fee'].astype('float64')
        if 'key_0' not in frame:
            frame.insert(0, 'key_0', times.dt.floor('h'))
        elif frame['key_0'].hasnans:
            frame['key_0'] = frame['key_0'].fillna(times.dt.floor('h'))
        if frame.empty or funding is None or funding.empty:
            return
        # Resampled candles and incremental batches are sorted by candle time.
        start = 0 if self.since is None else times.searchsorted(self.since)
        times = times.iloc[start:]
        if times.empty:
            return
        first_hour = times.iloc[0].floor('h')
        next_hour = times.iloc[-1].floor('h') + pd.Timedelta(hours=1)
        source_times = funding['candle_begin_time']
        left, right = source_times.searchsorted([first_hour, next_hour])
        source = funding.iloc[left:right]
        if source.empty:
            return
        rates = source.set_index('candle_begin_time')['funding_rate']
        matched = times.dt.floor('h').map(rates).dropna()
        if matched.empty:
            return
        old = frame.loc[matched.index, 'funding_fee']
        changes = matched[matched.ne(old)]
        if not changes.empty:
            frame.loc[changes.index, 'funding_fee'] = changes

        # Pass confirmed cells even when the raw batch was already updated by
        # an interrupted attempt. A missing source hour never clears a pivot.
        cells = pd.Series(matched.to_numpy(), index=pd.DatetimeIndex(times.loc[matched.index]))
        for year, group in cells.groupby(cells.index.year):
            self.by_year[year][symbol] = group

    def apply_to_pivot(self, year, pivot):
        table = pivot['funding_rate']
        changed = False
        for symbol, cells in self.by_year.get(year, {}).items():
            if symbol not in table:
                continue
            cells = cells[table.index.get_indexer(cells.index) >= 0]
            if cells.empty:
                continue
            current = table.loc[cells.index, symbol]
            updates = cells[cells.ne(current)]
            if not updates.empty:
                if not pd.api.types.is_float_dtype(table[symbol]):
                    table[symbol] = table[symbol].astype('float64')
                table.loc[updates.index, symbol] = updates.to_numpy()
                changed = True
        return changed
