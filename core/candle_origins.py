"""Preserve published strategy age across a rebuild of the rolling candle window."""
import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa


def _path(folder, market):
    folder = Path(folder)
    return folder.parent.with_name(folder.parent.name + '_origins') / folder.name / f'{market}.pkl'


def load_origins(folder, market):
    path = _path(folder, market)
    return pd.read_pickle(path)['origins'] if path.exists() else {}


def _bounds(first, last):
    first, last = pd.Timestamp(first), pd.Timestamp(last)
    if pd.isna(first) or pd.isna(last) or first > last:
        raise ValueError(f'Invalid published candle bounds: {first}, {last}')
    return first, last


def _arrow_origins(path):
    # Arrow may be tailed. The strategy origin is a column value, not the first row time.
    with pa.memory_map(str(path), 'r') as source:
        reader = pa.ipc.open_file(source)
        symbols = json.loads(reader.schema.metadata[b'symbols'])
        if len(symbols) != reader.num_record_batches or len(set(symbols)) != len(symbols):
            raise ValueError(f'Invalid symbol list in {path}')
        origins = {}
        for i, symbol in enumerate(symbols):
            batch = reader.get_batch(i)
            if batch.num_rows:
                origins[symbol] = _bounds(batch.column('first_candle_time')[0].as_py(),
                                          batch.column('candle_begin_time')[-1].as_py())
        return origins


def _batch_origins(mgr, market, gate):
    # Legacy pickle files have no epoch in the filename. Never adopt an in-flight write.
    prefix = f'{market}_dict_batch'
    names = [s for s in mgr.get_all_symbols() if s.startswith(prefix) and s[len(prefix):].isdigit()]
    if prefix + '1' not in names:
        raise ValueError(f'No published batches for {gate}')
    # The writer always starts at batch1 and writes in order. After a market
    # shrinks, surplus files from the preceding cycle can remain in the folder.
    start_ns = Path(mgr.format_data_file_path(prefix + '1')).stat().st_mtime_ns
    ready_ns = gate.stat().st_mtime_ns
    origins = {}
    for name in names:
        written = Path(mgr.format_data_file_path(name)).stat().st_mtime_ns
        if written < start_ns:
            continue
        if written > ready_ns:
            raise ValueError(f'Batch {name} is newer than {gate}; leaving history untouched')
        for symbol, frame in mgr.read_candle(name).items():
            if not frame.empty:
                bounds = _bounds(frame.first_candle_time.iloc[0], frame.candle_begin_time.iloc[-1])
                if symbol in origins and origins[symbol] != bounds:
                    raise ValueError(f'Conflicting published batches for {market}/{symbol} in {gate.parent}')
                origins[symbol] = bounds
    return origins


def _save(path, epoch, origins):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    try:
        with tmp.open('wb') as stream:
            pd.to_pickle({'epoch': epoch, 'origins': origins}, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def remember_ready_origins(folder, market, run_time, market_dict):
    origins = {s: _bounds(f.first_candle_time.iloc[0], f.candle_begin_time.iloc[-1])
               for s, f in market_dict.items() if not f.empty}
    _save(_path(folder, market), int(run_time.timestamp()), origins)


def preserve_published_origins(mgr):
    for market in ('spot', 'swap'):
        prefix = f'{market}_dict_'
        gates = [(int(p.stem[len(prefix):]), p) for p in mgr.base_dir.glob(prefix + '*.ready')
                 if p.stem[len(prefix):].isdigit()]
        path = _path(mgr.base_dir, market)
        previous = pd.read_pickle(path) if path.is_file() else None
        if not gates:
            if previous is None and any(mgr.base_dir.glob(f'market_pivot_{market}_[0-9]*.ready')):
                raise ValueError(f'No completed {market} dict gate or saved origins at {mgr.base_dir}; '
                                 'leaving history untouched')
            continue  # first installation, or retry before the new publication
        epoch, gate = max(gates)
        if previous is not None and previous['epoch'] == epoch:
            continue
        if not (mgr.base_dir / f'market_pivot_{market}_{epoch}.ready').is_file():
            raise ValueError(f'Incomplete publication for {gate}; leaving history untouched')
        arrow = gate.with_suffix('.arrow')
        origins = _arrow_origins(arrow) if arrow.is_file() else _batch_origins(mgr, market, gate)
        _save(path, epoch, origins)
