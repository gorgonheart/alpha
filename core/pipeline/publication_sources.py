"""Select a stable per-symbol source before dispatching one publication.

Only small bounds/coverage dictionaries cross the process boundary. An empty
successful response for a symbol with no stored history is a new listing, not
an excuse to hide a failed fetch for a mature symbol.
"""
from typing import Optional

from core.utils.time import convert_interval_to_timedelta, get_offset_from_runtime


def choose_publication_inputs(
    handle,
    run_time,
    binance_result,
    data_api_result,
    *,
    binance_done,
    data_api_done,
    confirmed_empty: Optional[dict[str, set[str]]] = None,
):
    if not binance_done:
        return None
    expected = run_time - convert_interval_to_timedelta(handle.resample_interval)
    offset = get_offset_from_runtime(run_time, handle.resample_interval)
    selected, bounds, missing = {}, {}, []
    get_market = getattr(handle, 'get_market_handle', None) or handle.get_binance_handle
    for i, kind in enumerate(('spot', 'swap')):
        selected[kind], bounds[kind] = {}, {}
        symbols = handle.exg_mgr.read_candle(f'exginfo_{kind}')['symbol'].tolist()
        frames = binance_result[i] if binance_result is not None else {}
        bn = binance_result[i + 2] if binance_result is not None else {}
        api = data_api_result[i] if data_api_result is not None else {}
        mgr = get_market(kind).resample_mgr_map[offset]
        kind_confirmed_empty = confirmed_empty.get(kind, ()) if confirmed_empty else ()
        for symbol in symbols:
            source = None
            for name, available in (('binance', bn), ('data_api', api)):
                pair = available.get(symbol)
                if pair is not None and pair[0] <= expected <= pair[1]:
                    source = name
                    bounds[kind][symbol] = pair
                    break
            if source:
                selected[kind][symbol] = source
            elif (
                ((symbol in frames and frames[symbol].empty) or (symbol in kind_confirmed_empty))
                and (not mgr.has_symbol(symbol) or mgr.read_candle(symbol).empty)
            ):
                # update_resample_candle persists an empty file for a new listing,
                # or WS REST fallback confirmed an empty DataFrame for an unopened listing.
                # Read only this exceptional empty-response case, never all symbols.
                continue
            else:
                missing.append(f'{kind}:{symbol}')
        if not selected[kind]:
            missing.append(f'{kind}:no-current-candles')
    if missing:
        if not data_api_done:
            return None
        raise ValueError(f'Incomplete current market coverage ({len(missing)}): {missing[:10]}')
    return selected, bounds['spot'], bounds['swap']
