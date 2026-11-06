from collections import defaultdict
from core.utils.log_kit import logger
import pandas as pd

from core.handle import BmacHandle

SPOT_SPLIT_MAP = {}

USDT_FUTURES_SPLIT_MAP = {
    'LUNAUSDT': ['LUNAUSDT', 'LUNA2USDT'],
    'DODOUSDT': ['DODOUSDT', 'DODOXUSDT'],
    'RAYUSDT': ['RAYUSDT', 'RAYSOLUSDT']
}


def get_normalized_symbol(s: str, split_map):
    for norm_sym, splits in split_map.items():
        if s in splits:
            s = norm_sym
            break
    if s.startswith('1000'):
        return s[4:]
    return s


def get_spot_swap_matches(spot_symbol_list, swap_symbol_list):
    spot_symbols = sorted(set(spot_symbol_list))
    swap_symbols = sorted(set(swap_symbol_list))

    exact_symbols = set(spot_symbols) & set(swap_symbols)
    records = [{'spot': symbol, 'swap': symbol} for symbol in sorted(exact_symbols)]

    spot_groups = defaultdict(list)
    for symbol in spot_symbols:
        if symbol not in exact_symbols:
            spot_groups[get_normalized_symbol(symbol, SPOT_SPLIT_MAP)].append(symbol)

    swap_groups = defaultdict(list)
    for symbol in swap_symbols:
        if symbol not in exact_symbols:
            swap_groups[get_normalized_symbol(symbol, USDT_FUTURES_SPLIT_MAP)].append(symbol)

    ambiguous_groups = []
    for norm_symbol in sorted(set(spot_groups) | set(swap_groups)):
        spots = spot_groups[norm_symbol]
        swaps = swap_groups[norm_symbol]
        if len(spots) == 1 and len(swaps) == 1:
            records.append({'spot': spots[0], 'swap': swaps[0]})
            continue

        records.extend({'spot': symbol, 'swap': None} for symbol in spots)
        records.extend({'spot': None, 'swap': symbol} for symbol in swaps)
        if spots and swaps:
            ambiguous_groups.append((norm_symbol, spots, swaps))

    if ambiguous_groups:
        detail = '; '.join(
            f"{norm_symbol}(spot={spots}, swap={swaps})"
            for norm_symbol, spots, swaps in ambiguous_groups
        )
        logger.warning(
            f'Binance symbol 匹配存在非唯一归一化分组，已保留为未匹配项: {detail}'
        )

    df_map = pd.DataFrame.from_records(records, columns=['spot', 'swap'])
    return df_map.sort_values(['spot', 'swap'], ignore_index=True)


def update_symbol_match(handle: BmacHandle, run_time):
    df_exginfo_spot = handle.exg_mgr.read_candle('exginfo_spot')
    spot_symbol_list = df_exginfo_spot['symbol'].to_list()

    df_exginfo_swap = handle.exg_mgr.read_candle('exginfo_swap')
    swap_symbol_list = df_exginfo_swap['symbol'].to_list()

    df_map = get_spot_swap_matches(spot_symbol_list, swap_symbol_list)

    handle.exg_mgr.set_candle('spot_swap_matches', run_time, df_map)
