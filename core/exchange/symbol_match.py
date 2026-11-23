from collections import defaultdict

import pandas as pd

from core.exchange.adapter import get_adapter
from core.handle import BmacHandle
from core.utils.log_kit import logger

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


def get_okx_spot_swap_matches(spot_symbol_list, swap_symbol_list):
    """OKX 现货 <-> 永续配对：剥离 swap 的 '-SWAP' 后缀得到归一化键。

    例：现货 'BTC-USDT' 与永续 'BTC-USDT-SWAP' 共享归一化键 'BTC-USDT'。
    """
    spot_norms = [(sym, sym) for sym in spot_symbol_list]
    df_spot_norm = pd.DataFrame(spot_norms, columns=['spot', 'norm'])

    swap_norms = [(sym, sym[:-len('-SWAP')] if sym.endswith('-SWAP') else sym) for sym in swap_symbol_list]
    df_swap_norm = pd.DataFrame(swap_norms, columns=['swap', 'norm'])

    df_map = pd.merge(df_spot_norm, df_swap_norm, how='outer', on='norm')
    df_map.drop(columns='norm', inplace=True)
    df_map = df_map.sort_values(['spot', 'swap'], ignore_index=True)
    return df_map


def update_symbol_match(handle: BmacHandle, run_time):
    df_exginfo_spot = handle.exg_mgr.read_candle('exginfo_spot')
    spot_symbol_list = df_exginfo_spot['symbol'].to_list()

    df_exginfo_swap = handle.exg_mgr.read_candle('exginfo_swap')
    swap_symbol_list = df_exginfo_swap['symbol'].to_list()

    df_map = get_adapter(handle.exchange).match_spot_swap(spot_symbol_list, swap_symbol_list)

    handle.exg_mgr.set_candle('spot_swap_matches', run_time, df_map)
