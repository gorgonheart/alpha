"""Shared CSV numeric contract; missing values keep pandas NA semantics."""
COIN_CAP_DTYPES = {name: 'float64' for name in (
    'usd_price', 'max_supply', 'circulating_supply', 'total_supply',
    'max_mcap', 'circulating_mcap', 'total_mcap',
)}
