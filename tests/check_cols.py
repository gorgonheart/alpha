import pandas as pd
df = pd.read_pickle('tests/fixtures/module6/input_btcusdt_candles.pkl')
print('rows:', len(df))
print('cols:', df.columns.tolist())
