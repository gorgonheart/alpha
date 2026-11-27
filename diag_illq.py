import sys
sys.path.insert(0, 'D:/alpha')
import numpy as np
import pandas as pd
import importlib

from tests.support.illq_harness import load_btcusdt_candles, load_golden, signal_multi_params_py
from factors.c_ext.illq_wrapper import compute_illq_c

df = load_btcusdt_candles()
golden = load_golden(10000)

c_arr = compute_illq_c(
    df['open'].values.astype('float64'),
    df['high'].values.astype('float64'),
    df['low'].values.astype('float64'),
    df['close'].values.astype('float64'),
    df['quote_volume'].values.astype('float64'),
    n=10000,
)

py_harness = signal_multi_params_py(df, [10000])['10000']
illq_mod = importlib.import_module('factors.ILLQStdBbi45208')
py_factor = np.asarray(illq_mod.signal_multi_params(df, [10000])['10000'], dtype=np.float64)

print('C vs golden mismatches:', np.sum(c_arr != golden))
py_harness_arr = np.asarray(py_harness, dtype=np.float64)
print('C vs py_harness mismatches (>1e-9):', np.sum(np.abs(c_arr - py_harness_arr) > 1e-9))
print('py_harness vs py_factor mismatches:', np.sum(py_harness_arr != py_factor))
print('golden vs py_factor mismatches:', np.sum(golden != py_factor))
print('C vs py_factor mismatches:', np.sum(c_arr != py_factor))
print()
# Show some bad indices for C vs golden
bad = np.where(c_arr != golden)[0]
if len(bad):
    print(f'First 5 bad idx for C vs golden: {bad[:5]}')
    print(f'  C:      {c_arr[bad[:5]]}')
    print(f'  golden: {golden[bad[:5]]}')
    print(f'  diff:   {c_arr[bad[:5]] - golden[bad[:5]]}')
    print(f'  rel:    {(c_arr[bad[:5]] - golden[bad[:5]]) / golden[bad[:5]]}')

# Check if golden matches py_harness (allowing 1e-9)
print()
bad2 = np.where(np.abs(golden - py_harness_arr) > 1e-9)[0]
print(f'golden vs py_harness mismatches (>1e-9): {len(bad2)}')
if len(bad2):
    print(f'First 5: idx={bad2[:5]}, golden={golden[bad2[:5]]}, py={py_harness_arr[bad2[:5]]}')
    print(f'  diff: {golden[bad2[:5]] - py_harness_arr[bad2[:5]]}')
