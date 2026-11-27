"""离线基准：合成固定输入，不读取生产数据、不启动数据中心。

python tests/benchmark_momentum_v2.py --repeats 31 [--legacy]
结果打印 JSON，允许在 Windows/Ubuntu 上各自比较优化前后。
"""
import argparse
import importlib
import json
from pathlib import Path
import statistics
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
NAME = '动量210401_双项异常隔离V2'
PARAMS = [13608, 5421, 3509, 12600]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repeats', type=int, default=31)
    parser.add_argument('--legacy', action='store_true')
    args = parser.parse_args()
    module = importlib.import_module('factors.' + NAME)
    bridge = importlib.import_module('factors.c_ext.momentum_v2_wrapper')
    report = {}
    for size in [3000, 7200, 14060]:
        x = np.arange(size, dtype=float)
        close = 100 + x*.001 + np.sin(x/19)
        df = pd.DataFrame(dict(high=close+1, low=close-1, close=close,
                               candle_begin_time=pd.date_range('2025-01-01', periods=size, freq='h')))
        prepared = bridge.inputs(df)
        ffi = bridge.native.ffi
        arrays, times, timed = prepared
        # Preallocate diagnostic buffers to measure native compute without pandas/allocation overhead.
        doubles = [np.empty(size) for _ in range(3)]
        flags = [np.empty(size, dtype=np.uint8) for _ in range(3)]
        counts = np.empty(size, dtype=np.int64)
        cargs = [*(ffi.from_buffer('double[]', a) for a in arrays),
                 ffi.from_buffer('long long[]', times), timed, size]
        outputs = [*(ffi.from_buffer('double[]', a) for a in doubles),
                   *(ffi.from_buffer('unsigned char[]', a) for a in flags),
                   ffi.from_buffer('long long[]', counts)]
        kernel = getattr(bridge.native.lib, 'compute_momentum_v2_fast', None)
        if kernel is None:
            kernel = bridge.native.lib.compute_momentum_v2
        def native_call():
            for n in PARAMS:
                assert kernel(*cargs, n, 4368, 167, 168, *outputs) == 0
        calls = {'end_to_end': lambda: module.signal_multi_params(df, PARAMS),
                 'native_4params': native_call, 'inputs': lambda: bridge.inputs(df)}
        if args.legacy:
            old = importlib.import_module('factors.动量因子210401修正')
            calls['legacy_end_to_end'] = lambda: old.signal_multi_params(df, PARAMS)
        timings = {name: [] for name in calls}
        for fn in calls.values():
            fn()
        for iteration in range(args.repeats):
            keys = list(calls) if iteration % 2 == 0 else list(reversed(calls))
            for name in keys:
                start = time.perf_counter()
                calls[name]()
                timings[name].append((time.perf_counter()-start)*1000)
        report[size] = {name: round(statistics.median(values), 4) for name, values in timings.items()}
    print(json.dumps({'unit': 'ms', 'median_repeats': args.repeats, 'parameters': PARAMS,
                      'results': report}, indent=2))


if __name__ == '__main__':
    main()
