"""alpha311 下运行；新扩展独立于历史 _momentum_core。"""
from pathlib import Path
import os
import cffi

HERE = Path(__file__).resolve().parent
ffi = cffi.FFI()
ffi.cdef('''
int compute_momentum_v2_fast(const double *, const double *, const double *,
    const long long *, int, int, int, int, int, int,
    double *, double *, double *, unsigned char *, unsigned char *,
    unsigned char *, long long *);
''')
ffi.set_source('_momentum_v2_core', (HERE / 'momentum_v2_core.c').read_text(encoding='utf-8'),
               extra_compile_args=['/O2'] if os.name == 'nt' else ['-O3'],
               libraries=[] if os.name == 'nt' else ['m'])
if __name__ == '__main__':
    ffi.compile(tmpdir=str(HERE), verbose=True)
