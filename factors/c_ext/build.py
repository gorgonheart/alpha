"""
运行方式：
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/build.py

产出：factors/c_ext/_momentum_core.cp311-win_amd64.pyd（或对应平台后缀）
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_momentum(
        const double *close,
        const double *high,
        const double *low,
        int n,
        int len,
        int trunc_len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "momentum_core.c")
with open(_src_path, "r") as f:
    _src = f.read()

ffi.set_source(
    "_momentum_core",
    _src,
    extra_compile_args=["/O2"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
