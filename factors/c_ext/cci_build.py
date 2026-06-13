# factors/c_ext/cci_build.py
"""
Run:
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/cci_build.py

Output: factors/c_ext/_cci_core.cp311-win_amd64.pyd (or platform suffix)
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_cci(
        const double *open,
        const double *high,
        const double *low,
        const double *close,
        int n,
        int window,
        int len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "cci_core.c")
with open(_src_path, "r") as f:
    _src = f.read()

ffi.set_source(
    "_cci_core",
    _src,
    extra_compile_args=["/O2"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
