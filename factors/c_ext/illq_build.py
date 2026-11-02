# factors/c_ext/illq_build.py
"""
Run:
    cd d:\\alpha
    conda run -n alpha311 python factors/c_ext/illq_build.py

Output: factors/c_ext/_illq_core.cp311-win_amd64.pyd (or platform suffix)
"""
import os
import cffi

_HERE = os.path.dirname(os.path.abspath(__file__))

ffi = cffi.FFI()
ffi.cdef("""
    void compute_illq(
        const double *open,
        const double *high,
        const double *low,
        const double *close,
        const double *qvol,
        int n,
        int len,
        double *out
    );
""")

_src_path = os.path.join(_HERE, "illq_core.c")
with open(_src_path, "r", encoding="utf-8") as f:
    _src = f.read()

ffi.set_source(
    "_illq_core",
    _src,
    extra_compile_args=["/O2", "/utf-8"] if os.name == "nt" else ["-O3", "-march=native"],
    libraries=["m"] if os.name != "nt" else [],
)

if __name__ == "__main__":
    import sys
    if os.name == "nt" and not sys.flags.utf8_mode:
        # CFFI reads/writes its generated C file with the process default encoding.
        # Re-enter in UTF-8 mode so an existing UTF-8 file is never read as GBK.
        import subprocess
        raise SystemExit(subprocess.call([sys.executable, "-X", "utf8",
                                          os.path.abspath(__file__), *sys.argv[1:]]))
    ffi.compile(tmpdir=_HERE, verbose=True)
    print("Build complete. Output in:", _HERE)
