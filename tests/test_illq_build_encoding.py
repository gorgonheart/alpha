"""The build CLI must handle CFFI's default-encoding read on Chinese Windows."""
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.skipif(os.name != 'nt', reason='Windows locale regression')
def test_plain_build_reexecutes_with_utf8_before_cffi_reads_generated_source(tmp_path):
    source = Path(__file__).resolve().parents[1]/'factors/c_ext'
    for name in ('illq_build.py', 'illq_core.c'):
        (tmp_path/name).write_bytes((source/name).read_bytes())
    # An existing UTF-8 generated file, as produced by the earlier isolated build.
    (tmp_path/'_illq_core.c').write_text('// — UTF-8 generated source\n', encoding='utf-8')
    (tmp_path/'cffi.py').write_text('''from pathlib import Path
import sys
class FFI:
    def cdef(self, *a, **k): pass
    def set_source(self, *a, **k): pass
    def compile(self, tmpdir, **kwargs):
        with open(Path(tmpdir)/'_illq_core.c') as f:
            assert f.read() == '// \\u2014 UTF-8 generated source\\n'
        assert sys.flags.utf8_mode == 1
        print('UTF8_BUILD_OK')
''', encoding='utf-8')
    result = subprocess.run([sys.executable, '-X', 'utf8=0', str(tmp_path/'illq_build.py')],
                            cwd=tmp_path, capture_output=True)
    assert result.returncode == 0, result.stderr.decode('utf-8', errors='replace')
    assert b'UTF8_BUILD_OK' in result.stdout
