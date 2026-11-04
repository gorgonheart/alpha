"""Run offline deployment checks in a disposable root/test directory.

Only the verification command uses this tool. Runtime imports and configuration
never refer to test/. Source comes from the current committed tree, including
tests omitted by a production sparse checkout. No machine secrets are copied.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


def prepare_run(root: Path) -> Path:
    root = root.resolve()
    directory = root / 'test'
    if directory.is_symlink():
        raise ValueError('test must be a real directory inside this installation')
    directory.mkdir(exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='verify-', dir=directory))
    source = run / 'source'
    revision = subprocess.check_output(['git', '-C', str(root), 'rev-parse', 'HEAD'], text=True).strip()
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout', str(root), str(source)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(source), 'checkout', '--quiet', '--detach', revision],
                   check=True, capture_output=True)
    extensions = source / 'factors/c_ext'
    extensions.mkdir(parents=True, exist_ok=True)
    for pattern in ('*.so', '*.pyd'):
        for binary in (root / 'factors/c_ext').glob(pattern):
            shutil.copy2(binary, extensions / binary.name)
    # Tests use isolated data and empty network credentials, never config.json
    # from the running installation.
    settings = dict(storage_path=str(run / 'data'), proxy=None, data_api_uuid='',
                    data_api_key='', error_webhook_url='', use_api=dict(kline=False, coin_cap=False))
    config = source / 'config.json'
    config.write_text(json.dumps(settings, indent=2) + '\n', encoding='utf-8')
    config.chmod(0o600)
    return run


def run_tests(root: Path, tests: list[str]) -> int:
    run = prepare_run(root)
    source = run / 'source'
    scratch = run / 'tmp'
    scratch.mkdir()
    env = dict(os.environ, PYTHONPATH=str(source), PYTHONDONTWRITEBYTECODE='1',
               PYTHONPYCACHEPREFIX=str(run / 'cache/pycache'), NUMBA_CACHE_DIR=str(run / 'cache/numba'),
               TEMP=str(scratch), TMP=str(scratch), TMPDIR=str(scratch))
    log = run / 'verify.log'
    command = [sys.executable, '-X', 'utf8', '-B', '-m', 'pytest', '-q', '-p', 'no:cacheprovider',
               '--basetemp=' + str(run / 'pytest'), *tests]
    print(f'Offline verification directory: {run}', flush=True)
    with log.open('wb') as output:
        result = subprocess.run(command, cwd=source, env=env, stdout=output, stderr=subprocess.STDOUT)
    print(log.read_text(encoding='utf-8', errors='replace'), end='')
    print(f'Log retained for manual cleanup: {log}', flush=True)
    return result.returncode


if __name__ == '__main__':
    if not sys.argv[1:]:
        raise SystemExit('Pass offline pytest file paths, or use tools/deploy_alpha.sh verify')
    raise SystemExit(run_tests(Path(__file__).resolve().parents[1], sys.argv[1:]))
