"""Production verification must be fully disposable under root/test."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

@pytest.fixture()
def runner():
    import importlib.util
    import importlib
    assert importlib.util.find_spec('tools.run_deployment_tests') is not None, 'Disposable deployment test runner is missing'
    return importlib.import_module('tools.run_deployment_tests')


@pytest.fixture()
def repository(tmp_path):
    root=tmp_path/'installation'
    root.mkdir()
    (root/'runtime.py').write_text('VALUE = 7\n')
    (root/'.gitignore').write_text('test/\nconfig.json\n*.so\n')
    tests=root/'tests'
    (tests/'fixtures').mkdir(parents=True)
    (tests/'__init__.py').write_text('')
    (tests/'fixtures'/'input.txt').write_text('fixture')
    (tests/'test_ok.py').write_text('import runtime\ndef test_runtime():\n    assert runtime.VALUE == 7\n')
    def git(*args):
        return subprocess.run(['git','-C',str(root),*args],check=True,capture_output=True)
    git('init','--quiet')
    git('config','user.name','Deployment test')
    git('config','user.email','test@localhost')
    git('add','--all')
    git('commit','--quiet','-m','source fixture')
    (root/'config.json').write_text(json.dumps({'data_api_key':'PRIVATE_SENTINEL','storage_path':'/production/data'}))
    extensions=root/'factors/c_ext'
    extensions.mkdir(parents=True)
    (extensions/'example.so').write_bytes(b'compiled binary')
    return root,git


def test_prepare_keeps_every_test_artifact_inside_test_and_no_production_credentials(repository, runner):
    root,_=repository
    run=runner.prepare_run(root)
    assert run.parent == root/'test'
    source=run/'source'
    assert (source/'tests/fixtures/input.txt').read_text()=='fixture'
    assert (source/'runtime.py').read_bytes()==(root/'runtime.py').read_bytes()
    settings=json.loads((source/'config.json').read_text())
    assert settings['data_api_key']==''
    assert Path(settings['storage_path']).is_relative_to(run)
    assert (source/'factors/c_ext/example.so').read_bytes()==b'compiled binary'
    assert (root/'config.json').read_text().find('PRIVATE_SENTINEL')>=0


def test_sparse_production_checkout_can_recreate_missing_test_sources(repository, runner):
    root,git=repository
    git('sparse-checkout','set','--no-cone','/*','!/tests/')
    assert not (root/'tests').exists()
    run=runner.prepare_run(root)
    assert (run/'source/tests/fixtures/input.txt').read_text()=='fixture'
    assert not (root/'tests').exists()


def test_deleting_test_does_not_change_runtime_and_tests_can_be_regenerated(repository, runner):
    root,git=repository
    run=runner.prepare_run(root)
    head=git('rev-parse','HEAD').stdout
    disposable=root/'test'
    assert disposable.resolve().parent==root.resolve()
    shutil.rmtree(disposable)
    result=subprocess.run([sys.executable,'-B','-c','import runtime; print(runtime.VALUE)'],cwd=root,check=True,capture_output=True,text=True)
    assert result.stdout.strip()=='7'
    assert git('rev-parse','HEAD').stdout==head
    assert (runner.prepare_run(root)/'source/tests/test_ok.py').exists()


def test_runner_stores_report_and_scratch_under_test(repository, runner):
    root,_=repository
    assert runner.run_tests(root,['tests/test_ok.py'])==0
    runs=list((root/'test').glob('verify-*'))
    assert len(runs)==1
    assert (runs[0]/'verify.log').is_file()
    assert '1 passed' in (runs[0]/'verify.log').read_text()
    assert not (root/'.pytest_cache').exists()
    assert not (root/'__pycache__').exists()
