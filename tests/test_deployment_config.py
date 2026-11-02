import importlib.util
import json
from pathlib import Path

from core.utils import path_kit


def test_ignored_json_overrides_server_paths_proxy_and_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr(path_kit, 'get_file_path', lambda *args, **kwargs: tmp_path/args[0])
    monkeypatch.setattr(path_kit, 'get_folder_path', lambda *args, **kwargs: str(tmp_path/'data'))
    settings = dict(proxy=None, storage_path=str(tmp_path/'isolated-data'), data_api_key='test-key',
                    data_api_uuid='test-id', error_webhook_url='', precompute_factor_workers=2)
    (tmp_path/'config.json').write_text(json.dumps(settings), encoding='utf-8')
    spec = importlib.util.spec_from_file_location('deployment_config', Path(__file__).resolve().parents[1]/'config.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for key, value in settings.items():
        assert getattr(module, key) == value
