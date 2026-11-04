"""Single-round publication: retain valid snapshots and preserve the wire format."""
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest

from core import arrow_publish, preprocess
from tests.test_module8_arrow_publish import _build_market_dict


@pytest.mark.parametrize('minute', range(5, 60, 5))
@pytest.mark.parametrize('keep', [1, 2])
def test_publication_uses_configured_retention_and_keeps_other_market(tmp_path, monkeypatch, keep, minute):
    monkeypatch.setattr(preprocess, 'bmac_config', SimpleNamespace(publish_kline_num=0, arrow_snapshot_keep=keep))
    handle = SimpleNamespace(resample_interval='1h', preprocess_handle=SimpleNamespace(
        preprocess_mgr_map={f'{minute}m': SimpleNamespace(base_dir=tmp_path)}))
    market = _build_market_dict()
    unrelated = tmp_path / 'swap_dict_1.arrow'
    unrelated.write_bytes(b'other market')
    times = pd.date_range(pd.Timestamp('2026-11-05', tz='UTC') + pd.Timedelta(minutes=minute), periods=3, freq='h')
    for run_time in times:
        preprocess.publish_arrow_snapshot_for_type(handle, run_time.to_pydatetime(), 'spot', market)
    snapshots = sorted(tmp_path.glob('spot_dict_*.arrow'))
    assert [p.name for p in snapshots] == [f'spot_dict_{int(t.timestamp())}.arrow' for t in times[-keep:]]
    assert unrelated.read_bytes() == b'other market'
    latest = tmp_path / f'spot_dict_{int(times[-1].timestamp())}.arrow'
    restored = arrow_publish.read_arrow_snapshot(latest)
    for symbol, frame in market.items():
        pd.testing.assert_frame_equal(restored[symbol], frame, check_exact=True)


def test_cleanup_defaults_to_one_round(tmp_path):
    for epoch in (1, 2):
        (tmp_path / f'spot_dict_{epoch}.arrow').write_bytes(b'complete')
    arrow_publish.cleanup_old_snapshots(tmp_path, 'spot')
    assert [p.name for p in tmp_path.glob('*.arrow')] == ['spot_dict_2.arrow']


@pytest.mark.parametrize('keep', [0, -1, 1.5, True])
def test_invalid_retention_never_deletes_snapshots(tmp_path, keep):
    path = tmp_path / 'spot_dict_1.arrow'
    path.write_bytes(b'complete')
    with pytest.raises(ValueError, match='keep'):
        arrow_publish.cleanup_old_snapshots(tmp_path, 'spot', keep=keep)
    assert path.read_bytes() == b'complete'


@pytest.mark.parametrize('same_round', [True, False])
def test_failed_publish_preserves_previous_file_and_cleans_its_tmp(tmp_path, monkeypatch, same_round):
    market = _build_market_dict()
    old = tmp_path / 'spot_dict_1.arrow'
    arrow_publish.publish_arrow_snapshot(market, old, 0)
    original = old.read_bytes()
    target = old if same_round else tmp_path / 'spot_dict_2.arrow'
    original_writer = pa.ipc.new_file

    @contextmanager
    def failing_writer(*args, **kwargs):
        with original_writer(*args, **kwargs) as writer:
            class Writer:
                count = 0

                def write_batch(self, batch):
                    self.count += 1
                    if self.count == 2:
                        raise OSError(28, 'No space left on device')
                    writer.write_batch(batch)
            yield Writer()

    monkeypatch.setattr(pa.ipc, 'new_file', failing_writer)
    with pytest.raises(OSError, match='No space'):
        arrow_publish.publish_arrow_snapshot(market, target, 0)
    assert old.read_bytes() == original
    assert old.with_suffix('.ready').exists()
    assert not target.with_suffix('.arrow.tmp').exists()
    if not same_round:
        assert not target.exists()
        assert not target.with_suffix('.ready').exists()


def test_same_round_replacement_is_atomic(tmp_path, monkeypatch):
    market = _build_market_dict()
    path = tmp_path / 'spot_dict_1.arrow'
    arrow_publish.publish_arrow_snapshot(market, path, 0)
    before = path.read_bytes()
    updated = {symbol: frame.assign(close=frame['close'] + 10) for symbol, frame in market.items()}
    real_replace = arrow_publish.os.replace
    observed = []

    def checked_replace(source, destination):
        observed.append(Path(destination).read_bytes() == before)
        return real_replace(source, destination)

    monkeypatch.setattr(arrow_publish.os, 'replace', checked_replace)
    arrow_publish.publish_arrow_snapshot(updated, path, 0)
    assert observed == [True]
    restored = arrow_publish.read_arrow_snapshot(path)
    for symbol, frame in updated.items():
        pd.testing.assert_frame_equal(restored[symbol], frame, check_exact=True)
    assert not path.with_suffix('.arrow.tmp').exists()


def test_backfill_keeps_just_published_round_even_if_a_newer_file_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(preprocess, 'bmac_config', SimpleNamespace(publish_kline_num=0, arrow_snapshot_keep=1))
    handle = SimpleNamespace(resample_interval='1h', preprocess_handle=SimpleNamespace(
        preprocess_mgr_map={'5m': SimpleNamespace(base_dir=tmp_path)}))
    market = _build_market_dict()
    future = pd.Timestamp('2026-11-05 01:05', tz='UTC')
    current = future - pd.Timedelta(hours=1)
    for run_time in (future, current):
        preprocess.publish_arrow_snapshot_for_type(handle, run_time.to_pydatetime(), 'spot', market)
    assert [p.name for p in tmp_path.glob('spot_dict_*.arrow')] == [f'spot_dict_{int(current.timestamp())}.arrow']


@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_invalid_config_retention_is_rejected_before_processing(tmp_path, monkeypatch, value):
    import importlib.util
    import json
    from core.utils import path_kit
    monkeypatch.setattr(path_kit, 'get_file_path', lambda *args, **kwargs: tmp_path/args[0])
    monkeypatch.setattr(path_kit, 'get_folder_path', lambda *args, **kwargs: str(tmp_path/'data'))
    (tmp_path/'config.json').write_text(json.dumps({'arrow_snapshot_keep': value}), encoding='utf-8')
    spec = importlib.util.spec_from_file_location('invalid_retention', Path(__file__).resolve().parents[1]/'config.py')
    with pytest.raises(ValueError, match='arrow_snapshot_keep'):
        spec.loader.exec_module(importlib.util.module_from_spec(spec))


def test_failed_atomic_replace_cleans_tmp_and_preserves_original(tmp_path, monkeypatch):
    market = _build_market_dict()
    target = tmp_path/'spot_dict_1.arrow'
    arrow_publish.publish_arrow_snapshot(market, target, 0)
    previous = target.read_bytes()
    def fail_replace(*args):
        raise OSError('replace failed')
    monkeypatch.setattr(arrow_publish.os, 'replace', fail_replace)
    with pytest.raises(OSError, match='replace failed'):
        arrow_publish.publish_arrow_snapshot(market, target, 0)
    assert target.read_bytes() == previous
    assert not target.with_suffix('.arrow.tmp').exists()
