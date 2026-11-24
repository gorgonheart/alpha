import importlib.util
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import pytest
from core.utils import path_kit


def _load_cfg(tmp_path, monkeypatch, overrides):
    monkeypatch.setattr(path_kit, "get_file_path", lambda *a, **kw: tmp_path / a[0])
    monkeypatch.setattr(path_kit, "get_folder_path", lambda *a, **kw: str(tmp_path / "data"))
    (tmp_path / "config.json").write_text(json.dumps(overrides), encoding="utf-8")
    spec = importlib.util.spec_from_file_location("cfg_2b_test", Path(__file__).resolve().parents[1] / "config.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_2b_config_defaults_and_single_kline_count_fallback(tmp_path, monkeypatch):
    cfg = _load_cfg(tmp_path, monkeypatch, {"kline_count_1h": 14400})
    assert cfg.use_websocket is True
    assert cfg.num_writers == 4
    assert cfg.ws_wait_sec == 14.0
    assert cfg.ws_flat_pkl_bars == 240
    assert cfg.base_5m_retention_days == 30
    assert cfg.min_history_start_date == "2025-03-25"
    assert cfg.history_trim_date is None
    assert cfg.kline_count_1h_spot == 14400
    assert cfg.kline_count_1h_swap == 14400


def test_handle_effective_kline_count_anchors_past_2026_12_16(tmp_path, monkeypatch):
    from core.handle import create_bmac_handle_from_config
    cfg = _load_cfg(tmp_path, monkeypatch, {
        "use_api": {"kline": False, "coin_cap": False},
        "kline_count_1h_spot": 15000, "kline_count_1h_swap": 15000,
        "min_history_start_date": "2025-03-25", "history_trim_date": None,
    })
    h = create_bmac_handle_from_config(cfg)
    assert h.binance_spot is h.spot and h.get_binance_handle("swap") is h.swap
    assert h.base_5m_retention_bars() == 30 * 24 * 12
    assert h.flat_pkl_max_bars() == 240
    assert h.history_trim_ts() is None
    late = datetime(2027, 1, 15, 0, 25, tzinfo=timezone.utc)
    # 2025-03-25 -> 2027-01-15 > 15,800h, +48h safety buffer
    assert h.effective_kline_count_1h("swap", late) > 15800


def test_history_trim_date_rejects_too_recent_and_overrides_floor(tmp_path, monkeypatch):
    from core.handle import create_bmac_handle_from_config
    too_recent = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=10000)).strftime("%Y-%m-%d")
    cfg_bad = _load_cfg(tmp_path, monkeypatch, {
        "use_api": {"kline": False, "coin_cap": False},
        "history_trim_date": too_recent,
    })
    with pytest.raises(ValueError, match="14400|600"):
        create_bmac_handle_from_config(cfg_bad)

    valid_trim = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=15000)).strftime("%Y-%m-%d")
    cfg_ok = _load_cfg(tmp_path, monkeypatch, {
        "use_api": {"kline": False, "coin_cap": False},
        "min_history_start_date": "2024-01-01",
        "history_trim_date": valid_trim,
    })
    h = create_bmac_handle_from_config(cfg_ok)
    assert h.history_trim_ts() == pd.Timestamp(valid_trim, tz="UTC")


@pytest.mark.parametrize(
    "override",
    [
        {"num_writers": 0},
        {"num_writers": True},
        {"ws_flat_pkl_bars": 0},
        {"ws_flat_pkl_bars": False},
        {"base_5m_retention_days": 6},
        {"base_5m_retention_days": True},
        {"rest_fetch_delay_seconds": -0.5},
        {"rest_fetch_delay_seconds": True},
    ],
)
def test_2b_config_validation_rejects_invalid_values(tmp_path, monkeypatch, override):
    with pytest.raises(ValueError):
        _load_cfg(tmp_path, monkeypatch, override)


def test_history_trim_date_later_than_min_history_start_overrides_effective_floor(tmp_path, monkeypatch):
    from core.handle import create_bmac_handle_from_config
    valid_trim = (pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=15000)).strftime("%Y-%m-%d")
    cfg = _load_cfg(tmp_path, monkeypatch, {
        "use_api": {"kline": False, "coin_cap": False},
        "kline_count_1h_spot": 15000,
        "kline_count_1h_swap": 15000,
        "min_history_start_date": "2020-01-01",
        "history_trim_date": valid_trim,
    })
    h = create_bmac_handle_from_config(cfg)
    run_time = pd.Timestamp.now(tz="UTC").to_pydatetime()
    # If min_history_start_date (2020-01-01) were used, effective count would be > 50,000.
    # Because history_trim_date > min_history_start_date, history_trim_date overrides the floor (~15000 + 48).
    eff = h.effective_kline_count_1h("swap", run_time)
    assert 15000 <= eff < 16000


def test_v202_skeleton_imports_and_thin_shims():
    import core.alert
    import core.utils.log_bridge
    import core.exchange
    import core.storage
    import core.snapshot
    import core.candle_manager as cm_shim
    import core.storage.candle_manager as cm_real
    import core.ts_manager as ts_shim
    import core.storage.ts_manager as ts_real
    import core.symbol_filter as sf_shim
    import core.exchange.symbol_filter as sf_real
    import core.symbol_match as sm_shim
    import core.exchange.symbol_match as sm_real
    from core.handle import MarketHandle, BinanceHandle, create_binance_handle

    assert BinanceHandle is MarketHandle
    assert callable(create_binance_handle)
    assert cm_shim.CandleFileManager is cm_real.CandleFileManager
    assert ts_shim.TSManager is ts_real.TSManager
    assert sf_shim.create_symbol_filter is sf_real.create_symbol_filter
    assert sm_shim.get_spot_swap_matches is sm_real.get_spot_swap_matches
