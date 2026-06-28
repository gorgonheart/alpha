"""
Tests for the Arrow publish step (design doc §3.4).

The producer publishes a single Arrow IPC file per trade_type containing one
record batch per symbol plus a manifest (batch index -> symbol), so the consumer
can zero-copy rebuild an equivalent ``{symbol: DataFrame}``.

Grounded in real BTCUSDT data from ``/data`` so the published columns match
exactly what the live preprocess pipeline produces (OHLCV + enrich + factor cols).
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from tests.support.harness_common import assert_dataframes_strict_equal
from tests.support.module3_harness import load_btcusdt_spot


def _build_market_dict() -> dict[str, pd.DataFrame]:
    """Two symbols with the realistic producer column layout, different lengths.

    Includes a float64 factor column with NaNs (head warm-up) to ensure factor
    columns survive the Arrow round-trip.
    """
    base = load_btcusdt_spot().tail(400).reset_index(drop=True)

    def make(symbol: str, n: int) -> pd.DataFrame:
        df = base.tail(n).reset_index(drop=True).copy(deep=True)
        df["symbol"] = symbol
        df["symbol_spot"] = symbol
        df["symbol_swap"] = ""
        # realistic float factor column with leading NaN warm-up window
        factor = df["close"].rolling(10).mean() / df["close"] - 1.0
        df["动量因子210401修正_168"] = factor.astype("float64")
        return df

    return {"BTCUSDT": make("BTCUSDT", 300), "ETHUSDT": make("ETHUSDT", 250)}


@pytest.fixture()
def tmp(tmp_path: Path):
    return tmp_path


def test_round_trip_preserves_market_dict(tmp):
    """publish -> read must reproduce an equivalent {symbol: DataFrame}."""
    from core.arrow_publish import publish_arrow_snapshot, read_arrow_snapshot

    market_dict = _build_market_dict()
    out_path = tmp / "spot_dict_1782234600.arrow"

    publish_arrow_snapshot(market_dict, out_path, publish_kline_num=5000)

    restored = read_arrow_snapshot(out_path)

    assert set(restored.keys()) == set(market_dict.keys())
    for symbol, df_expected in market_dict.items():
        assert_dataframes_strict_equal(
            restored[symbol].reset_index(drop=True),
            df_expected.reset_index(drop=True),
            context=f"round-trip:{symbol}",
        )


def test_tail_depth_applied(tmp):
    """Published file must keep only tail(publish_kline_num) rows per symbol."""
    from core.arrow_publish import publish_arrow_snapshot, read_arrow_snapshot

    market_dict = _build_market_dict()
    depth = 50
    out_path = tmp / "spot_dict_1782234600.arrow"

    publish_arrow_snapshot(market_dict, out_path, publish_kline_num=depth)
    restored = read_arrow_snapshot(out_path)

    for symbol, df_expected in market_dict.items():
        expected_tail = df_expected.tail(depth).reset_index(drop=True)
        assert len(restored[symbol]) == min(depth, len(df_expected))
        assert_dataframes_strict_equal(
            restored[symbol].reset_index(drop=True),
            expected_tail,
            context=f"tail-depth:{symbol}",
        )


def test_atomic_publish_no_tmp_and_ready_flag(tmp):
    """After publish: final .arrow exists, no leftover .tmp, and a .ready flag is written."""
    from core.arrow_publish import publish_arrow_snapshot

    market_dict = _build_market_dict()
    out_path = tmp / "spot_dict_1782234600.arrow"

    publish_arrow_snapshot(market_dict, out_path, publish_kline_num=5000)

    assert out_path.exists(), "final .arrow file missing"
    assert not list(tmp.glob("*.tmp")), "leftover .tmp file after atomic rename"
    assert (tmp / "spot_dict_1782234600.ready").exists(), "ready flag missing"


def test_manifest_maps_batches_to_symbols(tmp):
    """Symbol->batch manifest must round-trip even with differing per-symbol lengths."""
    from core.arrow_publish import publish_arrow_snapshot, read_arrow_snapshot

    market_dict = _build_market_dict()
    out_path = tmp / "swap_dict_1782234600.arrow"

    publish_arrow_snapshot(market_dict, out_path, publish_kline_num=5000)
    restored = read_arrow_snapshot(out_path)

    # lengths preserved per symbol (proves batch index -> symbol mapping is correct)
    assert len(restored["BTCUSDT"]) == 300
    assert len(restored["ETHUSDT"]) == 250


def test_empty_market_dict_is_noop(tmp):
    """Publishing an empty dict must not create a snapshot file (nothing to publish)."""
    from core.arrow_publish import publish_arrow_snapshot

    out_path = tmp / "spot_dict_1782234600.arrow"
    publish_arrow_snapshot({}, out_path, publish_kline_num=5000)

    assert not out_path.exists()
