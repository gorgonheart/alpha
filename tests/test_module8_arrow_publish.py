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


def _touch(path: Path, content: str = "x") -> None:
    path.write_text(content)


def test_cleanup_keeps_newest_n_snapshots(tmp):
    """cleanup_old_snapshots must keep the newest `keep` epochs and delete older ones.

    Finding 2: epoch-stamped {type}_dict_{epoch}.arrow files accumulate forever
    (only the .ready flag is overwritten). This leaks ~1.2 GB/cycle/offset on the
    8 GB server. Cleanup keeps a small safety margin (default 2) for consumers that
    may be mid-mmap-read of the previous snapshot.
    """
    from core.arrow_publish import cleanup_old_snapshots

    epochs = [1782616200, 1782619800, 1782623400, 1782627000]  # ascending
    for e in epochs:
        _touch(tmp / f"spot_dict_{e}.arrow")
    # an unrelated trade_type must be untouched
    _touch(tmp / "swap_dict_1782616200.arrow")

    cleanup_old_snapshots(tmp, "spot", keep=2)

    remaining = sorted(p.name for p in tmp.glob("spot_dict_*.arrow"))
    assert remaining == ["spot_dict_1782623400.arrow", "spot_dict_1782627000.arrow"], remaining
    # other trade_type snapshot left alone
    assert (tmp / "swap_dict_1782616200.arrow").exists()


def test_cleanup_removes_stale_tmp(tmp):
    """A crash can leave a *.arrow.tmp orphan; cleanup must remove stale tmp files."""
    from core.arrow_publish import cleanup_old_snapshots

    _touch(tmp / "spot_dict_1782623400.arrow")
    _touch(tmp / "spot_dict_1782627000.arrow.tmp")  # orphaned half-write

    cleanup_old_snapshots(tmp, "spot", keep=2)

    assert not list(tmp.glob("*.tmp")), "stale .arrow.tmp not cleaned up"
    assert (tmp / "spot_dict_1782623400.arrow").exists()


def test_cleanup_noop_when_within_keep(tmp):
    """Fewer snapshots than `keep` -> nothing deleted."""
    from core.arrow_publish import cleanup_old_snapshots

    _touch(tmp / "spot_dict_1782623400.arrow")
    cleanup_old_snapshots(tmp, "spot", keep=2)
    assert (tmp / "spot_dict_1782623400.arrow").exists()


@pytest.mark.parametrize("depth", [0, 50])
def test_full_history_marker_and_values_without_mutating_input(tmp_path, depth):
    """Depth zero preserves all rows; both modes preserve values and input indexes."""
    import pyarrow as pa
    from core.arrow_publish import publish_arrow_snapshot, read_arrow_snapshot

    market = _build_market_dict()
    for frame in market.values():
        frame.index = pd.Index(range(1000, 1000 + len(frame)), name="source_row")
    originals = {symbol: frame.copy(deep=True) for symbol, frame in market.items()}
    out = tmp_path / "spot_dict_1.arrow"
    publish_arrow_snapshot(market, out, depth, write_ready=False)
    with pa.memory_map(str(out), "r") as source:
        reader = pa.ipc.open_file(source)
        assert reader.schema.metadata[b"full_history"] == (b"1" if depth == 0 else b"0")
        assert reader.num_record_batches == len(market)
    restored = read_arrow_snapshot(out)
    for symbol, original in originals.items():
        expected = original if depth == 0 else original.tail(depth)
        pd.testing.assert_frame_equal(restored[symbol], expected.reset_index(drop=True), check_exact=True)
        pd.testing.assert_frame_equal(market[symbol], original, check_exact=True)
    assert not out.with_suffix(".ready").exists()
    assert not out.with_suffix(".arrow.tmp").exists()


def test_negative_publish_depth_does_not_publish(tmp_path):
    from core.arrow_publish import publish_arrow_snapshot

    out = tmp_path / "spot_dict_1.arrow"
    with pytest.raises(ValueError, match="publish_kline_num"):
        publish_arrow_snapshot(_build_market_dict(), out, -1)
    assert not out.exists()
    assert not out.with_suffix(".ready").exists()
    assert not out.with_suffix(".arrow.tmp").exists()
