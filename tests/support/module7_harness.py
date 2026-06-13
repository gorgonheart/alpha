# tests/support/module7_harness.py
from __future__ import annotations

import importlib
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from core.candle_manager import CandleFileManager
from core.utils.time import get_offset_from_runtime
from tests.support.module3_harness import RESAMPLE_INTERVAL

_ROOT = Path(__file__).resolve().parents[2]
_MODULE6_FIXTURE = _ROOT / "tests" / "fixtures" / "module6"

SYMBOLS_8 = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT",
    "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
]
SYMBOLS_4 = SYMBOLS_8[:4]

ALL_FACTOR_CONFIGS = {
    "动量因子210401修正": [168, 336],
    "ILLQStdBbi45208": [10000],
    "CciV3": [10000],
}


def load_btcusdt_candles() -> pd.DataFrame:
    """Returns the 7200-row BTCUSDT snapshot."""
    path = _MODULE6_FIXTURE / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(
            f"Fixture not found: {path}\n"
            "Run from project root:\n"
            "  conda run -n alpha311 python tests/fixtures/capture_illq_cci.py"
        )
    return pd.read_pickle(path).reset_index(drop=True)


def _make_symbol_df(base_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    """Clone base_df with symbol column set. Same OHLCV data, different symbol name."""
    df = base_df.copy(deep=True)
    df["symbol"] = symbol
    return df


_PRICE_COLS = ["open", "high", "low", "close"]


def _make_perturbed_symbol_df(base_df: pd.DataFrame, symbol: str, idx: int) -> pd.DataFrame:
    """Clone base_df with symbol column set and price columns scaled by a per-symbol factor.
    idx is 0-based index used to compute a unique but small scale factor.
    Scaling all OHLC by the same factor preserves ratios (so factor formulas remain valid)
    while making each symbol's factor output numerically distinct.
    """
    df = base_df.copy(deep=True)
    df["symbol"] = symbol
    scale = 1.0 + 0.01 * (idx + 1)  # 1.01, 1.02, 1.03, ... per symbol
    for col in _PRICE_COLS:
        if col in df.columns:
            df[col] = df[col] * scale
    return df


def make_multi_batch_handle(
    tmp_path: Path,
    base_df: pd.DataFrame,
    run_time,
    *,
    batches: list[list[str]],
    trade_type: str = "spot",
) -> tuple[SimpleNamespace, CandleFileManager]:
    """Build a handle with multiple batches, each holding a subset of symbols.

    Args:
        batches: list of symbol lists, one per batch.
                 e.g. [["BTCUSDT", "ETHUSDT"], ["BNBUSDT"]] -> 2 batches
    Returns:
        (handle, preprocess_mgr)
    """
    offset_str = get_offset_from_runtime(run_time, RESAMPLE_INTERVAL)
    preprocess_dir = tmp_path / "preprocess" / offset_str
    preprocess_dir.mkdir(parents=True, exist_ok=True)
    preprocess_mgr = CandleFileManager(base_dir=preprocess_dir, save_type="pickle")

    for batch_idx, symbol_list in enumerate(batches, start=1):
        batch_key = f"{trade_type}_dict_batch{batch_idx}"
        market_dict = {
            sym: _make_perturbed_symbol_df(base_df, sym, i)
            for i, sym in enumerate(symbol_list)
        }
        preprocess_mgr.save_data_file(batch_key, market_dict)

    handle = SimpleNamespace(
        resample_interval=RESAMPLE_INTERVAL,
        preprocess_handle=SimpleNamespace(
            preprocess_mgr_map={offset_str: preprocess_mgr},
            num_batch=256,
        ),
    )
    return handle, preprocess_mgr


def reference_gen_factor_data(
    preprocess_mgr: CandleFileManager,
    run_time,
    trade_type: str,
    factor_configs: dict,
) -> dict[str, dict[str, pd.DataFrame]]:
    """Serial reference baseline: compute factors batch-by-batch, symbol-by-symbol.

    Returns: {batch_key: {symbol: df_with_factor_columns}}
    This does NOT call gen_factor_data - it reimplements the logic independently,
    using the same factor module calls to match the original serial behaviour.
    """
    factor_modules_params = [
        (importlib.import_module(f"factors.{factor_name}"), factor_name, param_list)
        for factor_name, param_list in factor_configs.items()
    ]

    results: dict[str, dict[str, pd.DataFrame]] = {}
    batch_idx = 1
    while True:
        batch_key = f"{trade_type}_dict_batch{batch_idx}"
        if not preprocess_mgr.has_symbol(batch_key):
            break
        market_dict: dict[str, pd.DataFrame] = preprocess_mgr.read_candle(batch_key)
        updated: dict[str, pd.DataFrame] = {}
        for symbol, df in market_dict.items():
            factor_cols: dict = {}
            for module, factor_name, param_list in factor_modules_params:
                raw = module.signal_multi_params(df, param_list)
                for param_key, series in raw.items():
                    factor_cols[f"{factor_name}_{param_key}"] = series
            updated[symbol] = df.assign(**factor_cols)
        results[batch_key] = updated
        batch_idx += 1
    return results
