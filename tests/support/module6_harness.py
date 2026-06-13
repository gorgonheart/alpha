# tests/support/module6_harness.py
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "module6"
TRUNC_LEN = 52560

_MISSING_MSG = (
    "Fixture files not found. Run from project root:\n"
    "  conda run -n alpha311 python tests/fixtures/capture.py"
)


def load_btcusdt_candles() -> pd.DataFrame:
    path = FIXTURE_DIR / "input_btcusdt_candles.pkl"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return pd.read_pickle(path).reset_index(drop=True)


def load_golden(param: int) -> np.ndarray:
    path = FIXTURE_DIR / f"golden_btcusdt_{param}.npy"
    if not path.exists():
        raise FileNotFoundError(_MISSING_MSG)
    return np.load(path)


def signal_multi_params_py(df: pd.DataFrame, param_list: list[int]) -> dict[str, np.ndarray]:
    """Python reference implementation (source: factors/c_ext/validate.py)."""
    tp = (df["close"] + df["high"] + df["low"]) / 3
    tpnmax = df["high"].copy()
    tpnmin = df["low"].copy()
    if len(tp) >= 2:
        tp.iloc[0] = tp.iloc[1]
        tpnmax.iloc[0] = tpnmax.iloc[1]
        tpnmin.iloc[0] = tpnmin.iloc[1]

    tpn    = tp[-TRUNC_LEN:]
    tpnmax = tpnmax.rolling(4).min()[-TRUNC_LEN:]
    tpnmin = tpnmin.rolling(4).max()[-TRUNC_LEN:]

    ret = {}
    for param in param_list:
        n = min(int(param), tpn.shape[0])
        pctchange = (tpnmax - tpnmin) / tpnmin
        pctma     = pctchange.rolling(n, min_periods=1).mean()
        pctstd    = pctchange.rolling(n, min_periods=1).std()
        pctzscore = (pctchange - pctma) / pctstd
        is_anomaly = (np.abs(pctzscore) > 25) & ((pctchange > 0.2) | (pctchange < -0.5))
        tpn_copy = tpn.copy()
        tpn_copy.loc[tpn_copy[is_anomaly].index] = np.nan
        split_indices = np.where(is_anomaly)[0] + 1
        subfactor = []
        for sub_df in np.split(tpn_copy, split_indices):
            if sub_df.shape[0] > 168:
                ewm    = sub_df.ewm(span=n, adjust=False).mean()
                diff   = sub_df - ewm
                d_min  = diff.rolling(n, min_periods=167).min()
                d_max  = diff.rolling(n, min_periods=167).max()
                subfactor.append((diff - d_min) / (d_max - d_min))
            else:
                subfactor.append(pd.Series(np.full(sub_df.shape[0], np.nan)))
        factor   = pd.concat(subfactor, ignore_index=True)
        max_high = tpnmax.rolling(26 * 168, min_periods=1).max()
        min_low  = tpnmin.rolling(26 * 168, min_periods=1).min()
        factor   = factor + (tpn_copy - min_low) / (max_high - min_low)
        if len(tp) != len(factor):
            factor = pd.concat([pd.Series([np.nan] * (len(tp) - len(factor))), factor])
        ret[str(param)] = np.asarray(factor, dtype=np.float64)
    return ret
