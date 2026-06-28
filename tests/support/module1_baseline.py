from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd


@dataclass(frozen=True)
class Module1BaselineSnapshot:
    dataframe: pd.DataFrame
    first_begin_time: pd.Timestamp | None
    last_begin_time: pd.Timestamp | None


_NUMERIC_ZERO_FILL_COLUMNS = [
    "volume",
    "quote_volume",
    "trade_num",
    "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume",
]


def _baseline_filter_gaps(
    df: pd.DataFrame,
    gap_hours_thres: int = 48,
    price_change_thres: float = 0.1,
) -> pd.DataFrame:
    price_change = (df["open"] / df["close"].shift().fillna(df["open"]) - 1).fillna(0)
    time_gap = df["candle_begin_time"].diff().fillna(timedelta(minutes=0))
    cond = (time_gap > timedelta(hours=gap_hours_thres)) & (price_change.abs() > price_change_thres)

    if cond.any():
        last_idx = cond[cond].index[-1]
        df = df.loc[last_idx:].copy()

    return df


def _baseline_fill_gaps(df: pd.DataFrame, delta: timedelta) -> pd.DataFrame:
    first = df["candle_begin_time"].min()
    last = df["candle_begin_time"].max()
    time_range = pd.date_range(first, last, freq=delta)

    if len(df) == len(time_range) and (df["candle_begin_time"].reset_index(drop=True) == time_range).all():
        return df

    # 见 core.resample_candle.fill_gaps：merge 注入 NaN 会把整数列提升为 float64，
    # 填补完成后须还原为原始整数 dtype，使 golden baseline 与生产实现一致。
    int_dtypes = {col: dt for col, dt in df.dtypes.items() if pd.api.types.is_integer_dtype(dt)}

    benchmark = pd.DataFrame({"candle_begin_time": time_range})
    df = pd.merge(left=benchmark, right=df, on="candle_begin_time", how="left", sort=True)

    df["close"] = df["close"].ffill()
    df["open"] = df["open"].fillna(df["close"])
    df["high"] = df["high"].fillna(df["close"])
    df["low"] = df["low"].fillna(df["close"])

    if "avg_price_1m" in df.columns:
        df["avg_price_1m"] = df["avg_price_1m"].fillna(df["open"])

    for column in _NUMERIC_ZERO_FILL_COLUMNS:
        df[column] = df[column].fillna(0)

    for col, dt in int_dtypes.items():
        df[col] = df[col].astype(dt)

    df["candle_end_time"] = df["candle_begin_time"] + delta
    df.set_index("candle_end_time", inplace=True)
    return df


def baseline_update_resample_candle(
    df_old: pd.DataFrame | None,
    df_resample: pd.DataFrame,
    num_candles: int,
    resample_delta: timedelta,
) -> Module1BaselineSnapshot:
    """Frozen snapshot of update_resample_candle's observable dataframe contract."""
    if df_old is None or df_old.empty:
        df_total = df_resample.copy(deep=True)
    else:
        df_total = pd.concat([df_old.copy(deep=True), df_resample.copy(deep=True)])

    if df_total.empty:
        return Module1BaselineSnapshot(
            dataframe=df_total.copy(deep=True),
            first_begin_time=None,
            last_begin_time=None,
        )

    df_total = df_total.copy(deep=True)
    df_total.drop_duplicates(subset="candle_begin_time", keep="last", inplace=True)
    df_total.sort_values("candle_begin_time", inplace=True)
    df_total.reset_index(drop=True, inplace=True)
    df_total = _baseline_filter_gaps(df_total)
    df_total = _baseline_fill_gaps(df_total, resample_delta)
    df_total = df_total.tail(num_candles)

    return Module1BaselineSnapshot(
        dataframe=df_total,
        first_begin_time=df_total["candle_begin_time"].iloc[0],
        last_begin_time=df_total["candle_begin_time"].iloc[-1],
    )
