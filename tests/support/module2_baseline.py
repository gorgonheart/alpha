from __future__ import annotations

import pandas as pd


def baseline_merge_candles(
    df_old: pd.DataFrame | None,
    df_new: pd.DataFrame,
    num_candles: int | None,
) -> pd.DataFrame:
    """Frozen snapshot of update_candle's dataframe transformation only.

    This helper intentionally stops at the harness boundary. It does not call
    CandleFileManager methods or model storage side effects such as ready files.
    """
    if df_old is None or df_old.empty:
        df = df_new.copy(deep=True)
    else:
        df = pd.concat([df_old.copy(deep=True), df_new.copy(deep=True)])

    df = df.copy(deep=True)
    df.drop_duplicates(subset="candle_begin_time", keep="last", inplace=True)
    df.sort_values("candle_begin_time", inplace=True)
    if num_candles is not None:
        df = df.tail(num_candles)

    return df
