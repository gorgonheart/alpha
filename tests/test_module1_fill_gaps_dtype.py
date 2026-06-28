"""fill_gaps 必须在填补缺口后保持整数列的 int64 dtype。

背景：单文件 Arrow IPC 要求所有 record batch 共用一个 schema，消费者
（d:/alphaonline）对账时发现极少数曾有缺口的 symbol 的 trade_num 在 pkl 里是
float64、在 Arrow 里是 int64。根因是 fill_gaps 用 how='left' merge 注入 NaN
把整数列提升为 float64，fillna(0) 填了值但 dtype 不回落。整数列本应保持 int64，
float64 是 legacy 产物。本测试钉住"缺口填补后整数列仍为 int64"。
"""
from __future__ import annotations

import pandas as pd

from core.resample_candle import fill_gaps


def _build_gap_df() -> pd.DataFrame:
    """构造一个缺中间一根 K 线的 df，trade_num 为 int64。"""
    delta = pd.Timedelta(hours=1)
    # 三根时间戳，但只提供第 0、2 根 → 中间缺一根，触发 reindex/merge 填补
    times = pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 02:00:00"])
    df = pd.DataFrame({
        "candle_begin_time": times,
        "open": [100.0, 110.0],
        "high": [105.0, 115.0],
        "low": [99.0, 109.0],
        "close": [102.0, 112.0],
        "volume": [10.0, 20.0],
        "quote_volume": [1000.0, 2000.0],
        "trade_num": pd.array([5, 7], dtype="int64"),
        "taker_buy_base_asset_volume": [4.0, 8.0],
        "taker_buy_quote_asset_volume": [400.0, 800.0],
    })
    return df, delta


def test_fill_gaps_preserves_int64_for_trade_num():
    df, delta = _build_gap_df()
    assert df["trade_num"].dtype == "int64"

    out = fill_gaps(df, delta)

    # 缺口确实被填补（多出中间那一根）
    assert len(out) == 3
    # 填补的那一根 trade_num 应为 0，且无 NaN
    assert out["trade_num"].isna().any() == False
    assert (out["trade_num"] == 0).any()
    # 关键断言：整数列填补后仍为 int64，不被提升为 float64
    assert out["trade_num"].dtype == "int64", \
        f"trade_num 被提升为 {out['trade_num'].dtype}，应保持 int64"
