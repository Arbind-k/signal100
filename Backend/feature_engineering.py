import numpy as np
import pandas as pd


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(["ticker", "date"])

    grouped = df.groupby("ticker", group_keys=False)

    df["sma_5"] = grouped["close"].transform(lambda s: s.rolling(5).mean())
    df["sma_10"] = grouped["close"].transform(lambda s: s.rolling(10).mean())
    df["sma_20"] = grouped["close"].transform(lambda s: s.rolling(20).mean())

    df["ema_5"] = grouped["close"].transform(lambda s: s.ewm(span=5, adjust=False).mean())
    df["ema_10"] = grouped["close"].transform(lambda s: s.ewm(span=10, adjust=False).mean())

    df["rsi"] = grouped["close"].transform(lambda s: compute_rsi(s, 14))
    df["daily_return"] = grouped["close"].transform(lambda s: s.pct_change())
    df["volatility"] = grouped["daily_return"].transform(lambda s: s.rolling(10).std())

    df["price_t_1"] = grouped["close"].shift(1)
    df["price_t_2"] = grouped["close"].shift(2)
    df["price_t_3"] = grouped["close"].shift(3)
    df["price_t_5"] = grouped["close"].shift(5)

    df["next_day_close"] = grouped["close"].shift(-1)

    return df