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


def _find_first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower_to_original = {col.lower(): col for col in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower_to_original:
            return lower_to_original[candidate.lower()]
    return None


def _coerce_numeric_columns(df: pd.DataFrame, exclude: set[str] | None = None) -> pd.DataFrame:
    exclude = exclude or set()
    out = df.copy()

    for col in out.columns:
        if col in exclude:
            continue
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out


def prepare_fundamentals_features(fundamentals_df: pd.DataFrame) -> pd.DataFrame:
    if fundamentals_df is None or fundamentals_df.empty:
        return pd.DataFrame(columns=[
            "ticker", "pe_ratio", "peg_ratio", "market_cap", "price_to_book",
            "eps_ttm", "revenue_growth", "earnings_growth",
        ])

    df = fundamentals_df.copy()
    ticker_col = _find_first_column(df, ["ticker", "symbol"])
    if ticker_col is None:
        raise ValueError("Fundamentals file must contain a ticker/symbol column.")

    rename_map = {ticker_col: "ticker"}
    alias_groups = {
        "pe_ratio": ["pe_ratio", "pe", "trailingpe", "price_earnings_ratio"],
        "peg_ratio": ["peg_ratio", "peg", "pegratio"],
        "market_cap": ["market_cap", "marketcap"],
        "price_to_book": ["price_to_book", "pricetobook", "pb_ratio", "pb"],
        "eps_ttm": ["eps_ttm", "eps", "trailingeps"],
        "revenue_growth": ["revenue_growth", "revenuegrowth"],
        "earnings_growth": ["earnings_growth", "earningsgrowth", "profit_growth", "eps_growth"],
    }

    for target, candidates in alias_groups.items():
        found = _find_first_column(df, candidates)
        if found:
            rename_map[found] = target

    df = df.rename(columns=rename_map)
    keep = [c for c in ["ticker", *alias_groups.keys()] if c in df.columns]
    df = df[keep].copy()
    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df = _coerce_numeric_columns(df, exclude={"ticker"})

    if "peg_ratio" not in df.columns:
        df["peg_ratio"] = np.nan
    if "pe_ratio" in df.columns and "earnings_growth" in df.columns:
        growth = df["earnings_growth"].replace(0, np.nan)
        fallback_peg = df["pe_ratio"] / growth.abs()
        df["peg_ratio"] = df["peg_ratio"].fillna(fallback_peg)

    for col in ["pe_ratio", "peg_ratio", "market_cap", "price_to_book", "eps_ttm", "revenue_growth", "earnings_growth"]:
        if col not in df.columns:
            df[col] = np.nan

    df = df.drop_duplicates(subset=["ticker"])
    return df[["ticker", "pe_ratio", "peg_ratio", "market_cap", "price_to_book", "eps_ttm", "revenue_growth", "earnings_growth"]]


def prepare_filings_features(filings_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    tickers = pd.DataFrame({"ticker": sorted(price_df["ticker"].astype(str).str.upper().unique())})
    if filings_df is None or filings_df.empty:
        tickers["filing_count_365d"] = 0.0
        tickers["ten_q_count_365d"] = 0.0
        tickers["days_since_last_filing"] = 9999.0
        tickers["days_since_last_10q"] = 9999.0
        tickers["recent_10q_flag"] = 0.0
        return tickers

    df = filings_df.copy()
    ticker_col = _find_first_column(df, ["ticker", "symbol"])
    date_col = _find_first_column(df, ["filing_date", "filed_at", "date", "filingdate"])
    form_col = _find_first_column(df, ["form", "form_type", "filing_type"])

    if ticker_col is None or date_col is None:
        tickers["filing_count_365d"] = 0.0
        tickers["ten_q_count_365d"] = 0.0
        tickers["days_since_last_filing"] = 9999.0
        tickers["days_since_last_10q"] = 9999.0
        tickers["recent_10q_flag"] = 0.0
        return tickers

    df = df.rename(columns={ticker_col: "ticker", date_col: "filing_date"})
    if form_col:
        df = df.rename(columns={form_col: "form"})
    else:
        df["form"] = "UNKNOWN"

    df["ticker"] = df["ticker"].astype(str).str.upper().str.strip()
    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df = df.dropna(subset=["ticker", "filing_date"])

    if df.empty:
        tickers["filing_count_365d"] = 0.0
        tickers["ten_q_count_365d"] = 0.0
        tickers["days_since_last_filing"] = 9999.0
        tickers["days_since_last_10q"] = 9999.0
        tickers["recent_10q_flag"] = 0.0
        return tickers

    latest_price_date = pd.to_datetime(price_df["date"]).max()
    window_start = latest_price_date - pd.Timedelta(days=365)
    recent_df = df[df["filing_date"] >= window_start].copy()

    filing_counts = recent_df.groupby("ticker").size().rename("filing_count_365d")
    ten_q_counts = recent_df[recent_df["form"].astype(str).str.contains("10-Q", case=False, na=False)] \
        .groupby("ticker").size().rename("ten_q_count_365d")

    last_filing = df.groupby("ticker")["filing_date"].max().rename("last_filing_date")
    last_10q = df[df["form"].astype(str).str.contains("10-Q", case=False, na=False)] \
        .groupby("ticker")["filing_date"].max().rename("last_10q_date")

    merged = tickers.merge(filing_counts, on="ticker", how="left")
    merged = merged.merge(ten_q_counts, on="ticker", how="left")
    merged = merged.merge(last_filing, on="ticker", how="left")
    merged = merged.merge(last_10q, on="ticker", how="left")

    merged["filing_count_365d"] = merged["filing_count_365d"].fillna(0.0)
    merged["ten_q_count_365d"] = merged["ten_q_count_365d"].fillna(0.0)
    merged["days_since_last_filing"] = (latest_price_date - merged["last_filing_date"]).dt.days.fillna(9999).astype(float)
    merged["days_since_last_10q"] = (latest_price_date - merged["last_10q_date"]).dt.days.fillna(9999).astype(float)
    merged["recent_10q_flag"] = (merged["days_since_last_10q"] <= 120).astype(float)

    return merged[[
        "ticker", "filing_count_365d", "ten_q_count_365d",
        "days_since_last_filing", "days_since_last_10q", "recent_10q_flag",
    ]]


def merge_external_features(price_feature_df: pd.DataFrame, fundamentals_df: pd.DataFrame | None, filings_df: pd.DataFrame | None) -> pd.DataFrame:
    feature_df = price_feature_df.copy()
    feature_df["ticker"] = feature_df["ticker"].astype(str).str.upper().str.strip()

    fundamentals_features = prepare_fundamentals_features(fundamentals_df)
    filings_features = prepare_filings_features(filings_df, feature_df)

    feature_df = feature_df.merge(fundamentals_features, on="ticker", how="left")
    feature_df = feature_df.merge(filings_features, on="ticker", how="left")

    numeric_fill_defaults = {
        "pe_ratio": feature_df.get("pe_ratio", pd.Series(dtype=float)).median() if "pe_ratio" in feature_df else 0.0,
        "peg_ratio": feature_df.get("peg_ratio", pd.Series(dtype=float)).median() if "peg_ratio" in feature_df else 0.0,
        "market_cap": feature_df.get("market_cap", pd.Series(dtype=float)).median() if "market_cap" in feature_df else 0.0,
        "price_to_book": feature_df.get("price_to_book", pd.Series(dtype=float)).median() if "price_to_book" in feature_df else 0.0,
        "eps_ttm": feature_df.get("eps_ttm", pd.Series(dtype=float)).median() if "eps_ttm" in feature_df else 0.0,
        "revenue_growth": 0.0,
        "earnings_growth": 0.0,
        "filing_count_365d": 0.0,
        "ten_q_count_365d": 0.0,
        "days_since_last_filing": 9999.0,
        "days_since_last_10q": 9999.0,
        "recent_10q_flag": 0.0,
    }

    for col, default_value in numeric_fill_defaults.items():
        if col not in feature_df.columns:
            feature_df[col] = default_value
        elif pd.api.types.is_numeric_dtype(feature_df[col]):
            feature_df[col] = feature_df[col].fillna(default_value)
        else:
            feature_df[col] = pd.to_numeric(feature_df[col], errors="coerce").fillna(default_value)

    return feature_df
