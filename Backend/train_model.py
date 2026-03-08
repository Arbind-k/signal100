from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

from data_pipeline import load_and_prepare_data, write_to_sqlite
from utils import save_pickle, MODEL_PATH, FEATURES_PATH, METRICS_PATH

TECHNICAL_FEATURES = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "sma_5",
    "sma_10",
    "sma_20",
    "ema_5",
    "ema_10",
    "rsi",
    "daily_return",
    "volatility",
    "price_t_1",
    "price_t_2",
    "price_t_3",
    "price_t_5",
]

FUNDAMENTAL_FEATURES = [
    "pe_ratio",
    "peg_ratio",
    "market_cap",
    "price_to_book",
    "eps_ttm",
    "revenue_growth",
    "earnings_growth",
]

FILING_FEATURES = [
    "filing_count_365d",
    "ten_q_count_365d",
    "days_since_last_filing",
    "days_since_last_10q",
    "recent_10q_flag",
]

FEATURE_COLUMNS = TECHNICAL_FEATURES + FUNDAMENTAL_FEATURES + FILING_FEATURES


def train():
    print("Loading and preparing data...")
    price_df, feature_df = load_and_prepare_data()
    print(f"Loaded {price_df['ticker'].nunique()} tickers")
    print(f"Price rows: {len(price_df)}")
    print(f"Feature rows: {len(feature_df)}")

    print("Writing data to SQLite...")
    write_to_sqlite(price_df, feature_df)

    feature_df = feature_df.sort_values(["date", "ticker"]).reset_index(drop=True)

    missing_features = [col for col in FEATURE_COLUMNS if col not in feature_df.columns]
    if missing_features:
        raise ValueError(f"Missing expected model features: {missing_features}")

    X = feature_df[FEATURE_COLUMNS]
    y = feature_df["next_day_close"]

    split_idx = int(len(feature_df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print("Training RandomForest model with technical + fundamentals + filings...")
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=14,
        min_samples_split=8,
        min_samples_leaf=3,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)

    print("Evaluating model...")
    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    baseline_preds = X_test["close"]
    baseline_mae = mean_absolute_error(y_test, baseline_preds)

    improvement_pct = 0.0
    if baseline_mae != 0:
        improvement_pct = ((baseline_mae - mae) / baseline_mae) * 100

    feature_importances = dict(sorted(
        zip(FEATURE_COLUMNS, model.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    ))

    metrics = {
        "mae": float(mae),
        "baseline_mae": float(baseline_mae),
        "improvement_pct": float(improvement_pct),
        "num_training_rows": int(len(X_train)),
        "num_test_rows": int(len(X_test)),
        "num_tickers": int(price_df["ticker"].nunique()),
        "top_feature_importances": dict(list(feature_importances.items())[:10]),
        "feature_count": int(len(FEATURE_COLUMNS)),
    }

    print("Saving model artifacts...")
    save_pickle(model, MODEL_PATH)
    save_pickle(FEATURE_COLUMNS, FEATURES_PATH)
    save_pickle(metrics, METRICS_PATH)

    print("Training complete.")
    print(metrics)


if __name__ == "__main__":
    train()
