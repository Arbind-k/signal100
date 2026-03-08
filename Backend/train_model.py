from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error

from data_pipeline import load_and_prepare_data, write_to_sqlite
from utils import save_pickle, MODEL_PATH, FEATURES_PATH, METRICS_PATH

FEATURE_COLUMNS = [
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


def train():
    print("Loading and preparing data...")
    price_df, feature_df = load_and_prepare_data()
    print(f"Loaded {price_df['ticker'].nunique()} tickers")
    print(f"Price rows: {len(price_df)}")
    print(f"Feature rows: {len(feature_df)}")

    print("Writing data to SQLite...")
    write_to_sqlite(price_df, feature_df)

    feature_df = feature_df.sort_values(["ticker", "date"]).reset_index(drop=True)

    X = feature_df[FEATURE_COLUMNS]
    y = feature_df["next_day_close"]

    split_idx = int(len(feature_df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    print("Training RandomForest model...")
    model = RandomForestRegressor(
        n_estimators=50,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
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

    metrics = {
        "mae": float(mae),
        "baseline_mae": float(baseline_mae),
        "improvement_pct": float(improvement_pct),
        "num_training_rows": int(len(X_train)),
        "num_test_rows": int(len(X_test)),
        "num_tickers": int(price_df["ticker"].nunique()),
    }

    print("Saving model artifacts...")
    save_pickle(model, MODEL_PATH)
    save_pickle(FEATURE_COLUMNS, FEATURES_PATH)
    save_pickle(metrics, METRICS_PATH)

    print("Training complete.")
    print(metrics)


if __name__ == "__main__":
    train()