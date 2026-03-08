from data_pipeline import load_and_prepare_data
from database import get_connection
from utils import load_pickle, MODEL_PATH, FEATURES_PATH, METRICS_PATH


def generate_predictions():
    _, feature_df = load_and_prepare_data()

    model = load_pickle(MODEL_PATH)
    feature_columns = load_pickle(FEATURES_PATH)
    metrics = load_pickle(METRICS_PATH)

    latest_rows = (
        feature_df.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .copy()
    )

    latest_rows["predicted_price"] = model.predict(latest_rows[feature_columns])
    latest_rows["current_price"] = latest_rows["close"]
    latest_rows["predicted_return"] = (
        (latest_rows["predicted_price"] - latest_rows["current_price"])
        / latest_rows["current_price"]
    )

    latest_rows["model_mae"] = metrics["mae"]
    latest_rows["model_confidence"] = 1 / (1 + metrics["mae"])

    predictions = latest_rows[
        [
            "ticker",
            "current_price",
            "predicted_price",
            "predicted_return",
            "model_mae",
            "model_confidence",
        ]
    ].copy()

    conn = get_connection()
    predictions.to_sql("predictions", conn, if_exists="replace", index=False)
    conn.close()

    return predictions.sort_values("predicted_return", ascending=False).reset_index(drop=True)


if __name__ == "__main__":
    preds = generate_predictions()
    print(preds.head(10))