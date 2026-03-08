from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import get_connection
from predict import generate_predictions
from utils import load_pickle, METRICS_PATH

app = FastAPI(title="Signal100 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"message": "Signal100 API is running"}


@app.get("/stocks")
def get_stocks():
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT ticker, current_price, predicted_price, predicted_return,
               model_mae, model_confidence
        FROM predictions
        ORDER BY predicted_return DESC
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/winners")
def get_winners():
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT ticker, current_price, predicted_price, predicted_return,
               model_mae, model_confidence
        FROM predictions
        ORDER BY predicted_return DESC
        LIMIT 5
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/losers")
def get_losers():
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT ticker, current_price, predicted_price, predicted_return,
               model_mae, model_confidence
        FROM predictions
        ORDER BY predicted_return ASC
        LIMIT 5
        """
    ).fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/model-metrics")
def get_model_metrics():
    return load_pickle(METRICS_PATH)


@app.post("/refresh")
def refresh_predictions():
    predictions = generate_predictions()
    return {
        "message": "Predictions refreshed successfully",
        "count": int(len(predictions)),
    }