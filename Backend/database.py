from pathlib import Path
import sqlite3

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "signal100.db"


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stocks (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (ticker, date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS features (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            sma_5 REAL,
            sma_10 REAL,
            sma_20 REAL,
            ema_5 REAL,
            ema_10 REAL,
            rsi REAL,
            daily_return REAL,
            volatility REAL,
            price_t_1 REAL,
            price_t_2 REAL,
            price_t_3 REAL,
            price_t_5 REAL,
            next_day_close REAL,
            PRIMARY KEY (ticker, date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS predictions (
            ticker TEXT PRIMARY KEY,
            current_price REAL,
            predicted_price REAL,
            predicted_return REAL,
            model_mae REAL,
            model_confidence REAL
        )
        """
    )

    conn.commit()
    conn.close()