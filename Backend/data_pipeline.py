from pathlib import Path
import pandas as pd

from database import init_db, get_connection
from feature_engineering import add_features, merge_external_features

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
FUNDAMENTALS_PATH = BASE_DIR / "data" / "raw" / "fundamentals_peg.csv"
FILINGS_PATH = BASE_DIR / "data" / "raw" / "quarterly_filings.csv"

EXPECTED_FINAL_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "volume"]


def clean_single_stock_file(file_path: Path) -> pd.DataFrame:
    raw_df = pd.read_csv(file_path)

    # Kaggle/Yahoo-style format:
    # row 0 = ticker row
    # row 1 = date/header row
    # row 2+ = actual data
    df = raw_df.iloc[2:].copy().reset_index(drop=True)

    df.columns = ["date", "close", "high", "low", "open", "volume"]

    ticker = file_path.stem.upper()
    df["ticker"] = ticker
    df = df[["ticker", "date", "open", "high", "low", "close", "volume"]]

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=EXPECTED_FINAL_COLUMNS)
    df = df.sort_values("date")
    df = df.drop_duplicates(subset=["ticker", "date"])
    return df


def load_all_stock_files() -> pd.DataFrame:
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv")) # add limit here if need
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DATA_DIR}")

    all_frames = []
    for file_path in csv_files:
        try:
            cleaned = clean_single_stock_file(file_path)
            if not cleaned.empty:
                all_frames.append(cleaned)
        except Exception as e:
            print(f"Skipping {file_path.name}: {e}")

    if not all_frames:
        raise ValueError("No valid stock CSV files could be loaded.")

    df = pd.concat(all_frames, ignore_index=True)
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)
    return df


def load_external_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    fundamentals_df = None
    filings_df = None

    if FUNDAMENTALS_PATH.exists():
        fundamentals_df = pd.read_csv(FUNDAMENTALS_PATH)
        print(f"Loaded fundamentals rows: {len(fundamentals_df)}")
    else:
        print(f"Fundamentals file not found at {FUNDAMENTALS_PATH}; continuing without it.")

    if FILINGS_PATH.exists():
        filings_df = pd.read_csv(FILINGS_PATH)
        print(f"Loaded filings rows: {len(filings_df)}")
    else:
        print(f"Filings file not found at {FILINGS_PATH}; continuing without it.")

    return fundamentals_df, filings_df


def load_and_prepare_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    price_df = load_all_stock_files()
    feature_df = add_features(price_df)

    fundamentals_df, filings_df = load_external_data()
    feature_df = merge_external_features(feature_df, fundamentals_df, filings_df)

    feature_df = feature_df.dropna(subset=["next_day_close"]).reset_index(drop=True)
    return price_df, feature_df


def write_to_sqlite(price_df: pd.DataFrame, feature_df: pd.DataFrame):
    init_db()
    conn = get_connection()

    price_df.to_sql("stocks", conn, if_exists="replace", index=False)
    feature_df.to_sql("features", conn, if_exists="replace", index=False)

    conn.close()


if __name__ == "__main__":
    prices, features = load_and_prepare_data()
    write_to_sqlite(prices, features)

    print(f"Loaded {prices['ticker'].nunique()} tickers")
    print(f"Price rows: {len(prices)}")
    print(f"Feature rows: {len(features)}")
    print("Sample columns:")
    print(features.columns.tolist())
