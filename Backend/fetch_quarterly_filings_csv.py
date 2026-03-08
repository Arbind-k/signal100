import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SP500_FILE = DATA_DIR / "sp500_companies.csv"
OUT_FILE = DATA_DIR / "quarterly_filings.csv"

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

DEFAULT_USER_AGENT = "Signal100Hackathon/1.0 your_email@example.com"
REQUEST_DELAY_SECONDS = 0.2
VALID_FORMS = {"10-Q", "10-Q/A"}


def sec_headers() -> dict[str, str]:
    user_agent = os.getenv("SEC_USER_AGENT", DEFAULT_USER_AGENT)
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
    }


def normalize_ticker(symbol: str) -> str:
    return str(symbol).strip().upper().replace(".", "-")


def fetch_json(url: str) -> Any:
    response = requests.get(url, headers=sec_headers(), timeout=30)
    response.raise_for_status()
    return response.json()


def load_sp500_tickers() -> pd.DataFrame:
    if not SP500_FILE.exists():
        raise FileNotFoundError(
            f"Missing {SP500_FILE}. Run fetch_sp500_list.py first."
        )
    df = pd.read_csv(SP500_FILE)
    df["ticker"] = df["ticker"].map(normalize_ticker)
    return df


def build_cik_map() -> dict[str, str]:
    raw = fetch_json(COMPANY_TICKERS_URL)
    cik_map: dict[str, str] = {}
    for _, item in raw.items():
        ticker = normalize_ticker(item.get("ticker", ""))
        cik = str(item.get("cik_str", "")).zfill(10)
        if ticker:
            cik_map[ticker] = cik
    return cik_map


def extract_recent_quarterly_filings(ticker: str, cik: str, limit_per_ticker: int) -> list[dict[str, Any]]:
    url = SUBMISSIONS_URL.format(cik=cik)
    payload = fetch_json(url)

    company_name = payload.get("name")
    sic = payload.get("sic")
    sic_description = payload.get("sicDescription")
    recent = payload.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])

    rows: list[dict[str, Any]] = []
    for idx, form in enumerate(forms):
        if form not in VALID_FORMS:
            continue

        accession = recent.get("accessionNumber", [None])[idx]
        filing_date = recent.get("filingDate", [None])[idx]
        report_date = recent.get("reportDate", [None])[idx]
        primary_document = recent.get("primaryDocument", [None])[idx]
        primary_description = recent.get("primaryDocDescription", [None])[idx]
        accession_nodashes = str(accession).replace("-", "") if accession else None
        filing_url = None
        if accession_nodashes and primary_document:
            filing_url = (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{int(cik)}/{accession_nodashes}/{primary_document}"
            )

        rows.append(
            {
                "ticker": ticker,
                "company_name": company_name,
                "cik": cik,
                "sic": sic,
                "sic_description": sic_description,
                "form": form,
                "filing_date": filing_date,
                "report_date": report_date,
                "accession_number": accession,
                "primary_document": primary_document,
                "primary_doc_description": primary_description,
                "filing_url": filing_url,
            }
        )
        if len(rows) >= limit_per_ticker:
            break

    return rows


def main(limit_per_ticker: int = 4) -> None:
    sp500 = load_sp500_tickers()
    cik_map = build_cik_map()

    all_rows: list[dict[str, Any]] = []
    missing: list[str] = []

    for ticker in sp500["ticker"].tolist():
        cik = cik_map.get(ticker)
        if not cik:
            missing.append(ticker)
            continue
        try:
            rows = extract_recent_quarterly_filings(ticker, cik, limit_per_ticker=limit_per_ticker)
            all_rows.extend(rows)
        except Exception as exc:
            print(f"Skipping {ticker}: {exc}")
        time.sleep(REQUEST_DELAY_SECONDS)

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df = df.sort_values(["ticker", "filing_date"], ascending=[True, False]).reset_index(drop=True)
    df.to_csv(OUT_FILE, index=False)

    print(f"Saved {len(df)} filing rows to {OUT_FILE}")
    if missing:
        print(f"No SEC CIK match for {len(missing)} tickers")


if __name__ == "__main__":
    main()
