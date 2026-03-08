from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SP500_FILE = DATA_DIR / "sp500_companies.csv"
OUT_FILE = DATA_DIR / "fundamentals_peg.csv"
REQUEST_DELAY_SECONDS = 0.15


def normalize_ticker(symbol: str) -> str:
    return str(symbol).strip().upper().replace(".", "-")


def load_sp500_tickers() -> list[str]:
    if not SP500_FILE.exists():
        raise FileNotFoundError(
            f"Missing {SP500_FILE}. Run fetch_sp500_list.py first."
        )
    df = pd.read_csv(SP500_FILE)
    return [normalize_ticker(x) for x in df["ticker"].dropna().tolist()]


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None and not (isinstance(value, float) and math.isnan(value)):
            return value
    return None


def compute_peg(trailing_pe: Any, growth_decimal: Any) -> float | None:
    try:
        pe = float(trailing_pe)
        growth = float(growth_decimal)
    except (TypeError, ValueError):
        return None

    if growth <= 0:
        return None

    growth_pct = growth * 100.0
    if growth_pct == 0:
        return None

    return pe / growth_pct


def get_info(ticker: str) -> dict[str, Any]:
    t = yf.Ticker(ticker)
    return t.info or {}


def build_row(ticker: str) -> dict[str, Any]:
    info = get_info(ticker)

    company_name = first_not_none(info.get("longName"), info.get("shortName"), ticker)
    current_price = first_not_none(info.get("currentPrice"), info.get("regularMarketPrice"))
    market_cap = info.get("marketCap")
    sector = info.get("sector")
    industry = info.get("industry")
    trailing_pe = first_not_none(info.get("trailingPE"), info.get("trailingPe"))
    forward_pe = first_not_none(info.get("forwardPE"), info.get("forwardPe"))
    eps_current_year = info.get("epsCurrentYear")
    eps_forward = info.get("forwardEps")
    earnings_growth = first_not_none(
        info.get("earningsGrowth"),
        info.get("earningsQuarterlyGrowth"),
    )
    revenue_growth = info.get("revenueGrowth")
    raw_peg = first_not_none(info.get("pegRatio"), info.get("trailingPeg"))
    peg_fallback = compute_peg(trailing_pe, earnings_growth)
    peg_final = first_not_none(raw_peg, peg_fallback)

    return {
        "ticker": ticker,
        "company_name": company_name,
        "sector": sector,
        "industry": industry,
        "current_price": current_price,
        "market_cap": market_cap,
        "trailing_pe": trailing_pe,
        "forward_pe": forward_pe,
        "eps_current_year": eps_current_year,
        "forward_eps": eps_forward,
        "earnings_growth_decimal": earnings_growth,
        "revenue_growth_decimal": revenue_growth,
        "peg_ratio_raw": raw_peg,
        "peg_ratio_fallback": peg_fallback,
        "peg_ratio_final": peg_final,
    }


def main() -> None:
    rows = []
    for ticker in load_sp500_tickers():
        try:
            rows.append(build_row(ticker))
        except Exception as exc:
            print(f"Skipping {ticker}: {exc}")
        time.sleep(REQUEST_DELAY_SECONDS)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("ticker").reset_index(drop=True)
    df.to_csv(OUT_FILE, index=False)
    print(f"Saved {len(df)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
