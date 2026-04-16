"""Download and parse the full NSE equity list."""

import logging
import sys
from pathlib import Path

import pandas as pd
import requests

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import NSE_EQUITY_URL, NSE_HEADERS, RAW_DIR

logger = logging.getLogger(__name__)


def download_nse_equity_list() -> pd.DataFrame:
    """
    Download EQUITY_L.csv from NSE archives.

    The CSV has columns: SYMBOL, NAME OF COMPANY, SERIES, DATE OF LISTING,
    PAID UP VALUE, MARKET LOT, ISIN NUMBER, FACE VALUE.

    NSE blocks bot requests, so we need proper headers and potentially cookies.
    """
    # Approach 1: Direct download with browser-like headers
    try:
        session = requests.Session()
        # First visit NSE homepage to get cookies
        session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=10)
        # Then download the CSV
        response = session.get(NSE_EQUITY_URL, headers=NSE_HEADERS, timeout=30)
        response.raise_for_status()
        csv_path = RAW_DIR / "nse_equity_list.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text(response.text)
        logger.info(f"Downloaded NSE equity list to {csv_path}")
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        logger.warning(f"Direct NSE download failed: {e}. Trying alternative...")

    # Approach 2: Try without cookies
    try:
        response = requests.get(NSE_EQUITY_URL, headers=NSE_HEADERS, timeout=30)
        response.raise_for_status()
        csv_path = RAW_DIR / "nse_equity_list.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        csv_path.write_text(response.text)
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        logger.error(f"All NSE download attempts failed: {e}")
        raise


def filter_equity_series(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to only EQ series (regular equity shares)."""
    col_name = None
    for candidate in [" SERIES", "SERIES", "series"]:
        if candidate in df.columns:
            col_name = candidate
            break

    if col_name is None:
        logger.warning("SERIES column not found, returning all stocks")
        return df

    # Strip whitespace from series values
    df[col_name] = df[col_name].str.strip()
    filtered = df[df[col_name] == "EQ"].copy()
    logger.info(f"Filtered to {len(filtered)} EQ series stocks from {len(df)} total")
    return filtered


def get_symbol_column(df: pd.DataFrame) -> str:
    """Find the SYMBOL column name (may have leading space)."""
    for candidate in ["SYMBOL", " SYMBOL", "symbol"]:
        if candidate in df.columns:
            return candidate
    raise ValueError(f"SYMBOL column not found. Columns: {list(df.columns)}")


def get_yfinance_tickers(df: pd.DataFrame) -> list[str]:
    """Convert NSE symbols to yfinance format: 'RELIANCE' -> 'RELIANCE.NS'"""
    sym_col = get_symbol_column(df)
    symbols = df[sym_col].str.strip().tolist()
    return [f"{sym}.NS" for sym in symbols]


def get_company_names(df: pd.DataFrame) -> dict[str, str]:
    """Return mapping of yfinance ticker -> company name."""
    sym_col = get_symbol_column(df)
    name_col = None
    for candidate in ["NAME OF COMPANY", " NAME OF COMPANY", "name"]:
        if candidate in df.columns:
            name_col = candidate
            break

    if name_col is None:
        return {}

    result = {}
    for _, row in df.iterrows():
        ticker = f"{str(row[sym_col]).strip()}.NS"
        result[ticker] = str(row[name_col]).strip()
    return result


def run() -> tuple[pd.DataFrame, list[str], dict[str, str]]:
    """
    Main entry: download, filter, return (dataframe, ticker_list, name_map).
    """
    df = download_nse_equity_list()
    df = filter_equity_series(df)
    tickers = get_yfinance_tickers(df)
    names = get_company_names(df)
    logger.info(f"Total tickers for analysis: {len(tickers)}")
    return df, tickers, names
