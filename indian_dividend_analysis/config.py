"""Central configuration for the Indian Dividend Analysis pipeline."""

from pathlib import Path
from datetime import date, timedelta

# === Paths ===
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
STOCK_DATA_DIR = RAW_DIR / "stock_data"
PROCESSED_DIR = DATA_DIR / "processed"

# === Date Range ===
ANALYSIS_END_DATE = date.today()
ANALYSIS_START_DATE = ANALYSIS_END_DATE - timedelta(days=2 * 365)  # 2 years back
FETCH_START_DATE = ANALYSIS_END_DATE - timedelta(days=3 * 365)  # 3 years for growth calc
ONE_YEAR_AGO = ANALYSIS_END_DATE - timedelta(days=365)

# === NSE Data Source ===
NSE_EQUITY_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# === yfinance Rate Limiting ===
REQUEST_DELAY_SECONDS = 2.0
BATCH_SIZE = 50
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 5

# === Quality Filters ===
MIN_MARKET_CAP = 500_00_00_000  # Rs 500 crore
MIN_PRICE = 10.0
MIN_AVG_VOLUME = 10_000
MIN_DIVIDENDS_IN_2Y = 2
MAX_PAYOUT_RATIO = 1.5  # 150%

# === Report Settings ===
TOP_N = 50

# === USD to INR (approximate, for market cap conversion if needed) ===
USD_TO_INR = 83.0
