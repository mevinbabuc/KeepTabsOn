"""Fetch dividend and price data for all tickers using yfinance."""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import yfinance as yf

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    FETCH_START_DATE,
    ANALYSIS_END_DATE,
    REQUEST_DELAY_SECONDS,
    BATCH_SIZE,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    STOCK_DATA_DIR,
    USD_TO_INR,
)
from stage1_collect.cache_manager import (
    get_pending_tickers,
    save_ticker_data,
)

logger = logging.getLogger(__name__)


def fetch_single_ticker(ticker: str) -> dict:
    """
    Fetch both dividend and price data in one yfinance session per ticker.

    Returns a dict with all extracted data for storage.
    """
    stock = yf.Ticker(ticker)

    result = {
        "ticker": ticker,
        "dividends": [],
        "current_price": None,
        "price_2y_ago": None,
        "price_1y_ago": None,
        "avg_daily_volume_30d": None,
        "market_cap": None,
        "pe_ratio": None,
        "sector": None,
        "industry": None,
        "currency": None,
        "fetch_timestamp": datetime.now().isoformat(),
        "error": None,
    }

    try:
        # Fetch historical data (includes dividends in the Dividends column)
        hist = stock.history(
            start=str(FETCH_START_DATE),
            end=str(ANALYSIS_END_DATE),
            actions=True,
        )

        if hist.empty:
            result["error"] = "No historical data available"
            return result

        # Extract dividends
        if "Dividends" in hist.columns:
            div_data = hist[hist["Dividends"] > 0]["Dividends"]
            result["dividends"] = [
                {"date": str(idx.date()), "amount": float(val)}
                for idx, val in div_data.items()
            ]

        # Extract prices
        if "Close" in hist.columns and len(hist) > 0:
            result["current_price"] = float(hist["Close"].iloc[-1])

            # Price 2 years ago (find closest trading day)
            two_years_ago = str(FETCH_START_DATE + (ANALYSIS_END_DATE - FETCH_START_DATE) / 3)
            # Actually use ANALYSIS_START_DATE (2 years ago)
            from config import ANALYSIS_START_DATE, ONE_YEAR_AGO
            target_2y = str(ANALYSIS_START_DATE)
            target_1y = str(ONE_YEAR_AGO)

            # Find closest available price to 2 years ago
            mask_2y = hist.index >= str(ANALYSIS_START_DATE)
            if mask_2y.any():
                result["price_2y_ago"] = float(hist.loc[mask_2y, "Close"].iloc[0])

            # Find closest available price to 1 year ago
            mask_1y = hist.index >= str(ONE_YEAR_AGO)
            if mask_1y.any():
                result["price_1y_ago"] = float(hist.loc[mask_1y, "Close"].iloc[0])

        # Extract volume
        if "Volume" in hist.columns and len(hist) >= 30:
            result["avg_daily_volume_30d"] = float(hist["Volume"].tail(30).mean())
        elif "Volume" in hist.columns and len(hist) > 0:
            result["avg_daily_volume_30d"] = float(hist["Volume"].mean())

    except Exception as e:
        result["error"] = f"History fetch error: {str(e)}"
        logger.warning(f"Error fetching history for {ticker}: {e}")

    # Fetch info (market cap, P/E, sector) - separate try block as this can fail independently
    try:
        info = stock.info
        if info:
            result["market_cap"] = info.get("marketCap")
            result["pe_ratio"] = info.get("trailingPE")
            result["sector"] = info.get("sector")
            result["industry"] = info.get("industry")
            result["currency"] = info.get("currency")

            # Convert USD market cap to INR if needed
            if result["currency"] == "USD" and result["market_cap"]:
                result["market_cap"] = result["market_cap"] * USD_TO_INR

            # Fallback for current price from info
            if result["current_price"] is None:
                result["current_price"] = info.get("currentPrice") or info.get(
                    "regularMarketPrice"
                )
    except Exception as e:
        logger.warning(f"Error fetching info for {ticker}: {e}")
        if result["error"] is None:
            result["error"] = f"Info fetch error: {str(e)}"

    return result


def fetch_all_data(tickers: list[str]) -> None:
    """
    Iterate all tickers with rate limiting and checkpointing.
    Resumable: skips tickers already cached.
    """
    pending = get_pending_tickers(tickers, STOCK_DATA_DIR)
    total = len(tickers)
    already_done = total - len(pending)

    if already_done > 0:
        logger.info(f"Resuming: {already_done}/{total} already cached, {len(pending)} remaining")
    else:
        logger.info(f"Starting fresh: {len(pending)} tickers to fetch")

    STOCK_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for i, ticker in enumerate(pending, 1):
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                data = fetch_single_ticker(ticker)
                save_ticker_data(STOCK_DATA_DIR, ticker, data)
                break
            except Exception as e:
                retries += 1
                if retries > MAX_RETRIES:
                    logger.error(f"Failed {ticker} after {MAX_RETRIES} retries: {e}")
                    save_ticker_data(STOCK_DATA_DIR, ticker, {
                        "ticker": ticker,
                        "dividends": [],
                        "error": f"Max retries exceeded: {str(e)}",
                        "fetch_timestamp": datetime.now().isoformat(),
                    })
                    break
                wait_time = RETRY_BACKOFF_BASE ** retries
                logger.warning(f"Retry {retries}/{MAX_RETRIES} for {ticker}, waiting {wait_time}s")
                time.sleep(wait_time)

        # Progress logging
        if i % BATCH_SIZE == 0 or i == len(pending):
            done = already_done + i
            logger.info(f"Progress: {done}/{total} ({done/total*100:.1f}%)")

        # Rate limiting
        time.sleep(REQUEST_DELAY_SECONDS)


def run(tickers: list[str]) -> None:
    """Entry point for data fetching."""
    fetch_all_data(tickers)
