#!/usr/bin/env python3
"""
Value Stock Screener - Find beaten-down NSE stocks with large order books
and positive catalysts for potential multibagger returns.

Usage:
    python run_value_screener.py                # Full scan
    python run_value_screener.py --quick        # Quick mode (200 stocks)
    python run_value_screener.py --stage 2      # Run analysis only (requires stage 1 data)
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from config import (
    DATA_DIR, RAW_DIR, STOCK_DATA_DIR, PROCESSED_DIR,
    REQUEST_DELAY_SECONDS, BATCH_SIZE, MAX_RETRIES, RETRY_BACKOFF_BASE,
)
from stage1_collect.cache_manager import (
    save_ticker_data, load_ticker_data, load_all_ticker_data,
    get_pending_tickers,
)
from stage1_collect.fetch_nse_stocklist import run as fetch_stocklist

logger = logging.getLogger(__name__)

VALUE_DATA_DIR = RAW_DIR / "value_screen"
VALUE_PROCESSED_DIR = PROCESSED_DIR / "value_screen"


def fetch_52week_data(ticker: str) -> dict:
    """Fetch 52-week high/low, current price, volume, and key info."""
    import yfinance as yf

    stock = yf.Ticker(ticker)
    result = {
        "ticker": ticker,
        "current_price": None,
        "week_52_high": None,
        "week_52_low": None,
        "pct_from_52w_low": None,
        "pct_from_52w_high": None,
        "avg_volume_30d": None,
        "avg_volume_10d": None,
        "market_cap": None,
        "pe_ratio": None,
        "pb_ratio": None,
        "sector": None,
        "industry": None,
        "revenue_growth": None,
        "earnings_growth": None,
        "debt_to_equity": None,
        "roe": None,
        "fetch_timestamp": datetime.now().isoformat(),
        "error": None,
    }

    try:
        info = stock.info or {}
        result["current_price"] = info.get("currentPrice") or info.get("regularMarketPrice")
        result["week_52_high"] = info.get("fiftyTwoWeekHigh")
        result["week_52_low"] = info.get("fiftyTwoWeekLow")
        result["market_cap"] = info.get("marketCap")
        result["pe_ratio"] = info.get("trailingPE")
        result["pb_ratio"] = info.get("priceToBook")
        result["sector"] = info.get("sector")
        result["industry"] = info.get("industry")
        result["revenue_growth"] = info.get("revenueGrowth")
        result["earnings_growth"] = info.get("earningsGrowth")
        result["debt_to_equity"] = info.get("debtToEquity")
        result["roe"] = info.get("returnOnEquity")
        result["avg_volume_30d"] = info.get("averageVolume")
        result["avg_volume_10d"] = info.get("averageVolume10days")

        # Convert USD market cap to INR if needed
        if info.get("currency") == "USD" and result["market_cap"]:
            result["market_cap"] = result["market_cap"] * 83.0

        # Compute distance from 52-week extremes
        price = result["current_price"]
        if price and result["week_52_low"] and result["week_52_low"] > 0:
            result["pct_from_52w_low"] = (price - result["week_52_low"]) / result["week_52_low"] * 100
        if price and result["week_52_high"] and result["week_52_high"] > 0:
            result["pct_from_52w_high"] = (price - result["week_52_high"]) / result["week_52_high"] * 100

    except Exception as e:
        result["error"] = str(e)
        logger.warning(f"Error fetching {ticker}: {e}")

    return result


def run_stage1(tickers: list[str]) -> None:
    """Fetch 52-week data for all tickers."""
    VALUE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    pending = get_pending_tickers(tickers, VALUE_DATA_DIR)
    total = len(tickers)
    done = total - len(pending)

    if done > 0:
        logger.info(f"Resuming: {done}/{total} cached, {len(pending)} remaining")

    for i, ticker in enumerate(pending, 1):
        retries = 0
        while retries <= MAX_RETRIES:
            try:
                data = fetch_52week_data(ticker)
                save_ticker_data(VALUE_DATA_DIR, ticker, data)
                break
            except Exception as e:
                retries += 1
                if retries > MAX_RETRIES:
                    save_ticker_data(VALUE_DATA_DIR, ticker, {
                        "ticker": ticker, "error": str(e),
                        "fetch_timestamp": datetime.now().isoformat(),
                    })
                    break
                time.sleep(RETRY_BACKOFF_BASE ** retries)

        if i % BATCH_SIZE == 0 or i == len(pending):
            logger.info(f"Progress: {done + i}/{total} ({(done + i)/total*100:.1f}%)")
        time.sleep(REQUEST_DELAY_SECONDS)


def run_stage2() -> pd.DataFrame:
    """Analyze: find beaten-down stocks near 52-week lows with high volume."""
    raw_data = load_all_ticker_data(VALUE_DATA_DIR)
    if not raw_data:
        raise ValueError("No data found. Run stage 1 first.")

    df = pd.DataFrame(raw_data)
    logger.info(f"Loaded {len(df)} stocks")

    # Convert numeric columns
    numeric_cols = [
        "current_price", "week_52_high", "week_52_low", "pct_from_52w_low",
        "pct_from_52w_high", "avg_volume_30d", "avg_volume_10d", "market_cap",
        "pe_ratio", "pb_ratio", "revenue_growth", "earnings_growth",
        "debt_to_equity", "roe",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Filter: must have valid price and 52-week data
    df = df[df["current_price"].notna() & (df["current_price"] > 0)].copy()
    df = df[df["week_52_high"].notna() & df["week_52_low"].notna()].copy()
    logger.info(f"After valid data filter: {len(df)} stocks")

    # Filter: market cap > Rs 500 Cr (5 billion)
    df = df[df["market_cap"].notna() & (df["market_cap"] >= 500_00_00_000)].copy()
    logger.info(f"After market cap filter (>500 Cr): {len(df)} stocks")

    # Compute beaten-down score
    df["beaten_down_pct"] = (
        (df["week_52_high"] - df["current_price"]) / df["week_52_high"] * 100
    )

    # Filter: at least 25% below 52-week high
    beaten = df[df["beaten_down_pct"] >= 25].copy()
    logger.info(f"Stocks 25%+ below 52W high: {len(beaten)}")

    # Filter: within 20% of 52-week low
    near_low = beaten[beaten["pct_from_52w_low"] <= 20].copy()
    logger.info(f"Stocks within 20% of 52W low: {len(near_low)}")

    # Sort by volume (institutional interest proxy)
    near_low = near_low.sort_values("avg_volume_30d", ascending=False)

    # Add value score
    near_low["value_score"] = (
        near_low["beaten_down_pct"] * 0.3 +  # More beaten = higher score
        (100 - near_low["pct_from_52w_low"].clip(0, 100)) * 0.3 +  # Closer to low = higher
        near_low["avg_volume_30d"].rank(pct=True) * 30 * 0.2 +  # Higher volume = higher
        near_low["revenue_growth"].fillna(0).clip(-1, 5).rank(pct=True) * 30 * 0.2  # Revenue growth
    )

    near_low = near_low.sort_values("value_score", ascending=False)

    # Save results
    VALUE_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    display_cols = [
        "ticker", "sector", "current_price", "week_52_high", "week_52_low",
        "beaten_down_pct", "pct_from_52w_low", "avg_volume_30d", "market_cap",
        "pe_ratio", "pb_ratio", "revenue_growth", "earnings_growth",
        "debt_to_equity", "roe", "value_score",
    ]
    cols = [c for c in display_cols if c in near_low.columns]
    output = near_low[cols].head(50)
    output.to_csv(VALUE_PROCESSED_DIR / "beaten_down_high_volume.csv", index=False)

    # Print report
    print("\n" + "=" * 90)
    print("BEATEN-DOWN STOCKS NEAR 52-WEEK LOW WITH HIGH VOLUME")
    print("=" * 90)
    print(f"\nTotal stocks scanned: {len(df)}")
    print(f"Stocks 25%+ below 52W high: {len(beaten)}")
    print(f"Stocks within 20% of 52W low: {len(near_low)}")
    print(f"\nTop 30 by Value Score:\n")

    for i, (_, row) in enumerate(output.head(30).iterrows(), 1):
        mcap_cr = row["market_cap"] / 1e7 if pd.notna(row["market_cap"]) else 0
        rev_g = f"{row['revenue_growth']*100:.1f}%" if pd.notna(row["revenue_growth"]) else "N/A"
        print(
            f"{i:3d}. {row['ticker']:20s} | Price: Rs {row['current_price']:>8.2f} | "
            f"52W H/L: {row['week_52_high']:>8.2f}/{row['week_52_low']:>8.2f} | "
            f"Down: {row['beaten_down_pct']:>5.1f}% | "
            f"MCap: {mcap_cr:>10,.0f} Cr | "
            f"Vol: {row['avg_volume_30d']:>12,.0f} | "
            f"RevG: {rev_g}"
        )

    # Sector breakdown
    if "sector" in near_low.columns:
        print("\n" + "-" * 60)
        print("SECTOR BREAKDOWN OF BEATEN-DOWN STOCKS")
        print("-" * 60)
        sector_counts = near_low["sector"].value_counts().head(10)
        for sector, count in sector_counts.items():
            avg_down = near_low[near_low["sector"] == sector]["beaten_down_pct"].mean()
            print(f"  {sector}: {count} stocks (avg {avg_down:.1f}% below 52W high)")

    logger.info(f"Results saved to {VALUE_PROCESSED_DIR}")
    return near_low


def main():
    parser = argparse.ArgumentParser(description="Value Stock Screener")
    parser.add_argument("--stage", type=int, choices=[1, 2])
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(DATA_DIR / "value_screener.log"),
        ],
    )
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    start = time.time()

    if args.stage is None or args.stage == 1:
        _, tickers, _ = fetch_stocklist()
        if args.quick:
            tickers = tickers[:args.limit]
        run_stage1(tickers)

    if args.stage is None or args.stage == 2:
        run_stage2()

    logger.info(f"Done in {(time.time() - start) / 60:.1f} minutes")


if __name__ == "__main__":
    main()
