"""Detect anomalies that may make dividend yields misleading."""

import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import MAX_PAYOUT_RATIO, ANALYSIS_START_DATE, STOCK_DATA_DIR
from stage1_collect.cache_manager import load_ticker_data

logger = logging.getLogger(__name__)


def detect_special_dividends(ticker: str) -> list[dict]:
    """
    Flag one-time special dividends that inflate yield.

    A dividend is flagged as "special" if it's > 3x the median dividend amount.
    """
    data = load_ticker_data(STOCK_DATA_DIR, ticker)
    if not data or not data.get("dividends"):
        return []

    dividends = data["dividends"]
    if len(dividends) < 2:
        return []

    amounts = [d["amount"] for d in dividends]
    median_amount = float(np.median(amounts))

    if median_amount <= 0:
        return []

    flagged = []
    for d in dividends:
        if d["amount"] > 3 * median_amount:
            flagged.append({
                "date": d["date"],
                "amount": d["amount"],
                "median": median_amount,
                "ratio_to_median": d["amount"] / median_amount,
            })

    return flagged


def detect_unsustainable_payout(row: pd.Series) -> bool:
    """Flag if payout_ratio > MAX_PAYOUT_RATIO (150%)."""
    payout = row.get("payout_ratio")
    if pd.isna(payout) or payout is None:
        return False
    return payout > MAX_PAYOUT_RATIO


def detect_price_decline_inflating_yield(row: pd.Series) -> bool:
    """
    Flag if yield is high primarily because price crashed.
    If capital_appreciation_2y < -30% AND dividend_yield_ttm > 5%.
    """
    cap_app = row.get("capital_appreciation_2y")
    div_yield = row.get("dividend_yield_ttm")

    if pd.isna(cap_app) or pd.isna(div_yield):
        return False
    return cap_app < -30 and div_yield > 5


def detect_windfall_dividend(row: pd.Series) -> bool:
    """
    Flag mining/metals companies with possible commodity windfall dividends.
    """
    sector = str(row.get("sector", "")).lower()
    is_commodity = any(s in sector for s in ["metal", "mining", "basic materials", "energy"])

    if not is_commodity:
        return False

    # Check if growth rate suggests a windfall (huge growth then potential decline)
    growth = row.get("dividend_growth_rate")
    yield_val = row.get("dividend_yield_ttm", 0)

    if pd.isna(growth):
        return yield_val > 8  # High yield in commodity sector is suspicious

    return growth > 100 or (growth < -30 and yield_val > 5)


def annotate_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Add anomaly flag columns to the DataFrame."""
    df = df.copy()

    df["is_unsustainable_payout"] = df.apply(detect_unsustainable_payout, axis=1)
    df["is_price_decline_inflated"] = df.apply(detect_price_decline_inflating_yield, axis=1)
    df["is_commodity_windfall"] = df.apply(detect_windfall_dividend, axis=1)

    # Check for special dividends (slower - loads individual JSON files)
    special_div_flags = []
    for _, row in df.iterrows():
        ticker = row["ticker"]
        specials = detect_special_dividends(ticker)
        special_div_flags.append(len(specials) > 0)
    df["has_special_dividend"] = special_div_flags

    # Combine into summary column
    def summarize_flags(row):
        flags = []
        if row["is_unsustainable_payout"]:
            flags.append("UNSUSTAINABLE_PAYOUT")
        if row["is_price_decline_inflated"]:
            flags.append("PRICE_DECLINE_INFLATED")
        if row["is_commodity_windfall"]:
            flags.append("COMMODITY_WINDFALL")
        if row["has_special_dividend"]:
            flags.append("SPECIAL_DIVIDEND")
        return ", ".join(flags) if flags else ""

    df["anomaly_flags"] = df.apply(summarize_flags, axis=1)

    flagged_count = len(df[df["anomaly_flags"] != ""])
    logger.info(f"Anomalies detected in {flagged_count}/{len(df)} stocks")

    return df


def run(df: pd.DataFrame) -> pd.DataFrame:
    """Entry point: detect and annotate all anomalies."""
    print("\n" + "=" * 80)
    print("ANOMALY DETECTION RESULTS")
    print("=" * 80)

    annotated = annotate_anomalies(df)

    # Print summary
    for flag_col in ["is_unsustainable_payout", "is_price_decline_inflated",
                     "is_commodity_windfall", "has_special_dividend"]:
        count = annotated[flag_col].sum()
        print(f"  {flag_col}: {count} stocks flagged")

    # Show flagged stocks
    flagged = annotated[annotated["anomaly_flags"] != ""]
    if len(flagged) > 0:
        print(f"\nFlagged stocks ({len(flagged)}):")
        for _, row in flagged.iterrows():
            print(f"  {row['ticker']}: {row['anomaly_flags']} "
                  f"(yield: {row.get('dividend_yield_ttm', 'N/A')}%)")

    return annotated
