"""Calculate all dividend and return metrics per stock."""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    STOCK_DATA_DIR,
    PROCESSED_DIR,
    ANALYSIS_START_DATE,
    ANALYSIS_END_DATE,
    ONE_YEAR_AGO,
    MAX_PAYOUT_RATIO,
)
from stage1_collect.cache_manager import load_all_ticker_data

logger = logging.getLogger(__name__)


def load_all_data() -> pd.DataFrame:
    """Load all cached ticker data into a single DataFrame."""
    raw_data = load_all_ticker_data(STOCK_DATA_DIR)
    if not raw_data:
        raise ValueError("No cached data found. Run stage 1 first.")

    rows = []
    for item in raw_data:
        if item.get("error") and not item.get("current_price"):
            continue  # Skip completely failed fetches

        rows.append({
            "ticker": item.get("ticker", ""),
            "current_price": item.get("current_price"),
            "price_2y_ago": item.get("price_2y_ago"),
            "price_1y_ago": item.get("price_1y_ago"),
            "market_cap": item.get("market_cap"),
            "pe_ratio": item.get("pe_ratio"),
            "sector": item.get("sector"),
            "industry": item.get("industry"),
            "avg_volume": item.get("avg_daily_volume_30d"),
            "dividends_raw": item.get("dividends", []),
        })

    df = pd.DataFrame(rows)
    logger.info(f"Loaded data for {len(df)} stocks")
    return df


def _parse_dividends(dividends_raw: list[dict]) -> list[dict]:
    """Parse dividend list, ensuring dates are date objects."""
    parsed = []
    for d in dividends_raw:
        try:
            div_date = date.fromisoformat(str(d["date"]))
            parsed.append({"date": div_date, "amount": float(d["amount"])})
        except (ValueError, KeyError):
            continue
    return parsed


def compute_dividend_yield_ttm(row) -> float | None:
    """Trailing 12-month dividend yield as percentage."""
    dividends = _parse_dividends(row["dividends_raw"])
    price = row["current_price"]
    if not price or price <= 0:
        return None

    recent = [d["amount"] for d in dividends if d["date"] >= ONE_YEAR_AGO]
    if not recent:
        return 0.0
    return sum(recent) / price * 100


def compute_dividend_yield_2y_avg(row) -> float | None:
    """Average annual dividend yield over 2 years."""
    dividends = _parse_dividends(row["dividends_raw"])
    price = row["current_price"]
    price_2y = row["price_2y_ago"]
    if not price or not price_2y or price <= 0 or price_2y <= 0:
        return None

    divs_2y = [d["amount"] for d in dividends if d["date"] >= ANALYSIS_START_DATE]
    if not divs_2y:
        return 0.0
    avg_annual_div = sum(divs_2y) / 2.0
    avg_price = (price + price_2y) / 2.0
    return avg_annual_div / avg_price * 100


def compute_total_dividends_2y(row) -> float:
    """Sum of all dividend payments in the 2-year window."""
    dividends = _parse_dividends(row["dividends_raw"])
    return sum(d["amount"] for d in dividends if d["date"] >= ANALYSIS_START_DATE)


def compute_dividend_count_2y(row) -> int:
    """Number of dividend payments in 2-year window."""
    dividends = _parse_dividends(row["dividends_raw"])
    return len([d for d in dividends if d["date"] >= ANALYSIS_START_DATE])


def compute_dividend_growth_rate(row) -> float | None:
    """Year-over-year dividend growth as percentage."""
    dividends = _parse_dividends(row["dividends_raw"])

    year1_divs = sum(
        d["amount"] for d in dividends
        if ANALYSIS_START_DATE <= d["date"] < ONE_YEAR_AGO
    )
    year2_divs = sum(
        d["amount"] for d in dividends
        if d["date"] >= ONE_YEAR_AGO
    )

    if year1_divs <= 0:
        return None  # Cannot compute growth from zero
    return (year2_divs - year1_divs) / year1_divs * 100


def compute_total_return_2y(row) -> float | None:
    """Total return (capital appreciation + dividends) over 2 years as percentage."""
    price = row["current_price"]
    price_2y = row["price_2y_ago"]
    if not price or not price_2y or price_2y <= 0:
        return None

    total_divs = compute_total_dividends_2y(row)
    return ((price - price_2y) + total_divs) / price_2y * 100


def compute_capital_appreciation_2y(row) -> float | None:
    """Price change over 2 years as percentage."""
    price = row["current_price"]
    price_2y = row["price_2y_ago"]
    if not price or not price_2y or price_2y <= 0:
        return None
    return (price - price_2y) / price_2y * 100


def compute_dividend_contribution_pct(row) -> float | None:
    """What percentage of total return came from dividends."""
    total_return = compute_total_return_2y(row)
    if total_return is None or total_return == 0:
        return None

    total_divs = compute_total_dividends_2y(row)
    price_2y = row["price_2y_ago"]
    if not price_2y or price_2y <= 0:
        return None

    total_gain = (row["current_price"] - price_2y) + total_divs
    if total_gain <= 0:
        return None
    return (total_divs / total_gain) * 100


def compute_payout_ratio(row) -> float | None:
    """Estimate payout ratio using P/E as proxy. Returns ratio (not percentage)."""
    pe = row["pe_ratio"]
    price = row["current_price"]
    if not pe or not price or pe <= 0 or price <= 0:
        return None

    dividends = _parse_dividends(row["dividends_raw"])
    annual_divs = sum(d["amount"] for d in dividends if d["date"] >= ONE_YEAR_AGO)
    if annual_divs <= 0:
        return 0.0

    eps = price / pe
    if eps <= 0:
        return None
    return annual_divs / eps


def compute_consistency_score(row) -> float:
    """
    Score 0-100 measuring dividend regularity.

    Components:
    - Paid dividends in both years: +40 points
    - Number of payments (quarterly=4/yr gets max): +30 points
    - Dividend growth positive: +15 points
    - No single dividend > 60% of 2-year total (no special div reliance): +15 points
    """
    dividends = _parse_dividends(row["dividends_raw"])
    divs_2y = [d for d in dividends if d["date"] >= ANALYSIS_START_DATE]

    if not divs_2y:
        return 0.0

    score = 0.0

    # Paid in both years? (+40)
    year1 = [d for d in divs_2y if d["date"] < ONE_YEAR_AGO]
    year2 = [d for d in divs_2y if d["date"] >= ONE_YEAR_AGO]
    if year1 and year2:
        score += 40.0

    # Number of payments (+30, scaled: 8 payments in 2yr = max)
    count = len(divs_2y)
    score += min(count / 8.0, 1.0) * 30.0

    # Dividend growth positive (+15)
    growth = compute_dividend_growth_rate(row)
    if growth is not None and growth >= 0:
        score += 15.0

    # No over-reliance on single large dividend (+15)
    total = sum(d["amount"] for d in divs_2y)
    if total > 0:
        max_single = max(d["amount"] for d in divs_2y)
        if max_single / total <= 0.6:
            score += 15.0

    return round(score, 2)


def compute_all_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Apply all metric functions, return enriched DataFrame."""
    logger.info("Computing metrics for all stocks...")

    df["dividend_yield_ttm"] = df.apply(compute_dividend_yield_ttm, axis=1)
    df["dividend_yield_2y_avg"] = df.apply(compute_dividend_yield_2y_avg, axis=1)
    df["total_dividends_2y"] = df.apply(compute_total_dividends_2y, axis=1)
    df["dividend_count_2y"] = df.apply(compute_dividend_count_2y, axis=1)
    df["dividend_growth_rate"] = df.apply(compute_dividend_growth_rate, axis=1)
    df["total_return_2y"] = df.apply(compute_total_return_2y, axis=1)
    df["capital_appreciation_2y"] = df.apply(compute_capital_appreciation_2y, axis=1)
    df["dividend_contribution_pct"] = df.apply(compute_dividend_contribution_pct, axis=1)
    df["payout_ratio"] = df.apply(compute_payout_ratio, axis=1)
    df["consistency_score"] = df.apply(compute_consistency_score, axis=1)

    # Drop the raw dividends column from output
    output_df = df.drop(columns=["dividends_raw"])

    logger.info(f"Metrics computed for {len(output_df)} stocks")
    return output_df


def run() -> pd.DataFrame:
    """Entry point: load data, compute metrics, save to CSV."""
    df = load_all_data()
    metrics_df = compute_all_metrics(df)

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = PROCESSED_DIR / "metrics_all.csv"
    metrics_df.to_csv(output_path, index=False)
    logger.info(f"Saved metrics to {output_path}")
    return metrics_df
