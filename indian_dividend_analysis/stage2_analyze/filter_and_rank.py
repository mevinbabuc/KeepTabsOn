"""Quality filtering and multi-criteria ranking of dividend stocks."""

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    MIN_MARKET_CAP,
    MIN_PRICE,
    MIN_AVG_VOLUME,
    MIN_DIVIDENDS_IN_2Y,
    MAX_PAYOUT_RATIO,
    TOP_N,
    PROCESSED_DIR,
)

logger = logging.getLogger(__name__)


def apply_quality_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove stocks that shouldn't be in a dividend analysis.
    Logs how many stocks are removed at each step.
    """
    initial = len(df)
    logger.info(f"Starting quality filters with {initial} stocks")

    # Market cap filter
    if "market_cap" in df.columns:
        before = len(df)
        df = df[df["market_cap"].notna() & (df["market_cap"] >= MIN_MARKET_CAP)]
        logger.info(f"  Market cap >= Rs {MIN_MARKET_CAP/1e7:.0f} Cr: {before} -> {len(df)}")

    # Price filter
    before = len(df)
    df = df[df["current_price"].notna() & (df["current_price"] >= MIN_PRICE)]
    logger.info(f"  Price >= Rs {MIN_PRICE}: {before} -> {len(df)}")

    # Volume filter
    if "avg_volume" in df.columns:
        before = len(df)
        df = df[df["avg_volume"].notna() & (df["avg_volume"] >= MIN_AVG_VOLUME)]
        logger.info(f"  Avg volume >= {MIN_AVG_VOLUME}: {before} -> {len(df)}")

    # Dividend count filter
    before = len(df)
    df = df[df["dividend_count_2y"] >= MIN_DIVIDENDS_IN_2Y]
    logger.info(f"  Min {MIN_DIVIDENDS_IN_2Y} dividends in 2Y: {before} -> {len(df)}")

    # Must have current dividend yield
    before = len(df)
    df = df[df["dividend_yield_ttm"].notna() & (df["dividend_yield_ttm"] > 0)]
    logger.info(f"  Positive TTM yield: {before} -> {len(df)}")

    # Must have 2-year price history
    before = len(df)
    df = df[df["price_2y_ago"].notna()]
    logger.info(f"  Valid 2Y price history: {before} -> {len(df)}")

    logger.info(f"Quality filters: {initial} -> {len(df)} stocks remaining")
    return df.copy()


def _add_risk_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Add risk flag columns to the DataFrame."""
    flags = []
    for _, row in df.iterrows():
        row_flags = []
        if row.get("payout_ratio") and row["payout_ratio"] > 1.0:
            row_flags.append("HIGH_PAYOUT")
        if row.get("dividend_growth_rate") is not None and row["dividend_growth_rate"] < -20:
            row_flags.append("DECLINING_DIVIDEND")
        if row.get("capital_appreciation_2y") is not None and row["capital_appreciation_2y"] < -30:
            if row.get("dividend_yield_ttm") and row["dividend_yield_ttm"] > 5:
                row_flags.append("PRICE_DECLINE_INFLATING_YIELD")
        flags.append(", ".join(row_flags) if row_flags else "")

    df = df.copy()
    df["risk_flags"] = flags
    return df


def _normalize(series: pd.Series) -> pd.Series:
    """Min-max normalize a series to 0-1 range."""
    min_val = series.min()
    max_val = series.max()
    if max_val == min_val:
        return pd.Series(0.5, index=series.index)
    return (series - min_val) / (max_val - min_val)


def rank_by_yield(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by dividend_yield_ttm descending."""
    ranked = df.sort_values("dividend_yield_ttm", ascending=False).head(TOP_N)
    ranked = ranked.reset_index(drop=True)
    ranked.index = ranked.index + 1  # 1-based ranking
    ranked.index.name = "rank"
    return _add_risk_flags(ranked)


def rank_by_total_return(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by total_return_2y descending."""
    valid = df[df["total_return_2y"].notna()].copy()
    valid["annualized_return"] = ((1 + valid["total_return_2y"] / 100) ** 0.5 - 1) * 100
    ranked = valid.sort_values("total_return_2y", ascending=False).head(TOP_N)
    ranked = ranked.reset_index(drop=True)
    ranked.index = ranked.index + 1
    ranked.index.name = "rank"
    return _add_risk_flags(ranked)


def rank_by_consistency(df: pd.DataFrame) -> pd.DataFrame:
    """Sort by consistency_score descending, then by dividend_yield_ttm."""
    ranked = df.sort_values(
        ["consistency_score", "dividend_yield_ttm"],
        ascending=[False, False],
    ).head(TOP_N)
    ranked = ranked.reset_index(drop=True)
    ranked.index = ranked.index + 1
    ranked.index.name = "rank"
    return _add_risk_flags(ranked)


def rank_composite(df: pd.DataFrame) -> pd.DataFrame:
    """
    Composite ranking combining multiple factors.

    Composite Score = (
        0.30 * normalized(dividend_yield_ttm) +
        0.25 * normalized(total_return_2y) +
        0.20 * normalized(consistency_score) +
        0.15 * normalized(dividend_growth_rate) +
        0.10 * (1 - normalized(payout_ratio))  # Lower payout = more sustainable
    )
    """
    valid = df.copy()

    # Fill NaN for normalization
    for col in ["dividend_yield_ttm", "total_return_2y", "consistency_score",
                "dividend_growth_rate", "payout_ratio"]:
        if col not in valid.columns:
            valid[col] = 0.0

    # Only include rows with enough data
    valid = valid[valid["total_return_2y"].notna()].copy()

    # Clip extreme outliers before normalization
    valid["dividend_growth_rate"] = valid["dividend_growth_rate"].fillna(0).clip(-100, 500)
    valid["payout_ratio"] = valid["payout_ratio"].fillna(0.5).clip(0, 3)

    # Compute composite score
    valid["composite_score"] = (
        0.30 * _normalize(valid["dividend_yield_ttm"].fillna(0)) +
        0.25 * _normalize(valid["total_return_2y"].fillna(0)) +
        0.20 * _normalize(valid["consistency_score"].fillna(0)) +
        0.15 * _normalize(valid["dividend_growth_rate"]) +
        0.10 * (1 - _normalize(valid["payout_ratio"]))
    )

    ranked = valid.sort_values("composite_score", ascending=False).head(TOP_N)
    ranked = ranked.reset_index(drop=True)
    ranked.index = ranked.index + 1
    ranked.index.name = "rank"
    return _add_risk_flags(ranked)


def run() -> dict[str, pd.DataFrame]:
    """
    Entry point: load metrics, filter, produce all rankings.
    Saves each ranking as CSV.
    """
    metrics_path = PROCESSED_DIR / "metrics_all.csv"
    if not metrics_path.exists():
        raise FileNotFoundError(f"Metrics file not found: {metrics_path}. Run stage 2 compute first.")

    df = pd.read_csv(metrics_path)
    logger.info(f"Loaded {len(df)} stocks from metrics file")

    filtered = apply_quality_filters(df)

    rankings = {
        "top50_yield": rank_by_yield(filtered),
        "top50_total_return": rank_by_total_return(filtered),
        "top50_consistent": rank_by_consistency(filtered),
        "top50_composite": rank_composite(filtered),
    }

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    for name, ranked_df in rankings.items():
        path = PROCESSED_DIR / f"{name}.csv"
        ranked_df.to_csv(path)
        logger.info(f"Saved {name} ({len(ranked_df)} stocks) to {path}")

    return rankings
