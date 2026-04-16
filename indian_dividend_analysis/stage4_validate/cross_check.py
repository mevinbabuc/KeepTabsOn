"""Validate analysis results against known high-dividend Indian stocks."""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)

# Known high-dividend Indian stocks with expected yield ranges (as of 2025-2026)
KNOWN_HIGH_DIVIDEND_STOCKS = {
    "VEDL.NS": {"expected_yield_range": (8, 15), "sector": "Metals"},
    "COALINDIA.NS": {"expected_yield_range": (5, 9), "sector": "Mining"},
    "ITC.NS": {"expected_yield_range": (3, 5), "sector": "FMCG"},
    "HINDPETRO.NS": {"expected_yield_range": (3, 8), "sector": "Oil & Gas"},
    "BPCL.NS": {"expected_yield_range": (3, 8), "sector": "Oil & Gas"},
    "CANBK.NS": {"expected_yield_range": (3, 5), "sector": "Banking"},
    "BANKBARODA.NS": {"expected_yield_range": (2, 5), "sector": "Banking"},
    "NHPC.NS": {"expected_yield_range": (3, 6), "sector": "Power"},
    "POWERGRID.NS": {"expected_yield_range": (3, 6), "sector": "Power"},
    "ONGC.NS": {"expected_yield_range": (3, 7), "sector": "Oil & Gas"},
    "NMDC.NS": {"expected_yield_range": (4, 10), "sector": "Mining"},
    "SAIL.NS": {"expected_yield_range": (2, 6), "sector": "Metals"},
    "IRFC.NS": {"expected_yield_range": (2, 4), "sector": "Financials"},
    "RECLTD.NS": {"expected_yield_range": (3, 7), "sector": "Financials"},
    "PFC.NS": {"expected_yield_range": (3, 7), "sector": "Financials"},
    "SJVN.NS": {"expected_yield_range": (2, 5), "sector": "Power"},
    "NTPC.NS": {"expected_yield_range": (2, 4), "sector": "Power"},
    "HINDUNILVR.NS": {"expected_yield_range": (1, 3), "sector": "FMCG"},
}


def validate_known_stocks_present(top50: pd.DataFrame) -> dict:
    """
    Check how many known high-dividend stocks appear in our top 50.

    Returns dict with present, missing, unexpected lists.
    """
    top50_tickers = set(top50["ticker"].tolist())
    known_tickers = set(KNOWN_HIGH_DIVIDEND_STOCKS.keys())

    present = top50_tickers & known_tickers
    missing = known_tickers - top50_tickers
    unexpected = top50_tickers - known_tickers

    result = {
        "present": sorted(present),
        "missing": sorted(missing),
        "unexpected": sorted(unexpected),
        "present_count": len(present),
        "total_known": len(known_tickers),
    }

    return result


def validate_yield_ranges(df: pd.DataFrame) -> list[dict]:
    """
    For each known stock in our dataset, check if computed yield
    falls within expected range. Flag outliers.
    """
    issues = []
    for ticker, info in KNOWN_HIGH_DIVIDEND_STOCKS.items():
        stock_row = df[df["ticker"] == ticker]
        if stock_row.empty:
            issues.append({
                "ticker": ticker,
                "issue": "NOT_FOUND",
                "detail": f"Known high-div stock not found in dataset",
            })
            continue

        row = stock_row.iloc[0]
        computed_yield = row.get("dividend_yield_ttm")
        if pd.isna(computed_yield) or computed_yield is None:
            issues.append({
                "ticker": ticker,
                "issue": "NO_YIELD",
                "detail": "No dividend yield computed",
            })
            continue

        low, high = info["expected_yield_range"]
        if computed_yield < low * 0.5:  # Allow 50% tolerance below
            issues.append({
                "ticker": ticker,
                "issue": "YIELD_TOO_LOW",
                "detail": f"Computed: {computed_yield:.2f}%, Expected: {low}-{high}%",
            })
        elif computed_yield > high * 2:  # Allow 2x tolerance above
            issues.append({
                "ticker": ticker,
                "issue": "YIELD_TOO_HIGH",
                "detail": f"Computed: {computed_yield:.2f}%, Expected: {low}-{high}%",
            })

    return issues


def run_sanity_checks(df: pd.DataFrame) -> list[str]:
    """Run basic sanity checks on the full dataset."""
    warnings = []

    # Check for negative dividends
    neg_divs = df[df["total_dividends_2y"] < 0]
    if len(neg_divs) > 0:
        warnings.append(f"WARNING: {len(neg_divs)} stocks have negative dividends")

    # Check for extreme yields (>50%)
    extreme_yield = df[df["dividend_yield_ttm"] > 50]
    if len(extreme_yield) > 0:
        warnings.append(
            f"WARNING: {len(extreme_yield)} stocks have >50% yield (likely data error): "
            f"{extreme_yield['ticker'].tolist()}"
        )

    # Check for extreme total returns (>500% in 2 years)
    extreme_return = df[df["total_return_2y"] > 500]
    if len(extreme_return) > 0:
        warnings.append(
            f"NOTE: {len(extreme_return)} stocks have >500% total return in 2Y"
        )

    # Check median payout ratio
    valid_payout = df[df["payout_ratio"].notna() & (df["payout_ratio"] > 0)]
    if len(valid_payout) > 0:
        median_payout = valid_payout["payout_ratio"].median()
        if median_payout < 0.1 or median_payout > 1.0:
            warnings.append(
                f"WARNING: Median payout ratio ({median_payout:.2f}) outside expected range 0.1-1.0"
            )

    return warnings


def run(rankings: dict[str, pd.DataFrame]) -> None:
    """Run all validation checks, print results."""
    print("\n" + "=" * 80)
    print("VALIDATION & CROSS-CHECK RESULTS")
    print("=" * 80)

    # Validate top50 by yield
    if "top50_yield" in rankings:
        top50 = rankings["top50_yield"]
        result = validate_known_stocks_present(top50)
        print(f"\nKnown high-div stocks in Top 50 Yield: {result['present_count']}/{result['total_known']}")
        if result["present"]:
            print(f"  Present: {', '.join(result['present'])}")
        if result["missing"]:
            print(f"  Missing: {', '.join(result['missing'])}")

        if result["present_count"] < 5:
            print("  *** WARNING: Fewer than 5 known stocks in top 50. Results may be unreliable. ***")

    # Validate yield ranges from full dataset
    if "top50_composite" in rankings:
        all_ranked = rankings["top50_composite"]
        # Try to load full metrics for better validation
        from config import PROCESSED_DIR
        metrics_path = PROCESSED_DIR / "metrics_all.csv"
        if metrics_path.exists():
            full_df = pd.read_csv(metrics_path)
            issues = validate_yield_ranges(full_df)
            if issues:
                print(f"\nYield range validation issues ({len(issues)}):")
                for issue in issues:
                    print(f"  {issue['ticker']}: {issue['issue']} - {issue['detail']}")
            else:
                print("\nYield range validation: All known stocks within expected ranges")

            # Sanity checks
            warnings = run_sanity_checks(full_df)
            if warnings:
                print(f"\nSanity checks ({len(warnings)} issues):")
                for w in warnings:
                    print(f"  {w}")
            else:
                print("\nSanity checks: All passed")
