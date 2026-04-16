"""Generate human-readable reports from ranked dividend data."""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import PROCESSED_DIR

logger = logging.getLogger(__name__)

# Display columns for each table
DISPLAY_COLS = [
    "ticker", "sector", "current_price", "dividend_yield_ttm",
    "total_return_2y", "consistency_score", "dividend_growth_rate",
    "payout_ratio", "risk_flags",
]


def print_summary_stats(df: pd.DataFrame) -> str:
    """Generate summary statistics text."""
    lines = []
    lines.append("=" * 80)
    lines.append("INDIAN STOCK DIVIDEND ANALYSIS - SUMMARY")
    lines.append("=" * 80)
    lines.append(f"Total stocks analyzed: {len(df)}")

    # Dividend stats
    div_payers = df[df["dividend_yield_ttm"].notna() & (df["dividend_yield_ttm"] > 0)]
    lines.append(f"Stocks paying dividends: {len(div_payers)}")

    if len(div_payers) > 0:
        lines.append(f"Average dividend yield (TTM): {div_payers['dividend_yield_ttm'].mean():.2f}%")
        lines.append(f"Median dividend yield (TTM): {div_payers['dividend_yield_ttm'].median():.2f}%")
        lines.append(f"Max dividend yield (TTM): {div_payers['dividend_yield_ttm'].max():.2f}%")

    # Total return stats
    valid_return = df[df["total_return_2y"].notna()]
    if len(valid_return) > 0:
        lines.append(f"\nAverage 2Y total return: {valid_return['total_return_2y'].mean():.2f}%")
        lines.append(f"Median 2Y total return: {valid_return['total_return_2y'].median():.2f}%")
        gt100 = len(valid_return[valid_return["total_return_2y"] > 100])
        lines.append(f"Stocks with >100% total return in 2Y: {gt100}")

    # Sector breakdown
    if "sector" in df.columns:
        sector_counts = div_payers["sector"].value_counts().head(10)
        if len(sector_counts) > 0:
            lines.append("\nTop sectors by number of dividend payers:")
            for sector, count in sector_counts.items():
                avg_yield = div_payers[div_payers["sector"] == sector]["dividend_yield_ttm"].mean()
                lines.append(f"  {sector}: {count} stocks (avg yield: {avg_yield:.2f}%)")

    lines.append("")
    return "\n".join(lines)


def format_ranking_table(df: pd.DataFrame, title: str) -> str:
    """Format a ranking as a readable table."""
    lines = []
    lines.append("")
    lines.append("=" * 80)
    lines.append(title)
    lines.append("=" * 80)

    # Select available display columns
    cols = [c for c in DISPLAY_COLS if c in df.columns]
    display = df[cols].copy()

    # Format numeric columns
    for col in ["dividend_yield_ttm", "total_return_2y", "dividend_growth_rate"]:
        if col in display.columns:
            display[col] = display[col].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"
            )
    for col in ["current_price"]:
        if col in display.columns:
            display[col] = display[col].apply(
                lambda x: f"Rs {x:.2f}" if pd.notna(x) else "N/A"
            )
    for col in ["consistency_score"]:
        if col in display.columns:
            display[col] = display[col].apply(
                lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
            )
    for col in ["payout_ratio"]:
        if col in display.columns:
            display[col] = display[col].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )

    lines.append(display.to_string())
    lines.append("")
    return "\n".join(lines)


def generate_sector_summary(df: pd.DataFrame) -> str:
    """Generate sector-level dividend summary."""
    lines = []
    lines.append("")
    lines.append("=" * 80)
    lines.append("SECTOR-LEVEL DIVIDEND SUMMARY")
    lines.append("=" * 80)

    if "sector" not in df.columns:
        lines.append("Sector data not available.")
        return "\n".join(lines)

    div_payers = df[df["dividend_yield_ttm"].notna() & (df["dividend_yield_ttm"] > 0)].copy()

    if len(div_payers) == 0:
        lines.append("No dividend payers found.")
        return "\n".join(lines)

    sector_stats = div_payers.groupby("sector").agg(
        count=("ticker", "count"),
        avg_yield=("dividend_yield_ttm", "mean"),
        max_yield=("dividend_yield_ttm", "max"),
        avg_total_return=("total_return_2y", "mean"),
        best_stock=("dividend_yield_ttm", "idxmax"),
    ).sort_values("avg_yield", ascending=False)

    for sector, row in sector_stats.iterrows():
        lines.append(f"\n{sector} ({int(row['count'])} stocks)")
        lines.append(f"  Avg Yield: {row['avg_yield']:.2f}% | Max Yield: {row['max_yield']:.2f}%")
        if pd.notna(row["avg_total_return"]):
            lines.append(f"  Avg Total Return (2Y): {row['avg_total_return']:.2f}%")
        # Find the best stock in this sector
        sector_stocks = div_payers[div_payers["sector"] == sector]
        best = sector_stocks.loc[sector_stocks["dividend_yield_ttm"].idxmax()]
        lines.append(f"  Top Yielder: {best['ticker']} ({best['dividend_yield_ttm']:.2f}%)")

    lines.append("")
    return "\n".join(lines)


def generate_investment_insights(rankings: dict[str, pd.DataFrame]) -> str:
    """Generate key investment insights from the analysis."""
    lines = []
    lines.append("")
    lines.append("=" * 80)
    lines.append("KEY INVESTMENT INSIGHTS")
    lines.append("=" * 80)

    # Yield analysis
    if "top50_yield" in rankings:
        top_yield = rankings["top50_yield"]
        lines.append(f"\n1. HIGHEST DIVIDEND YIELD STOCKS:")
        for i, (_, row) in enumerate(top_yield.head(10).iterrows(), 1):
            flags = f" [{row['risk_flags']}]" if row.get("risk_flags") else ""
            lines.append(
                f"   {i}. {row['ticker']} - Yield: {row['dividend_yield_ttm']:.2f}%"
                f" | Sector: {row.get('sector', 'N/A')}{flags}"
            )

    # Total return analysis
    if "top50_total_return" in rankings:
        top_return = rankings["top50_total_return"]
        lines.append(f"\n2. BEST TOTAL RETURN (Dividends + Price Growth, 2 Years):")
        for i, (_, row) in enumerate(top_return.head(10).iterrows(), 1):
            lines.append(
                f"   {i}. {row['ticker']} - Total Return: {row['total_return_2y']:.2f}%"
                f" | Yield: {row['dividend_yield_ttm']:.2f}%"
                f" | Sector: {row.get('sector', 'N/A')}"
            )

    # Composite (best overall)
    if "top50_composite" in rankings:
        top_comp = rankings["top50_composite"]
        lines.append(f"\n3. BEST OVERALL (Composite Score - Yield + Return + Consistency + Growth):")
        for i, (_, row) in enumerate(top_comp.head(10).iterrows(), 1):
            score = row.get("composite_score", 0)
            lines.append(
                f"   {i}. {row['ticker']} - Score: {score:.3f}"
                f" | Yield: {row['dividend_yield_ttm']:.2f}%"
                f" | Return: {row.get('total_return_2y', 0):.2f}%"
                f" | Sector: {row.get('sector', 'N/A')}"
            )

    # Warnings
    lines.append(f"\n4. IMPORTANT NOTES:")
    lines.append("   - High dividend yield alone does NOT guarantee good returns")
    lines.append("   - Stocks flagged HIGH_PAYOUT may cut dividends in future")
    lines.append("   - PRICE_DECLINE_INFLATING_YIELD means the stock price crashed, making yield look high")
    lines.append("   - PSU stocks often have high yields due to government mandate, not business strength")
    lines.append("   - Dividends in India are taxed at your income tax slab rate")
    lines.append("   - Past performance does not guarantee future results")
    lines.append("   - Always do your own research before investing")
    lines.append("")

    return "\n".join(lines)


def run(rankings: dict[str, pd.DataFrame]) -> None:
    """Entry point: generate all reports."""
    # Load full metrics for summary stats
    metrics_path = PROCESSED_DIR / "metrics_all.csv"
    full_df = pd.read_csv(metrics_path) if metrics_path.exists() else pd.DataFrame()

    report_parts = []

    # Summary stats
    if len(full_df) > 0:
        report_parts.append(print_summary_stats(full_df))

    # Each ranking table
    titles = {
        "top50_yield": "TOP 50 - HIGHEST DIVIDEND YIELD (TTM)",
        "top50_total_return": "TOP 50 - BEST TOTAL RETURN (2 Years)",
        "top50_consistent": "TOP 50 - MOST CONSISTENT DIVIDEND PAYERS",
        "top50_composite": "TOP 50 - BEST OVERALL (COMPOSITE SCORE)",
    }
    for key, title in titles.items():
        if key in rankings:
            report_parts.append(format_ranking_table(rankings[key], title))

    # Sector summary
    if len(full_df) > 0:
        report_parts.append(generate_sector_summary(full_df))

    # Investment insights
    report_parts.append(generate_investment_insights(rankings))

    # Print to console
    full_report = "\n".join(report_parts)
    print(full_report)

    # Save to file
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    report_path = PROCESSED_DIR / "report.txt"
    report_path.write_text(full_report)
    logger.info(f"Report saved to {report_path}")
