#!/usr/bin/env python3
"""
Indian Stock Dividend Analysis Pipeline - Master Orchestrator

Usage:
    python run_pipeline.py                    # Run everything
    python run_pipeline.py --stage 1          # Only data collection
    python run_pipeline.py --stage 2          # Only analysis (requires stage 1 data)
    python run_pipeline.py --stage 3          # Only reporting (requires stage 2 data)
    python run_pipeline.py --stage 4          # Only validation (requires stage 2 data)
    python run_pipeline.py --quick            # Only fetch top 200 by market cap (quick test)
    python run_pipeline.py --quick --limit 50 # Custom limit for quick mode
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure project root is in path
sys.path.insert(0, str(Path(__file__).parent))

from config import DATA_DIR, RAW_DIR, STOCK_DATA_DIR, PROCESSED_DIR


def setup_logging(verbose: bool = False) -> None:
    """Configure logging to both console and file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(DATA_DIR / "pipeline.log"),
        ],
    )


def ensure_directories() -> None:
    """Create data directory structure."""
    for d in [RAW_DIR, STOCK_DATA_DIR, PROCESSED_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def run_stage1(quick: bool = False, limit: int = 200) -> list[str]:
    """Stage 1: Data collection."""
    logger = logging.getLogger("stage1")
    logger.info("=" * 60)
    logger.info("STAGE 1: DATA COLLECTION")
    logger.info("=" * 60)

    from stage1_collect.fetch_nse_stocklist import run as fetch_stocklist
    from stage1_collect.fetch_all_data import run as fetch_all_data

    df, tickers, names = fetch_stocklist()
    logger.info(f"Got {len(tickers)} tickers from NSE")

    if quick:
        tickers = tickers[:limit]
        logger.info(f"Quick mode: limiting to {len(tickers)} tickers")

    fetch_all_data(tickers)
    logger.info("Stage 1 complete")
    return tickers


def run_stage2():
    """Stage 2: Analysis (compute metrics + filter/rank)."""
    logger = logging.getLogger("stage2")
    logger.info("=" * 60)
    logger.info("STAGE 2: ANALYSIS")
    logger.info("=" * 60)

    from stage2_analyze.compute_metrics import run as compute_metrics
    from stage2_analyze.filter_and_rank import run as filter_and_rank

    metrics_df = compute_metrics()
    logger.info(f"Computed metrics for {len(metrics_df)} stocks")

    rankings = filter_and_rank()
    for name, df in rankings.items():
        logger.info(f"  {name}: {len(df)} stocks")

    logger.info("Stage 2 complete")
    return rankings


def run_stage3(rankings: dict) -> None:
    """Stage 3: Report generation."""
    logger = logging.getLogger("stage3")
    logger.info("=" * 60)
    logger.info("STAGE 3: REPORTING")
    logger.info("=" * 60)

    from stage3_report.generate_report import run as generate_report
    generate_report(rankings)
    logger.info("Stage 3 complete")


def run_stage4(rankings: dict) -> None:
    """Stage 4: Validation."""
    logger = logging.getLogger("stage4")
    logger.info("=" * 60)
    logger.info("STAGE 4: VALIDATION")
    logger.info("=" * 60)

    from stage4_validate.cross_check import run as cross_check
    from stage4_validate.anomaly_detector import run as detect_anomalies

    cross_check(rankings)

    # Run anomaly detection on the composite ranking
    if "top50_composite" in rankings:
        detect_anomalies(rankings["top50_composite"])

    logger.info("Stage 4 complete")


def main():
    parser = argparse.ArgumentParser(
        description="Indian Stock Dividend Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--stage", type=int, choices=[1, 2, 3, 4],
        help="Run a specific stage only",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Quick mode: analyze fewer stocks for testing",
    )
    parser.add_argument(
        "--limit", type=int, default=200,
        help="Number of stocks in quick mode (default: 200)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose/debug logging",
    )

    args = parser.parse_args()

    setup_logging(args.verbose)
    ensure_directories()

    logger = logging.getLogger("pipeline")
    start = time.time()
    logger.info("Indian Dividend Analysis Pipeline starting")

    rankings = None

    try:
        if args.stage is None or args.stage == 1:
            run_stage1(quick=args.quick, limit=args.limit)

        if args.stage is None or args.stage == 2:
            rankings = run_stage2()

        if args.stage is None or args.stage == 3:
            if rankings is None:
                # Load rankings from CSV files
                import pandas as pd
                rankings = {}
                for name in ["top50_yield", "top50_total_return", "top50_consistent", "top50_composite"]:
                    path = PROCESSED_DIR / f"{name}.csv"
                    if path.exists():
                        rankings[name] = pd.read_csv(path, index_col=0)
            run_stage3(rankings)

        if args.stage is None or args.stage == 4:
            if rankings is None:
                import pandas as pd
                rankings = {}
                for name in ["top50_yield", "top50_total_return", "top50_consistent", "top50_composite"]:
                    path = PROCESSED_DIR / f"{name}.csv"
                    if path.exists():
                        rankings[name] = pd.read_csv(path, index_col=0)
            run_stage4(rankings)

    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user. Progress has been saved.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)

    elapsed = time.time() - start
    logger.info(f"Pipeline complete in {elapsed / 60:.1f} minutes")


if __name__ == "__main__":
    main()
