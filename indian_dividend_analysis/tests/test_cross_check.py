"""Tests for cross_check.py - validation logic."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage4_validate.cross_check import (
    validate_known_stocks_present,
    validate_yield_ranges,
    run_sanity_checks,
    KNOWN_HIGH_DIVIDEND_STOCKS,
)


@pytest.fixture
def sample_top50():
    """Top50 DataFrame with some known stocks."""
    tickers = [
        "VEDL.NS", "COALINDIA.NS", "ITC.NS", "BPCL.NS", "ONGC.NS",
        "NTPC.NS", "PFC.NS", "RECLTD.NS", "NHPC.NS", "SAIL.NS",
        "UNKNOWN1.NS", "UNKNOWN2.NS", "UNKNOWN3.NS",
    ]
    return pd.DataFrame({
        "ticker": tickers,
        "dividend_yield_ttm": [10.0, 7.0, 4.0, 5.0, 5.5, 3.0, 5.0, 5.5, 4.5, 3.5,
                                8.0, 6.0, 4.0],
    })


@pytest.fixture
def sample_full_metrics():
    """Full metrics DataFrame for sanity checks."""
    return pd.DataFrame({
        "ticker": ["A.NS", "B.NS", "C.NS", "D.NS", "E.NS"],
        "dividend_yield_ttm": [5.0, 8.0, 3.0, 2.0, 60.0],  # E.NS has extreme yield
        "total_dividends_2y": [10.0, 16.0, 6.0, 4.0, 120.0],
        "total_return_2y": [25.0, 50.0, 15.0, 10.0, 600.0],  # E.NS extreme return
        "payout_ratio": [0.4, 0.6, 0.3, 0.5, 0.8],
    })


class TestValidateKnownStocksPresent:
    def test_finds_present_stocks(self, sample_top50):
        result = validate_known_stocks_present(sample_top50)
        assert "VEDL.NS" in result["present"]
        assert "COALINDIA.NS" in result["present"]
        assert result["present_count"] == 10

    def test_identifies_missing_stocks(self, sample_top50):
        result = validate_known_stocks_present(sample_top50)
        # Stocks in KNOWN_HIGH_DIVIDEND_STOCKS but not in top50
        assert "HDFCBANK.NS" in result["missing"] or "HINDUNILVR.NS" in result["missing"]

    def test_identifies_unexpected_stocks(self, sample_top50):
        result = validate_known_stocks_present(sample_top50)
        assert "UNKNOWN1.NS" in result["unexpected"]

    def test_empty_top50(self):
        empty_df = pd.DataFrame({"ticker": []})
        result = validate_known_stocks_present(empty_df)
        assert result["present_count"] == 0
        assert result["total_known"] == len(KNOWN_HIGH_DIVIDEND_STOCKS)


class TestValidateYieldRanges:
    def test_normal_yields(self):
        df = pd.DataFrame({
            "ticker": ["VEDL.NS", "ITC.NS"],
            "dividend_yield_ttm": [10.0, 4.0],
        })
        issues = validate_yield_ranges(df)
        # Filter to only yield issues (not NOT_FOUND for missing known stocks)
        yield_issues = [i for i in issues if i["issue"] not in ("NOT_FOUND", "NO_YIELD")]
        assert len(yield_issues) == 0  # Both present stocks are within range

    def test_too_low_yield(self):
        df = pd.DataFrame({
            "ticker": ["VEDL.NS"],
            "dividend_yield_ttm": [1.0],  # Expected 8-15, 1 is way too low
        })
        issues = validate_yield_ranges(df)
        vedl_issues = [i for i in issues if i["ticker"] == "VEDL.NS"]
        assert len(vedl_issues) == 1
        assert vedl_issues[0]["issue"] == "YIELD_TOO_LOW"

    def test_too_high_yield(self):
        df = pd.DataFrame({
            "ticker": ["ITC.NS"],
            "dividend_yield_ttm": [50.0],  # Expected 3-5, 50 is way too high
        })
        issues = validate_yield_ranges(df)
        itc_issues = [i for i in issues if i["ticker"] == "ITC.NS"]
        assert len(itc_issues) == 1
        assert itc_issues[0]["issue"] == "YIELD_TOO_HIGH"

    def test_missing_stock(self):
        df = pd.DataFrame({
            "ticker": ["RANDOM.NS"],
            "dividend_yield_ttm": [5.0],
        })
        issues = validate_yield_ranges(df)
        # Should flag known stocks as NOT_FOUND
        not_found = [i for i in issues if i["issue"] == "NOT_FOUND"]
        assert len(not_found) == len(KNOWN_HIGH_DIVIDEND_STOCKS)


class TestRunSanityChecks:
    def test_no_issues_clean_data(self):
        df = pd.DataFrame({
            "ticker": ["A.NS"],
            "dividend_yield_ttm": [5.0],
            "total_dividends_2y": [10.0],
            "total_return_2y": [25.0],
            "payout_ratio": [0.4],
        })
        warnings = run_sanity_checks(df)
        assert len(warnings) == 0

    def test_negative_dividends_warning(self):
        df = pd.DataFrame({
            "ticker": ["A.NS"],
            "total_dividends_2y": [-5.0],
            "dividend_yield_ttm": [3.0],
            "total_return_2y": [10.0],
            "payout_ratio": [0.3],
        })
        warnings = run_sanity_checks(df)
        assert any("negative dividends" in w for w in warnings)

    def test_extreme_yield_warning(self, sample_full_metrics):
        warnings = run_sanity_checks(sample_full_metrics)
        assert any(">50% yield" in w for w in warnings)
