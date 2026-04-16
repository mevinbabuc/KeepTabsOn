"""Tests for anomaly_detector.py - anomaly detection flags."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage4_validate.anomaly_detector import (
    detect_unsustainable_payout,
    detect_price_decline_inflating_yield,
    detect_windfall_dividend,
)
from config import MAX_PAYOUT_RATIO


class TestDetectUnsustainablePayout:
    def test_high_payout(self):
        row = pd.Series({"payout_ratio": 2.0})
        assert detect_unsustainable_payout(row) == True

    def test_normal_payout(self):
        row = pd.Series({"payout_ratio": 0.5})
        assert detect_unsustainable_payout(row) == False

    def test_boundary_payout(self):
        row = pd.Series({"payout_ratio": MAX_PAYOUT_RATIO})
        assert detect_unsustainable_payout(row) == False  # Equal to max, not above

    def test_above_boundary(self):
        row = pd.Series({"payout_ratio": MAX_PAYOUT_RATIO + 0.01})
        assert detect_unsustainable_payout(row) == True

    def test_nan_payout(self):
        row = pd.Series({"payout_ratio": float("nan")})
        assert detect_unsustainable_payout(row) == False

    def test_none_payout(self):
        row = pd.Series({"payout_ratio": None})
        assert detect_unsustainable_payout(row) == False


class TestDetectPriceDeclineInflatingYield:
    def test_crash_with_high_yield(self):
        row = pd.Series({
            "capital_appreciation_2y": -40.0,
            "dividend_yield_ttm": 10.0,
        })
        assert detect_price_decline_inflating_yield(row) == True

    def test_crash_with_low_yield(self):
        row = pd.Series({
            "capital_appreciation_2y": -40.0,
            "dividend_yield_ttm": 3.0,
        })
        assert detect_price_decline_inflating_yield(row) == False

    def test_growth_with_high_yield(self):
        row = pd.Series({
            "capital_appreciation_2y": 20.0,
            "dividend_yield_ttm": 10.0,
        })
        assert detect_price_decline_inflating_yield(row) == False

    def test_nan_values(self):
        row = pd.Series({
            "capital_appreciation_2y": float("nan"),
            "dividend_yield_ttm": 10.0,
        })
        assert detect_price_decline_inflating_yield(row) == False


class TestDetectWindfallDividend:
    def test_commodity_high_growth(self):
        row = pd.Series({
            "sector": "Basic Materials",
            "dividend_growth_rate": 150.0,
            "dividend_yield_ttm": 10.0,
        })
        assert detect_windfall_dividend(row) == True

    def test_commodity_declining_high_yield(self):
        row = pd.Series({
            "sector": "Metals & Mining",
            "dividend_growth_rate": -40.0,
            "dividend_yield_ttm": 8.0,
        })
        assert detect_windfall_dividend(row) == True

    def test_it_sector_high_growth(self):
        """Non-commodity sectors should not be flagged."""
        row = pd.Series({
            "sector": "Technology",
            "dividend_growth_rate": 150.0,
            "dividend_yield_ttm": 3.0,
        })
        assert detect_windfall_dividend(row) == False

    def test_commodity_normal_growth(self):
        row = pd.Series({
            "sector": "Energy",
            "dividend_growth_rate": 10.0,
            "dividend_yield_ttm": 4.0,
        })
        assert detect_windfall_dividend(row) == False

    def test_commodity_no_growth_high_yield(self):
        """Commodity stock with no growth data but very high yield."""
        row = pd.Series({
            "sector": "Mining",
            "dividend_growth_rate": float("nan"),
            "dividend_yield_ttm": 12.0,
        })
        assert detect_windfall_dividend(row) == True
