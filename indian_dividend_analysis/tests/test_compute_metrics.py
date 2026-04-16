"""Tests for compute_metrics.py - all metric calculations with known inputs/outputs."""

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage2_analyze.compute_metrics import (
    _parse_dividends,
    compute_dividend_yield_ttm,
    compute_dividend_yield_2y_avg,
    compute_total_dividends_2y,
    compute_dividend_count_2y,
    compute_dividend_growth_rate,
    compute_total_return_2y,
    compute_capital_appreciation_2y,
    compute_dividend_contribution_pct,
    compute_payout_ratio,
    compute_consistency_score,
)
from config import ANALYSIS_START_DATE, ONE_YEAR_AGO, ANALYSIS_END_DATE


def _make_row(
    current_price=100.0,
    price_2y_ago=80.0,
    price_1y_ago=90.0,
    pe_ratio=20.0,
    dividends=None,
):
    """Create a test row (dict) mimicking DataFrame row."""
    if dividends is None:
        # Default: 2 dividends per year for 2 years
        dividends = [
            {"date": str(ANALYSIS_START_DATE + timedelta(days=60)), "amount": 5.0},
            {"date": str(ANALYSIS_START_DATE + timedelta(days=240)), "amount": 5.0},
            {"date": str(ONE_YEAR_AGO + timedelta(days=60)), "amount": 6.0},
            {"date": str(ONE_YEAR_AGO + timedelta(days=240)), "amount": 6.0},
        ]
    return {
        "current_price": current_price,
        "price_2y_ago": price_2y_ago,
        "price_1y_ago": price_1y_ago,
        "pe_ratio": pe_ratio,
        "dividends_raw": dividends,
    }


class TestParseDividends:
    def test_basic_parse(self):
        raw = [{"date": "2025-01-15", "amount": 5.0}]
        result = _parse_dividends(raw)
        assert len(result) == 1
        assert result[0]["date"] == date(2025, 1, 15)
        assert result[0]["amount"] == 5.0

    def test_empty_list(self):
        assert _parse_dividends([]) == []

    def test_invalid_date(self):
        raw = [
            {"date": "2025-01-15", "amount": 5.0},
            {"date": "invalid", "amount": 3.0},
        ]
        result = _parse_dividends(raw)
        assert len(result) == 1

    def test_missing_keys(self):
        raw = [{"other_key": "value"}]
        result = _parse_dividends(raw)
        assert len(result) == 0


class TestDividendYieldTTM:
    def test_normal_case(self):
        row = _make_row(current_price=100.0)
        # Last 12 months dividends: 6.0 + 6.0 = 12.0
        result = compute_dividend_yield_ttm(row)
        assert result == pytest.approx(12.0, rel=0.01)  # 12/100 * 100 = 12%

    def test_no_price(self):
        row = _make_row(current_price=None)
        assert compute_dividend_yield_ttm(row) is None

    def test_zero_price(self):
        row = _make_row(current_price=0)
        assert compute_dividend_yield_ttm(row) is None

    def test_no_dividends(self):
        row = _make_row(dividends=[])
        assert compute_dividend_yield_ttm(row) == 0.0

    def test_high_yield(self):
        row = _make_row(current_price=50.0)
        # 12/50 * 100 = 24%
        result = compute_dividend_yield_ttm(row)
        assert result == pytest.approx(24.0, rel=0.01)


class TestDividendYield2YAvg:
    def test_normal_case(self):
        row = _make_row(current_price=100.0, price_2y_ago=80.0)
        # Total 2Y divs: 5+5+6+6 = 22. Avg annual = 11. Avg price = 90.
        result = compute_dividend_yield_2y_avg(row)
        assert result == pytest.approx(11.0 / 90.0 * 100, rel=0.01)

    def test_missing_2y_price(self):
        row = _make_row(price_2y_ago=None)
        assert compute_dividend_yield_2y_avg(row) is None


class TestTotalDividends2Y:
    def test_normal_case(self):
        row = _make_row()
        result = compute_total_dividends_2y(row)
        assert result == pytest.approx(22.0, rel=0.01)

    def test_no_dividends(self):
        row = _make_row(dividends=[])
        assert compute_total_dividends_2y(row) == 0.0


class TestDividendCount2Y:
    def test_normal_case(self):
        row = _make_row()
        assert compute_dividend_count_2y(row) == 4

    def test_no_dividends(self):
        row = _make_row(dividends=[])
        assert compute_dividend_count_2y(row) == 0


class TestDividendGrowthRate:
    def test_positive_growth(self):
        row = _make_row()
        # Year 1: 5+5=10, Year 2: 6+6=12
        result = compute_dividend_growth_rate(row)
        assert result == pytest.approx(20.0, rel=0.01)  # (12-10)/10 * 100 = 20%

    def test_negative_growth(self):
        dividends = [
            {"date": str(ANALYSIS_START_DATE + timedelta(days=60)), "amount": 10.0},
            {"date": str(ONE_YEAR_AGO + timedelta(days=60)), "amount": 5.0},
        ]
        row = _make_row(dividends=dividends)
        result = compute_dividend_growth_rate(row)
        assert result == pytest.approx(-50.0, rel=0.01)

    def test_no_year1_dividends(self):
        """Growth rate should be None if year 1 has no dividends."""
        dividends = [
            {"date": str(ONE_YEAR_AGO + timedelta(days=60)), "amount": 5.0},
        ]
        row = _make_row(dividends=dividends)
        assert compute_dividend_growth_rate(row) is None


class TestTotalReturn2Y:
    def test_positive_return(self):
        row = _make_row(current_price=100.0, price_2y_ago=80.0)
        # Capital gain: 100-80=20. Dividends: 22. Total: 42. Return: 42/80 * 100 = 52.5%
        result = compute_total_return_2y(row)
        assert result == pytest.approx(52.5, rel=0.01)

    def test_negative_capital_positive_total(self):
        row = _make_row(current_price=75.0, price_2y_ago=80.0)
        # Capital loss: -5. Dividends: 22. Total: 17. Return: 17/80 * 100 = 21.25%
        result = compute_total_return_2y(row)
        assert result == pytest.approx(21.25, rel=0.01)

    def test_missing_price(self):
        row = _make_row(price_2y_ago=None)
        assert compute_total_return_2y(row) is None


class TestCapitalAppreciation2Y:
    def test_positive(self):
        row = _make_row(current_price=120.0, price_2y_ago=100.0)
        assert compute_capital_appreciation_2y(row) == pytest.approx(20.0, rel=0.01)

    def test_negative(self):
        row = _make_row(current_price=80.0, price_2y_ago=100.0)
        assert compute_capital_appreciation_2y(row) == pytest.approx(-20.0, rel=0.01)


class TestDividendContributionPct:
    def test_dividend_dominated_return(self):
        # Price dropped but dividends saved the day
        row = _make_row(current_price=75.0, price_2y_ago=80.0)
        # Capital loss: -5, Dividends: 22, Total gain: 17
        # Dividend contribution: 22/17 * 100 > 100% (dividends exceeded total return)
        result = compute_dividend_contribution_pct(row)
        assert result is not None
        assert result > 100  # Dividends contributed more than 100% since price dropped


class TestPayoutRatio:
    def test_normal_case(self):
        row = _make_row(current_price=100.0, pe_ratio=20.0)
        # EPS = 100/20 = 5. Annual divs (TTM) = 12. Payout = 12/5 = 2.4
        result = compute_payout_ratio(row)
        assert result == pytest.approx(2.4, rel=0.01)

    def test_no_pe(self):
        row = _make_row(pe_ratio=None)
        assert compute_payout_ratio(row) is None

    def test_negative_pe(self):
        row = _make_row(pe_ratio=-5.0)
        assert compute_payout_ratio(row) is None

    def test_no_dividends(self):
        row = _make_row(dividends=[])
        result = compute_payout_ratio(row)
        assert result == 0.0


class TestConsistencyScore:
    def test_perfect_score(self):
        """Stock with dividends in both years, growing, no concentration."""
        # 8 dividends over 2 years, growing, no single one > 60%
        dividends = []
        for m in range(0, 24, 3):  # Every 3 months for 2 years
            d = ANALYSIS_START_DATE + timedelta(days=m * 30 + 15)
            if d < ANALYSIS_END_DATE:
                amt = 5.0 if d < ONE_YEAR_AGO else 6.0
                dividends.append({"date": str(d), "amount": amt})

        row = _make_row(dividends=dividends)
        score = compute_consistency_score(row)
        assert score == 100.0  # All criteria met

    def test_zero_score(self):
        """Stock with no dividends."""
        row = _make_row(dividends=[])
        assert compute_consistency_score(row) == 0.0

    def test_partial_score_one_year_only(self):
        """Stock that only paid in year 2 (no 40-point bonus for both years)."""
        dividends = [
            {"date": str(ONE_YEAR_AGO + timedelta(days=30)), "amount": 5.0},
        ]
        row = _make_row(dividends=dividends)
        score = compute_consistency_score(row)
        assert 0 < score < 50  # Some points but not the 40 for both years

    def test_concentrated_dividend_loses_points(self):
        """One huge dividend should lose the concentration penalty."""
        dividends = [
            {"date": str(ANALYSIS_START_DATE + timedelta(days=60)), "amount": 1.0},
            {"date": str(ONE_YEAR_AGO + timedelta(days=60)), "amount": 100.0},
        ]
        row = _make_row(dividends=dividends)
        score = compute_consistency_score(row)
        # Should not get the 15 points for even distribution
        assert score <= 85
