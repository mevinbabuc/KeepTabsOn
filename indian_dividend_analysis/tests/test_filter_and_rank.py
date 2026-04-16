"""Tests for filter_and_rank.py - quality filters and ranking logic."""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage2_analyze.filter_and_rank import (
    apply_quality_filters,
    _add_risk_flags,
    _normalize,
    rank_by_yield,
    rank_by_total_return,
    rank_by_consistency,
    rank_composite,
)


@pytest.fixture
def sample_metrics_df():
    """Create a sample metrics DataFrame with known values."""
    return pd.DataFrame([
        {
            "ticker": "HIGH_YIELD.NS",
            "current_price": 100.0,
            "price_2y_ago": 120.0,
            "market_cap": 50000_00_00_000,  # 5000 Cr
            "avg_volume": 500000,
            "dividend_yield_ttm": 15.0,
            "dividend_yield_2y_avg": 14.0,
            "total_dividends_2y": 30.0,
            "dividend_count_2y": 4,
            "dividend_growth_rate": -10.0,
            "total_return_2y": 8.0,
            "capital_appreciation_2y": -16.7,
            "payout_ratio": 1.2,
            "consistency_score": 70.0,
            "sector": "Energy",
        },
        {
            "ticker": "GROWTH.NS",
            "current_price": 200.0,
            "price_2y_ago": 100.0,
            "market_cap": 100000_00_00_000,
            "avg_volume": 1000000,
            "dividend_yield_ttm": 3.0,
            "dividend_yield_2y_avg": 3.5,
            "total_dividends_2y": 12.0,
            "dividend_count_2y": 4,
            "dividend_growth_rate": 20.0,
            "total_return_2y": 112.0,
            "capital_appreciation_2y": 100.0,
            "payout_ratio": 0.4,
            "consistency_score": 90.0,
            "sector": "IT",
        },
        {
            "ticker": "CONSISTENT.NS",
            "current_price": 150.0,
            "price_2y_ago": 130.0,
            "market_cap": 80000_00_00_000,
            "avg_volume": 800000,
            "dividend_yield_ttm": 5.0,
            "dividend_yield_2y_avg": 5.5,
            "total_dividends_2y": 16.0,
            "dividend_count_2y": 8,
            "dividend_growth_rate": 5.0,
            "total_return_2y": 27.7,
            "capital_appreciation_2y": 15.4,
            "payout_ratio": 0.5,
            "consistency_score": 100.0,
            "sector": "FMCG",
        },
        {
            "ticker": "PENNY.NS",
            "current_price": 5.0,  # Below min price
            "price_2y_ago": 3.0,
            "market_cap": 100_00_00_000,  # 100 Cr - below min
            "avg_volume": 5000,  # Below min
            "dividend_yield_ttm": 20.0,
            "dividend_yield_2y_avg": 18.0,
            "total_dividends_2y": 2.0,
            "dividend_count_2y": 2,
            "dividend_growth_rate": 0.0,
            "total_return_2y": 133.0,
            "capital_appreciation_2y": 66.7,
            "payout_ratio": 0.3,
            "consistency_score": 40.0,
            "sector": "Unknown",
        },
        {
            "ticker": "NODIV.NS",
            "current_price": 500.0,
            "price_2y_ago": 400.0,
            "market_cap": 200000_00_00_000,
            "avg_volume": 2000000,
            "dividend_yield_ttm": 0.0,  # No dividends
            "dividend_yield_2y_avg": 0.0,
            "total_dividends_2y": 0.0,
            "dividend_count_2y": 0,
            "dividend_growth_rate": None,
            "total_return_2y": 25.0,
            "capital_appreciation_2y": 25.0,
            "payout_ratio": None,
            "consistency_score": 0.0,
            "sector": "Banking",
        },
    ])


class TestApplyQualityFilters:
    def test_removes_penny_stock(self, sample_metrics_df):
        result = apply_quality_filters(sample_metrics_df)
        assert "PENNY.NS" not in result["ticker"].values

    def test_removes_no_dividend_stock(self, sample_metrics_df):
        result = apply_quality_filters(sample_metrics_df)
        assert "NODIV.NS" not in result["ticker"].values

    def test_keeps_quality_stocks(self, sample_metrics_df):
        result = apply_quality_filters(sample_metrics_df)
        assert "HIGH_YIELD.NS" in result["ticker"].values
        assert "GROWTH.NS" in result["ticker"].values
        assert "CONSISTENT.NS" in result["ticker"].values

    def test_filter_count(self, sample_metrics_df):
        result = apply_quality_filters(sample_metrics_df)
        assert len(result) == 3  # Only 3 quality stocks remain


class TestAddRiskFlags:
    def test_high_payout_flag(self, sample_metrics_df):
        result = _add_risk_flags(sample_metrics_df)
        high_yield_row = result[result["ticker"] == "HIGH_YIELD.NS"].iloc[0]
        assert "HIGH_PAYOUT" in high_yield_row["risk_flags"]

    def test_no_flags_for_good_stock(self, sample_metrics_df):
        result = _add_risk_flags(sample_metrics_df)
        growth_row = result[result["ticker"] == "GROWTH.NS"].iloc[0]
        assert growth_row["risk_flags"] == ""

    def test_price_decline_flag(self):
        df = pd.DataFrame([{
            "ticker": "CRASH.NS",
            "capital_appreciation_2y": -40.0,
            "dividend_yield_ttm": 10.0,
            "payout_ratio": 0.5,
            "dividend_growth_rate": 5.0,
        }])
        result = _add_risk_flags(df)
        assert "PRICE_DECLINE_INFLATING_YIELD" in result.iloc[0]["risk_flags"]


class TestNormalize:
    def test_basic_normalization(self):
        s = pd.Series([0, 50, 100])
        result = _normalize(s)
        assert result.iloc[0] == 0.0
        assert result.iloc[1] == 0.5
        assert result.iloc[2] == 1.0

    def test_constant_series(self):
        s = pd.Series([5, 5, 5])
        result = _normalize(s)
        assert all(result == 0.5)

    def test_negative_values(self):
        s = pd.Series([-100, 0, 100])
        result = _normalize(s)
        assert result.iloc[0] == 0.0
        assert result.iloc[1] == 0.5
        assert result.iloc[2] == 1.0


class TestRankByYield:
    def test_highest_yield_first(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_by_yield(filtered)
        assert result.iloc[0]["ticker"] == "HIGH_YIELD.NS"

    def test_ranking_order(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_by_yield(filtered)
        yields = result["dividend_yield_ttm"].tolist()
        assert yields == sorted(yields, reverse=True)


class TestRankByTotalReturn:
    def test_highest_return_first(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_by_total_return(filtered)
        assert result.iloc[0]["ticker"] == "GROWTH.NS"

    def test_has_annualized_return(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_by_total_return(filtered)
        assert "annualized_return" in result.columns


class TestRankByConsistency:
    def test_most_consistent_first(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_by_consistency(filtered)
        assert result.iloc[0]["ticker"] == "CONSISTENT.NS"


class TestRankComposite:
    def test_composite_has_score(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_composite(filtered)
        assert "composite_score" in result.columns

    def test_balanced_stock_ranks_well(self, sample_metrics_df):
        """GROWTH.NS has good yield, great return, and great consistency."""
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_composite(filtered)
        # GROWTH.NS should be top 2 in composite (great return + consistency)
        top2_tickers = result.head(2)["ticker"].tolist()
        assert "GROWTH.NS" in top2_tickers

    def test_scores_are_bounded(self, sample_metrics_df):
        filtered = apply_quality_filters(sample_metrics_df)
        result = rank_composite(filtered)
        assert result["composite_score"].min() >= 0
        assert result["composite_score"].max() <= 1
