"""Tests for fetch_nse_stocklist.py - CSV parsing, filtering, ticker conversion."""

import sys
from pathlib import Path
from io import StringIO
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage1_collect.fetch_nse_stocklist import (
    filter_equity_series,
    get_symbol_column,
    get_yfinance_tickers,
    get_company_names,
)


@pytest.fixture
def sample_nse_df():
    """Create a sample DataFrame mimicking NSE EQUITY_L.csv format."""
    data = {
        "SYMBOL": ["RELIANCE", "TCS", "M&M", "INFY", "HDFCBANK"],
        "NAME OF COMPANY": [
            "Reliance Industries Limited",
            "Tata Consultancy Services Limited",
            "Mahindra & Mahindra Limited",
            "Infosys Limited",
            "HDFC Bank Limited",
        ],
        " SERIES": ["EQ", "EQ", "EQ", "BE", "EQ"],
        "DATE OF LISTING": [
            "29-NOV-1995",
            "25-AUG-2004",
            "03-JAN-1996",
            "08-JUN-1993",
            "08-JAN-1998",
        ],
        "ISIN NUMBER": [
            "INE002A01018",
            "INE467B01029",
            "INE101A01026",
            "INE009A01021",
            "INE040A01034",
        ],
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_nse_df_with_spaces():
    """DataFrame with leading spaces in column names (as NSE CSV sometimes has)."""
    data = {
        " SYMBOL": ["RELIANCE", "TCS"],
        " NAME OF COMPANY": ["Reliance Industries", "TCS Ltd"],
        " SERIES": ["EQ", "EQ"],
    }
    return pd.DataFrame(data)


class TestFilterEquitySeries:
    def test_filters_non_eq(self, sample_nse_df):
        result = filter_equity_series(sample_nse_df)
        assert len(result) == 4  # INFY (BE series) should be removed
        assert "INFY" not in result["SYMBOL"].values

    def test_keeps_eq(self, sample_nse_df):
        result = filter_equity_series(sample_nse_df)
        assert "RELIANCE" in result["SYMBOL"].values
        assert "TCS" in result["SYMBOL"].values
        assert "M&M" in result["SYMBOL"].values

    def test_handles_whitespace_in_series(self):
        df = pd.DataFrame({
            "SYMBOL": ["A", "B"],
            " SERIES": ["  EQ  ", " BE "],
        })
        result = filter_equity_series(df)
        assert len(result) == 1
        assert result.iloc[0]["SYMBOL"] == "A"

    def test_no_series_column(self):
        df = pd.DataFrame({"SYMBOL": ["A", "B"]})
        result = filter_equity_series(df)
        assert len(result) == 2  # Returns all stocks if no SERIES column


class TestGetSymbolColumn:
    def test_standard_name(self, sample_nse_df):
        assert get_symbol_column(sample_nse_df) == "SYMBOL"

    def test_space_prefix(self, sample_nse_df_with_spaces):
        assert get_symbol_column(sample_nse_df_with_spaces) == " SYMBOL"

    def test_missing_column(self):
        df = pd.DataFrame({"other_col": [1, 2]})
        with pytest.raises(ValueError, match="SYMBOL column not found"):
            get_symbol_column(df)


class TestGetYfinanceTickers:
    def test_basic_conversion(self, sample_nse_df):
        tickers = get_yfinance_tickers(sample_nse_df)
        assert "RELIANCE.NS" in tickers
        assert "TCS.NS" in tickers
        assert "M&M.NS" in tickers  # Special character handling

    def test_count(self, sample_nse_df):
        tickers = get_yfinance_tickers(sample_nse_df)
        assert len(tickers) == 5  # All symbols, not just EQ

    def test_suffix(self, sample_nse_df):
        tickers = get_yfinance_tickers(sample_nse_df)
        for t in tickers:
            assert t.endswith(".NS")


class TestGetCompanyNames:
    def test_mapping(self, sample_nse_df):
        names = get_company_names(sample_nse_df)
        assert names["RELIANCE.NS"] == "Reliance Industries Limited"
        assert names["TCS.NS"] == "Tata Consultancy Services Limited"
        assert names["M&M.NS"] == "Mahindra & Mahindra Limited"

    def test_all_tickers_mapped(self, sample_nse_df):
        names = get_company_names(sample_nse_df)
        assert len(names) == 5
