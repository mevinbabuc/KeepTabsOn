"""Tests for fetch_all_data.py - data extraction from mocked yfinance responses."""

import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch, MagicMock, PropertyMock

import pandas as pd
import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from stage1_collect.fetch_all_data import fetch_single_ticker


def _make_mock_history(
    dates, closes, volumes, dividends=None,
):
    """Create a mock DataFrame matching yfinance history output."""
    index = pd.DatetimeIndex(dates)
    data = {
        "Open": closes,
        "High": [c * 1.02 for c in closes],
        "Low": [c * 0.98 for c in closes],
        "Close": closes,
        "Volume": volumes,
        "Dividends": dividends if dividends else [0.0] * len(dates),
        "Stock Splits": [0.0] * len(dates),
    }
    return pd.DataFrame(data, index=index)


class TestFetchSingleTicker:
    @patch("stage1_collect.fetch_all_data.yf.Ticker")
    def test_basic_fetch(self, mock_ticker_class):
        """Test fetching a stock with dividends and price data."""
        mock_ticker = MagicMock()
        mock_ticker_class.return_value = mock_ticker

        # Mock history data
        dates = pd.date_range("2023-04-16", "2026-04-16", freq="ME")
        closes = [100 + i * 2 for i in range(len(dates))]
        volumes = [1000000] * len(dates)
        dividends = [0.0] * len(dates)
        # Add dividend payments
        dividends[6] = 5.0   # Approx Oct 2023
        dividends[18] = 6.0  # Approx Oct 2024
        dividends[30] = 7.0  # Approx Oct 2025

        hist = _make_mock_history(dates.tolist(), closes, volumes, dividends)
        mock_ticker.history.return_value = hist

        # Mock info
        mock_ticker.info = {
            "marketCap": 500000000000,
            "trailingPE": 25.0,
            "sector": "Energy",
            "industry": "Oil & Gas",
            "currency": "INR",
        }

        result = fetch_single_ticker("RELIANCE.NS")

        assert result["ticker"] == "RELIANCE.NS"
        assert result["error"] is None
        assert result["current_price"] is not None
        assert result["market_cap"] == 500000000000
        assert result["pe_ratio"] == 25.0
        assert result["sector"] == "Energy"
        assert len(result["dividends"]) == 3  # 3 dividend payments

    @patch("stage1_collect.fetch_all_data.yf.Ticker")
    def test_empty_history(self, mock_ticker_class):
        """Test handling of stock with no history."""
        mock_ticker = MagicMock()
        mock_ticker_class.return_value = mock_ticker
        mock_ticker.history.return_value = pd.DataFrame()
        mock_ticker.info = {}

        result = fetch_single_ticker("DELISTED.NS")

        assert result["ticker"] == "DELISTED.NS"
        assert result["error"] == "No historical data available"
        assert result["dividends"] == []

    @patch("stage1_collect.fetch_all_data.yf.Ticker")
    def test_no_dividends(self, mock_ticker_class):
        """Test stock with price data but no dividends."""
        mock_ticker = MagicMock()
        mock_ticker_class.return_value = mock_ticker

        dates = pd.date_range("2023-04-16", "2026-04-16", freq="ME")
        closes = [100.0] * len(dates)
        volumes = [500000] * len(dates)

        hist = _make_mock_history(dates.tolist(), closes, volumes)
        mock_ticker.history.return_value = hist
        mock_ticker.info = {"marketCap": 100000000, "currency": "INR"}

        result = fetch_single_ticker("NODIV.NS")

        assert result["dividends"] == []
        assert result["current_price"] == 100.0
        assert result["error"] is None

    @patch("stage1_collect.fetch_all_data.yf.Ticker")
    def test_usd_market_cap_conversion(self, mock_ticker_class):
        """Test that USD market cap is converted to INR."""
        mock_ticker = MagicMock()
        mock_ticker_class.return_value = mock_ticker

        dates = pd.date_range("2024-01-01", "2026-04-16", freq="ME")
        closes = [50.0] * len(dates)
        volumes = [100000] * len(dates)

        hist = _make_mock_history(dates.tolist(), closes, volumes)
        mock_ticker.history.return_value = hist
        mock_ticker.info = {
            "marketCap": 1000000,
            "currency": "USD",
        }

        result = fetch_single_ticker("TEST.NS")

        # Should be converted: 1000000 * 83 = 83000000
        assert result["market_cap"] == 1000000 * 83.0

    @patch("stage1_collect.fetch_all_data.yf.Ticker")
    def test_info_fetch_failure(self, mock_ticker_class):
        """Test graceful handling when info fetch fails."""
        mock_ticker = MagicMock()
        mock_ticker_class.return_value = mock_ticker

        dates = pd.date_range("2024-01-01", "2026-04-16", freq="ME")
        closes = [200.0] * len(dates)
        volumes = [100000] * len(dates)

        hist = _make_mock_history(dates.tolist(), closes, volumes)
        mock_ticker.history.return_value = hist

        # Make info raise an exception
        type(mock_ticker).info = PropertyMock(side_effect=Exception("API Error"))

        result = fetch_single_ticker("FAILINFO.NS")

        # Price data should still be there
        assert result["current_price"] == 200.0
        # Info fields should be None
        assert result["sector"] is None
        assert result["market_cap"] is None
