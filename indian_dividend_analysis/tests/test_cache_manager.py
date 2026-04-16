"""Tests for cache_manager.py - save/load/resume logic."""

import json
import tempfile
from pathlib import Path

import pytest

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from stage1_collect.cache_manager import (
    get_completed_tickers,
    save_ticker_data,
    load_ticker_data,
    load_all_ticker_data,
    get_pending_tickers,
)


@pytest.fixture
def cache_dir(tmp_path):
    """Create a temporary cache directory."""
    d = tmp_path / "cache"
    d.mkdir()
    return d


class TestGetCompletedTickers:
    def test_empty_directory(self, cache_dir):
        result = get_completed_tickers(cache_dir)
        assert result == set()

    def test_nonexistent_directory(self, tmp_path):
        result = get_completed_tickers(tmp_path / "nonexistent")
        assert result == set()

    def test_with_cached_files(self, cache_dir):
        (cache_dir / "RELIANCE.NS.json").write_text('{}')
        (cache_dir / "TCS.NS.json").write_text('{}')
        result = get_completed_tickers(cache_dir)
        assert result == {"RELIANCE.NS", "TCS.NS"}

    def test_ignores_non_json(self, cache_dir):
        (cache_dir / "RELIANCE.NS.json").write_text('{}')
        (cache_dir / "readme.txt").write_text('hello')
        result = get_completed_tickers(cache_dir)
        assert result == {"RELIANCE.NS"}


class TestSaveTickerData:
    def test_save_creates_file(self, cache_dir):
        data = {"ticker": "RELIANCE.NS", "dividends": []}
        save_ticker_data(cache_dir, "RELIANCE.NS", data)

        filepath = cache_dir / "RELIANCE.NS.json"
        assert filepath.exists()

        loaded = json.loads(filepath.read_text())
        assert loaded["ticker"] == "RELIANCE.NS"

    def test_save_creates_directory(self, tmp_path):
        new_dir = tmp_path / "new" / "cache"
        data = {"ticker": "TCS.NS"}
        save_ticker_data(new_dir, "TCS.NS", data)
        assert (new_dir / "TCS.NS.json").exists()

    def test_save_overwrites_existing(self, cache_dir):
        save_ticker_data(cache_dir, "ITC.NS", {"version": 1})
        save_ticker_data(cache_dir, "ITC.NS", {"version": 2})
        loaded = json.loads((cache_dir / "ITC.NS.json").read_text())
        assert loaded["version"] == 2


class TestLoadTickerData:
    def test_load_existing(self, cache_dir):
        data = {"ticker": "VEDL.NS", "yield": 10.5}
        save_ticker_data(cache_dir, "VEDL.NS", data)
        result = load_ticker_data(cache_dir, "VEDL.NS")
        assert result["ticker"] == "VEDL.NS"
        assert result["yield"] == 10.5

    def test_load_nonexistent(self, cache_dir):
        result = load_ticker_data(cache_dir, "NONEXIST.NS")
        assert result is None


class TestLoadAllTickerData:
    def test_load_all(self, cache_dir):
        save_ticker_data(cache_dir, "A.NS", {"ticker": "A.NS"})
        save_ticker_data(cache_dir, "B.NS", {"ticker": "B.NS"})
        result = load_all_ticker_data(cache_dir)
        assert len(result) == 2
        tickers = {r["ticker"] for r in result}
        assert tickers == {"A.NS", "B.NS"}

    def test_load_all_empty(self, cache_dir):
        result = load_all_ticker_data(cache_dir)
        assert result == []

    def test_load_all_nonexistent_dir(self, tmp_path):
        result = load_all_ticker_data(tmp_path / "nope")
        assert result == []


class TestGetPendingTickers:
    def test_all_pending(self, cache_dir):
        tickers = ["A.NS", "B.NS", "C.NS"]
        result = get_pending_tickers(tickers, cache_dir)
        assert result == tickers

    def test_some_completed(self, cache_dir):
        save_ticker_data(cache_dir, "A.NS", {"ticker": "A.NS"})
        tickers = ["A.NS", "B.NS", "C.NS"]
        result = get_pending_tickers(tickers, cache_dir)
        assert result == ["B.NS", "C.NS"]

    def test_all_completed(self, cache_dir):
        for t in ["A.NS", "B.NS"]:
            save_ticker_data(cache_dir, t, {"ticker": t})
        result = get_pending_tickers(["A.NS", "B.NS"], cache_dir)
        assert result == []

    def test_preserves_order(self, cache_dir):
        save_ticker_data(cache_dir, "B.NS", {"ticker": "B.NS"})
        tickers = ["C.NS", "A.NS", "B.NS", "D.NS"]
        result = get_pending_tickers(tickers, cache_dir)
        assert result == ["C.NS", "A.NS", "D.NS"]
