"""Resume-capable caching layer for per-ticker data storage."""

import json
from pathlib import Path
from typing import Any


def get_completed_tickers(cache_dir: Path) -> set[str]:
    """Return set of ticker symbols already fetched (based on existing JSON files)."""
    if not cache_dir.exists():
        return set()
    return {f.stem for f in cache_dir.glob("*.json")}


def save_ticker_data(cache_dir: Path, ticker: str, data: dict[str, Any]) -> None:
    """Save fetched data as JSON. Uses ticker as filename."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    filepath = cache_dir / f"{ticker}.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2, default=str)


def load_ticker_data(cache_dir: Path, ticker: str) -> dict[str, Any] | None:
    """Load previously cached data for a ticker. Returns None if not found."""
    filepath = cache_dir / f"{ticker}.json"
    if not filepath.exists():
        return None
    with open(filepath, "r") as f:
        return json.load(f)


def load_all_ticker_data(cache_dir: Path) -> list[dict[str, Any]]:
    """Load all cached ticker data files. Returns list of dicts."""
    if not cache_dir.exists():
        return []
    results = []
    for filepath in sorted(cache_dir.glob("*.json")):
        with open(filepath, "r") as f:
            data = json.load(f)
            results.append(data)
    return results


def get_pending_tickers(all_tickers: list[str], cache_dir: Path) -> list[str]:
    """Return tickers not yet cached. Used for resumable fetching."""
    completed = get_completed_tickers(cache_dir)
    return [t for t in all_tickers if t not in completed]
