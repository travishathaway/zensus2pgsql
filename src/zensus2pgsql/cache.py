"""
Functions for interacting with application cache
"""

import os
import shutil
from pathlib import Path

import platformdirs

from .constants import APP_NAME
from .errors import Zensus2PgsqlError

CACHE = platformdirs.user_cache_dir(APP_NAME)


def create_cache_dir():
    """
    Ensures cache directory exists.
    """
    if not os.path.exists(CACHE):
        os.makedirs(CACHE)


def save_to_cache(path: Path, save_bytes: bool = False):
    """
    Saves file to cache directory.
    """
    try:
        if save_bytes:
            path.write_bytes(path.read_bytes())
        else:
            path.write_text(path.read_text())
    except OSError as exc:
        raise Zensus2PgsqlError("Unable to save file to cache") from exc


def get_cache_size() -> dict[str, int]:
    """
    Calculate cache sizes for ZIP and CSV caches.

    Returns
    -------
    dict[str, int]
        Dictionary with keys: "zip_bytes", "csv_bytes", "total_bytes"
    """
    cache_path = Path(CACHE)
    csv_cache_path = cache_path / "extracted"

    zip_bytes = 0
    csv_bytes = 0

    # Calculate ZIP cache size
    if cache_path.exists():
        for item in cache_path.iterdir():
            if item.is_file() and item.suffix == ".zip":
                zip_bytes += item.stat().st_size

    # Calculate CSV cache size
    if csv_cache_path.exists():
        for item in csv_cache_path.rglob("*"):
            if item.is_file():
                csv_bytes += item.stat().st_size

    return {"zip_bytes": zip_bytes, "csv_bytes": csv_bytes, "total_bytes": zip_bytes + csv_bytes}


def format_bytes(bytes_size: int) -> str:
    """
    Format bytes as human-readable string.

    Parameters
    ----------
    bytes_size : int
        Size in bytes

    Returns
    -------
    str
        Formatted string (e.g., "1.5 GB", "234.2 MB")
    """
    size_float = float(bytes_size)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_float < 1024.0:
            return f"{size_float:.1f} {unit}"
        size_float /= 1024.0
    return f"{size_float:.1f} PB"


def clear_csv_cache(dataset_name: str | None = None) -> int:
    """
    Clear CSV cache for a specific dataset or all datasets.

    Parameters
    ----------
    dataset_name : str | None
        Dataset name to clear, or None to clear all

    Returns
    -------
    int
        Number of bytes freed
    """
    csv_cache_path = Path(CACHE) / "extracted"

    if not csv_cache_path.exists():
        return 0

    bytes_freed = 0

    if dataset_name is None:
        # Clear entire CSV cache
        for item in csv_cache_path.iterdir():
            if item.is_dir():
                size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
                shutil.rmtree(item)
                bytes_freed += size
    else:
        # Clear specific dataset
        dataset_path = csv_cache_path / dataset_name
        if dataset_path.exists():
            bytes_freed = sum(f.stat().st_size for f in dataset_path.rglob("*") if f.is_file())
            shutil.rmtree(dataset_path)

    return bytes_freed
