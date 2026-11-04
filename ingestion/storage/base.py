"""Storage interface for the Bronze layer.

Bronze is append-only: implementations must refuse to overwrite an existing object.
Paths are always forward-slash keys such as "bronze/encounters/available_date=2026-01-15/file.csv".
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class ObjectExistsError(Exception):
    """Raised when writing to a key that already exists (Bronze is immutable)."""


class StorageClient(ABC):
    @abstractmethod
    def upload_file(self, local_path: Path, destination: str) -> None:
        """Copy a local file to `destination`. Raises ObjectExistsError if it already exists."""

    @abstractmethod
    def download_file(self, source: str, local_path: Path) -> None:
        """Copy object `source` to a local file."""

    @abstractmethod
    def exists(self, key: str) -> bool: ...

    @abstractmethod
    def list(self, prefix: str) -> list[str]:
        """Return all keys starting with `prefix`, sorted."""
