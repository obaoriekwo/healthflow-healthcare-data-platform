from __future__ import annotations

import shutil
from pathlib import Path

from ingestion.storage.base import ObjectExistsError, StorageClient


class LocalStorageClient(StorageClient):
    """Bronze on a local folder. Used for fast development and unit tests."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, key: str) -> Path:
        path = (self.root / key).resolve()
        if self.root.resolve() not in path.parents:
            raise ValueError(f"Key escapes storage root: {key}")
        return path

    def upload_file(self, local_path: Path, destination: str) -> None:
        target = self._path(destination)
        if target.exists():
            raise ObjectExistsError(destination)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(target.name + ".partial")
        shutil.copyfile(local_path, tmp)
        tmp.rename(target)  # atomic, so a crash never leaves a half-written Bronze file

    def download_file(self, source: str, local_path: Path) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self._path(source), local_path)

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def list(self, prefix: str) -> list[str]:
        keys = [
            p.relative_to(self.root).as_posix()
            for p in self.root.rglob("*")
            if p.is_file() and not p.name.endswith(".partial")
        ]
        return sorted(k for k in keys if k.startswith(prefix))
