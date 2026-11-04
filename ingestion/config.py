"""Settings read from environment variables, so the same code runs locally, in Docker and in Azure."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    warehouse_dsn: str
    storage_backend: str  # "local" | "azure"
    storage_local_root: Path
    azure_connection_string: str | None
    azure_container: str
    landing_dir: Path


def load_settings() -> Settings:
    return Settings(
        warehouse_dsn=os.environ.get(
            "WAREHOUSE_DSN",
            "postgresql://healthflow:healthflow@localhost:5433/healthflow_warehouse",
        ),
        storage_backend=os.environ.get("STORAGE_BACKEND", "local"),
        storage_local_root=Path(os.environ.get("STORAGE_LOCAL_ROOT", "./data/bronze")),
        azure_connection_string=os.environ.get("AZURE_STORAGE_CONNECTION_STRING"),
        azure_container=os.environ.get("AZURE_STORAGE_CONTAINER", "healthflow-data"),
        landing_dir=Path(os.environ.get("LANDING_DIR", "./data/landing")),
    )
