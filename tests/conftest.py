from __future__ import annotations

import os

import pytest

from ingestion.storage.local_storage import LocalStorageClient


@pytest.fixture
def storage(tmp_path) -> LocalStorageClient:
    return LocalStorageClient(tmp_path / "lake")


@pytest.fixture
def warehouse():
    """A clean warehouse database. Needs HEALTHFLOW_TEST_DSN; integration tests skip without it.

    WARNING: drops and recreates the HealthFlow schemas. Point it at a throwaway database.
    """
    dsn = os.environ.get("HEALTHFLOW_TEST_DSN")
    if not dsn:
        pytest.skip("set HEALTHFLOW_TEST_DSN to run integration tests")
    from ingestion.db import apply_ddl, connect

    conn = connect(dsn)
    for schema in ("raw_staging", "staging", "intermediate", "marts", "audit"):
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    apply_ddl(conn)
    yield conn
    conn.close()
