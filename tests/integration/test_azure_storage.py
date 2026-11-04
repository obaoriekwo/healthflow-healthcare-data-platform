"""Checks the Azure Blob client against Azurite (or a real dev account).

Runs only when HEALTHFLOW_TEST_AZURE_CONNECTION_STRING is set, e.g. with Azurite from docker compose:
    DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=<well-known key>;
    BlobEndpoint=http://localhost:10000/devstoreaccount1;
"""

import os
import uuid

import pytest

from ingestion.storage import ObjectExistsError


@pytest.fixture
def azure_storage():
    conn_str = os.environ.get("HEALTHFLOW_TEST_AZURE_CONNECTION_STRING")
    if not conn_str:
        pytest.skip("set HEALTHFLOW_TEST_AZURE_CONNECTION_STRING to run Azure storage tests")
    from ingestion.storage.azure_storage import AzureBlobStorageClient

    return AzureBlobStorageClient(conn_str, f"test-{uuid.uuid4().hex[:12]}")


def test_upload_is_immutable_and_round_trips(azure_storage, tmp_path):
    src = tmp_path / "a.csv"
    src.write_text("Id\n1\n")
    azure_storage.upload_file(src, "bronze/encounters/available_date=2026-01-01/a.csv")
    with pytest.raises(ObjectExistsError):
        azure_storage.upload_file(src, "bronze/encounters/available_date=2026-01-01/a.csv")

    azure_storage.download_file("bronze/encounters/available_date=2026-01-01/a.csv", tmp_path / "b.csv")
    assert (tmp_path / "b.csv").read_text() == "Id\n1\n"
    assert azure_storage.exists("bronze/encounters/available_date=2026-01-01/a.csv")
    assert azure_storage.list("bronze/") == ["bronze/encounters/available_date=2026-01-01/a.csv"]
    azure_storage.container.delete_container()
