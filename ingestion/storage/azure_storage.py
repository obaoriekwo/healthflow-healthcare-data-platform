"""Bronze on Azure Blob Storage: Azurite locally, ADLS Gen2 (Blob endpoint) in Azure.

Only Blob APIs are used, because Azurite does not emulate the ADLS Gen2 hierarchical
namespace or Data Lake filesystem APIs. Behaviour that depends on hierarchical
namespace (directory ACLs, atomic renames) must be tested against a real dev account.
"""

from __future__ import annotations

import logging
from pathlib import Path

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

from ingestion.storage.base import ObjectExistsError, StorageClient

# The SDK logs every HTTP request and response at INFO; keep pipeline logs readable.
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


class AzureBlobStorageClient(StorageClient):
    def __init__(self, connection_string: str, container: str):
        service = BlobServiceClient.from_connection_string(connection_string)
        self.container = service.get_container_client(container)
        try:
            self.container.create_container()
        except ResourceExistsError:
            pass

    def upload_file(self, local_path: Path, destination: str) -> None:
        try:
            with open(local_path, "rb") as fh:
                self.container.upload_blob(destination, fh, overwrite=False)
        except ResourceExistsError as exc:
            raise ObjectExistsError(destination) from exc

    def download_file(self, source: str, local_path: Path) -> None:
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as fh:
            self.container.download_blob(source).readinto(fh)

    def exists(self, key: str) -> bool:
        return self.container.get_blob_client(key).exists()

    def list(self, prefix: str) -> list[str]:
        return sorted(b.name for b in self.container.list_blobs(name_starts_with=prefix))
