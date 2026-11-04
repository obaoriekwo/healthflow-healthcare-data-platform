from ingestion.config import Settings
from ingestion.storage.base import ObjectExistsError, StorageClient


def get_storage(settings: Settings) -> StorageClient:
    """Return the storage backend named in settings. Ingestion code only ever sees StorageClient."""
    if settings.storage_backend == "local":
        from ingestion.storage.local_storage import LocalStorageClient

        return LocalStorageClient(settings.storage_local_root)
    if settings.storage_backend == "azure":
        from ingestion.storage.azure_storage import AzureBlobStorageClient

        if not settings.azure_connection_string:
            raise ValueError("STORAGE_BACKEND=azure needs AZURE_STORAGE_CONNECTION_STRING")
        return AzureBlobStorageClient(settings.azure_connection_string, settings.azure_container)
    raise ValueError(f"Unknown STORAGE_BACKEND: {settings.storage_backend!r}")


__all__ = ["ObjectExistsError", "StorageClient", "get_storage"]
