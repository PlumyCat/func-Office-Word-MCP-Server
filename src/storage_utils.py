import logging
import os
from datetime import datetime, timedelta, timezone


# Lazily initialized Blob service client and container name
_blob_service_client = None
_blob_container_name = None


def _init_storage():
    global _blob_service_client, _blob_container_name
    if _blob_service_client is not None:
        return
    try:
        from azure.storage.blob import BlobServiceClient
        try:
            from azure.core.pipeline.transport import RequestsTransport
        except Exception:
            RequestsTransport = None
    except Exception as exc:
        logging.error("Failed to import azure-storage-blob: %s", exc)
        raise
    connection_string = os.environ.get(
        "WORD_STORAGE_CONNECTION_STRING") or os.environ.get("AzureWebJobsStorage")
    if not connection_string:
        raise RuntimeError("AzureWebJobsStorage is not configured")
    _blob_container_name = os.environ.get("WORD_DOCS_CONTAINER", "stword")
    if RequestsTransport is not None:
        try:
            conn_timeout = float(os.environ.get("BLOB_CONN_TIMEOUT", "60"))
            read_timeout = float(os.environ.get("BLOB_READ_TIMEOUT", "300"))
            transport = RequestsTransport(
                connection_timeout=conn_timeout, read_timeout=read_timeout)
            _blob_service_client = BlobServiceClient.from_connection_string(
                connection_string, transport=transport)
        except Exception:
            _blob_service_client = BlobServiceClient.from_connection_string(
                connection_string)
    else:
        _blob_service_client = BlobServiceClient.from_connection_string(
            connection_string)
    try:
        _blob_service_client.create_container(_blob_container_name)
    except Exception as exc:
        # Likely already exists or insufficient permissions; continue
        logging.warning(
            "Could not create container %s: %s", _blob_container_name, exc,
        )


def _get_container_client():
    _init_storage()
    return _blob_service_client.get_container_client(_blob_container_name)


def _get_blob_client(blob_name: str):
    container_client = _get_container_client()
    return container_client.get_blob_client(blob_name)


def _blob_exists(blob_name: str) -> bool:
    try:
        client = _get_blob_client(blob_name)
        try:
            return bool(client.exists())
        except Exception:
            # Fallback for older SDKs
            client.get_blob_properties()
            return True
    except Exception:
        return False


def _with_retries(operation, *, max_attempts: int = 3, base_delay_seconds: float = 0.5):
    """Run an idempotent operation with basic exponential backoff."""
    attempt = 0
    last_exc = None
    while attempt < max_attempts:
        try:
            return operation()
        except Exception as exc:
            last_exc = exc
            attempt += 1
            if attempt >= max_attempts:
                break
            sleep_for = base_delay_seconds * (2 ** (attempt - 1))
            try:
                import time
                time.sleep(sleep_for)
            except Exception as exc:
                logging.warning(
                    "Retry sleep failed for operation %s (user: unknown, file: unknown): %s",
                    getattr(operation, "__name__", repr(operation)),
                    exc,
                )
    if last_exc:
        raise last_exc
    return None


def _ensure_user_paths(user_id: str) -> list[str]:
    """Ensure the user prefix and required sub-prefixes exist by creating placeholder blobs."""
    _init_storage()
    created: list[str] = []
    placeholders = [
        f"{user_id}/.keep",
        f"{user_id}/image_blob/.keep",
        f"{user_id}/templates/.keep",
    ]
    for ph in placeholders:
        try:
            client = _get_blob_client(ph)

            def _op():
                return client.upload_blob(b"", overwrite=False)

            _with_retries(_op)
            created.append(ph)
        except Exception as exc:
            # Exists or not allowed; ignore
            logging.warning("Failed to create placeholder %s: %s", ph, exc)
    return created


def _parse_storage_connection_string(connection_string: str) -> dict:
    parts = connection_string.split(";")
    kv = {}
    for part in parts:
        if not part:
            continue
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        kv[k.strip().lower()] = v.strip()
    return kv


def _generate_blob_sas_url(blob_name: str, permissions: str = "r") -> dict:
    _init_storage()
    try:
        from azure.storage.blob import generate_blob_sas, BlobSasPermissions
    except Exception as exc:
        logging.error(
            "Failed to import azure-storage-blob SAS helpers: %s", exc)
        raise
    conn = os.environ.get("WORD_STORAGE_CONNECTION_STRING") or os.environ.get(
        "AzureWebJobsStorage")
    if not conn:
        raise RuntimeError(
            "No storage connection string available for SAS generation")
    kv = _parse_storage_connection_string(conn)
    account_name = kv.get("accountname")
    account_key = kv.get("accountkey")
    if not account_name or not account_key:
        raise RuntimeError(
            "Storage connection string missing AccountName/AccountKey for SAS generation")
    ttl_seconds_str = os.environ.get("WORD_BLOB_TTL_SECONDS", "3600")
    try:
        ttl_seconds = int(ttl_seconds_str)
    except Exception:
        ttl_seconds = 3600
    expiry = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=_blob_container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions.from_string(permissions),
        expiry=expiry,
    )
    blob_url = _get_blob_client(blob_name).url
    return {"url": f"{blob_url}?{sas_token}", "expiresUtc": expiry.isoformat()}


def _word_doc_path(filename: str) -> str:
    safe_name = os.path.basename(filename)
    return os.path.join("/tmp", safe_name)


def _download_blob_to_temp(blob_name: str) -> str:
    blob_client = _get_blob_client(blob_name)
    temp_path = _word_doc_path(blob_name)
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    with open(temp_path, "wb") as f:
        data = blob_client.download_blob()
        f.write(data.readall())
    return temp_path


def _upload_file_to_blob(local_path: str, blob_name: str) -> None:
    blob_client = _get_blob_client(blob_name)
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True, max_concurrency=1)
    _apply_blob_ttl(blob_client)


def _apply_blob_ttl(blob_client) -> None:
    ttl_seconds_str = os.environ.get("WORD_BLOB_TTL_SECONDS", "3600")
    try:
        ttl_seconds = int(ttl_seconds_str)
    except Exception:
        ttl_seconds = 3600
    if ttl_seconds <= 0:
        return
    expires_on = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    # Try SDK-level expiry; fall back to metadata if unavailable
    try:
        # Some SDK versions use set_blob_expiry(expiry_time=...); others use expires_on
        try:
            blob_client.set_blob_expiry(expires_on=expires_on)
            return
        except TypeError:
            blob_client.set_blob_expiry(expiry_time=expires_on)
            return
    except Exception as exc:
        logging.warning(
            "Failed to set expiry for blob %s: %s",
            getattr(blob_client, "blob_name", "unknown"),
            exc,
        )
    try:
        blob_client.set_blob_metadata({"expiry_utc": expires_on.isoformat()})
    except Exception as exc:
        logging.warning(
            "Failed to set metadata expiry for blob %s: %s",
            getattr(blob_client, "blob_name", "unknown"),
            exc,
        )


def _upload_bytes_to_blob(blob_client, data: bytes, content_settings=None) -> None:
    def _op():
        return blob_client.upload_blob(
            data,
            overwrite=True,
            content_settings=content_settings,
            max_concurrency=1,
        )

    _with_retries(_op)


__all__ = [
    "_init_storage",
    "_get_container_client",
    "_get_blob_client",
    "_blob_exists",
    "_with_retries",
    "_ensure_user_paths",
    "_parse_storage_connection_string",
    "_generate_blob_sas_url",
    "_word_doc_path",
    "_download_blob_to_temp",
    "_upload_file_to_blob",
    "_apply_blob_ttl",
    "_upload_bytes_to_blob",
    "_blob_service_client",
    "_blob_container_name",
]

