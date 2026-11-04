"""Landing -> Bronze -> raw_staging, for one source and one delivery date.

Idempotency comes from three mechanisms, not from upserting on business keys:

1. A delivery is identified by (source, delivery date, sha256 of the bytes).
   file_id = uuid5 of that triple, so the same delivery always gets the same id.
   A delivery already in audit.ingestion_file_audit was fully ingested: it is skipped.
   The date is part of the identity because legitimate files can be byte-identical
   on different days (for example two empty daily files).
2. Bronze keys include a checksum prefix and are never overwritten. Re-uploading
   identical bytes after a crash is accepted; anything else raises.
3. The audit row and the raw rows for a file are written in ONE transaction. A
   crash mid-load leaves nothing behind, so the rerun starts clean.

Corrected records arrive in later files and are appended as new raw rows.
Choosing the latest version per encounter is dbt's job (staging layer).
"""

from __future__ import annotations

import csv
import hashlib
import logging
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import psycopg
from psycopg import sql

from ingestion.contracts import CONTRACTS, SourceContract
from ingestion.storage import ObjectExistsError, StorageClient

log = logging.getLogger(__name__)

FILE_ID_NAMESPACE = uuid.UUID("6f1c2a9e-3b7d-4c55-9a40-1e2f6d8b7c01")
DEFAULT_MAX_REJECT_RATIO = 0.10  # more bad rows than this and the whole file is quarantined


class SourceFileMissing(Exception):
    """No file was delivered for the requested date."""


class StorageIntegrityError(Exception):
    """An existing storage object has different content than expected."""


@dataclass
class FileResult:
    file_name: str
    status: str  # loaded | quarantined | skipped
    records_loaded: int = 0
    records_rejected: int = 0
    reason: str | None = None


@dataclass
class RunSummary:
    run_id: str
    source_name: str
    process_date: date
    files: list[FileResult] = field(default_factory=list)

    def count(self, status: str) -> int:
        return sum(1 for f in self.files if f.status == status)

    @property
    def records_loaded(self) -> int:
        return sum(f.records_loaded for f in self.files)

    @property
    def records_rejected(self) -> int:
        return sum(f.records_rejected for f in self.files)


# --- helpers ------------------------------------------------------------------


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def landing_files(landing_dir: Path, source: str, day: date) -> list[Path]:
    return sorted((Path(landing_dir) / source / f"available_date={day.isoformat()}").glob("*.csv"))


def _put_immutable(storage: StorageClient, local: Path, key: str, checksum: str, workdir: Path) -> None:
    """Upload once. If the key exists, accept it only if its bytes are identical."""
    try:
        storage.upload_file(local, key)
    except ObjectExistsError:
        existing = workdir / f"existing_{uuid.uuid4().hex}"
        storage.download_file(key, existing)
        if sha256_file(existing) != checksum:
            raise StorageIntegrityError(f"{key} already exists with different content") from None
        log.info("Storage object %s already present with identical content (rerun)", key)


def _insert_file_audit(
    cur,
    *,
    file_id,
    contract,
    landing_file,
    available_date,
    checksum,
    storage_path,
    run_id,
    record_count,
    loaded,
    rejected,
    status,
    reason,
) -> None:
    cur.execute(
        """
        INSERT INTO audit.ingestion_file_audit
            (file_id, source_name, original_file_name, available_date, file_checksum, storage_path,
             run_id, record_count, records_loaded, records_rejected, ingestion_status, rejection_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            file_id,
            contract.name,
            landing_file.name,
            available_date,
            checksum,
            storage_path,
            run_id,
            record_count,
            loaded,
            rejected,
            status,
            reason,
        ),
    )


def _copy_rows(
    cur, contract: SourceContract, file_id: uuid.UUID, available_date: date, rows: list[tuple[int, dict[str, str]]]
) -> None:
    schema, table = contract.raw_table.split(".")
    columns = [c.lower() for c in contract.columns] + ["_file_id", "_row_number", "_event_date", "_available_date"]
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(schema, table), sql.SQL(", ").join(map(sql.Identifier, columns))
    )
    with cur.copy(statement) as copy:
        for row_number, row in rows:
            values = [row[c] if row[c] != "" else None for c in contract.columns]  # empty string -> NULL
            copy.write_row([*values, file_id, row_number, contract.event_date(row), available_date])


# --- one file -----------------------------------------------------------------


def ingest_file(
    conn: psycopg.Connection,
    storage: StorageClient,
    contract: SourceContract,
    landing_file: Path,
    available_date: date,
    run_id: str,
    max_reject_ratio: float = DEFAULT_MAX_REJECT_RATIO,
) -> FileResult:
    checksum = sha256_file(landing_file)
    file_id = uuid.uuid5(FILE_ID_NAMESPACE, f"{contract.name}|{available_date.isoformat()}|{checksum}")

    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM audit.ingestion_file_audit WHERE file_id = %s", (file_id,))
        if cur.fetchone():
            log.info("%s already ingested (checksum %s...), skipping", landing_file.name, checksum[:12])
            return FileResult(landing_file.name, "skipped", reason="already ingested")

    day = available_date.isoformat()
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)

        with open(landing_file, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            header_error = contract.header_error(reader.fieldnames)
            rows = [] if header_error else list(reader)

        def quarantine_whole_file(reason: str, record_count: int) -> FileResult:
            key = f"quarantine/rejected_files/{contract.name}/available_date={day}/{file_id}__{landing_file.name}"
            _put_immutable(storage, landing_file, key, checksum, workdir)
            with conn.transaction(), conn.cursor() as cur:
                _insert_file_audit(
                    cur,
                    file_id=file_id,
                    contract=contract,
                    landing_file=landing_file,
                    available_date=available_date,
                    checksum=checksum,
                    storage_path=key,
                    run_id=run_id,
                    record_count=record_count,
                    loaded=0,
                    rejected=record_count,
                    status="quarantined",
                    reason=reason,
                )
            log.warning("Quarantined %s: %s", landing_file.name, reason)
            return FileResult(landing_file.name, "quarantined", records_rejected=record_count, reason=reason)

        if header_error:
            return quarantine_whole_file(header_error, record_count=0)

        # 1. Bronze: immutable copy of exactly what the source sent.
        bronze_key = f"bronze/{contract.name}/available_date={day}/{landing_file.stem}__{checksum[:12]}.csv"
        _put_immutable(storage, landing_file, bronze_key, checksum, workdir)

        # 2. Row checks.
        valid: list[tuple[int, dict[str, str]]] = []
        invalid: list[tuple[int, dict[str, str], str]] = []
        for row_number, row in enumerate(rows, start=1):
            error = contract.row_error(row)
            if error:
                invalid.append((row_number, row, error))
            else:
                valid.append((row_number, row))

        if rows and len(invalid) / len(rows) > max_reject_ratio:
            return quarantine_whole_file(
                f"{len(invalid)} of {len(rows)} rows failed validation (limit {max_reject_ratio:.0%})",
                record_count=len(rows),
            )

        if invalid:
            reject_file = workdir / f"{file_id}.csv"
            with open(reject_file, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(["_row_number", "_rejection_reason", *contract.columns])
                for row_number, row, error in invalid:
                    writer.writerow([row_number, error, *(row[c] for c in contract.columns)])
            _put_immutable(
                storage,
                reject_file,
                f"quarantine/invalid_records/{contract.name}/available_date={day}/{file_id}.csv",
                sha256_file(reject_file),
                workdir,
            )

        # 3. Audit row + raw rows, all or nothing.
        with conn.transaction(), conn.cursor() as cur:
            _insert_file_audit(
                cur,
                file_id=file_id,
                contract=contract,
                landing_file=landing_file,
                available_date=available_date,
                checksum=checksum,
                storage_path=bronze_key,
                run_id=run_id,
                record_count=len(rows),
                loaded=len(valid),
                rejected=len(invalid),
                status="loaded",
                reason=f"{len(invalid)} rows quarantined" if invalid else None,
            )
            _copy_rows(cur, contract, file_id, available_date, valid)

    log.info("Loaded %s: %d rows, %d rejected", landing_file.name, len(valid), len(invalid))
    return FileResult(landing_file.name, "loaded", records_loaded=len(valid), records_rejected=len(invalid))


# --- one run (one source, one date) ---------------------------------------------


def _start_run(conn, run_id: str, dag_id: str | None, source: str, process_date: date) -> None:
    with conn.transaction(), conn.cursor() as cur:
        # A retry of the same Airflow run reuses run_id: reset the row instead of failing.
        cur.execute(
            """
            INSERT INTO audit.pipeline_run_audit (run_id, dag_id, source_name, process_date, status)
            VALUES (%s, %s, %s, %s, 'running')
            ON CONFLICT (run_id) DO UPDATE SET
                start_time = now(), end_time = NULL, status = 'running', error_message = NULL,
                files_seen = 0, files_loaded = 0, files_skipped = 0, files_quarantined = 0,
                records_loaded = 0, records_rejected = 0
            """,
            (run_id, dag_id, source, process_date),
        )


def _finish_run(conn, summary: RunSummary, status: str, error: str | None = None) -> None:
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            """
            UPDATE audit.pipeline_run_audit SET
                end_time = now(), status = %s, error_message = %s,
                files_seen = %s, files_loaded = %s, files_skipped = %s, files_quarantined = %s,
                records_loaded = %s, records_rejected = %s
            WHERE run_id = %s
            """,
            (
                status,
                error,
                len(summary.files),
                summary.count("loaded"),
                summary.count("skipped"),
                summary.count("quarantined"),
                summary.records_loaded,
                summary.records_rejected,
                summary.run_id,
            ),
        )


def run_ingestion(
    conn: psycopg.Connection,
    storage: StorageClient,
    *,
    source: str,
    process_date: date,
    landing_dir: Path,
    run_id: str | None = None,
    dag_id: str | None = None,
) -> RunSummary:
    if not conn.autocommit:
        # Each file must commit on its own; inside an outer transaction conn.transaction()
        # would only create savepoints and nothing would be durable until the very end.
        raise ValueError("run_ingestion needs an autocommit connection (see ingestion.db.connect)")
    contract = CONTRACTS[source]
    run_id = run_id or f"manual__{source}__{process_date.isoformat()}__{uuid.uuid4().hex[:8]}"
    summary = RunSummary(run_id=run_id, source_name=source, process_date=process_date)

    _start_run(conn, run_id, dag_id, source, process_date)
    try:
        files = landing_files(landing_dir, source, process_date)
        if not files:
            raise SourceFileMissing(f"No {source} file delivered for {process_date.isoformat()}")
        for landing_file in files:
            summary.files.append(ingest_file(conn, storage, contract, landing_file, process_date, run_id))
    except Exception as exc:
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            conn.rollback()
        _finish_run(conn, summary, "failed", f"{type(exc).__name__}: {exc}")
        raise
    _finish_run(conn, summary, "success")
    return summary
