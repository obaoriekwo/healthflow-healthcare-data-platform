from __future__ import annotations

from pathlib import Path

import psycopg

DDL_DIR = Path(__file__).resolve().parent.parent / "sql" / "ddl"


def connect(dsn: str) -> psycopg.Connection:
    """Autocommit connection: the pipeline opens explicit transactions where it needs them."""
    return psycopg.connect(dsn, autocommit=True)


def apply_ddl(conn: psycopg.Connection, ddl_dir: Path = DDL_DIR) -> list[str]:
    """Apply every sql/ddl/*.sql file in name order. The files are idempotent."""
    applied = []
    for path in sorted(ddl_dir.glob("*.sql")):
        with conn.transaction():
            conn.execute(path.read_text(encoding="utf-8"))
        applied.append(path.name)
    return applied
