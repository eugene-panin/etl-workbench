from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess

from sqlalchemy import MetaData, create_engine, func, select, text
from sqlalchemy.engine import Engine, make_url


AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/var/lib/airflow"))
SQLITE_SOURCE = AIRFLOW_HOME / "airflow.db"
BACKUP_DIRECTORY = AIRFLOW_HOME / "backups"
SQLITE_BACKUP = BACKUP_DIRECTORY / "airflow-sqlite-pre-postgres.db"
MIGRATION_MARKER = AIRFLOW_HOME / ".metadata-postgres-migrated.json"
MIGRATION_VERSION = 1
MEANINGFUL_TARGET_TABLES = (
    "connection",
    "dag_run",
    "task_instance",
    "variable",
    "xcom",
)


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def target_identity(database_url: str) -> dict[str, object]:
    url = make_url(database_url)
    return {
        "drivername": url.drivername,
        "host": url.host,
        "port": url.port,
        "database": url.database,
    }


def write_private_json(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def database_migrate() -> None:
    subprocess.run(["airflow", "db", "migrate"], check=True)


def sqlite_backup(source: Path, backup: Path) -> str:
    backup.parent.mkdir(mode=0o700, exist_ok=True)
    backup.parent.chmod(0o700)
    temporary = backup.with_suffix(backup.suffix + ".tmp")
    temporary.unlink(missing_ok=True)
    source_uri = f"file:{source}?mode=ro"
    with (
        sqlite3.connect(source_uri, uri=True) as source_connection,
        sqlite3.connect(temporary) as backup_connection,
    ):
        source_connection.backup(backup_connection)
    temporary.chmod(0o600)

    with sqlite3.connect(temporary) as connection:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    if not integrity or integrity[0] != "ok":
        raise RuntimeError(f"SQLite backup integrity check failed: {temporary}")
    temporary.replace(backup)

    digest = hashlib.sha256()
    with backup.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def meaningful_target_rows(engine: Engine) -> dict[str, int]:
    metadata = MetaData()
    metadata.reflect(engine, only=lambda name, _: name in MEANINGFUL_TARGET_TABLES)
    counts: dict[str, int] = {}
    with engine.connect() as connection:
        for name in MEANINGFUL_TARGET_TABLES:
            table = metadata.tables.get(name)
            if table is not None:
                counts[name] = connection.execute(
                    select(func.count()).select_from(table)
                ).scalar_one()
    return counts


def reset_postgres_sequences(connection, metadata: MetaData) -> int:
    reset_count = 0
    for table in metadata.sorted_tables:
        for column in table.columns:
            sequence = connection.execute(
                text(
                    "SELECT pg_get_serial_sequence(:table_name, :column_name)"
                ),
                {"table_name": table.name, "column_name": column.name},
            ).scalar_one()
            if not sequence:
                continue
            maximum = connection.execute(select(func.max(column))).scalar_one()
            if maximum is None:
                connection.execute(
                    text(
                        "SELECT setval(CAST(:sequence AS regclass), 1, false)"
                    ),
                    {"sequence": sequence},
                )
            else:
                connection.execute(
                    text(
                        "SELECT setval("
                        "CAST(:sequence AS regclass), :maximum, true)"
                    ),
                    {"sequence": sequence, "maximum": maximum},
                )
            reset_count += 1
    return reset_count


def copy_sqlite_to_postgres(source_url: str, target_url: str) -> dict[str, int]:
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    source_metadata = MetaData()
    target_metadata = MetaData()
    source_metadata.reflect(source_engine)
    target_metadata.reflect(target_engine)

    source_tables = set(source_metadata.tables)
    target_tables = set(target_metadata.tables)
    missing_tables = sorted(source_tables - target_tables)
    if missing_tables:
        raise RuntimeError(
            "PostgreSQL schema is missing SQLite tables: "
            + ", ".join(missing_tables)
        )

    table_names = sorted((source_tables & target_tables) - {"alembic_version"})
    preparer = target_engine.dialect.identifier_preparer
    quoted_tables = ", ".join(preparer.quote(name) for name in table_names)
    copied: dict[str, int] = {}

    with source_engine.connect() as source, target_engine.begin() as target:
        target.execute(text("SET LOCAL session_replication_role = replica"))
        if table_names:
            target.execute(
                text(
                    f"TRUNCATE TABLE {quoted_tables} "
                    "RESTART IDENTITY CASCADE"
                )
            )

        for name in table_names:
            source_table = source_metadata.tables[name]
            target_table = target_metadata.tables[name]
            columns = [
                column.name
                for column in target_table.columns
                if column.name in source_table.c
            ]
            result = source.execute(
                select(*(source_table.c[column] for column in columns))
            )
            row_count = 0
            while batch := result.mappings().fetchmany(1000):
                target.execute(
                    target_table.insert(), [dict(row) for row in batch]
                )
                row_count += len(batch)
            copied[name] = row_count

        for name, expected_count in copied.items():
            actual_count = target.execute(
                select(func.count()).select_from(target_metadata.tables[name])
            ).scalar_one()
            if actual_count != expected_count:
                raise RuntimeError(
                    f"row count mismatch for {name}: "
                    f"source={expected_count}, target={actual_count}"
                )
        reset_postgres_sequences(target, target_metadata)

    source_engine.dispose()
    target_engine.dispose()
    return copied


def main() -> None:
    target_url = required_environment(
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    )
    identity = target_identity(target_url)
    if identity["drivername"].startswith("sqlite"):
        raise RuntimeError("PostgreSQL metadata URL is required")

    if MIGRATION_MARKER.exists():
        marker = json.loads(MIGRATION_MARKER.read_text())
        if marker.get("target") != identity:
            raise RuntimeError(
                "metadata target changed after migration; "
                f"marker={marker.get('target')}, current={identity}"
            )
        database_migrate()
        print(f"Airflow metadata is already initialized in {identity}")
        return

    backup_digest: str | None = None
    if SQLITE_SOURCE.exists():
        backup_digest = sqlite_backup(SQLITE_SOURCE, SQLITE_BACKUP)

    database_migrate()
    target_engine = create_engine(target_url)
    target_rows = meaningful_target_rows(target_engine)
    target_engine.dispose()
    if any(target_rows.values()):
        raise RuntimeError(
            "target Airflow metadata database is not empty: "
            f"{target_rows}"
        )

    copied: dict[str, int] = {}
    mode = "fresh-postgresql"
    if backup_digest:
        copied = copy_sqlite_to_postgres(
            f"sqlite:///{SQLITE_BACKUP}", target_url
        )
        mode = "sqlite-to-postgresql"

    marker = {
        "version": MIGRATION_VERSION,
        "mode": mode,
        "migrated_at": datetime.now(UTC).isoformat(),
        "source": str(SQLITE_SOURCE) if SQLITE_SOURCE.exists() else None,
        "backup": str(SQLITE_BACKUP) if backup_digest else None,
        "backup_sha256": backup_digest,
        "target": identity,
        "tables": copied,
        "rows": sum(copied.values()),
    }
    write_private_json(MIGRATION_MARKER, marker)
    if copied:
        print(
            "Migrated Airflow metadata from SQLite to PostgreSQL: "
            f"{len(copied)} tables, {sum(copied.values())} rows"
        )
        print(f"Rollback SQLite backup: {SQLITE_BACKUP}")
    else:
        print(f"Initialized fresh Airflow metadata in PostgreSQL: {identity}")


if __name__ == "__main__":
    main()
