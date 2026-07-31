#!/usr/bin/env sh
set -eu

expected_connection_id="${EXPECTED_AIRFLOW_CONNECTION_ID:-}"

docker compose exec -T \
    -e EXPECTED_AIRFLOW_CONNECTION_ID="$expected_connection_id" \
    airflow python - <<'PY'
from __future__ import annotations

import json
import os
from pathlib import Path

from airflow.models.connection import Connection
from airflow.settings import Session, engine
from sqlalchemy import text


if engine.dialect.name != "postgresql":
    raise RuntimeError(
        f"Airflow metadata backend is {engine.dialect.name}, expected postgresql"
    )

with engine.connect() as connection:
    connection.execute(text("SELECT 1"))

marker_path = Path("/var/lib/airflow/.metadata-postgres-migrated.json")
marker = json.loads(marker_path.read_text())
if marker.get("target", {}).get("database") != "airflow":
    raise RuntimeError(f"unexpected metadata marker target: {marker}")
if marker_path.stat().st_mode & 0o777 != 0o600:
    raise RuntimeError("metadata migration marker must have mode 0600")

expected = os.environ.get("EXPECTED_AIRFLOW_CONNECTION_ID")
if expected:
    session = Session()
    try:
        migrated = (
            session.query(Connection).filter(Connection.conn_id == expected).one()
        )
        if not migrated.host:
            raise RuntimeError(f"migrated connection has no host: {expected}")
    finally:
        session.close()

print(
    "Airflow metadata contract passed: "
    f"backend=postgresql mode={marker['mode']}"
)
PY
