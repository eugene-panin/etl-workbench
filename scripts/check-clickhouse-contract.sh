#!/usr/bin/env sh
set -eu

docker compose --profile analytics exec -T airflow python - <<'PY'
from __future__ import annotations

from uuid import uuid4

from airflow.sdk.bases.hook import BaseHook
import clickhouse_connect


connection = BaseHook.get_connection("local_clickhouse")
table = "_workbench_contract_" + uuid4().hex
client = clickhouse_connect.get_client(
    host=connection.host,
    port=connection.port or 8123,
    username=connection.login,
    password=connection.password,
    database=connection.schema or "analytics",
)

try:
    if not client.ping():
        raise RuntimeError("ClickHouse ping failed")
    client.command(
        f"CREATE TABLE {table} "
        "(event_id UInt64, payload String) "
        "ENGINE = MergeTree ORDER BY event_id"
    )
    client.insert(
        table,
        [[1, "etl-workbench-clickhouse-contract"]],
        column_names=["event_id", "payload"],
    )
    result = client.query(
        f"SELECT event_id, payload FROM {table} WHERE event_id = 1"
    ).result_rows
    expected = [(1, "etl-workbench-clickhouse-contract")]
    if result != expected:
        raise RuntimeError(f"unexpected ClickHouse result: {result!r}")
finally:
    client.command(f"DROP TABLE IF EXISTS {table}")
    client.close()

print("ClickHouse contract passed: Airflow created, queried and dropped a table")
PY
