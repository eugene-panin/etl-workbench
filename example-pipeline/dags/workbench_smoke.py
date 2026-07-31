from __future__ import annotations

import os
from datetime import datetime
from hashlib import sha256

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Param, dag, get_current_context, task


@dag(
    dag_id="workbench_runtime_smoke",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={"message": Param("hello", type="string", minLength=1, maxLength=100)},
    tags=["workbench", "smoke"],
)
def runtime_smoke():
    @task
    def create_message() -> dict[str, str]:
        context = get_current_context()
        return {"message": context["params"]["message"].strip()}

    @task
    def log_message(payload: dict[str, str]) -> None:
        print(payload["message"])

    log_message(create_message())


@dag(
    dag_id="workbench_storage_smoke",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "postgres_conn_id": Param("local_postgres", type="string"),
        "s3_conn_id": Param("local_s3", type="string"),
        "bucket": Param(os.environ.get("ETL_LOCAL_BUCKET", "etl-local"), type="string"),
    },
    tags=["workbench", "smoke"],
)
def storage_smoke():
    @task
    def check_postgres() -> None:
        conn_id = get_current_context()["params"]["postgres_conn_id"]
        row = PostgresHook(postgres_conn_id=conn_id).get_first("SELECT 1")
        if row != (1,):
            raise RuntimeError(f"unexpected PostgreSQL result: {row!r}")

    @task
    def check_object_store() -> None:
        context = get_current_context()
        params = context["params"]
        hook = S3Hook(aws_conn_id=params["s3_conn_id"])
        if not hook.check_for_bucket(params["bucket"]):
            raise RuntimeError(f"bucket does not exist: {params['bucket']}")
        client = hook.get_conn()
        run_token = sha256(str(context["run_id"]).encode()).hexdigest()[:16]
        key = f"_workbench_smoke/{run_token}.txt"
        payload = b"etl-workbench-storage-smoke\n"
        try:
            client.put_object(Bucket=params["bucket"], Key=key, Body=payload)
            response = client.get_object(Bucket=params["bucket"], Key=key)
            downloaded = response["Body"].read()
            if sha256(downloaded).digest() != sha256(payload).digest():
                raise RuntimeError("object content differs after upload")
            listing = client.list_objects_v2(Bucket=params["bucket"], Prefix=key)
            if not any(item["Key"] == key for item in listing.get("Contents", [])):
                raise RuntimeError("uploaded object is missing from listing")
        except BaseException as operation_error:
            try:
                client.delete_object(Bucket=params["bucket"], Key=key)
            except BaseException as cleanup_error:
                operation_error.add_note(
                    f"object cleanup also failed for {key!r}: {cleanup_error!r}"
                )
            raise
        else:
            client.delete_object(Bucket=params["bucket"], Key=key)

    check_postgres() >> check_object_store()


runtime_smoke()
storage_smoke()
