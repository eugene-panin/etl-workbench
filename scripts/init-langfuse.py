from __future__ import annotations

import os

import boto3
from botocore.exceptions import ClientError
import psycopg2
from psycopg2 import sql


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def ensure_database() -> None:
    database = required_environment("LANGFUSE_POSTGRES_DATABASE")
    admin_url = required_environment("LANGFUSE_POSTGRES_ADMIN_URL")
    connection = psycopg2.connect(admin_url)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,))
            if cursor.fetchone() is None:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
                )
                print(f"Created PostgreSQL database: {database}")
            else:
                print(f"PostgreSQL database already exists: {database}")
    finally:
        connection.close()


def ensure_bucket() -> None:
    bucket = required_environment("LANGFUSE_S3_EVENT_UPLOAD_BUCKET")
    region = required_environment("LANGFUSE_S3_EVENT_UPLOAD_REGION")
    client = boto3.client(
        "s3",
        endpoint_url=required_environment("LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT"),
        aws_access_key_id=required_environment(
            "LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID"
        ),
        aws_secret_access_key=required_environment(
            "LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY"
        ),
        region_name=region,
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status not in {403, 404}:
            raise
        create_arguments: dict[str, object] = {"Bucket": bucket}
        if region != "us-east-1":
            create_arguments["CreateBucketConfiguration"] = {
                "LocationConstraint": region
            }
        client.create_bucket(**create_arguments)
        print(f"Created S3 bucket: {bucket}")
    else:
        print(f"S3 bucket already exists: {bucket}")


if __name__ == "__main__":
    ensure_database()
    ensure_bucket()
