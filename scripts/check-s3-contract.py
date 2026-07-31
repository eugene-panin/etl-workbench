#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import os
from time import sleep
from urllib.request import urlopen
from uuid import uuid4

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def read_object(client, bucket: str, key: str) -> bytes:
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


def wait_for_bucket(client, bucket: str) -> None:
    for attempt in range(20):
        try:
            client.head_bucket(Bucket=bucket)
            return
        except Exception:
            if attempt == 19:
                raise
            sleep(0.5)


def cleanup_objects(client, bucket: str, prefix: str, keys: list[str]) -> None:
    deleted = client.delete_objects(
        Bucket=bucket,
        Delete={"Objects": [{"Key": key} for key in keys], "Quiet": True},
    )
    if deleted.get("Errors"):
        raise RuntimeError(f"contract cleanup failed: {deleted['Errors']!r}")
    remaining = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if remaining.get("Contents"):
        raise RuntimeError(f"contract cleanup left objects below {prefix!r}")


def main() -> None:
    conn_id = os.environ.get("S3_CONTRACT_CONN_ID", "local_s3")
    bucket = os.environ.get("ETL_LOCAL_BUCKET", "etl-local")
    client = S3Hook(aws_conn_id=conn_id).get_conn()
    prefix = f"_workbench_contract/{uuid4().hex}"
    small_key = f"{prefix}/small.txt"
    copy_key = f"{prefix}/copy.txt"
    multipart_key = f"{prefix}/multipart.bin"
    keys = [small_key, copy_key, multipart_key]
    small_payload = b"etl-workbench-s3-contract\n"
    multipart_payload = b"x" * (11 * 1024 * 1024)
    bucket_ready = False

    try:
        wait_for_bucket(client, bucket)
        bucket_ready = True
        client.put_object(
            Bucket=bucket,
            Key=small_key,
            Body=small_payload,
            ContentType="text/plain",
            Metadata={"contract": "etl-workbench"},
        )

        head = client.head_object(Bucket=bucket, Key=small_key)
        if head["ContentLength"] != len(small_payload):
            raise RuntimeError("small object size differs after upload")
        if head["Metadata"].get("contract") != "etl-workbench":
            raise RuntimeError("small object metadata differs after upload")
        if read_object(client, bucket, small_key) != small_payload:
            raise RuntimeError("small object content differs after upload")

        listed = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        listed_keys = {item["Key"] for item in listed.get("Contents", [])}
        if small_key not in listed_keys:
            raise RuntimeError("uploaded object is missing from prefix listing")

        presigned_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": small_key},
            ExpiresIn=60,
        )
        with urlopen(presigned_url, timeout=30) as response:
            if response.read() != small_payload:
                raise RuntimeError("presigned GET returned different content")

        client.copy_object(
            Bucket=bucket,
            Key=copy_key,
            CopySource={"Bucket": bucket, "Key": small_key},
        )
        if read_object(client, bucket, copy_key) != small_payload:
            raise RuntimeError("copied object content differs from source")

        multipart = client.create_multipart_upload(
            Bucket=bucket,
            Key=multipart_key,
            ContentType="application/octet-stream",
        )
        upload_id = multipart["UploadId"]
        try:
            parts = []
            chunk_size = 5 * 1024 * 1024
            for part_number, offset in enumerate(
                range(0, len(multipart_payload), chunk_size),
                start=1,
            ):
                uploaded = client.upload_part(
                    Bucket=bucket,
                    Key=multipart_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=multipart_payload[offset : offset + chunk_size],
                )
                parts.append({"ETag": uploaded["ETag"], "PartNumber": part_number})
            client.complete_multipart_upload(
                Bucket=bucket,
                Key=multipart_key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            upload_id = None
        except BaseException as operation_error:
            try:
                client.abort_multipart_upload(
                    Bucket=bucket,
                    Key=multipart_key,
                    UploadId=upload_id,
                )
            except BaseException as abort_error:
                operation_error.add_note(
                    f"multipart abort also failed: {abort_error!r}"
                )
            raise

        downloaded = read_object(client, bucket, multipart_key)
        if sha256(downloaded).digest() != sha256(multipart_payload).digest():
            raise RuntimeError("multipart object content differs after upload")
    except BaseException as operation_error:
        if bucket_ready:
            try:
                cleanup_objects(client, bucket, prefix, keys)
            except BaseException as cleanup_error:
                operation_error.add_note(
                    f"contract cleanup also failed: {cleanup_error!r}"
                )
        raise
    else:
        cleanup_objects(client, bucket, prefix, keys)

    print(
        f"S3 contract passed for connection {conn_id!r} and bucket {bucket!r}: "
        "put, head, get, list, presigned GET, copy, multipart upload, delete"
    )


if __name__ == "__main__":
    main()
