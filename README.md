# ETL Workbench

A small local Apache Airflow workbench for trusted, code-defined ETL pipelines.
It runs Airflow and, when requested, local PostgreSQL and S3-compatible object
storage. Pipeline code and data contracts stay in their own repositories.

This is a single-user development tool. It is not a shared scheduler, control
plane, deployment platform, or isolation boundary for untrusted DAG code.

## Requirements

- Docker Desktop or Docker Engine with Compose
- at least 4 GB of memory available to Docker
- a pipeline Git repository containing `dags/`
- `Dockerfile.airflow` in that repository when the pipeline needs its own image

## Start a Git pipeline

For a public repository:

```bash
./bin/etl-workbench https://github.com/example/acme-pipeline.git
```

For a private SSH repository:

```bash
./bin/etl-workbench git@github.com:example/acme-pipeline.git \
  --ssh-key ~/.ssh/id_ed25519
```

The command builds the workbench image, builds the pipeline's
`Dockerfile.airflow`, configures Airflow's native `GitDagBundle`, starts local
PostgreSQL and object storage, and waits for the services to become healthy.
Airflow then clones and refreshes the DAG bundle itself. Each task run records
the Git version of the DAG code that produced it.

Open <http://127.0.0.1:18080>. The generated local login is stored inside the
`airflow-home` volume:

```bash
docker compose exec airflow \
  cat /var/lib/airflow/simple_auth_manager_passwords.json.generated
```

To expose only the authenticated Airflow UI on a trusted local network, set
`AIRFLOW_UI_HOST=0.0.0.0` when starting the launcher. Database and object-store
ports keep their localhost-only defaults.

Useful options:

```text
--ref VERSION            branch, tag, or commit; default: main
--subdir PATH            DAG directory; default: dags
--image IMAGE            use a prebuilt pipeline image
--bundle-manifest FILE   load several Git DAG sources; requires --image
--env FILE               pipeline-owned runtime environment
--external-db            do not start local PostgreSQL
--external-objects       do not start local object storage
--git-connection ID      use an existing Airflow Git connection
--observability          persist Airflow metrics and traces in local ClickStack
--llm-observability      start Langfuse v4; implies --observability
```

With `--ssh-key`, the launcher writes a generated Airflow connection to the
ignored `.workbench/runtime.env` with mode `0600`. The private key is used by
Docker BuildKit and the local Airflow container; it is not copied into the
image. Host-key checking uses `~/.ssh/known_hosts` by default.

## Several product sources in one Airflow

One Airflow can load DAG entrypoints from several independent Git repositories.
Use a versioned JSON manifest when a shared factory serves several trusted
products:

```json
{
  "version": 1,
  "sources": [
    {
      "name": "learning-platform",
      "repository": "git@github.com:example/learning-platform.git",
      "ref": "main",
      "subdir": "airflow/dags"
    },
    {
      "name": "beavers-data",
      "repository": "git@github.com:example/beavers-data-pipelines.git",
      "ref": "main",
      "subdir": "dags"
    }
  ]
}
```

Then start the factory with an image which contains the compatible Python
packages of **every** listed product:

```bash
./bin/etl-workbench \
  --bundle-manifest trusted-products.json \
  --image trusted-airflow-pipelines:2026-07-22 \
  --ssh-key ~/.ssh/id_ed25519
```

The factory creates one Git Connection per source and configures Airflow's
native `GitDagBundle` list. A Git bundle provides DAG files only; it must never
install arbitrary dependencies at parse time. The shared image is therefore an
explicit release artifact, built and tested from pinned product revisions.

Keep source-specific Connections, object prefixes and Pools named by product.
That separates operational ownership inside one trusted Airflow, but does not
turn this local workbench into an isolation boundary for untrusted code.

## Pipeline repository contract

The smallest repository contains one or more DAG files:

```text
acme-pipeline/
├── dags/
│   └── pipeline.py
└── Dockerfile.airflow
```

A pipeline image can add Python packages or application code:

```dockerfile
ARG ETL_WORKBENCH_IMAGE=etl-workbench:local
FROM ${ETL_WORKBENCH_IMAGE}

COPY --chown=airflow:root pyproject.toml src/ /tmp/pipeline/
RUN pip install --no-cache-dir /tmp/pipeline
```

The launcher overrides `ETL_WORKBENCH_IMAGE` with the locally built workbench
image. Runtime secrets belong in an ignored pipeline environment file and are
passed with `--env`; never bake them into the image or DAG files.

Airflow discovers compatible DAGs from the Git bundle and displays them in its
UI. The pipeline repository owns schemas and migrations, retry and idempotency
behavior, object keys and retention, and all business logic.

Local profile connection IDs are `local_postgres` and `local_s3`; the local
bucket is `etl-local`. SeaweedFS supplies the local S3-compatible endpoint.
External connections may be created in the Airflow UI or provided as
`AIRFLOW_CONN_*` variables in the pipeline environment file.

## LLM connections

The workbench image includes the Airflow OpenAI provider. Create each provider
as an independent `openai` Connection in the Airflow UI; its **Password** is
the provider-specific API key. Use the **Host** field for the OpenAI client's
base URL (or set `openai_client_kwargs.base_url` in Extra).

| Connection ID | Host |
| --- | --- |
| `llm_kimi` | `https://api.moonshot.ai/v1` |
| `llm_deepseek` | `https://api.deepseek.com` |
| `llm_gemini` | `https://generativelanguage.googleapis.com/v1beta/openai/` |
| `llm_qwen` | Model Studio endpoint for the selected region and workspace |
| `llm_mistral` | `https://api.mistral.ai/v1` |
| `llm_xai` | `https://api.x.ai/v1` |

Pipeline code selects the `conn_id` and model name. It must not contain API
keys. For portability across these providers, use the Chat Completions API and
avoid OpenAI-specific APIs unless that pipeline is intentionally tied to
OpenAI.

Connection testing is enabled for this trusted, single-user workbench. It
makes a live request with the stored credential; for the OpenAI provider, this
is a model-list request. Gemini's OpenAI-compatible endpoint does not expose
that model-list route, so validate a Gemini connection with a Chat Completions
task instead.

## Local observability with ClickStack

Add `--observability` to start ClickStack and persist Airflow metrics and traces
over OTLP:

```bash
./bin/etl-workbench https://github.com/example/acme-pipeline.git \
  --observability
```

Open the ClickStack UI at <http://127.0.0.1:18081>. The embedded ClickHouse and
OTLP ports remain internal to the Compose network.

This profile uses the official ClickStack All-in-One image and persistent
volumes for ClickHouse data, MongoDB application state and ClickHouse logs.
It is intended for this local, single-server Workbench; the All-in-One
distribution is not a production deployment.

The committed `etl-workbench-local` ingestion key activates the internal OTLP
receiver before the first ClickStack user exists and remains accepted after
team authentication is enabled. It is a local development credential, not an
API key for any model provider. Override it before sharing a Docker network:

```bash
CLICKSTACK_INGESTION_KEY="$(openssl rand -hex 32)" \
  ./bin/etl-workbench https://github.com/example/acme-pipeline.git \
  --observability
```

ClickStack's ClickHouse stores technical telemetry only. It does not replace
Airflow metadata storage and pipelines should not use its `otel_*` tables for
business or ETL datasets.

Telemetry is for technical operation only. Do not put credentials, headers,
SQL parameters, object contents, documents, prompts, model responses or
personal data into span attributes or metric labels. Keep unique run IDs on
traces and logs rather than metric labels.

For local-path development, enable the same profile explicitly:

```bash
AIRFLOW_OTEL_ENABLED=true docker compose \
  -f compose.yaml -f compose.local.yaml \
  --profile local-db --profile local-objects --profile observability up
```

Verify persisted telemetry after running a DAG:

```bash
scripts/check-otel-contract.sh
```

## Local LLM observability with Langfuse

Add `--llm-observability` to start Langfuse v4 together with ClickStack:

```bash
./bin/etl-workbench https://github.com/example/acme-pipeline.git \
  --llm-observability
```

Open the Langfuse UI at <http://127.0.0.1:18082>. The launcher generates the
initial local user, project keys and service secrets once in the ignored
`.workbench/langfuse.env` file with mode `0600`:

```bash
grep '^LANGFUSE_INIT_' .workbench/langfuse.env
```

Langfuse reuses Workbench PostgreSQL and SeaweedFS through its own `langfuse`
database and bucket. It runs dedicated ClickHouse and Redis services because
they are part of Langfuse's storage and queue contract. ClickStack's embedded
ClickHouse remains isolated and stores infrastructure telemetry only.

Only the Langfuse UI is published, on loopback. ClickHouse, Redis and the worker
health port remain inside the Compose network. The generated project keys are
for Langfuse ingestion; they are not model-provider API keys.

This profile provides the LLM observability backend. A pipeline still needs
OpenTelemetry-compatible LLM instrumentation and must send only approved prompt
and response content. Do not export credentials, authorization headers,
personal data or unrestricted source documents.

Verify the complete OTLP ingestion path with a synthetic, non-sensitive
generation span:

```bash
scripts/check-langfuse-contract.sh
```

SeaweedFS keeps a free-space reserve before allocating new volumes. If an S3
upload returns `InternalError`, check `docker logs etl-workbench-object-store-1`
for `No writable volumes and no free volumes left`, then reclaim Docker image or
build cache space. Do not delete named volumes as a cleanup shortcut.

## Local path development

The included example can be mounted read-only without Git:

```bash
docker build -t etl-workbench:local .
docker compose -f compose.yaml -f compose.local.yaml \
  --profile local-db --profile local-objects up
```

Set `PIPELINE_ROOT` to use another local repository. This fallback expects both
`dags/` and `src/`; GitDagBundle is the normal repository integration.

## Verify

```bash
docker compose config --quiet
docker compose -f compose.yaml -f compose.local.yaml config --quiet
docker build -t etl-workbench:local .
docker compose -f compose.yaml -f compose.local.yaml run --rm airflow python -c \
  'from airflow.models import DagBag; b=DagBag("/opt/airflow/dags"); assert not b.import_errors, b.import_errors'
docker compose -f compose.yaml -f compose.local.yaml \
  --profile local-objects run --rm \
  -v "$PWD/scripts:/opt/workbench/scripts:ro" airflow \
  python /opt/workbench/scripts/check-s3-contract.py
```

The S3 contract check writes only below a unique `_workbench_contract/` prefix
and removes its objects before returning. It verifies put, metadata, get, list,
presigned GET, copy, multipart upload and delete through the same Airflow
`local_s3` Connection that pipeline tasks use.

## Upgrade from MinIO

SeaweedFS uses a new `seaweedfs-data` volume; it cannot read the MinIO volume
format directly. The old `minio-data` volume is never removed by the upgrade.
If it contains objects that must be retained, stop the old stack without
deleting volumes and run the one-time copy:

```bash
docker compose down --remove-orphans
docker volume inspect etl-workbench_minio-data
docker compose -f compose.yaml -f compose.minio-migration.yaml \
  --profile local-objects --profile migrate-minio \
  up --abort-on-container-exit migrate-minio
docker compose -f compose.yaml -f compose.minio-migration.yaml \
  --profile local-objects --profile migrate-minio down
```

The migration copies the current contents of `ETL_LOCAL_BUCKET` and verifies
that the complete, sorted object key-and-size inventory has the same SHA-256
digest in both stores. This avoids order-dependent `mc diff` output; the copy
still fails if an object is missing, added only to the target, renamed or has a
different size. It does not delete the source volume. After it succeeds, start
the workbench normally. To roll back, stop the new stack without `--volumes`
and run the previous workbench release against the preserved `minio-data`
volume.

## Stop

Keep local history and data:

```bash
docker compose --profile local-db --profile local-objects down
```

Explicitly delete workbench volumes and generated Git credentials:

```bash
docker compose --profile local-db --profile local-objects down --volumes
rm -rf .workbench
```

Scheduled runs stop when the laptop or Compose stack stops. Shared scheduling,
remote Airflow metadata, distributed executors, monitoring, and untrusted DAG
execution are outside this workbench's scope.

## License

Apache-2.0. See `LICENSE`.
