#!/usr/bin/env sh
set -eu

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
langfuse_env="${LANGFUSE_ENV_FILE:-$root/.workbench/langfuse.env}"

LANGFUSE_ENV_FILE="$langfuse_env" docker compose \
    -f "$root/compose.yaml" \
    -f "$root/compose.langfuse.yaml" \
    --profile local-db \
    --profile local-objects \
    --profile llm-observability \
    run --rm --no-deps langfuse-init \
    python /opt/workbench/scripts/check-langfuse-contract.py
