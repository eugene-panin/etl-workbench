#!/usr/bin/env sh
set -eu

root="$(cd -- "$(dirname -- "$0")/.." && pwd)"
n8n_env="${N8N_ENV_FILE:-$root/.workbench/n8n.env}"

N8N_ENV_FILE="$n8n_env" docker compose \
    -f "$root/compose.yaml" \
    -f "$root/compose.n8n.yaml" \
    --profile local-db --profile automation \
    exec -T n8n node - <<'JS'
const endpoints = ["/healthz", "/healthz/readiness"];

Promise.all(
  endpoints.map(async (path) => {
    const response = await fetch(`http://127.0.0.1:5678${path}`);
    if (!response.ok) {
      throw new Error(`${path} returned HTTP ${response.status}`);
    }
  }),
)
  .then(() => {
    console.log("n8n contract passed: service is reachable and PostgreSQL is ready");
  })
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
JS
