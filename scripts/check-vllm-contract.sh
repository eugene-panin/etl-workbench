#!/usr/bin/env sh
set -eu

docker compose --profile inference-gpu exec -T airflow python - <<'PY'
from __future__ import annotations

import os

from airflow.providers.openai.hooks.openai import OpenAIHook


model = os.environ.get("VLLM_SERVED_MODEL_NAME", "local-model")
client = OpenAIHook(conn_id="llm_local_vllm").get_conn()
models = {item.id for item in client.models.list().data}
if model not in models:
    raise RuntimeError(f"vLLM model is not listed: expected={model}, actual={models}")

response = client.chat.completions.create(
    model=model,
    messages=[
        {
            "role": "user",
            "content": "Reply with the single word OK.",
        }
    ],
    max_tokens=32,
    temperature=0,
)
if not response.choices or response.choices[0].finish_reason is None:
    raise RuntimeError("vLLM returned no completed choice")

print(
    "vLLM contract passed: "
    f"model={model} finish_reason={response.choices[0].finish_reason}"
)
PY
