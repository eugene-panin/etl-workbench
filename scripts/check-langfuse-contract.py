from __future__ import annotations

import base64
import json
import os
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor


LANGFUSE_HOST = "http://langfuse-web:3000"


def required_environment(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def authorization() -> str:
    public_key = required_environment("LANGFUSE_INIT_PROJECT_PUBLIC_KEY")
    secret_key = required_environment("LANGFUSE_INIT_PROJECT_SECRET_KEY")
    token = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    return f"Basic {token}"


def export_contract_span(auth_header: str) -> str:
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": "etl-workbench-langfuse-contract",
                "deployment.environment.name": "local",
            }
        )
    )
    exporter = OTLPSpanExporter(
        endpoint=f"{LANGFUSE_HOST}/api/public/otel/v1/traces",
        headers={
            "Authorization": auth_header,
            "x-langfuse-ingestion-version": "4",
        },
    )
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)
    with tracer.start_as_current_span("workbench.langfuse.contract") as span:
        span.set_attribute("langfuse.observation.type", "generation")
        span.set_attribute("langfuse.observation.input", '{"prompt":"contract-check"}')
        span.set_attribute("langfuse.observation.output", '{"result":"ok"}')
        span.set_attribute("gen_ai.operation.name", "chat")
        span.set_attribute("gen_ai.request.model", "contract-model")
        trace_id = f"{span.get_span_context().trace_id:032x}"
    provider.shutdown()
    return trace_id


def observation_count(trace_id: str, auth_header: str) -> int:
    query = urlencode({"traceId": trace_id, "limit": 10, "fields": "core"})
    request = Request(
        f"{LANGFUSE_HOST}/api/public/v2/observations?{query}",
        headers={"Authorization": auth_header},
    )
    with urlopen(request, timeout=10) as response:
        payload = json.load(response)
    return len(payload.get("data", []))


if __name__ == "__main__":
    auth = authorization()
    contract_trace_id = export_contract_span(auth)
    for _ in range(30):
        count = observation_count(contract_trace_id, auth)
        if count:
            print(
                "Langfuse contract passed: "
                f"trace {contract_trace_id} has {count} observation(s)"
            )
            break
        time.sleep(1)
    else:
        raise RuntimeError(
            f"Langfuse did not expose trace {contract_trace_id} within 30 seconds"
        )
