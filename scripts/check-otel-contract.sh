#!/usr/bin/env sh
set -eu

attempt=1
while [ "$attempt" -le 20 ]; do
    logs="$(docker compose --profile observability logs --no-color otel-collector 2>&1)"
    traces_seen=false
    metrics_seen=false

    if printf '%s\n' "$logs" | grep -Eq 'Traces.*[Ss]pans[^0-9]*[1-9]'; then
        traces_seen=true
    fi
    if printf '%s\n' "$logs" | grep -Eq 'Metrics.*[Mm]etrics[^0-9]*[1-9]'; then
        metrics_seen=true
    fi
    if [ "$traces_seen" = true ] && [ "$metrics_seen" = true ]; then
        printf 'OpenTelemetry contract passed: Airflow traces and metrics reached the Collector\n'
        exit 0
    fi

    attempt=$((attempt + 1))
    sleep 2
done

printf 'OpenTelemetry contract failed: Collector did not receive both Airflow traces and metrics\n' >&2
printf '%s\n' "$logs" >&2
exit 1
