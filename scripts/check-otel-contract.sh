#!/usr/bin/env sh
set -eu

attempt=1
while [ "$attempt" -le 30 ]; do
    traces="$(
        docker compose --profile observability exec -T clickstack clickhouse-client \
            --query "SELECT count() FROM otel_traces WHERE ServiceName = 'etl-workbench-airflow'"
    )"
    metrics="$(
        docker compose --profile observability exec -T clickstack clickhouse-client \
            --query "
                SELECT sum(metric_count)
                FROM
                (
                    SELECT count() AS metric_count FROM otel_metrics_gauge WHERE ServiceName = 'etl-workbench-airflow'
                    UNION ALL
                    SELECT count() AS metric_count FROM otel_metrics_sum WHERE ServiceName = 'etl-workbench-airflow'
                    UNION ALL
                    SELECT count() AS metric_count FROM otel_metrics_histogram WHERE ServiceName = 'etl-workbench-airflow'
                    UNION ALL
                    SELECT count() AS metric_count FROM otel_metrics_summary WHERE ServiceName = 'etl-workbench-airflow'
                    UNION ALL
                    SELECT count() AS metric_count FROM otel_metrics_exponential_histogram WHERE ServiceName = 'etl-workbench-airflow'
                )
            "
    )"

    if [ "$traces" -gt 0 ] && [ "$metrics" -gt 0 ]; then
        printf 'OpenTelemetry contract passed: ClickStack stored %s Airflow spans and %s metric points\n' \
            "$traces" "$metrics"
        exit 0
    fi

    attempt=$((attempt + 1))
    sleep 2
done

printf 'OpenTelemetry contract failed: ClickStack did not store both Airflow traces and metrics\n' >&2
printf 'Airflow spans: %s; metric points: %s\n' "$traces" "$metrics" >&2
exit 1
