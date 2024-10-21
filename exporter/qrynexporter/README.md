# qryn Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [alpha]               |
| Supported pipeline types | traces, logs, metrics |
| Distributions            | [qryn]                |


# Configuration options:

- `dsn` (required): Clickhouse's dsn.
- `clustered_clickhouse` (required): true if clickhouse cluster is used
- `traces_distributed_export_v2`: use improved ingestion algorythm for traces. Data ingestion is sess performant but more evenly distributed 

# Example:
## Simple Trace Data

```yaml
receivers:
  examplereceiver:

exporters:
  qryn:
    dsn: tcp://localhost:9000/?database=cloki
    clustered_clickhouse: false

service:
  pipelines:
    traces:
      receivers: [examplereceiver]
      exporters: [qryn]
```

[beta]:https://github.com/open-telemetry/opentelemetry-collector#beta
[contrib]:https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
