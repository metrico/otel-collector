# qryn Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [alpha]               |
| Supported pipeline types | traces, logs, metrics |
| Distributions            | [qryn]                |


# Configuration options:
- `dsn` (required): Data Source Name for Clickhouse.
  - Example: `tcp://localhost:9000/?database=cloki`

- `clustered_clickhouse` (required): 
  - Type: boolean
  - Description: Set to `true` if using a Clickhouse cluster; otherwise, set to `false`.

- `client_side_trace_processing` (required):
  - Type: boolean
  - Default: `true`
  - Description: Enables client-side processing of trace data. This can improve performance but may increase client-side resource usage.

<<<<<<< HEAD
- `dsn` (required): Clickhouse's dsn.
- `clustered_clickhouse` (required): true if clickhouse cluster is used
- `client_side_trace_processing`: use improved traces ingestion algorythm for clickhouse clusters. 
Data ingestion is sess performant but more evenly distributed 
=======
>>>>>>> e35202d (refactor: improve the code structure and documentation of qrynexporter.)

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
