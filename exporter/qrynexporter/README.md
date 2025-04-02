# qryn Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [alpha]               |
| Supported pipeline types | traces, logs, metrics |
| Distributions            | [qryn]                |


# Configuration options:
- `dsn` (required): Data Source Name for Clickhouse.
  - Example: `tcp://localhost:9000/qryn`

- `clustered_clickhouse` (required): 
  - Type: boolean
  - Description: Set to `true` if using a Clickhouse cluster; otherwise, set to `false`.

- `client_side_trace_processing` (required):
  - Type: boolean
  - Default: `true`
  - Description: Enables client-side processing of trace data. This can improve performance but may increase client-side resource usage.

- `trace_payload_type` (optional):
  - Type: string
  - Default: `json`
  - Supported values: `json`, `proto`
  - Description: Specifies the format of trace data sent to ClickHouse. Please use `json` for compatibility with qryn up to 3.2.39. 
For "Gigapipe" readers please use `proto`.


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
