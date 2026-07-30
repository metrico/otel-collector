# Gigapipe Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [alpha]               |
| Supported pipeline types | traces, logs, metrics |
| Distributions            | [Gigapipe]            |


# Schema model

This exporter writes the Gigapipe/qryn **polyglot fingerprint schema**
(`samples_v3` + `time_series` keyed by a Loki-style fingerprint; `tempo_traces` +
`tempo_traces_attrs_gin`; Prometheus-compliant metric names and labels), so a
single ClickHouse instance is queryable through the Loki, Prometheus, Tempo and
Pyroscope APIs served by [Gigapipe](https://github.com/metrico/gigapipe).

This is a different goal from the generic contrib
[`clickhouseexporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter),
which writes generic OTel tables intended to be queried as ClickHouse SQL. Choose
`clickhouseexporter` to query ClickHouse directly; choose this exporter to serve
the LogQL/PromQL/Tempo/Pyroscope query surface. See
[Schema model, and how this differs from `clickhouseexporter`](docs/schema.md).


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

- `logs` (optional): controls how log attributes become stream labels. See [Logs: attributes vs. labels](docs/logs-labels.md).
  - `logs.attribute_labels` (optional, string): comma-separated **log-record** attribute names to promote to labels.
  - `logs.resource_labels` (optional, string): comma-separated **resource** attribute names to promote to labels.
  - `logs.promote_all_attributes` (optional, bool, default `false`): when `true`, promote **every** log-record attribute to a label. Leave `false` to avoid unbounded label cardinality; only `level` and the attributes you name are promoted.
  - `logs.format` (optional, string): body format (`raw`, `json`, `logfmt`).


# Example:
## Simple Trace Data

```yaml
receivers:
  examplereceiver:

exporters:
  gigapipe:
    dsn: tcp://localhost:9000/?database=cloki
    clustered_clickhouse: false

service:
  pipelines:
    traces:
      receivers: [examplereceiver]
      exporters: [gigapipe]
```

> **Note:** the `qryn` exporter type is a deprecated alias for `gigapipe`.
> Existing `qryn:` configurations keep working but log a deprecation warning;
> use `gigapipe` in new configurations.

[beta]:https://github.com/open-telemetry/opentelemetry-collector#beta
[contrib]:https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
