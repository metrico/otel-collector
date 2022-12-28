# Clickhouse Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [beta]                |
| Supported pipeline types | traces                |
| Distributions            | [lb]                  |


# Configuration options:

- `datasource` (required): Clickhouse's dsn.

# Example:
## Simple Trace Data

```yaml
receivers:
  examplereceiver:

exporters:
  clickhousetraces:
    datasource: tcp://localhost:9000/?database=cloki

service:
  pipelines:
    traces:
      receivers: [examplereceiver]
      exporters: [clickhousetraces]
```

[beta]:https://github.com/open-telemetry/opentelemetry-collector#beta
[contrib]:https://github.com/open-telemetry/opentelemetry-collector-releases/tree/main/distributions/otelcol-contrib
