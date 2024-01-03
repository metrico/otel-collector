# Clickhouse Profile Exporter

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [beta]                |
| Supported pipeline types | logs                  |

Exports conveyed OpenTelemetry logs backed IR for profiles into a Clickhouse cluster. See [Pyropscope Receiver](../../receiver/pyroscopereceiver), which can send compatible profiles.

## Configuration

- `dsn` (required): sets the ClickHouse server Data Source Name. For tcp protocol reference: [ClickHouse/clickhouse-go#dsn](https://github.com/ClickHouse/clickhouse-go#dsn). For http protocol reference: [mailru/go-clickhouse/#dsn](https://github.com/mailru/go-clickhouse/#dsn).

## Example

```yaml
receivers:
  pyroscopereceiver:
    protocols:
      http:
        endpoint: 0.0.0.0:8062
    timeout: 30s
      
exporters:
  clickhouseprofileexporter:
      dsn: tcp://0.0.0.0:9000/cloki
      timeout: 10s
      sending_queue:
        queue_size: 100
      retry_on_failure:
        enabled: true
        initial_interval: 5s
        max_interval: 30s
        max_elapsed_time: 300s

service:
  pipelines:
    logs/profiles:
      receivers: [pyroscopereceiver]
      processors: [batch]
      exporters: [clickhouseprofileexporter]
```
