# Pyroscope Receiver

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [beta]                |
| Supported pipeline types | logs                  |

Implements the Pyroscope ingest protocol and conveys the accepted profiles as OpenTelemetry logs backed IR for further processing and export.

## Configuration

- `protocols`: sets the application layer protocols that the receiver will serve. See [Supported Protocols](#supported-protocols). Default is http/s on 0.0.0.0:8062 with max request body size of: 5e6 + 1e6.
- `timeout`: sets the server reponse timeout. Default is 10 seconds.

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
      dsn: tcp://0.0.0.0:9000/qryn
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

## Supported Protocols

Http

## Supported Languages

Java
