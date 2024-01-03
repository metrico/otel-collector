# Pyroscope REceiver

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [beta]                |
| Supported pipeline types | logs                  |

The pyroscope receiver implements the pyroscope ingest protocol and conveys the accepted profiles as OpenTelemtry logs IR for further processing and export.

## Configuration

- `protocols`: sets the application layer protocols that the receiver will serve. See [Supported Protocols](#supported-protocols). Default is http/s on 0.0.0.0:8062 with max request body size of: 5e6 + 1e6.
- `timeout`: sets the server reponse timeout. Default is 10 seconds.
- `request_body_size_expected_value`: sets the expected decompressed request body size in bytes to size pipeline buffers and optimize allocations. Default is 0.

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

## Supported Protocols

Http

## Supported Encodings

JFR
