# Pyroscope Receiver

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [beta]                |
| Supported pipeline types | logs                  |

Implements the Pyroscope ingest protocol and conveys the accepted profiles as OpenTelemetry logs backed IR for further processing and export.

## Configuration

- `protocols`: sets the application layer protocols that the receiver will serve. See [Supported Protocols](#supported-protocols). Default is http/s on 0.0.0.0:8062 with max request body size of: 5e6 + 1e6.
- `timeout`: sets the server reponse timeout. Default is 10 seconds.
- `metrics`: configures the metrics collection for the Pyroscope receiver.
    - `enable`: enables or disables metrics collection. Default is true.
    - `exclude_labels`: a list of metrics and label names to exclude from the metrics. 
       Available metrics are listed further. Metric name may be empty. Available labels are:
      - `service`: name of the service provided the pprof request
      - `type`: type of pprof request (jfr or pprof)
      - `encoding`: not used, empty
      - `error_code`: http error code response for http request count
      - `status_code`: http response status code for http request count 
    - `exclude_metrics`: a list of metric names to exclude from collection. Available metrics are:
      - `http_request_total`: Pyroscope receiver http request count.
      - `request_body_uncompressed_size_bytes`: Pyroscope receiver uncompressed request body size in bytes.
      - `parsed_body_uncompressed_size_bytes`: Pyroscope receiver uncompressed parsed body size in bytes.

## Example

```yaml
receivers:
  pyroscopereceiver:
    metrics:
      enable: true
      exclude_labels:
      - metric: request_body_uncompressed_size_bytes
        label: service
      exclude_metrics:
      - http_requests_total
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
