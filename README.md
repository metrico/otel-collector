<a href="https://qryn.dev" target="_blank"><img src='https://user-images.githubusercontent.com/1423657/218816262-e0e8d7ad-44d0-4a7d-9497-0d383ed78b83.png' width=250></a>

# qryn-otel-collector

Open Telemetry distribution for [qryn](https://qryn.dev)


### About
The **qryn-otel-collector** is designed to store OpenTelemetry data _(Traces, Logs, Metrics)_ in [ClickHouse](https://github.com/clickhouse/clicklhouse) using [qryn](https://github.com/metrico/qryn) _fingerprinting and table formats_ transparently accessed and comsumed through any of the [qryn API integrations](https://qryn.dev) such as _LogQL, PromQL and Tempo_ in Grafana.

#### Popular ingestion formats _(out of many more)_:

- Logs
  - [Loki](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/lokireceiver)
  - [Splunk](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/splunkhecreceiver)
  - [Fluentd](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/fluentforwardreceiver)
  - [Cloudwatch](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/awscloudwatchreceiver)
  - [Syslog](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/syslogreceiver)
- Metrics
  - [Prometheus](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver)
  - [InfluxDB](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver)
  - OTLP
- Traces
  - [Zipkin](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/zipkinreceiver)
  - [Jaeger](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/jaegerreceiver)
  - [Skywalking](github.com/open-telemetry/opentelemetry-collector-contrib/receiver/skywalkingreceiver)
  - OTLP



### Usage
```yaml
otel-collector:
    container_name: otel-collector
    image: ghcr.io/metrico/qryn-otel-collector:latest
    volumes:
      - ./otel-collector-config.yaml:/etc/otel/config.yaml
    ports:
      - "4317:4317"     # OTLP gRPC receiver
      - "4318:4318"     # OTLP HTTP receiver
      - "14250:14250"   # Jaeger gRPC receiver
      - "14268:14268"   # Jaeger thrift HTTP receiver
      - "9411:9411"     # Zipkin Trace receiver
      - "11800:11800"   # Skywalking gRPC receiver
      - "12800:12800"   # Skywalking HTTP receiver
      - "24224:24224".  # Fluent Forward receiver
      - "8086:8086"     # InfluxDB Line proto HTTP
      - "3100:3100".    # Loki/Logql HTTP receiver
      - "3200:3200".    # Loki/Logql gRPC receiver
      - "8088:8088"     # Splunk HEC receiver
      - "5514:5514"     # Syslog TCP Rereceiverceiver
    restart: on-failure
```

### Config Template
The following template enables popular log, metric and tracing ingestion formats supported by qryn

```yaml
receivers:
  loki:
    use_incoming_timestamp: true
    protocols:
      http:
        endpoint: 0.0.0.0:3100
      grpc:
        endpoint: 0.0.0.0:3200
  syslog:
    protocol: rfc5424
    tcp:
      listen_address: "0.0.0.0:5514"
    protocol: rfc5424
  fluentforward:
    endpoint: 0.0.0.0:24224
  splunk_hec:
    endpoint: 0.0.0.0:8088
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  jaeger:
    protocols:
      grpc:
        endpoint: 0.0.0.0:14250
      thrift_http:
        endpoint: 0.0.0.0:14268
  zipkin:
    endpoint: 0.0.0.0:9411
  skywalking:
    protocols:
      grpc:
        endpoint: 0.0.0.0:11800
      http:
        endpoint: 0.0.0.0:12800
  prometheus:
    config:
      scrape_configs:
        - job_name: 'otel-collector'
          scrape_interval: 5s
          static_configs:
            - targets: ['exporter:8080']
  influxdb:
    endpoint: 0.0.0.0:8086
    
processors:
  batch:
    send_batch_size: 10000
    timeout: 5s
  memory_limiter:
    check_interval: 2s
    limit_mib: 1800
    spike_limit_mib: 500
  resourcedetection/system:
    detectors: ['system']
    system:
      hostname_sources: ['os']
  resource:
    attributes:
      - key: service.name
        value: "serviceName"
        action: upsert
  spanmetrics:
    metrics_exporter: otlp/spanmetrics
    latency_histogram_buckets: [100us, 1ms, 2ms, 6ms, 10ms, 100ms, 250ms]
    dimensions_cache_size: 1500
  servicegraph:
    metrics_exporter: otlp/spanmetrics
    latency_histogram_buckets: [100us, 1ms, 2ms, 6ms, 10ms, 100ms, 250ms]
    dimensions: [cluster, namespace]
    store:
      ttl: 2s
      max_items: 200
  metricstransform:
    transforms:
      - include: calls_total
        action: update
        new_name: traces_spanmetrics_calls_total
      - include: latency
        action: update
        new_name: traces_spanmetrics_latency
exporters:
  qryn:
    dsn: tcp://clickhouse-server:9000/cloki?username=default&password=*************
    timeout: 10s
    sending_queue:
      queue_size: 100
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 300s
    logs:
       format: json
  otlp/spanmetrics:
    endpoint: localhost:4317
    tls:
      insecure: true
extensions:
  health_check:
  pprof:
  zpages:
  memory_ballast:
    size_mib: 1000

service:
  extensions: [pprof, zpages, health_check]
  pipelines:
    logs:
      receivers: [fluentforward, otlp, loki]
      processors: [memory_limiter, resourcedetection/system, resource, batch]
      exporters: [qryn]
    traces:
      receivers: [otlp, jaeger, zipkin, skywalking]
      processors: [memory_limiter, resourcedetection/system, resource, spanmetrics, servicegraph, batch]
      exporters: [qryn]
    metrics/spanmetrics:
      receivers: [otlp]
      processors: [metricstransform]
      exporters: [qryn]
    metrics:
      receivers: [prometheus, influxdb]
      processors: [memory_limiter, resourcedetection/system, resource, batch]
      exporters: [qryn]
```
