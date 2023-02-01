<img src="https://user-images.githubusercontent.com/1423657/173144443-fc7ba783-d5bf-47f9-bf59-707693da5ed1.png" width=200 />

# qryn-otel-collector

Open Telemetry distribution for [qryn](https://qryn.dev)


### About
The **qryn-otel-collector** is designed to store OpenTelemetry data _(Traces, Logs, Metrics)_ in [ClickHouse](https://github.com/clickhouse/clicklhouse) using [qryn](https://github.com/metrico/qryn) _fingerprinting and table formats_ transparently accessed and comsumed through any of the [qryn API integrations](https://qryn.dev) such as _LogQL, PromQL and Tempo_ in Grafana.


### Usage
```
otel-collector:
    container_name: otel-collector
    image: ghcr.io/metrico/qryn-otel-collector:latest
    volumes:
      - ./otel-collector-config.yaml:/etc/otel/config.yaml
    ports:
      - "4317:4317"     # OTLP gRPC receiver
      - "4318:4318"     # OTLP HTTP receiver
      - "14250:14250"   # Jaeger gRPC
      - "14268:14268"   # Jaeger thrift HTTP
      - "9411:9411"     # Zipkin port
      - "24224:24224".  # Fluent Forward
    restart: on-failure
```

### Config
```
receivers:
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
  fluentforward:
    endpoint: 0.0.0.0:24224
  prometheus:
    config:
      scrape_configs:
        - job_name: 'otel-collector'
          scrape_interval: 5s
          static_configs:
            - targets: ['exporter:8080']
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
      receivers: [fluentforward, otlp]
      processors: [memory_limiter, resourcedetection/system, resource, batch]
      exporters: [qryn]
    traces:
      receivers: [otlp, jaeger, zipkin]
      processors: [memory_limiter, resourcedetection/system, resource, spanmetrics, servicegraph, batch]
      exporters: [qryn]
    # for align with https://grafana.com/docs/tempo/latest/metrics-generator/span_metrics/#how-to-run
    metrics/spanmetrics:
      receivers: [otlp]
      processors: [metricstransform]
      exporters: [qryn]
    metrics:
      receivers: [prometheus]
      processors: [memory_limiter, resourcedetection/system, resource, batch]
      exporters: [qryn]
```
