# Schema model, and how this differs from `clickhouseexporter`

The OpenTelemetry Collector already ships a generic
[`clickhouseexporter`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter).
This exporter is not a competitor to it — it targets a different schema for a
different query surface. This page explains that schema and when each exporter
is the right choice.

## Two different goals

| | `clickhouseexporter` (contrib) | `gigapipeexporter` (this component) |
| --- | --- | --- |
| Table model | Generic OTel tables (`otel_logs`, `otel_traces`, `otel_metrics_*`) | Gigapipe/qryn polyglot schema (`samples_v3`, `time_series`, `tempo_traces`, `tempo_traces_attrs_gin`) |
| Intended reader | ClickHouse SQL / Grafana ClickHouse plugin | The Loki, Prometheus, Tempo and Pyroscope APIs served by [Gigapipe](https://github.com/metrico/gigapipe) |
| Series identity | Row columns | A Loki-style **fingerprint** over the label set |
| Metric naming | OTel metric names | Prometheus-compliant names + labels (`__name__`) |

If you query ClickHouse directly, `clickhouseexporter` is the natural fit. If you
run Gigapipe and want the same data to answer LogQL, PromQL, Tempo and Pyroscope
queries, you need this schema — it is not reproducible with the generic tables.

## The fingerprint model (logs and metrics)

Logs and metrics are both written as two joined tables:

- **`samples_v3`** — one row per data point: `fingerprint`, `timestamp_ns`,
  `value`, `string`, `type` (`1` = log, `2` = metric).
- **`time_series`** — one row per unique series: `fingerprint`, `labels` (JSON),
  `name`, `type`.

The **fingerprint** is a hash of the label set. It is the join key between a
sample and its series, exactly as in Loki/Prometheus on ClickHouse. Identical
label sets share a fingerprint; any difference produces a new one. This is what
makes the data addressable as streams/series by LogQL and PromQL.

- **Metrics** are normalised to the Prometheus data model: metric names are made
  Prometheus-compliant (e.g. `http.server.duration` → `http_server_duration`),
  the name is carried in the `__name__` label, and `service.name`/
  `service.namespace`/`service.instance.id` map to `job`/`instance`.
- **Logs** promote attributes to labels following Loki semantics — see
  [Logs: attributes vs. labels](logs-labels.md).

## The Tempo model (traces)

Traces have two write paths:

- **Server-side (default single node):** `traces_input` (a `Null`-engine ingest
  table) receiving span rows with an OTLP payload.
- **Client-side (`client_side_trace_processing: true`, clustered):** the span is
  split across **two** tables in one batch:
  - `tempo_traces` — the trace/span rows with the OTLP payload
    (`json` or `proto`, per `trace_payload_type`).
  - `tempo_traces_attrs_gin` — one row per tag, forming the GIN tag index Tempo
    search relies on.

The generic exporter writes a single flat traces table with no equivalent tag
index, so Tempo-style tag search is not available from it.

## Executable contract

These schema guarantees are pinned by characterization tests in
[`schema_contract_test.go`](../schema_contract_test.go): the fingerprint join,
the Prometheus read model, the Tempo two-table split, and Loki label promotion.
They exist so the distinction from `clickhouseexporter` is verifiable, not just
described.
