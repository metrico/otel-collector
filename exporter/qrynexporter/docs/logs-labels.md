# Logs: attributes vs. labels

How the Gigapipe exporter turns OTLP log attributes into Gigapipe/Loki stream labels, and how to control it.

## Attributes vs. labels

An OTLP log carries two attribute bags:

- **Resource attributes** describe the *source* (e.g. `service.name`, `k8s.namespace.name`, `signoz.component`). One set per batch, stable, low-cardinality.
- **Log-record attributes** describe the *individual event* (e.g. `k8s.event.reason`, `http.status_code`). Per-record, can vary on every line.

A **label** is different from an attribute: labels form the **stream identity** in Gigapipe/Loki. Every unique combination of label values is a separate stream with its own index entry. Attributes stay in the log payload; only *promoted* attributes become labels. Promoting a high-cardinality attribute (e.g. an event message or an id) to a label creates many streams and slows queries — the classic Loki cardinality anti-pattern.

The `level` label is always derived from the log's severity and promoted automatically.

## Default behaviour

The two bags are treated differently, on purpose, because they carry different cardinality risk:

- **Resource attributes** are low-cardinality (one set per source), so **all** of them are promoted to labels by default.
- **Log-record attributes** are per-event and can be high-cardinality, so **none** of them are promoted by default — only the `level` label plus any attributes you explicitly name. Promoting all log attributes would let a single noisy attribute (an event message, a request id) create thousands of streams.

`level` is always promoted, regardless of any setting.

| | Promoted to labels by default |
| --- | --- |
| Resource attributes | **all** |
| Log-record attributes | **none** (except `level`) |

## How to control which attributes become labels

### Promote specific attributes

Checked independently for each bag; works via config (whole pipeline) or per-payload hints (set by the sender).

1. **Exporter config:**

   ```yaml
   exporters:
     qryn:
       dsn: tcp://localhost:9000/?database=cloki
       clustered_clickhouse: false
       logs:
         resource_labels: "service.name,k8s.namespace.name"   # promote only these resource attrs
         attribute_labels: "k8s.event.reason"                  # promote only these log attrs
   ```

2. **Per-payload hints** (attributes set by the sender on the log/resource):
   - `loki.resource.labels` — comma list of resource attributes to promote.
   - `loki.attribute.labels` — comma list of log-record attributes to promote.
   - `loki.tenant` — names the attribute whose value routes the log to a tenant (this is **not** a label).
   - `loki.format` — output body format.

For **resource** attributes, naming specific ones (via `resource_labels` or `loki.resource.labels`) also opts out of the promote-all default for that bag.

### Promote *all* log attributes

If you really want every log-record attribute as a label — and accept the cardinality cost — set:

```yaml
   logs:
     promote_all_attributes: true
```

This is a blunt switch: when `true`, every log attribute becomes a label. It does **not** disable the specific selectors above (they always apply); it only turns the blanket export on. Default is `false`.

## Control matrix

| Resource attrs → labels | Log attrs → labels | How |
| --- | --- | --- |
| all (default) | only `level` (default) | set nothing |
| all (default) | `level` + listed | set `attribute_labels` or `loki.attribute.labels` |
| all (default) | all | set `promote_all_attributes: true` |
| only listed | only `level` (default) | set `resource_labels` or `loki.resource.labels` |
| only listed | `level` + listed | set `resource_labels` **and** `attribute_labels` |

> **Cardinality tip:** resource attributes are safe to promote broadly. Log-record attributes are per-event — prefer naming the specific ones you need via `attribute_labels` (or `loki.attribute.labels`). Reach for `promote_all_attributes: true` only when you understand the label-cardinality impact on your Gigapipe/ClickHouse backend.
