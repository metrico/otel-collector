# Logs: attributes vs. labels

How the qryn exporter turns OTLP log attributes into qryn/Loki stream labels, and how to control it.

## Attributes vs. labels

An OTLP log carries two attribute bags:

- **Resource attributes** describe the *source* (e.g. `service.name`, `k8s.namespace.name`, `signoz.component`). One set per batch, stable, low-cardinality.
- **Log-record attributes** describe the *individual event* (e.g. `k8s.event.reason`, `http.status_code`). Per-record, can vary on every line.

A **label** is different from an attribute: labels form the **stream identity** in qryn/Loki. Every unique combination of label values is a separate stream with its own index entry. Attributes stay in the log payload; only *promoted* attributes become labels. Promoting a high-cardinality attribute (e.g. an event message or an id) to a label creates many streams and slows queries — the classic Loki cardinality anti-pattern.

The `level` label is always derived from the log's severity and promoted automatically.

## How to control which attributes become labels

Two ways, checked independently for resource attrs and for log-record attrs.

1. **Exporter config** (applies to every log in the pipeline):

   ```yaml
   exporters:
     qryn:
       dsn: tcp://localhost:9000/?database=cloki
       clustered_clickhouse: false
       logs:
         resource_labels: "service.name,k8s.namespace.name"   # only these resource attrs → labels
         attribute_labels: "k8s.event.reason"                  # only these log attrs → labels
   ```

2. **Per-payload hints** (set by the sender as attributes on the log/resource):
   - `loki.resource.labels` — comma list of resource attributes to promote.
   - `loki.attribute.labels` — comma list of log-record attributes to promote.
   - `loki.tenant` — names the attribute whose value routes the log to a tenant (this is **not** a label).
   - `loki.format` — output body format.

## Default behaviour

For each bag, **if you name nothing** (no config key and no hint for that bag), the exporter promotes **all** attributes in that bag to labels. As soon as you set the config key **or** the matching hint for a bag, only the names you listed are promoted — that is your opt-out of the all-attributes default.

| Resource attrs → labels | Log attrs → labels | How |
| --- | --- | --- |
| all (default) | all (default) | set nothing |
| only listed | all (default) | set `resource_labels` or `loki.resource.labels` |
| all (default) | only listed | set `attribute_labels` or `loki.attribute.labels` |
| only listed | only listed | set both |

> **Cardinality tip:** resource attributes are safe to promote broadly. Log-record attributes are per-event and can be high-cardinality — prefer naming the specific ones you need via `attribute_labels` (or `loki.attribute.labels`) rather than relying on the promote-all default in high-volume pipelines.
