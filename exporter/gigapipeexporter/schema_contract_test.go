package gigapipeexporter

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// The tests in this file are characterization tests for the Gigapipe (formerly
// qryn) polyglot schema. They exist to document, in executable form, what makes
// this exporter distinct from the generic contrib `clickhouseexporter`:
//
//   - clickhouseexporter writes a generic OTel table model (otel_logs,
//     otel_traces, otel_metrics_*), designed to be queried as ClickHouse tables.
//   - this exporter writes the Gigapipe fingerprint schema (samples_v3 +
//     time_series keyed by a Loki-style label fingerprint; tempo_traces +
//     tempo_traces_attrs_gin; Prometheus-compliant metric names/labels) so the
//     same ClickHouse instance is queryable through the Loki, Prometheus, Tempo
//     and Pyroscope APIs.
//
// They intentionally assert the schema contract (table names, the fingerprint
// join, the Prometheus read model, the Tempo two-table split), not just that the
// code runs — that contract is the justification for the component existing
// alongside clickhouseexporter.

// TestSchemaContract_FingerprintLinksSamplesToTimeSeries pins the Loki fingerprint
// model: every Sample shares a fingerprint with its TimeSerie (that is the join
// key between samples_v3 and time_series), and distinct label sets get distinct
// fingerprints. clickhouseexporter has no fingerprint concept.
func TestSchemaContract_FingerprintLinksSamplesToTimeSeries(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("requests")

	dpA := pmetric.NewNumberDataPoint()
	dpA.SetDoubleValue(1)
	dpA.Attributes().PutStr("route", "/a")

	dpB := pmetric.NewNumberDataPoint()
	dpB.SetDoubleValue(1)
	dpB.Attributes().PutStr("route", "/b")

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.exportNumberDataPoint(dpA, res, m, &samples, &ts))
	require.NoError(t, e.exportNumberDataPoint(dpB, res, m, &samples, &ts))

	require.Len(t, samples, 2)
	require.Len(t, ts, 2)
	// sample[i] and timeSeries[i] are joined by fingerprint
	assert.Equal(t, samples[0].Fingerprint, ts[0].Fingerprint)
	assert.Equal(t, samples[1].Fingerprint, ts[1].Fingerprint)
	// different label sets -> different fingerprints (no collision on the join key)
	assert.NotEqual(t, samples[0].Fingerprint, samples[1].Fingerprint)
}

// TestSchemaContract_MetricsProducePrometheusReadModel asserts metrics are written
// as a Prometheus-readable series: the __name__ label carries the
// Prom-normalised metric name and the row Type marks it as a metric sample. This
// is what lets PromQL read the data back through Gigapipe.
func TestSchemaContract_MetricsProducePrometheusReadModel(t *testing.T) {
	e := &metricsExporter{}
	res := pcommon.NewResource()
	m := pmetric.NewMetric()
	m.SetName("http.server.duration") // dotted OTel name

	dp := pmetric.NewNumberDataPoint()
	dp.SetDoubleValue(1)

	var samples []Sample
	var ts []TimeSerie
	require.NoError(t, e.exportNumberDataPoint(dp, res, m, &samples, &ts))

	require.Len(t, ts, 1)
	// the labels JSON is the Prometheus label set, keyed by __name__
	assert.Contains(t, ts[0].Labels, `"`+model.MetricNameLabel+`"`)
	// name is Prom-normalised (dots -> underscores)
	assert.Equal(t, "http_server_duration", ts[0].Name)
	assert.Equal(t, int8(SAMPLE_TYPE_METRIC), samples[0].Type)
}

// TestSchemaContract_TracesProduceTempoModel pins the Tempo-compatible two-table
// write: client-side processing inserts into tempo_traces AND
// tempo_traces_attrs_gin (the GIN tag index Tempo search relies on), with the
// span payload stored as OTLP. clickhouseexporter writes a single flat traces
// table with no such tag index.
func TestSchemaContract_TracesProduceTempoModel(t *testing.T) {
	fc := &fakeConn{}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, cluster: true, clientSide: true, tracePayloadType: "json"}

	require.NoError(t, e.pushTraceData(context.Background(), newTraces()))

	joined := strings.Join(fc.queries, "\n")
	assert.Contains(t, joined, "tempo_traces")
	assert.Contains(t, joined, "tempo_traces_attrs_gin")

	require.Len(t, fc.batches, 2)
	// batch 0: the trace rows; batch 1: the GIN tag-index rows
	require.NotEmpty(t, fc.batches[0].appended)
	_, ok := fc.batches[0].appended[0].(*TempoTrace)
	assert.True(t, ok)
	require.NotEmpty(t, fc.batches[1].appended)
	for _, row := range fc.batches[1].appended {
		_, ok := row.(*TempoTraceTag)
		assert.True(t, ok, "attrs_gin batch must hold TempoTraceTag rows")
	}
}

// TestSchemaContract_LokiLabelPromotion documents the Loki ingestion semantics
// this exporter implements and clickhouseexporter does not: the level label is
// always promoted, resource attributes are promoted by default, and
// high-cardinality log-record attributes are held back unless explicitly opted
// in. This is what makes the resulting stream queryable as Loki labels.
func TestSchemaContract_LokiLabelPromotion(t *testing.T) {
	exp := &logsExporter{} // defaults: promoteAllAttributes off

	logAttrs := pcommon.NewMap()
	logAttrs.PutStr("high.cardinality.id", "req-123")

	resAttrs := pcommon.NewMap()
	resAttrs.PutStr("service.instance", "pod-1")

	log := newLogRecord(plog.SeverityNumberWarn)
	logAttrs.CopyTo(log.Attributes())
	addLogLevelAttributeAndHint(log)

	labels := exp.convertAttributesAndMerge(log.Attributes(), resAttrs)

	assert.Equal(t, model.LabelValue("WARN"), labels["level"], "level is always promoted")
	assert.Equal(t, model.LabelValue("pod-1"), labels["service.instance"], "resource attrs promoted by default")
	assert.NotContains(t, labels, "high.cardinality.id", "log attrs held back to protect label cardinality")
}
