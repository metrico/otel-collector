package gigapipeexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// prefix is the current self-metric name prefix.
	prefix = "exporter_gigapipe_"
	// deprecatedPrefix is the former self-metric name prefix, still emitted
	// alongside prefix during the deprecation window so existing dashboards
	// keep working. Remove once consumers have migrated.
	deprecatedPrefix = "exporter_qryn_"

	errorCodeError   = "1"
	errorCodeSuccess = ""

	dataTypeLogs    = "logs"
	dataTypeMetrics = "metrics"
	dataTypeTraces  = "traces"
)

// dualHistogram records the same measurement to two histograms, letting the
// exporter emit self-metrics under both the current "exporter_gigapipe_" and
// the deprecated "exporter_qryn_" prefixes.
type dualHistogram struct {
	current    metric.Int64Histogram
	deprecated metric.Int64Histogram
}

func (d dualHistogram) Record(ctx context.Context, incr int64, options ...metric.RecordOption) {
	if d.current != nil {
		d.current.Record(ctx, incr, options...)
	}
	if d.deprecated != nil {
		d.deprecated.Record(ctx, incr, options...)
	}
}

var (
	otelcolExporterGigapipeBatchInsertDurationMillis dualHistogram
)

func initMetrics(meter metric.Meter) error {
	buckets := metric.WithExplicitBucketBoundaries(0, 5, 10, 20, 50, 100, 200, 500, 1000, 5000)

	current, err := meter.Int64Histogram(
		fmt.Sprint(prefix, "batch_insert_duration_millis"),
		metric.WithDescription("Gigapipe exporter batch insert duration in millis"),
		buckets,
	)
	if err != nil {
		return err
	}

	deprecated, err := meter.Int64Histogram(
		fmt.Sprint(deprecatedPrefix, "batch_insert_duration_millis"),
		metric.WithDescription("Gigapipe exporter batch insert duration in millis (deprecated, use the exporter_gigapipe_ prefix)"),
		buckets,
	)
	if err != nil {
		return err
	}

	otelcolExporterGigapipeBatchInsertDurationMillis = dualHistogram{current: current, deprecated: deprecated}
	return nil
}

func newOtelcolAttrSetBatch(errorCode string, dataType string) *attribute.Set {
	s := attribute.NewSet(
		attribute.KeyValue{Key: "error_code", Value: attribute.StringValue(errorCode)},
		attribute.KeyValue{Key: "data_type", Value: attribute.StringValue(dataType)},
	)
	return &s
}
