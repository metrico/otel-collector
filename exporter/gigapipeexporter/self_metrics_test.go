package gigapipeexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestDualHistogramEmitsBothPrefixes verifies a single Record produces the
// self-metric under both the current "exporter_gigapipe_" prefix and the
// deprecated "exporter_qryn_" prefix.
func TestDualHistogramEmitsBothPrefixes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := mp.Meter("test")

	require.NoError(t, initMetrics(meter))

	otelcolExporterGigapipeBatchInsertDurationMillis.Record(
		context.Background(), 42,
		metric.WithAttributeSet(*newOtelcolAttrSetBatch(errorCodeSuccess, dataTypeLogs)),
	)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}

	assert.True(t, names["exporter_gigapipe_batch_insert_duration_millis"],
		"expected current-prefix metric, got %v", names)
	assert.True(t, names["exporter_qryn_batch_insert_duration_millis"],
		"expected deprecated-prefix metric, got %v", names)
}
