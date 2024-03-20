package qrynexporter

import (
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	prefix = "exporter_qryn_"

	errorCodeError   = "1"
	errorCodeSuccess = ""

	dataTypeLogs    = "logs"
	dataTypeMetrics = "metrics"
	dataTypeTraces  = "traces"
)

var (
	otelcolExporterQrynBatchInsertDurationMillis metric.Int64Histogram
)

func initMetrics(meter metric.Meter) error {
	var err error
	if otelcolExporterQrynBatchInsertDurationMillis, err = meter.Int64Histogram(
		fmt.Sprint(prefix, "batch_insert_duration_millis"),
		metric.WithDescription("Qryn exporter batch insert duration in millis"),
		metric.WithExplicitBucketBoundaries(0, 5, 10, 20, 50, 100, 200, 500, 1000, 5000),
	); err != nil {
		return err
	}
	return nil
}

func newOtelcolAttrSetBatch(errorCode string, dataType string) *attribute.Set {
	s := attribute.NewSet(
		attribute.KeyValue{Key: "error_code", Value: attribute.StringValue(errorCode)},
		attribute.KeyValue{Key: "data_type", Value: attribute.StringValue(dataType)},
	)
	return &s
}
