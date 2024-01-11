package clickhouseprofileexporter

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
)

const prefix = "exporter_clickhouse_profile_"

var (
	otelcolExporterClickhouseProfileFlushTimeMillis metric.Int64Histogram
)

func initMetrics(meter metric.Meter) error {
	var err error
	if otelcolExporterClickhouseProfileFlushTimeMillis, err = meter.Int64Histogram(
		fmt.Sprint(prefix, "flush_time_millis"),
		metric.WithDescription("Clickhouse profile exporter flush time in millis"),
		metric.WithExplicitBucketBoundaries(0, 5, 10, 20, 50, 100, 200, 500, 1000, 5000),
	); err != nil {
		return err
	}
	return nil
}
