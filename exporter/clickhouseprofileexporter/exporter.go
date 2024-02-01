package clickhouseprofileexporter

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/otel-collector/exporter/clickhouseprofileexporter/ch"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	errorCodeError   = "1"
	errorCodeSuccess = ""
)

type clickhouseProfileExporter struct {
	cfg    *Config
	set    *exporter.CreateSettings
	logger *zap.Logger
	meter  metric.Meter

	ch clickhouseAccess
}

type clickhouseAccess interface {
	// Inserts a profile batch into the clickhouse server
	InsertBatch(profiles plog.Logs) error

	// Shuts down the clickhouse connection
	Shutdown() error
}

// TODO: batch like this https://github.com/open-telemetry/opentelemetry-collector/issues/8122
func newClickhouseProfileExporter(ctx context.Context, set *exporter.CreateSettings, cfg *Config) (*clickhouseProfileExporter, error) {
	exp := &clickhouseProfileExporter{
		cfg:    cfg,
		set:    set,
		logger: set.Logger,
		meter:  set.MeterProvider.Meter(typeStr),
	}
	opts, err := clickhouse.ParseDSN(cfg.Dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse dsn: %w", err)
	}
	ch, err := ch.NewClickhouseAccessNativeColumnar(opts, exp.logger)
	if err != nil {
		return nil, fmt.Errorf("failed to init native ch storage: %w", err)
	}
	exp.ch = ch
	if err := initMetrics(exp.meter); err != nil {
		exp.logger.Error(fmt.Sprintf("failed to init metrics: %s", err.Error()))
		return exp, err
	}
	return exp, nil
}

// Sends the profiles to clickhouse server using the configured connection
func (exp *clickhouseProfileExporter) send(ctx context.Context, logs plog.Logs) error {
	start := time.Now().UnixMilli()
	if err := exp.ch.InsertBatch(logs); err != nil {
		otelcolExporterClickhouseProfileBatchInsertDurationMillis.Record(ctx, time.Now().UnixMilli()-start, metric.WithAttributeSet(*newOtelcolAttrSetBatch(errorCodeError)))
		exp.logger.Error(fmt.Sprintf("failed to insert batch: [%s]", err.Error()))
		return err
	}
	otelcolExporterClickhouseProfileBatchInsertDurationMillis.Record(ctx, time.Now().UnixMilli()-start, metric.WithAttributeSet(*newOtelcolAttrSetBatch(errorCodeSuccess)))
	exp.logger.Info("inserted batch", zap.Int("size", logs.ResourceLogs().Len()))
	return nil
}

func newOtelcolAttrSetBatch(errorCode string) *attribute.Set {
	s := attribute.NewSet(attribute.KeyValue{Key: "error_code", Value: attribute.StringValue(errorCode)})
	return &s
}

// Shuts down the exporter, by shutting down the ch connection pull
func (exp *clickhouseProfileExporter) Shutdown(ctx context.Context) error {
	if err := exp.ch.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown: %w", err)
	}
	return nil
}
