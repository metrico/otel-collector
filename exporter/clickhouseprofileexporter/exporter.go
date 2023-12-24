package clickhouseprofileexporter

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/metrico/otel-collector/exporter/clickhouseprofileexporter/ch"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type clickhouseProfileExporter struct {
	cfg    *Config
	set    *exporter.CreateSettings
	logger *zap.Logger

	ch clickhouseAccess
}

type clickhouseAccess interface {
	// Inserts a profile batch into the clickhouse server
	InsertBatch(profiles plog.Logs) error

	// Shuts down the clickhouse connection
	Shutdown() error
}

func newClickhouseProfileExporter(ctx context.Context, set *exporter.CreateSettings, cfg *Config) (*clickhouseProfileExporter, error) {
	opts, err := clickhouse.ParseDSN(cfg.Dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse clickhouse dsn: %w", err)
	}
	ch, err := ch.NewClickhouseAccessNativeColumnar(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to init native ch storage: %w", err)
	}
	return &clickhouseProfileExporter{
		cfg:    cfg,
		set:    set,
		logger: set.Logger,
		ch:     ch,
	}, nil
}

// Sends the profiles to clickhouse server using the configured connection
func (exp *clickhouseProfileExporter) send(ctx context.Context, logs plog.Logs) error {
	if err := exp.ch.InsertBatch(logs); err != nil {
		msg := fmt.Sprintf("failed to insert batch: [%s]", err.Error())
		exp.logger.Error(msg)
		return err
	}
	exp.logger.Info("inserted batch", zap.Int("size", logs.ResourceLogs().Len()))
	return nil
}

// Shuts down the exporter, by shutting down the ch connection pull
func (exp *clickhouseProfileExporter) Shutdown(ctx context.Context) error {
	if err := exp.ch.Shutdown(); err != nil {
		return fmt.Errorf("failed to shutdown: %w", err)
	}
	return nil
}
