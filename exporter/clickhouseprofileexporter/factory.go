package clickhouseprofileexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr = "clickhouseprofileexporter"

	defaultDsn = "tcp://127.0.0.1:9000/qryn"
)

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:   QueueSettings{QueueSize: exporterhelper.NewDefaultQueueSettings().QueueSize},
		BackOffConfig:   configretry.NewDefaultBackOffConfig(),
		Dsn:             defaultDsn,
	}
}

func createLogsExporter(ctx context.Context, set exporter.CreateSettings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp, err := newClickhouseProfileExporter(ctx, &set, cfg.(*Config))
	if err != nil {
		return nil, fmt.Errorf("cannot init clickhouse profile exporter: %w", err)
	}
	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exp.send,
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithQueue(c.enforceQueueSettings()),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

// Creates a factory for the clickhouse profile exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		typeStr,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelAlpha),
	)
}
