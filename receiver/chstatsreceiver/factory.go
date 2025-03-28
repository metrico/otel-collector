package chstatsreceiver

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr        = "chstatsreceiver"
	defaultTimeout = 15 * time.Second
)

func createDefaultConfig() component.Config {
	return &Config{
		DSN:     "",
		Timeout: defaultTimeout,
		Queries: []string{},
	}
}

func createMetricsReceiver(_ context.Context, set receiver.Settings, cfg component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {
	return &chReceiver{
		cfg:             cfg.(*Config),
		logger:          set.Logger,
		metricsConsumer: consumer,
	}, nil
}

func createLogsReceiver(_ context.Context, set receiver.Settings, cfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	return &chReceiver{
		cfg:          cfg.(*Config),
		logger:       set.Logger,
		logsConsumer: consumer,
	}, nil
}

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha),
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha))
}
