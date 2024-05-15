package chstatsreceiver

import (
	"context"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"time"
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

func createMetricsReceiver(_ context.Context, set receiver.CreateSettings, cfg component.Config, consumer consumer.Metrics) (receiver.Metrics, error) {
	return &chReceiver{
		cfg:      cfg.(*Config),
		logger:   set.Logger,
		consumer: consumer,
	}, nil
}

func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, component.StabilityLevelAlpha))
}
