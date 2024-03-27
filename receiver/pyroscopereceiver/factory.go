package pyroscopereceiver

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr = "pyroscopereceiver"

	defaultHttpAddr           = "0.0.0.0:8062"
	defaultMaxRequestBodySize = 5e6 + 1e6 // reserve for metadata
	defaultTimeout            = 10 * time.Second
)

func createDefaultConfig() component.Config {
	return &Config{
		Protocols: Protocols{
			Http: &confighttp.ServerConfig{
				Endpoint:           defaultHttpAddr,
				MaxRequestBodySize: defaultMaxRequestBodySize,
			},
		},
		Timeout: defaultTimeout,
	}
}

func createLogsReceiver(_ context.Context, set receiver.CreateSettings, cfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	if nil == consumer {
		return nil, errors.New("nil next Consumer")
	}
	recv, err := newPyroscopeReceiver(cfg.(*Config), consumer, &set)
	if err != nil {
		return nil, err
	}
	return recv, nil
}

// Creates a factory for the pyroscope receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha))
}
