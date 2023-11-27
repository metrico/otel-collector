package pyroscopereceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

const (
	typeStr = "pyroscopereceiver"

	defaultHttpAddr           = "0.0.0.0:8062"
	defaultMaxRequestBodySize = 5e6
)

func createDefaultConfig() component.Config {
	return &Config{
		Protocols: Protocols{
			Http: &confighttp.HTTPServerSettings{
				Endpoint:           defaultHttpAddr,
				MaxRequestBodySize: defaultMaxRequestBodySize,
			},
		},
	}
}

func createLogsReceiver(_ context.Context, params receiver.CreateSettings, baseCfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	if nil == consumer {
		return nil, component.ErrNilNextConsumer
	}

	return newPyroscopeReceiver(baseCfg.(*Config), consumer, &params), nil
}

// Creates a factory for the pyroscope receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		typeStr,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, component.StabilityLevelAlpha))
}