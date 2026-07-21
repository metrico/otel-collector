// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package gigapipeexporter

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

const (
	// The value of "type" key in configuration.
	typeStr = "gigapipe"
	// deprecatedTypeStr is the former component type, kept as a
	// backward-compatible alias. Use typeStr in new configurations.
	deprecatedTypeStr = "qryn"
	// The stability level of the exporter.
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a factory for the Gigapipe exporter.
func NewFactory() exporter.Factory {
	return newFactory(typeStr, false)
}

// NewFactoryQryn creates a factory registered under the deprecated "qryn"
// type. It behaves identically to NewFactory but logs a deprecation warning
// the first time an exporter is created. New configurations should use the
// "gigapipe" type instead.
func NewFactoryQryn() exporter.Factory {
	return newFactory(deprecatedTypeStr, true)
}

func newFactory(typeStr string, deprecated bool) exporter.Factory {
	var warnOnce sync.Once
	warn := func(logger *zap.Logger) {
		if !deprecated || logger == nil {
			return
		}
		warnOnce.Do(func() {
			logger.Warn(`the "qryn" exporter type is deprecated and will be removed in a future release; use "gigapipe" instead`)
		})
	}
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithTraces(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
			warn(set.Logger)
			return createTracesExporter(ctx, set, cfg)
		}, stability),
		exporter.WithLogs(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
			warn(set.Logger)
			return createLogsExporter(ctx, set, cfg)
		}, stability),
		exporter.WithMetrics(func(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Metrics, error) {
			warn(set.Logger)
			return createMetricsExporter(ctx, set, cfg)
		}, stability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		TimeoutConfig:    exporterhelper.NewDefaultTimeoutConfig(),
		QueueConfig:      configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
		BackOffConfig:    configretry.NewDefaultBackOffConfig(),
		DSN:              defaultDSN,
		TracePayloadType: defaultTracePayloadType,
	}
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly insert into clickhouse.
func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	c := cfg.(*Config)
	oce, err := newTracesExporter(set.Logger, c, &set)
	if err != nil {
		return nil, fmt.Errorf("cannot configure gigapipe traces exporter: %w", err)
	}

	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		oce.pushTraceData,
		exporterhelper.WithShutdown(oce.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly insert into clickhouse.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	c := cfg.(*Config)
	exporter, err := newLogsExporter(set.Logger, c, &set)
	if err != nil {
		return nil, fmt.Errorf("cannot configure gigapipe logs exporter: %w", err)
	}

	return exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		exporter.pushLogsData,
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}

// createMetricsExporter creates a new exporter for metrics.
// Metrics are directly insert into clickhouse.
func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	c := cfg.(*Config)
	exporter, err := newMetricsExporter(set.Logger, c, &set)
	if err != nil {
		return nil, fmt.Errorf("cannot configure gigapipe logs exporter: %w", err)
	}

	return exporterhelper.NewMetrics(
		ctx,
		set,
		cfg,
		exporter.pushMetricsData,
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutConfig),
		exporterhelper.WithQueue(c.QueueConfig),
		exporterhelper.WithRetry(c.BackOffConfig),
	)
}
