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
package qrynexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// The value of "type" key in configuration.
	typeStr = "qryn"
	// The stability level of the exporter.
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a factory for Logging exporter
func NewFactory() component.ExporterFactory {
	return component.NewExporterFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesExporter(createTracesExporter, stability),
		component.WithLogsExporter(createLogsExporter, stability),
		component.WithMetricsExporter(createMetricsExporter, stability),
	)
}

func createDefaultConfig() component.ExporterConfig {
	return &Config{
		ExporterSettings: config.NewExporterSettings(component.NewID(typeStr)),
		TimeoutSettings:  exporterhelper.NewDefaultTimeoutSettings(),
		QueueSettings:    QueueSettings{QueueSize: exporterhelper.NewDefaultQueueSettings().QueueSize},
		RetrySettings:    exporterhelper.NewDefaultRetrySettings(),
	}
}

// createTracesExporter creates a new exporter for traces.
// Traces are directly insert into clickhouse.
func createTracesExporter(
	ctx context.Context,
	params component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.TracesExporter, error) {

	c := cfg.(*Config)
	oce, err := newTracesExporter(ctx, params.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure qryn traces exporter: %w", err)
	}

	return exporterhelper.NewTracesExporter(
		ctx,
		params,
		cfg,
		oce.pushTraceData,
		exporterhelper.WithShutdown(oce.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.enforcedQueueSettings()),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

// createLogsExporter creates a new exporter for logs.
// Logs are directly insert into clickhouse.
func createLogsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.LogsExporter, error) {
	c := cfg.(*Config)
	exporter, err := newLogsExporter(ctx, set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure qryn logs exporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exporter.pushLogsData,
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.enforcedQueueSettings()),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}

// createMetricsExporter creates a new exporter for metrics.
// Metrics are directly insert into clickhouse.
func createMetricsExporter(
	ctx context.Context,
	set component.ExporterCreateSettings,
	cfg component.ExporterConfig,
) (component.MetricsExporter, error) {
	c := cfg.(*Config)
	exporter, err := newMetricsExporter(ctx, set.Logger, c)
	if err != nil {
		return nil, fmt.Errorf("cannot configure qryn logs exporter: %w", err)
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exporter.pushMetricsData,
		exporterhelper.WithShutdown(exporter.Shutdown),
		exporterhelper.WithTimeout(c.TimeoutSettings),
		exporterhelper.WithQueue(c.enforcedQueueSettings()),
		exporterhelper.WithRetry(c.RetrySettings),
	)
}
