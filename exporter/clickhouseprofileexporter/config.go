package clickhouseprofileexporter

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Represents the receiver config within the collector's config.yaml
type Config struct {
	exporterhelper.TimeoutSettings `mapstructure:",squash"`
	exporterhelper.RetrySettings   `mapstructure:"retry_on_failure"`
	QueueSettings                  `mapstructure:"sending_queue"`

	ClusteredClickhouse bool `mapstructure:"clustered_clickhouse"`
	// DSN is the ClickHouse server Data Source Name.
	// For tcp protocol reference: [ClickHouse/clickhouse-go#dsn](https://github.com/ClickHouse/clickhouse-go#dsn).
	// For http protocol reference: [mailru/go-clickhouse/#dsn](https://github.com/mailru/go-clickhouse/#dsn).
	Dsn string `mapstructure:"dsn"`
}

type QueueSettings struct {
	// Length of the sending queue
	QueueSize int `mapstructure:"queue_size"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}

func (cfg *Config) enforceQueueSettings() exporterhelper.QueueSettings {
	return exporterhelper.QueueSettings{
		Enabled:      true,
		NumConsumers: 1,
		QueueSize:    cfg.QueueSettings.QueueSize,
	}
}
