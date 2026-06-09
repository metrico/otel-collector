package clickhouseprofileexporter

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Represents the receiver config within the collector's config.yaml
type Config struct {
	exporterhelper.TimeoutConfig `mapstructure:",squash"`
	configretry.BackOffConfig    `mapstructure:"retry_on_failure"`
	QueueConfig                  configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// DSN is the ClickHouse server Data Source Name.
	// For tcp protocol reference: [ClickHouse/clickhouse-go#dsn](https://github.com/ClickHouse/clickhouse-go#dsn).
	// For http protocol reference: [mailru/go-clickhouse/#dsn](https://github.com/mailru/go-clickhouse/#dsn).
	Dsn string `mapstructure:"dsn"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	return nil
}
