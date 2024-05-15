package chstatsreceiver

import (
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Represents the receiver config within the collector's config.yaml
type Config struct {
	DSN     string        `mapstructure:"dsn"`
	Timeout time.Duration `mapstructure:"timeout"`
	Queries []string      `mapstructure:"queries"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Timeout < 15*time.Second {
		return fmt.Errorf("timeout must be at least 15 seconds")
	}
	chDSN, err := url.Parse(cfg.DSN)
	if err != nil {
		return fmt.Errorf("invalid dsn: %w", err)
	}
	if chDSN.Scheme != "clickhouse" {
		return fmt.Errorf("invalid dsn: scheme should be clickhouse://")
	}
	return nil
}
