package pyroscopereceiver

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Configures supported protocols
type Protocols struct {
	// HTTP.MaxRequestBodySize configures max uncompressed body size in bytes
	HTTP *confighttp.ServerConfig `mapstructure:"http"`
}

type ExcludeLabel struct {
	Metric string `mapstructure:"metric"`
	Label  string `mapstructure:"label"`
}

type MetricsConfig struct {
	Enable         bool           `mapstructure:"enable" default:"true"`
	ExcludeLabels  []ExcludeLabel `mapstructure:"exclude_labels"`
	ExcludeMetrics []string       `mapstructure:"exclude_metrics"`
}

// Represents the receiver config within the collector's config.yaml
type Config struct {
	Protocols Protocols `mapstructure:"protocols"`

	// Cofigures timeout for synchronous request handling by the receiver server
	Timeout time.Duration `mapstructure:"timeout"`

	Metrics MetricsConfig `mapstructure:"metrics"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if cfg.Protocols.HTTP.MaxRequestBodySize < 1 {
		return fmt.Errorf("max_request_body_size must be positive")
	}
	return nil
}
