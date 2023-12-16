package pyroscopereceiver

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Configures supported protocols
type Protocols struct {
	Http *confighttp.HTTPServerSettings `mapstructure:"http"`
}

// Represents the receiver config within the collector's config.yaml
type Config struct {
	Protocols Protocols `mapstructure:"protocols"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Protocols.Http.MaxRequestBodySize < 1 {
		return fmt.Errorf("max_request_body_size must be positive")
	}
	return nil
}
