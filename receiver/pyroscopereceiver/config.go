package pyroscopereceiver

import (
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Configures supported protocols
type Protocols struct {
	// Http.MaxRequestBodySize configures max decompressed body size in bytes
	Http *confighttp.HTTPServerSettings `mapstructure:"http"`
}

// Represents the receiver config within the collector's config.yaml
type Config struct {
	Protocols Protocols `mapstructure:"protocols"`

	// Configures expected decompressed request body size in bytes to optimize pipeline allocations
	DecompressedRequestBodySizeBytesExpectedValue int64 `mapstructure:"request_body_size_expected_value"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Protocols.Http.MaxRequestBodySize < 1 {
		return fmt.Errorf("max_request_body_size must be positive")
	}
	if cfg.DecompressedRequestBodySizeBytesExpectedValue < 1 {
		return fmt.Errorf("request_body_size_expected_value must be positive")
	}
	if cfg.DecompressedRequestBodySizeBytesExpectedValue > cfg.Protocols.Http.MaxRequestBodySize {
		return fmt.Errorf("expected value cannot be greater than max")
	}
	return nil
}
