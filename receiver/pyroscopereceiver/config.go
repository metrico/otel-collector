package pyroscopereceiver

import (
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
)

// Configures supported protocols
type Protocols struct {
	// Http.MaxRequestBodySize configures max uncompressed body size in bytes
	Http *confighttp.HTTPServerSettings `mapstructure:"http"`
}

// Represents the receiver config within the collector's config.yaml
type Config struct {
	Protocols Protocols `mapstructure:"protocols"`

	// Cofigures timeout for synchronous request handling by the receiver server
	Timeout time.Duration `mapstructure:"timeout"`
	// Configures the expected value for uncompressed request body size in bytes to size pipeline buffers
	// and optimize allocations based on exported metrics
	RequestBodyUncompressedSizeBytes int64 `mapstructure:"request_body_uncompressed_size_bytes"`
	// Configures the expected value for uncompressed parsed body size in bytes to size pipeline buffers
	// and optimize allocations based on exported metrics
	ParsedBodyUncompressedSizeBytes int64 `mapstructure:"parsed_body_uncompressed_size_bytes"`
}

var _ component.Config = (*Config)(nil)

// Checks that the receiver configuration is valid
func (cfg *Config) Validate() error {
	if cfg.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if cfg.Protocols.Http.MaxRequestBodySize < 1 {
		return fmt.Errorf("max_request_body_size must be positive")
	}
	if cfg.RequestBodyUncompressedSizeBytes < 0 {
		return fmt.Errorf("request_body_uncompressed_size_bytes must be positive")
	}
	if cfg.RequestBodyUncompressedSizeBytes > cfg.Protocols.Http.MaxRequestBodySize {
		return fmt.Errorf("expected value cannot be greater than max")
	}
	if cfg.ParsedBodyUncompressedSizeBytes < 0 {
		return fmt.Errorf("parsed_body_uncompressed_size_bytes must be positive")
	}
	return nil
}
