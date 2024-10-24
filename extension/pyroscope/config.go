package pyroscope

import "go.opentelemetry.io/collector/component"

type Config struct {
	ApplicationName string            `mapstructure:"application_name"`
	Tags            map[string]string `mapstructure:"tags"`
	ServerAddress   string            `mapstructure:"server_address"`
	BasicAuth       BasicAuth         `mapstructure:"basic_auth"`
	ProfileTypes    []string          `mapstructure:"profile_types"`
	TenantID        string            `mapstructure:"tenant_id"`
}

type BasicAuth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

func defaultConfig() component.Config {
	return &Config{
		ApplicationName: "opentelemetry-collector",
		ServerAddress:   "http://localhost:8062",
		Tags:            map[string]string{},
		BasicAuth:       BasicAuth{},
		ProfileTypes:    []string{"cpu", "alloc_objects", "alloc_space", "inuse_objects", "inuse_space"},
	}
}
