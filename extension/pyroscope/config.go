package pyroscope

import "go.opentelemetry.io/collector/component"

type Config struct {
	ApplicationName string            `json:"application_name"`
	Tags            map[string]string `json:"tags"`
	ServerAddress   string            `json:"server_address"`
	BasicAuth       BasicAuth         `json:"basic_auth"`
	ProfileTypes    []string          `json:"profile_types"`
	TenantID        string            `json:"tenant_id"`
}

type BasicAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
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
