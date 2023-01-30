package qrynexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	defaultCfg := createDefaultConfig()
	defaultCfg.(*Config).DSN = defaultDSN

	tests := []struct {
		id       component.ID
		expected component.Config
	}{

		{
			id:       component.NewIDWithName(typeStr, ""),
			expected: defaultCfg,
		},
		{
			id: component.NewIDWithName(typeStr, "full"),
			expected: &Config{
				DSN: defaultDSN,
				TimeoutSettings: exporterhelper.TimeoutSettings{
					Timeout: 5 * time.Second,
				},
				RetrySettings: exporterhelper.RetrySettings{
					Enabled:         true,
					InitialInterval: 5 * time.Second,
					MaxInterval:     30 * time.Second,
					MaxElapsedTime:  300 * time.Second,
				},
				QueueSettings: QueueSettings{
					QueueSize: 100,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, component.UnmarshalConfig(sub, cfg))

			assert.NoError(t, component.ValidateConfig(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
