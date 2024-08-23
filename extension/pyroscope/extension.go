// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package pyroscope

import (
	"context"
	"github.com/google/uuid"
	"github.com/grafana/pyroscope-go"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension"
)

var pyroscopeType = component.MustNewType("pyroscope")

// NewNopCreateSettings returns a new nop settings for extension.Factory Create* functions.
func NewPyroscopeCreateSettings() extension.CreateSettings {
	return extension.CreateSettings{
		ID:                component.NewIDWithName(pyroscopeType, uuid.NewString()),
		TelemetrySettings: componenttest.NewNopTelemetrySettings(),
		BuildInfo:         component.NewDefaultBuildInfo(),
	}
}

// NewNopFactory returns an extension.Factory that constructs nop extensions.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		pyroscopeType,
		defaultConfig,
		func(ctx context.Context, settings extension.CreateSettings, config component.Config) (extension.Extension, error) {
			return &PyroscopeExtension{
				config: config.(*Config),
			}, nil
		},
		component.StabilityLevelStable)
}

type PyroscopeExtension struct {
	config   *Config
	settings extension.CreateSettings
	ctx      context.Context
	profiler *pyroscope.Profiler
}

func (p PyroscopeExtension) Start(ctx context.Context, host component.Host) error {
	cfg := pyroscope.Config{
		ApplicationName: p.config.ApplicationName,

		// replace this with the address of pyroscope server
		ServerAddress: p.config.ServerAddress,

		// you can disable logging by setting this to nil
		Logger: pyroscope.StandardLogger,

		// Optional HTTP Basic authentication (Grafana Cloud)
		BasicAuthUser:     p.config.BasicAuth.Username,
		BasicAuthPassword: p.config.BasicAuth.Password,
		Tags:              p.config.Tags,
		TenantID:          p.config.TenantID,
		ProfileTypes:      nil,
	}
	for _, profileType := range p.config.ProfileTypes {
		cfg.ProfileTypes = append(cfg.ProfileTypes, pyroscope.ProfileType(profileType))
	}
	var err error
	p.profiler, err = pyroscope.Start(cfg)
	return err
}

func (p PyroscopeExtension) Shutdown(ctx context.Context) error {
	return p.profiler.Stop()
}
