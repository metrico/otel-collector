package main

import (
	"fmt"
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	envprovider "go.opentelemetry.io/collector/confmap/provider/envprovider"
	fileprovider "go.opentelemetry.io/collector/confmap/provider/fileprovider"
	httpprovider "go.opentelemetry.io/collector/confmap/provider/httpprovider"
	httpsprovider "go.opentelemetry.io/collector/confmap/provider/httpsprovider"
	yamlprovider "go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/otelcol"

	_ "github.com/KimMachineGun/automemlimit" // default == 0.9 * cgroup_memory_limit
	s3provider "github.com/open-telemetry/opentelemetry-collector-contrib/confmap/provider/s3provider"
	secretsmanagerprovider "github.com/open-telemetry/opentelemetry-collector-contrib/confmap/provider/secretsmanagerprovider"
	_ "go.uber.org/automaxprocs" // default == cgroup_cpu_limit
)

func main() {
	info := component.BuildInfo{
		Command:     "otel-collector",
		Description: "qryn OTEL Collector",
		Version:     "latest",
	}

	set := otelcol.CollectorSettings{
		BuildInfo: info,
		Factories: components,
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				ProviderFactories: []confmap.ProviderFactory{
					envprovider.NewFactory(),
					fileprovider.NewFactory(),
					httpprovider.NewFactory(),
					httpsprovider.NewFactory(),
					yamlprovider.NewFactory(),
					s3provider.NewFactory(),
					secretsmanagerprovider.NewFactory(),
				},
			},
		},
	}

	if err := run(set); err != nil {
		log.Fatal(err)
	}
}

func runInteractive(set otelcol.CollectorSettings) error {
	cmd := otelcol.NewCommand(set)
	err := cmd.Execute()
	if err != nil {
		return fmt.Errorf("application run finished with error: %w", err)
	}

	return nil
}
