package main

import (
	"fmt"
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/otelcol"
	_ "go.uber.org/automaxprocs"
)

func main() {

	factories, err := components()
	if err != nil {
		log.Fatalf("failed to build default components: %v", err)
	}

	info := component.BuildInfo{
		Command:     "otel-collector",
		Description: "qryn OTEL Collector",
		Version:     "latest",
	}

	params := otelcol.CollectorSettings{
		Factories: factories,
		BuildInfo: info,
	}

	if err := run(params); err != nil {
		log.Fatal(err)
	}
}

func runInteractive(params otelcol.CollectorSettings) error {
	cmd := otelcol.NewCommand(params)
	err := cmd.Execute()
	if err != nil {
		return fmt.Errorf("application run finished with error: %w", err)
	}

	return nil
}
