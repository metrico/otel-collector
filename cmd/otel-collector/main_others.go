package main

import "go.opentelemetry.io/collector/otelcol"

func run(set otelcol.CollectorSettings) error {
	return runInteractive(set)
}
