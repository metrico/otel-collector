package pyroscopereceiver

import (
	"path/filepath"
	"testing"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/testclient"
	"github.com/stretchr/testify/assert"
)

type request struct {
	urlParams map[string]string
	jfr       string
}

// Benchmarks a running otelcol pyroscope write pipeline (collector and Clickhouse).
// Adjust collectorAddr to bench a your target if needed.
// Example: GOMAXPROCS=1 go test -bench ^BenchmarkPyroscopePipeline$ github.com/metrico/otel-collector/receiver/pyroscopereceiver -benchtime 10s -count 6
func BenchmarkPyroscopePipeline(b *testing.B) {
	dist := []request{
		{
			urlParams: map[string]string{
				"name":       "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
				"from":       "1700332322",
				"until":      "1700332329",
				"format":     "jfr",
				"sampleRate": "100",
			},
			jfr: filepath.Join("..", "receiver", "pyroscopereceiver", "testdata", "cortex-dev-01__kafka-0__cpu__0.jfr"),
		},
		{
			urlParams: map[string]string{
				"name":   "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
				"from":   "1700332322",
				"until":  "1700332329",
				"format": "jfr",
			},
			jfr: filepath.Join("..", "receiver", "pyroscopereceiver", "testdata", "memory_alloc_live_example.jfr"),
		},
	}
	collectorAddr := "http://0.0.0.0:8062"

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		j := 0
		for pb.Next() {
			err := testclient.Ingest(collectorAddr, dist[j].urlParams, dist[j].jfr)
			assert.NoError(b, err, "failed to ingest")
			j = (j + 1) % len(dist)
		}
	})
}
