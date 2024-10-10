package pyroscopereceiver

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-faster/city"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/testclient"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

type datatest struct {
	name      string
	urlParams map[string]string
	filename  string
	expected  plog.Logs
}

type profileLog struct {
	timestamp uint64
	body      []byte
	attrs     map[string]any
}

func loadTestData(t *testing.T, filename string) []byte {
	b, err := os.ReadFile(filepath.Join("testdata", filename))
	assert.NoError(t, err, "failed to load expected pprof payload")
	return b
}

func run(t *testing.T, tests []datatest, collectorAddr string, sink *consumertest.LogsSink) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, testclient.Ingest(collectorAddr, tt.urlParams, tt.filename), "send shouldn't have been failed")
			actual := sink.AllLogs()
			logRecords := actual[0].ResourceLogs().At(0).ScopeLogs().At(0).
				LogRecords()
			for i := 0; i < logRecords.Len(); i++ {
				tree := logRecords.At(i).Attributes().AsRaw()["tree"].([]byte)
				functions := logRecords.At(i).Attributes().AsRaw()["functions"].([]byte)
				logRecords.At(i).Attributes().PutStr("tree", fmt.Sprintf("%d", city.CH64(tree)))
				logRecords.At(i).Attributes().PutStr("functions", fmt.Sprintf("%d", city.CH64(functions)))
			}
			assert.NoError(t, plogtest.CompareLogs(tt.expected, actual[0]))
			sink.Reset()
		})
	}
}

func startHttpServer(t *testing.T) (string, *consumertest.LogsSink) {
	addr := getAvailableLocalTcpPort(t)
	cfg := &Config{
		Protocols: Protocols{
			HTTP: &confighttp.ServerConfig{
				Endpoint:           addr,
				MaxRequestBodySize: defaultMaxRequestBodySize,
			},
		},
		Timeout: defaultTimeout,
	}
	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopSettings()
	set.Logger = zap.Must(zap.NewDevelopment())
	recv, err := newPyroscopeReceiver(cfg, sink, &set)
	require.NoError(t, err)

	require.NoError(t, recv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, recv.Shutdown(context.Background())) })

	return addr, sink
}

func TestPyroscopeIngestJfrCpu(t *testing.T) {
	tests := make([]datatest, 1)
	pb := loadTestData(t, "cortex-dev-01__kafka-0__cpu__0.pb")
	tests[0] = datatest{
		name: "send labeled multipart form data gzipped cpu jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":       "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
			"from":       "1700332322",
			"until":      "1700332329",
			"format":     "jfr",
			"sampleRate": "100",
		},
		filename: filepath.Join("testdata", "cortex-dev-01__kafka-0__cpu__0.jfr"),
		expected: gen([]profileLog{{
			timestamp: 1700332322000000000,
			attrs: map[string]any{
				"service_name": "com.example.App",
				"tags": map[string]any{
					"dc":                  "us-east-1",
					"kubernetes_pod_name": "app-abcd1234",
				},
				"duration_ns":  "7000000000",
				"type":         "process_cpu",
				"period_type":  "cpu",
				"period_unit":  "nanoseconds",
				"payload_type": "0",
				"sample_types": []any{"cpu"},
				"sample_units": []any{"nanoseconds"},
				"values_agg":   []any{[]any{"cpu:nanoseconds", 4780000000, 370}},
				"tree":         "8815968544084662059",
				"functions":    "12509046500621493878",
			},
			body: pb,
		}}),
	}
	addr, sink := startHttpServer(t)
	collectorAddr := fmt.Sprintf("http://%s", addr)
	run(t, tests, collectorAddr, sink)
}

func TestPyroscopeIngestJfrMemory(t *testing.T) {
	tests := make([]datatest, 1)
	pbAllocInNewTlab := loadTestData(t, "memory_example_alloc_in_new_tlab.pb")
	pbLiveObject := loadTestData(t, "memory_example_live_object.pb")
	tests[0] = datatest{
		name: "send labeled multipart form data gzipped memory jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":   "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
			"from":   "1700332322",
			"until":  "1700332329",
			"format": "jfr",
		},
		filename: filepath.Join("testdata", "memory_alloc_live_example.jfr"),
		expected: gen([]profileLog{
			{
				timestamp: 1700332322000000000,
				attrs: map[string]any{
					"service_name": "com.example.App",
					"tags": map[string]any{
						"dc":                  "us-east-1",
						"kubernetes_pod_name": "app-abcd1234",
					},
					"duration_ns":  "7000000000",
					"type":         "memory",
					"period_type":  "space",
					"period_unit":  "bytes",
					"payload_type": "0",
					"sample_types": []any{"alloc_in_new_tlab_objects", "alloc_in_new_tlab_bytes"},
					"sample_units": []any{"count", "bytes"},
					"values_agg": []any{
						[]any{"alloc_in_new_tlab_objects:count", 977, 471},
						[]any{"alloc_in_new_tlab_bytes:bytes", 512229376, 471},
					},
					"tree":      "16287638610851960464",
					"functions": "14254943256614951927",
				},
				body: pbAllocInNewTlab,
			},
			{
				timestamp: 1700332322000000000,
				attrs: map[string]any{
					"service_name": "com.example.App",
					"tags": map[string]any{
						"dc":                  "us-east-1",
						"kubernetes_pod_name": "app-abcd1234",
					},
					"duration_ns":  "7000000000",
					"type":         "memory",
					"period_type":  "objects",
					"period_unit":  "count",
					"payload_type": "0",
					"sample_types": []any{"live"},
					"sample_units": []any{"count"},
					"values_agg":   []any{[]any{"live:count", 976, 471}},
					"tree":         "1969295976612920317",
					"functions":    "14254943256614951927",
				},
				body: pbLiveObject,
			},
		}),
	}

	addr, sink := startHttpServer(t)
	collectorAddr := fmt.Sprintf("http://%s", addr)
	run(t, tests, collectorAddr, sink)
}

// TODO: add block, lock, wall test cases

// Returns an available local tcp port. It doesnt bind the port, and there is a race condition as
// another process maybe bind the port before the test does
func getAvailableLocalTcpPort(t *testing.T) string {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err, "failed to bind a free local tcp port")
	defer func() {
		assert.NoError(t, l.Close())
	}()
	return l.Addr().String()
}

func gen(pl []profileLog) plog.Logs {
	newpl := plog.NewLogs()
	rs := newpl.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, l := range pl {
		r := rs.AppendEmpty()
		_ = r.Attributes().FromRaw(l.attrs)
		r.SetTimestamp(pcommon.Timestamp(l.timestamp))
		r.Body().SetEmptyBytes().FromRaw(l.body)
	}
	return newpl
}
