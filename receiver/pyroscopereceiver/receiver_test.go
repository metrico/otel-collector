package pyroscopereceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

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

type jfrtest struct {
	name      string
	urlParams map[string]string
	jfr       string
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

func run(t *testing.T, tests []jfrtest, collectorAddr string, sink *consumertest.LogsSink) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, send(t, collectorAddr, tt.urlParams, tt.jfr), "send shouldn't have been failed")
			actual := sink.AllLogs()
			assert.NoError(t, plogtest.CompareLogs(tt.expected, actual[0]))
			sink.Reset()
		})
	}
}

func startHttpServer(t *testing.T) (string, *consumertest.LogsSink) {
	addr := getAvailableLocalTcpPort(t)
	cfg := &Config{
		Protocols: Protocols{
			Http: &confighttp.HTTPServerSettings{
				Endpoint:           addr,
				MaxRequestBodySize: defaultMaxRequestBodySize,
			},
		},
		Timeout: defaultTimeout,
	}
	sink := new(consumertest.LogsSink)
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.Must(zap.NewDevelopment())
	recv, err := newPyroscopeReceiver(cfg, sink, &set)
	require.NoError(t, err)

	require.NoError(t, recv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, recv.Shutdown(context.Background())) })

	return addr, sink
}

func send(t *testing.T, addr string, urlParams map[string]string, jfr string) error {
	data, err := os.ReadFile(jfr)
	if err != nil {
		return err
	}

	body := new(bytes.Buffer)

	mw := multipart.NewWriter(body)
	part, err := mw.CreateFormFile("jfr", "jfr")
	if err != nil {
		return fmt.Errorf("failed to create form file: %w", err)
	}
	gw := gzip.NewWriter(part)
	if _, err := gw.Write(data); err != nil {
		return err
	}
	gw.Close()
	mw.Close()

	req, err := http.NewRequest("POST", addr, body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", mw.FormDataContentType())

	q := req.URL.Query()
	for k, v := range urlParams {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("failed to upload profile; http status code: %d", resp.StatusCode)
	}
	return nil
}

func TestPyroscopeIngestJfrCpu(t *testing.T) {
	tests := make([]jfrtest, 1)
	pb := loadTestData(t, "cortex-dev-01__kafka-0__cpu__0.pb")
	tests[0] = jfrtest{
		name: "send labeled multipart form data gzipped cpu jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":       "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
			"from":       "1700332322",
			"until":      "1700332329",
			"format":     "jfr",
			"sampleRate": "100",
		},
		jfr: filepath.Join("testdata", "cortex-dev-01__kafka-0__cpu__0.jfr"),
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
			},
			body: pb,
		}}),
	}
	addr, sink := startHttpServer(t)
	collectorAddr := fmt.Sprintf("http://%s%s", addr, ingestPath)
	run(t, tests, collectorAddr, sink)
}

func TestPyroscopeIngestJfrMemory(t *testing.T) {
	tests := make([]jfrtest, 1)
	pbAllocInNewTlab := loadTestData(t, "memory_example_alloc_in_new_tlab.pb")
	pbLiveObject := loadTestData(t, "memory_example_live_object.pb")
	tests[0] = jfrtest{
		name: "send labeled multipart form data gzipped memoty jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":   "com.example.App{dc=us-east-1,kubernetes_pod_name=app-abcd1234}",
			"from":   "1700332322",
			"until":  "1700332329",
			"format": "jfr",
		},
		jfr: filepath.Join("testdata", "memory_alloc_live_example.jfr"),
		expected: gen([]profileLog{{
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
				},
				body: pbLiveObject,
			},
		}),
	}

	addr, sink := startHttpServer(t)
	collectorAddr := fmt.Sprintf("http://%s%s", addr, ingestPath)
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
