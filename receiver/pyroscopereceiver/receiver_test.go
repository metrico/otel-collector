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
)

func startHttpServer(t *testing.T) (string, *consumertest.LogsSink) {
	addr := getAvailableLocalTcpPort(t)
	config := &Config{
		Protocols: Protocols{
			Http: &confighttp.HTTPServerSettings{
				Endpoint:           addr,
				MaxRequestBodySize: defaultMaxRequestBodySize,
			},
		},
	}
	sink := new(consumertest.LogsSink)
	sett := receivertest.NewNopCreateSettings()
	recv := newPyroscopeReceiver(config, sink, &sett)

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

func TestPyroscopeIngest(t *testing.T) {
	type test_t struct {
		name      string
		urlParams map[string]string
		jfr       string
		expected  *plog.Logs
		err       error
	}
	tests := make([]test_t, 2)

	jfr := filepath.Join("testdata", "profile.jfr")
	data, err := os.ReadFile(jfr)
	assert.NoError(t, err, "failed to load expected jfr")

	tests[0] = test_t{
		name: "send labeled multipart form data gzipped jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":   "com.example.App{dc=\"us-east-1\",kubernetes_pod_name=\"app-abcd1234\"}",
			"from":   "1700332322",
			"until":  "1700332329",
			"format": "jfr",
		},
		jfr: jfr,
		expected: gen(&profile_t{
			timestamp: 1700332329,
			attrs: map[string]any{
				"__name__":            "com.example.App",
				"dc":                  "us-east-1",
				"kubernetes_pod_name": "app-abcd1234",
				"start_time":          "1700332322",
			},
			body: &data,
		}),
		err: nil,
	}
	tests[1] = test_t{
		name: "send not labeled multipart form data gzipped jfr to http ingest endpoint",
		urlParams: map[string]string{
			"name":   "com.example.App",
			"from":   "1700332332",
			"until":  "1700332339",
			"format": "jfr",
		},
		jfr: jfr,
		expected: gen(&profile_t{
			timestamp: 1700332339,
			attrs: map[string]any{
				"__name__":   "com.example.App",
				"start_time": "1700332332",
			},
			body: &data,
		}),
		err: nil,
	}

	addr, sink := startHttpServer(t)
	collectorAddr := fmt.Sprintf("http://%s%s", addr, ingestPath)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, send(t, collectorAddr, tt.urlParams, tt.jfr), "send shouldn't have been failed")
			actual := sink.AllLogs()
			assert.NoError(t, plogtest.CompareLogs(*tt.expected, actual[0]))
			sink.Reset()
		})
	}
}

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

type profile_t struct {
	timestamp int64
	body      *[]byte
	attrs     map[string]any
}

func gen(in *profile_t) *plog.Logs {
	profiles := plog.NewLogs()
	s := profiles.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	rec := s.AppendEmpty()
	_ = rec.Attributes().FromRaw(in.attrs)
	rec.SetTimestamp(pcommon.Timestamp(in.timestamp))
	rec.Body().SetEmptyBytes().FromRaw(*in.body)
	return &profiles
}
