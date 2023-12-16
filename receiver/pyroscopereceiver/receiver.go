package pyroscopereceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/compress"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/jfrparser"
	profile_types "github.com/metrico/otel-collector/receiver/pyroscopereceiver/types"
	"github.com/prometheus/prometheus/model/labels"
	promql_parser "github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

const (
	ingestPath = "/ingest"
	formatJfr  = "jfr"
)

type pyroscopeReceiver struct {
	cfg    *Config
	next   consumer.Logs
	set    *receiver.CreateSettings
	logger *zap.Logger
	host   component.Host

	httpMux      *http.ServeMux
	decompressor *compress.Decompressor
	httpServer   *http.Server
	shutdownWg   sync.WaitGroup
}

type parser interface {
	// Parses the given input buffer into the collector's profile IR
	Parse(buf *bytes.Buffer, md profile_types.Metadata, maxDecompressedSizeBytes int64) ([]profile_types.ProfileIR, error)
}

type attrs struct {
	start  uint64
	end    uint64
	name   string
	labels labels.Labels
}

func newPyroscopeReceiver(cfg *Config, consumer consumer.Logs, set *receiver.CreateSettings) *pyroscopeReceiver {
	recv := &pyroscopeReceiver{
		cfg:    cfg,
		next:   consumer,
		set:    set,
		logger: set.Logger,
	}
	recv.decompressor = compress.NewDecompressor(recv.cfg.Protocols.Http.MaxRequestBodySize)
	recv.httpMux = http.NewServeMux()
	recv.httpMux.HandleFunc(ingestPath, func(resp http.ResponseWriter, req *http.Request) {
		handleIngest(resp, req, recv)
	})
	return recv
}

func parse(req *http.Request, recv *pyroscopeReceiver) (plog.Logs, error) {
	var (
		tmp []string
		ok  bool
		pa  parser
	)
	logs := plog.NewLogs()

	params := req.URL.Query()
	if tmp, ok = params["format"]; ok && tmp[0] == "jfr" {
		pa = jfrparser.NewJfrPprofParser()
	} else {
		return logs, fmt.Errorf("unsupported format, supported: [jfr]")
	}

	att, err := getAttrsFromParams(&params)
	if err != nil {
		return logs, err
	}

	// support only multipart/form-data
	f, err := recv.openMultipartJfr(req)
	if err != nil {
		return logs, err
	}
	defer f.Close()

	buf, err := recv.decompressor.Decompress(f, compress.Gzip)
	if err != nil {
		return logs, fmt.Errorf("failed to decompress body: %w", err)
	}
	resetHeaders(req)

	md := profile_types.Metadata{SampleRateHertz: 0}
	tmp, ok = params["sampleRate"]
	if ok {
		hz, err := strconv.ParseUint(tmp[0], 10, 64)
		if err != nil {
			return logs, fmt.Errorf("failed to parse rate: %w", err)
		}
		md.SampleRateHertz = hz
	}

	ps, err := pa.Parse(buf, md, recv.cfg.Protocols.Http.MaxRequestBodySize)
	if err != nil {
		return logs, fmt.Errorf("failed to parse pprof: %w", err)
	}

	rs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, pr := range ps {
		r := rs.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(att.start))
		m := r.Attributes()
		m.PutStr("duration_ns", fmt.Sprint((att.end-att.start)*1e9))
		m.PutStr("service_name", att.name)
		tm := m.PutEmptyMap("tags")
		for _, l := range att.labels {
			tm.PutStr(l.Name, l.Value)
		}
		setAttrsFromProfile(pr, m)
		r.Body().SetEmptyBytes().FromRaw(pr.Payload.Bytes())
	}
	return logs, nil
}

func (recv *pyroscopeReceiver) openMultipartJfr(req *http.Request) (multipart.File, error) {
	if err := req.ParseMultipartForm(recv.cfg.Protocols.Http.MaxRequestBodySize); err != nil {
		return nil, fmt.Errorf("failed to parse multipart request: %w", err)
	}
	mf := req.MultipartForm
	defer func() {
		_ = mf.RemoveAll()
	}()

	part, ok := mf.File[formatJfr]
	if !ok {
		return nil, fmt.Errorf("required jfr part is missing")
	}
	fh := part[0]
	if fh.Filename != formatJfr {
		return nil, fmt.Errorf("jfr filename is not '%s'", formatJfr)
	}
	f, err := fh.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open jfr file")
	}
	return f, nil
}

func resetHeaders(req *http.Request) {
	// reset content-type for the new binary jfr body
	req.Header.Set("Content-Type", "application/octet-stream")
	// multipart content-types cannot have encodings so no need to Del() Content-Encoding
	// reset "Content-Length" to -1 as the size of the decompressed body is unknown
	req.Header.Del("Content-Length")
	req.ContentLength = -1
}

func getAttrsFromParams(params *url.Values) (attrs, error) {
	var (
		tmp []string
		ok  bool
		pv        = *params
		att attrs = attrs{}
	)

	if tmp, ok = pv["from"]; !ok {
		return att, fmt.Errorf("required start time is missing")
	}
	start, err := strconv.ParseUint(tmp[0], 10, 64)
	if err != nil {
		return att, fmt.Errorf("failed to parse start time: %w", err)
	}
	att.start = start

	if tmp, ok = pv["name"]; !ok {
		return att, fmt.Errorf("required labels are missing")
	}
	i := strings.Index(tmp[0], "{")
	length := len(tmp[0])
	if i < 0 {
		i = length
	} else { // optional labels
		promql := tmp[0][i:length]
		labels, err := promql_parser.ParseMetric(promql)
		if err != nil {
			return att, fmt.Errorf("failed to parse labels: %w", err)
		}
		att.labels = labels
	}
	// required app name
	att.name = tmp[0][:i]

	if tmp, ok = pv["until"]; !ok {
		return att, fmt.Errorf("required end time is missing")
	}
	end, err := strconv.ParseUint(tmp[0], 10, 64)
	if err != nil {
		return att, fmt.Errorf("failed to parse end time: %w", err)
	}
	att.end = end
	return att, nil
}

func setAttrsFromProfile(prof profile_types.ProfileIR, m pcommon.Map) {
	m.PutStr("type", prof.Type.Type)
	m.PutStr("period_type", prof.Type.PeriodType)
	m.PutStr("period_unit", prof.Type.PeriodUnit)
	m.PutStr("payload_type", fmt.Sprint(prof.PayloadType))
}

func handleIngest(resp http.ResponseWriter, req *http.Request, recv *pyroscopeReceiver) {
	if req.Method != http.MethodPost {
		msg := fmt.Sprintf("method not allowed, supported: [%s]", http.MethodPost)
		recv.logger.Error(msg)
		writeResponse(resp, "text/plain", http.StatusMethodNotAllowed, []byte(msg))
		return
	}

	ctx := req.Context()
	logs, err := parse(req, recv)
	if err != nil {
		msg := err.Error()
		recv.logger.Error(msg)
		writeResponse(resp, "text/plain", http.StatusBadRequest, []byte(msg))
		return
	}

	// delegate to next consumer in the pipeline
	// TODO: support memorylimiter processor, apply retry policy on "oom", and consider to shift right allocs from the receiver
	err = recv.next.ConsumeLogs(ctx, logs)
	if err != nil {
		msg := err.Error()
		recv.logger.Error(msg)
		writeResponse(resp, "text/plain", http.StatusInternalServerError, []byte(msg))
		return
	}
}

// Starts a http server that receives profiles of supported protocols
func (recv *pyroscopeReceiver) Start(_ context.Context, host component.Host) error {
	recv.host = host
	var err error

	// applies an interceptor that enforces the configured request body limit
	if recv.httpServer, err = recv.cfg.Protocols.Http.ToServer(host, recv.set.TelemetrySettings, recv.httpMux); err != nil {
		return fmt.Errorf("failed to create http server: %w", err)
	}

	recv.logger.Info("server listening on", zap.String("endpoint", recv.cfg.Protocols.Http.Endpoint))
	var l net.Listener
	if l, err = recv.cfg.Protocols.Http.ToListener(); err != nil {
		return fmt.Errorf("failed to create tcp listener: %w", err)
	}

	// TODO: rate limit clients and add timeout
	recv.shutdownWg.Add(1)
	go func() {
		defer recv.shutdownWg.Done()
		if err := recv.httpServer.Serve(l); !errors.Is(err, http.ErrServerClosed) && err != nil {
			host.ReportFatalError(err)
		}
	}()
	return nil
}

// Shuts down the receiver, by shutting down the server
func (recv *pyroscopeReceiver) Shutdown(ctx context.Context) error {
	if err := recv.httpServer.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown: %w", err)
	}
	recv.shutdownWg.Wait()
	return nil
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	_, _ = w.Write(msg)
}
