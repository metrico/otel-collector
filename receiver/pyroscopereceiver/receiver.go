package pyroscopereceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/nodeparser"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/jfrparser"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/pprofparser"

	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/compress"
	profile_types "github.com/metrico/otel-collector/receiver/pyroscopereceiver/types"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	ingestPath = "/ingest"

	formatJfr   = "jfr"
	formatPprof = "profile"
	filePprof   = "profile.pprof"

	errorCodeError   = "1"
	errorCodeSuccess = ""

	keyService        = "service"
	keyStart   ctxkey = "start_time"
)

// avoids context key collision, need public getter/setter because should be propagated to other packages
type ctxkey string

type pyroscopeReceiver struct {
	cfg    *Config
	set    *receiver.CreateSettings
	logger *zap.Logger
	meter  metric.Meter
	next   consumer.Logs
	host   component.Host

	httpMux      *http.ServeMux
	decompressor *compress.Decompressor
	httpServer   *http.Server
	shutdownWg   sync.WaitGroup

	uncompressedBufPool *sync.Pool
}

type parser interface {
	// Parses the given input buffer into the collector's profile IR
	Parse(buf *bytes.Buffer, md profile_types.Metadata) ([]profile_types.ProfileIR, error)
}

type params struct {
	start  uint64
	end    uint64
	name   string
	labels labels.Labels
}

func newPyroscopeReceiver(cfg *Config, consumer consumer.Logs, set *receiver.CreateSettings) (*pyroscopeReceiver, error) {
	recv := &pyroscopeReceiver{
		cfg:                 cfg,
		set:                 set,
		logger:              set.Logger,
		meter:               set.MeterProvider.Meter(typeStr),
		next:                consumer,
		uncompressedBufPool: &sync.Pool{},
	}
	recv.decompressor = compress.NewDecompressor(recv.cfg.Protocols.Http.MaxRequestBodySize)
	recv.httpMux = http.NewServeMux()
	recv.httpMux.HandleFunc(ingestPath, func(resp http.ResponseWriter, req *http.Request) {
		recv.httpHandlerIngest(resp, req)
	})
	if err := initMetrics(recv.meter); err != nil {
		recv.logger.Error(fmt.Sprintf("failed to init metrics: %s", err.Error()))
		return recv, err
	}
	return recv, nil
}

// TODO: rate limit clients
func (recv *pyroscopeReceiver) httpHandlerIngest(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithTimeout(req.Context(), recv.cfg.Timeout)
	defer cancel()

	// all compute should be bounded by timeout, so dont add compute here

	select {
	case <-ctx.Done():
		recv.handleError(ctx, resp, "text/plain", http.StatusRequestTimeout, fmt.Sprintf("receiver timeout elapsed: %s", recv.cfg.Timeout), "", errorCodeError)
		return
	case <-recv.handle(ctx, resp, req):
	}
}

func (recv *pyroscopeReceiver) handle(ctx context.Context, resp http.ResponseWriter, req *http.Request) <-chan struct{} {
	c := make(chan struct{})
	go func() {
		// signal completion event
		defer func() { c <- struct{}{} }()

		qs := req.URL.Query()
		pm, err := readParams(&qs)
		if err != nil {
			recv.handleError(ctx, resp, "text/plain", http.StatusBadRequest, "bad url query", "", errorCodeError)
			return
		}

		if req.Method != http.MethodPost {
			recv.handleError(ctx, resp, "text/plain", http.StatusMethodNotAllowed, fmt.Sprintf("method not allowed, supported: [%s]", http.MethodPost), pm.name, errorCodeError)
			return
		}

		pl, err := recv.readProfiles(ctx, req, pm)
		if err != nil {
			recv.handleError(ctx, resp, "text/plain", http.StatusBadRequest, err.Error(), pm.name, errorCodeError)
			return
		}

		// if no profiles have been parsed, dont error but return
		if pl.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 0 {
			writeResponseNoContent(resp)
			return
		}

		// delegate to next consumer in the pipeline
		// TODO: support memorylimiter processor, apply retry policy on "oom" event, depends on https://github.com/open-telemetry/opentelemetry-collector/issues/9196
		err = recv.next.ConsumeLogs(ctx, pl)
		if err != nil {
			recv.handleError(ctx, resp, "text/plain", http.StatusInternalServerError, err.Error(), pm.name, errorCodeError)
			return
		}

		otelcolReceiverPyroscopeHttpRequestTotal.Add(ctx, 1, metric.WithAttributeSet(*newOtelcolAttrSetHttp(pm.name, errorCodeSuccess, http.StatusNoContent)))
		writeResponseNoContent(resp)
	}()
	return c
}

func (recv *pyroscopeReceiver) handleError(ctx context.Context, resp http.ResponseWriter, contentType string, statusCode int, msg string, service string, errorCode string) {
	otelcolReceiverPyroscopeHttpRequestTotal.Add(ctx, 1, metric.WithAttributeSet(*newOtelcolAttrSetHttp(service, errorCode, statusCode)))
	recv.logger.Error(msg)
	writeResponse(resp, "text/plain", statusCode, []byte(msg))
}

func readParams(qs *url.Values) (params, error) {
	var (
		tmp []string
		ok  bool
		qsv        = *qs
		p   params = params{}
	)

	tmp, ok = qsv["from"]
	if ok {
		start, err := strconv.ParseUint(tmp[0], 10, 64)
		if err != nil {
			return p, fmt.Errorf("failed to parse start time: %w", err)
		}
		p.start = start
	}
	if tmp, ok = qsv["name"]; !ok {
		return p, fmt.Errorf("required labels are missing")
	}
	i := strings.Index(tmp[0], "{")
	length := len(tmp[0])
	if i < 0 {
		i = length
	} else { // optional labels
		// TODO: improve this stupid {k=v(,k=v)*} compiler, checkout pyroscope's implementation
		promqllike := tmp[0][i+1 : length-1] // stripe {}
		if len(promqllike) > 0 {
			words := strings.FieldsFunc(promqllike, func(r rune) bool { return r == '=' || r == ',' })
			sz := len(words)
			if sz == 0 || sz%2 != 0 {
				return p, fmt.Errorf("failed to compile labels")
			}
			for j := 0; j < len(words); j += 2 {
				p.labels = append(p.labels, labels.Label{Name: words[j], Value: words[j+1]})
			}
		}
	}
	// required app name
	p.name = tmp[0][:i]
	tmp, ok = qsv["until"]
	if ok {
		end, err := strconv.ParseUint(tmp[0], 10, 64)
		if err != nil {
			return p, fmt.Errorf("failed to parse end time: %w", err)
		}
		p.end = end
	}

	return p, nil
}

func newOtelcolAttrSetHttp(service string, errorCode string, statusCode int) *attribute.Set {
	s := attribute.NewSet(
		attribute.KeyValue{Key: keyService, Value: attribute.StringValue(service)},
		attribute.KeyValue{Key: "error_code", Value: attribute.StringValue(errorCode)},
		attribute.KeyValue{Key: "status_code", Value: attribute.IntValue(statusCode)},
	)
	return &s
}

func acquireBuf(p *sync.Pool) *bytes.Buffer {
	v := p.Get()
	if v == nil {
		return new(bytes.Buffer)
	}
	return v.(*bytes.Buffer)
}

func releaseBuf(p *sync.Pool, buf *bytes.Buffer) {
	buf.Reset()
	p.Put(buf)
}

func (recv *pyroscopeReceiver) readProfiles(ctx context.Context, req *http.Request, pm params) (plog.Logs, error) {
	var (
		tmp []string
		ok  bool
		pa  parser
	)
	logs := plog.NewLogs()

	recv.logger.Debug("received profiles", zap.String("url_query", req.URL.RawQuery))
	qs := req.URL.Query()
	if tmp, ok = qs["format"]; ok && (tmp[0] == "jfr") {
		pa = jfrparser.NewJfrPprofParser()
	} else if tmp, ok = qs["spyName"]; ok && (tmp[0] == "nodespy") {
		pa = nodeparser.NewNodePprofParser()
	} else {
		pa = pprofparser.NewPprofParser()

	}
	// support only multipart/form-data
	f, err := recv.openMultipart(req)
	if err != nil {
		return logs, err
	}
	defer f.Close()

	buf := acquireBuf(recv.uncompressedBufPool)
	defer func() {
		releaseBuf(recv.uncompressedBufPool, buf)
	}()

	err = recv.decompressor.Decompress(f, compress.Gzip, buf)
	if err != nil {
		return logs, fmt.Errorf("failed to decompress body: %w", err)
	}
	// TODO: try measure compressed size
	otelcolReceiverPyroscopeRequestBodyUncompressedSizeBytes.Record(ctx, int64(buf.Len()), metric.WithAttributeSet(*newOtelcolAttrSetPayloadSizeBytes(pm.name, formatJfr, "")))
	resetHeaders(req)

	md := profile_types.Metadata{SampleRateHertz: 0}
	tmp, ok = qs["sampleRate"]
	if ok {
		hz, err := strconv.ParseUint(tmp[0], 10, 64)
		if err != nil {
			return logs, fmt.Errorf("failed to parse rate: %w", err)
		}
		md.SampleRateHertz = hz
	}

	ps, err := pa.Parse(buf, md)
	if err != nil {
		return logs, fmt.Errorf("failed to parse pprof: %w", err)
	}

	sz := 0
	rs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i, pr := range ps {
		var timestampNs uint64
		var durationNs uint64
		r := rs.AppendEmpty()
		if tmp, ok = qs["format"]; ok && (tmp[0] == "jfr") {
			timestampNs = ns(pm.start)
			durationNs = pm.end - pm.start
			durationNs = ns(durationNs)
		} else if tmp, ok = qs["spyName"]; ok && (tmp[0] == "nodespy") {
			timestampNs = uint64(pr.TimeStampNao)
			durationNs = uint64(pr.DurationNano)
		} else {
			timestampNs = pm.start
			durationNs = pm.end - pm.start
			durationNs = ns(durationNs)
		}
		r.SetTimestamp(pcommon.Timestamp(timestampNs))
		m := r.Attributes()
		m.PutStr("duration_ns", fmt.Sprint(durationNs))
		m.PutStr("service_name", pm.name)
		tm := m.PutEmptyMap("tags")
		for _, l := range pm.labels {
			tm.PutStr(l.Name, l.Value)
		}
		err = setAttrsFromProfile(pr, m)
		if err != nil {
			return logs, fmt.Errorf("failed to parse sample types: %v", err)
		}
		r.Body().SetEmptyBytes().FromRaw(pr.Payload.Bytes())
		sz += pr.Payload.Len()
		recv.logger.Debug(
			fmt.Sprintf("parsed profile %d", i),
			zap.Uint64("timestamp_ns", timestampNs),
			zap.String("type", pr.Type.Type),
			zap.String("service_name", pm.name),
			zap.String("period_type", pr.Type.PeriodType),
			zap.String("period_unit", pr.Type.PeriodUnit),
			zap.String("sample_types", strings.Join(pr.Type.SampleType, ",")),
			zap.String("sample_units", strings.Join(pr.Type.SampleUnit, ",")),
			zap.Uint8("payload_type", uint8(pr.PayloadType)),
		)
	}
	// sz may be 0 and it will be recorded
	otelcolReceiverPyroscopeParsedBodyUncompressedSizeBytes.Record(ctx, int64(sz), metric.WithAttributeSet(*newOtelcolAttrSetPayloadSizeBytes(pm.name, formatPprof, "")))
	return logs, nil
}

func ns(sec uint64) uint64 {
	return sec * 1e9
}

func newOtelcolAttrSetPayloadSizeBytes(service string, typ string, encoding string) *attribute.Set {
	s := attribute.NewSet(attribute.KeyValue{Key: keyService, Value: attribute.StringValue(service)}, attribute.KeyValue{Key: "type", Value: attribute.StringValue(typ)}, attribute.KeyValue{Key: "encoding", Value: attribute.StringValue(encoding)})
	return &s
}

func (recv *pyroscopeReceiver) openMultipart(req *http.Request) (multipart.File, error) {
	if err := req.ParseMultipartForm(recv.cfg.Protocols.Http.MaxRequestBodySize); err != nil {
		return nil, fmt.Errorf("failed to parse multipart request: %w", err)
	}
	mf := req.MultipartForm
	defer func() {
		_ = mf.RemoveAll()
	}()
	formats := []string{formatJfr, formatPprof}
	var part []*multipart.FileHeader // Replace YourPartType with the actual type of your 'part' variable
	for _, f := range formats {
		if p, ok := mf.File[f]; ok {
			part = p
			break
		}
	}
	if part == nil {
		return nil, fmt.Errorf("required jfr/pprof/node part is missing")
	}
	fh := part[0]
	if fh.Filename != formatJfr && fh.Filename != filePprof && fh.Filename != formatPprof {
		return nil, fmt.Errorf("filename is not '%s or %s'", formatJfr, formatPprof)
	}
	f, err := fh.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open  file")
	}
	return f, nil
}

func resetHeaders(req *http.Request) {
	// reset content-type for the new binary jfr body
	req.Header.Set("Content-Type", "application/octet-stream")
	// multipart content-types cannot have encodings so no need to Del() Content-Encoding
	// reset "Content-Length" to -1 as the size of the uncompressed body is unknown
	req.Header.Del("Content-Length")
	req.ContentLength = -1
}

func stringToAnyArray(s []string) []any {
	res := make([]any, len(s))
	for i, v := range s {
		res[i] = v
	}
	return res
}
func entitiesToStrings(entities []profile_types.SampleType) []any {
	var result []any
	for _, entity := range entities {
		result = append(result,
			[]any{entity.Key, entity.Sum, entity.Count},
		)
	}
	return result
}

func setAttrsFromProfile(prof profile_types.ProfileIR, m pcommon.Map) error {

	m.PutStr("type", prof.Type.Type)

	s := m.PutEmptySlice("sample_types")
	err := s.FromRaw(stringToAnyArray(prof.Type.SampleType))
	if err != nil {
		return err
	}

	s = m.PutEmptySlice("sample_units")
	err = s.FromRaw(stringToAnyArray(prof.Type.SampleUnit))
	if err != nil {
		return err
	}

	// Correct type assertion for []profile.SampleType
	result := prof.ValueAggregation.([]profile_types.SampleType)
	s = m.PutEmptySlice("values_agg")
	err = s.FromRaw(entitiesToStrings(result))
	if err != nil {
		return err
	}

	m.PutStr("period_type", prof.Type.PeriodType)
	m.PutStr("period_unit", prof.Type.PeriodUnit)
	m.PutStr("payload_type", fmt.Sprint(prof.PayloadType))
	return nil
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

func writeResponseNoContent(w http.ResponseWriter) {
	writeResponse(w, "", http.StatusNoContent, nil)
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, payload []byte) {
	if len(contentType) > 0 {
		w.Header().Set("Content-Type", contentType)
	}
	w.WriteHeader(statusCode)
	if payload != nil {
		_, _ = w.Write(payload)
	}
}
