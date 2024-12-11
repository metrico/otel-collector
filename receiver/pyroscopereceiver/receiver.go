package pyroscopereceiver

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"connectrpc.com/connect"
	mux "github.com/gorilla/mux"
	pushv1 "github.com/grafana/pyroscope/api/gen/proto/go/push/v1"
	pushv1connect "github.com/grafana/pyroscope/api/gen/proto/go/push/v1/pushv1connect"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/compress"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/jfrparser"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/nodeparser"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/pprofparser"
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

var (
	gzipReaderPool = sync.Pool{
		New: func() any {
			return &gzipReader{
				reader: bytes.NewReader(nil),
			}
		},
	}
	gzipWriterPool = sync.Pool{
		New: func() any {
			return gzip.NewWriter(io.Discard)
		},
	}
	bufPool = sync.Pool{
		New: func() any {
			return bytes.NewBuffer(nil)
		},
	}
)

// avoids context key collision, need public getter/setter because should be propagated to other packages
type ctxkey string

type pyroscopeReceiver struct {
	cfg     *Config
	setting *receiver.Settings
	logger  *zap.Logger
	meter   metric.Meter
	next    consumer.Logs
	host    component.Host

	mux          *mux.Router
	decompressor *compress.Decompressor
	httpServer   *http.Server
	shutdownWg   sync.WaitGroup

	uncompressedBufPool *sync.Pool

	pushv1connect.UnimplementedPusherServiceHandler
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

func newPyroscopeReceiver(cfg *Config, consumer consumer.Logs, set *receiver.Settings) (*pyroscopeReceiver, error) {
	r := &pyroscopeReceiver{
		cfg:                 cfg,
		setting:             set,
		logger:              set.Logger,
		meter:               set.MeterProvider.Meter(typeStr),
		next:                consumer,
		uncompressedBufPool: &sync.Pool{},
	}
	r.decompressor = compress.NewDecompressor(r.cfg.Protocols.HTTP.MaxRequestBodySize)
	r.mux = mux.NewRouter()
	r.mux.HandleFunc(ingestPath, func(resp http.ResponseWriter, req *http.Request) {
		r.httpHandlerIngest(resp, req)
	})
	if err := initMetrics(r.meter); err != nil {
		r.logger.Error(fmt.Sprintf("failed to init metrics: %s", err.Error()))
		return r, err
	}
	return r, nil
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

func (r *pyroscopeReceiver) handle(ctx context.Context, resp http.ResponseWriter, req *http.Request) <-chan struct{} {
	c := make(chan struct{})
	go func() {
		// signal completion event
		defer func() { c <- struct{}{} }()

		qs := req.URL.Query()
		pm, err := readParams(&qs)
		if err != nil {
			r.handleError(ctx, resp, "text/plain", http.StatusBadRequest, "bad url query", "", errorCodeError)
			return
		}

		if req.Method != http.MethodPost {
			r.handleError(ctx, resp, "text/plain", http.StatusMethodNotAllowed, fmt.Sprintf("method not allowed, supported: [%s]", http.MethodPost), pm.name, errorCodeError)
			return
		}

		pl, err := r.readProfiles(ctx, req, pm)
		if err != nil {
			r.handleError(ctx, resp, "text/plain", http.StatusBadRequest, err.Error(), pm.name, errorCodeError)
			return
		}

		// if no profiles have been parsed, dont error but return
		if pl.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 0 {
			writeResponseNoContent(resp)
			return
		}

		// delegate to next consumer in the pipeline
		// TODO: support memorylimiter processor, apply retry policy on "oom" event, depends on https://github.com/open-telemetry/opentelemetry-collector/issues/9196
		err = r.next.ConsumeLogs(ctx, pl)
		if err != nil {
			r.handleError(ctx, resp, "text/plain", http.StatusInternalServerError, err.Error(), pm.name, errorCodeError)
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

func (r *pyroscopeReceiver) Push(ctx context.Context, req *connect.Request[pushv1.PushRequest]) (*connect.Response[pushv1.PushResponse], error) {
	logs := plog.NewLogs()
	sz := 0
	rs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for _, serie := range req.Msg.Series {
		serviceName := ""
		// find serviceName from label
		for _, label := range serie.Labels {
			if label.Name == "service_name" {
				serviceName = label.Value
				break
			}
		}
		for i, sample := range serie.Samples {
			err := fromBytes(sample.RawProfile, func(p *profile_types.ProfileIR) error {
				record := rs.AppendEmpty()
				record.SetTimestamp(pcommon.Timestamp(p.Profile.TimeNanos))
				m := record.Attributes()
				m.PutStr("duration_ns", fmt.Sprint(p.DurationNano))
				m.PutStr("service_name", serviceName)
				tm := m.PutEmptyMap("tags")
				for _, l := range serie.Labels {
					tm.PutStr(l.Name, l.Value)
				}
				err := setAttrsFromProfile(*p, m)
				if err != nil {
					return fmt.Errorf("failed to parse sample types: %v", err)
				}
				postProcessProf(p.Profile, &m)
				record.Body().SetEmptyBytes().FromRaw(sample.RawProfile)
				sz += p.Payload.Len()
				r.logger.Debug(
					fmt.Sprintf("parsed profile %d", i),
					zap.Int64("timestamp_ns", p.Profile.DurationNanos),
					zap.String("type", p.Type.Type),
					zap.String("service_name", serviceName),
					zap.String("period_type", p.Type.PeriodType),
					zap.String("period_unit", p.Type.PeriodUnit),
					zap.String("sample_types", strings.Join(p.Type.SampleType, ",")),
					zap.String("sample_units", strings.Join(p.Type.SampleUnit, ",")),
					zap.Uint8("payload_type", uint8(p.PayloadType)),
				)
				return nil
			})
			if err != nil {
				return nil, err
			}
		}
	}

	// if no profiles have been parsed, dont error but return
	if logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().Len() == 0 {
		return nil, errors.New("no profiles have been parsed")
	}
	err := r.next.ConsumeLogs(ctx, logs)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pushv1.PushResponse{}), nil
}

func (r *pyroscopeReceiver) getProfilesBuff(req *http.Request) (*bytes.Buffer, error) {
	var err error
	var buf *bytes.Buffer
	defer func() {
		if err != nil && buf != nil {
			releaseBuf(r.uncompressedBufPool, buf)
		}
	}()
	contentType := ""
	if len(req.Header["Content-Type"]) > 0 {
		contentType = req.Header["Content-Type"][0]
	}
	if strings.HasPrefix(contentType, "multipart/form-data") {
		var f multipart.File
		f, err = r.openMultipart(req)
		if err != nil {
			fmt.Println(req.URL.String())
			for k, v := range req.Header {
				fmt.Printf("Header: %s: %v", k, v)
			}
			b, _ := io.ReadAll(req.Body)
			//TODO: encode b to hex
			fmt.Printf("Body: %s\n", hex.EncodeToString(b))
			return nil, err
		}
		defer f.Close()

		buf = acquireBuf(r.uncompressedBufPool)
		err = r.decompressor.Decompress(f, compress.Gzip, buf)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress body: %w", err)
		}
		return buf, nil
	}
	if strings.HasPrefix(contentType, "binary/octet-stream") {
		buf = acquireBuf(r.uncompressedBufPool)
		_, err = io.Copy(buf, req.Body)
		if err != nil {
			return buf, fmt.Errorf("failed to read body: %w", err)
		}
		return buf, nil
	}
	return nil, fmt.Errorf("unsupported content type: %s", contentType)
}

func (r *pyroscopeReceiver) readProfiles(ctx context.Context, req *http.Request, pm params) (plog.Logs, error) {
	var (
		tmp []string
		ok  bool
		p   parser
	)
	logs := plog.NewLogs()

	r.logger.Debug("received profiles", zap.String("url_query", req.URL.RawQuery))
	qs := req.URL.Query()
	if tmp, ok = qs["format"]; ok && (tmp[0] == "jfr") {
		p = jfrparser.NewJfrPprofParser()
	} else if tmp, ok = qs["spyName"]; ok && (tmp[0] == "nodespy") {
		p = nodeparser.NewNodePprofParser()
	} else {
		p = pprofparser.NewPprofParser()
	}
	// support only multipart/form-data
	buf, err := r.getProfilesBuff(req)
	if err != nil {
		return logs, err
	}
	defer func() {
		releaseBuf(r.uncompressedBufPool, buf)
	}()

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

	ps, err := p.Parse(buf, md)
	if err != nil {
		return logs, fmt.Errorf("failed to parse pprof: %w", err)
	}

	sz := 0
	rs := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i, pr := range ps {
		var timestampNs uint64
		var durationNs uint64
		record := rs.AppendEmpty()
		if tmp, ok = qs["format"]; ok && (tmp[0] == "jfr") {
			timestampNs = ns(pm.start)
			durationNs = ns(pm.end) - ns(pm.start)
		} else if tmp, ok = qs["spyName"]; ok && (tmp[0] == "nodespy") {
			timestampNs = uint64(pr.TimeStampNao)
			durationNs = uint64(pr.DurationNano)
		} else {
			timestampNs = ns(pm.start)
			durationNs = ns(pm.end) - ns(pm.start)
		}
		record.SetTimestamp(pcommon.Timestamp(timestampNs))
		m := record.Attributes()
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
		postProcessProf(pr.Profile, &m)
		record.Body().SetEmptyBytes().FromRaw(pr.Payload.Bytes())
		sz += pr.Payload.Len()
		r.logger.Info(
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
	if sec < 10000000000000 {
		return sec * 1e9
	}
	if sec < 10000000000000000 {
		return sec * 1e6
	}
	if sec < 10000000000000000000 {
		return sec * 1e3
	}
	return sec
}

func newOtelcolAttrSetPayloadSizeBytes(service string, typ string, encoding string) *attribute.Set {
	s := attribute.NewSet(attribute.KeyValue{Key: keyService, Value: attribute.StringValue(service)}, attribute.KeyValue{Key: "type", Value: attribute.StringValue(typ)}, attribute.KeyValue{Key: "encoding", Value: attribute.StringValue(encoding)})
	return &s
}

func (r *pyroscopeReceiver) openMultipart(req *http.Request) (multipart.File, error) {
	if err := req.ParseMultipartForm(r.cfg.Protocols.HTTP.MaxRequestBodySize); err != nil {
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
func (r *pyroscopeReceiver) Start(ctx context.Context, host component.Host) error {
	r.host = host
	var err error

	if r.cfg.Protocols.HTTP.Endpoint != "" {
		// applies an interceptor that enforces the configured request body limit
		if r.httpServer, err = r.cfg.Protocols.HTTP.ToServer(ctx, host, r.setting.TelemetrySettings, r.mux); err != nil {
			return fmt.Errorf("failed to create http server: %w", err)
		}

		var l net.Listener
		if l, err = r.cfg.Protocols.HTTP.ToListener(ctx); err != nil {
			return fmt.Errorf("failed to create tcp listener: %w", err)
		}
		pushv1connect.RegisterPusherServiceHandler(r.mux, r)
		r.shutdownWg.Add(1)
		r.setting.Logger.Info("Starting HTTP server", zap.String("endpoint", r.cfg.Protocols.HTTP.Endpoint))
		go func() {
			defer r.shutdownWg.Done()
			if err := r.httpServer.Serve(l); !errors.Is(err, http.ErrServerClosed) && err != nil {
				log.Fatalf("HTTP server failed: %v", err)
			}
		}()
	}

	return nil
}

// Shuts down the receiver, by shutting down the server
func (r *pyroscopeReceiver) Shutdown(ctx context.Context) error {
	if r.httpServer != nil {
		if err := r.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown: %w", err)
		}
	}
	r.shutdownWg.Wait()
	return nil
}

func writeResponseNoContent(w http.ResponseWriter) {
	writeResponse(w, "", http.StatusOK, nil)
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

type gzipReader struct {
	gzip   *gzip.Reader
	reader *bytes.Reader
}

// open gzip, create reader if required
func (r *gzipReader) gzipOpen() error {
	var err error
	if r.gzip == nil {
		r.gzip, err = gzip.NewReader(r.reader)
	} else {
		err = r.gzip.Reset(r.reader)
	}
	return err
}

func (r *gzipReader) openBytes(input []byte) (io.Reader, error) {
	r.reader.Reset(input)

	// handle if data is not gzipped at all
	if err := r.gzipOpen(); err == gzip.ErrHeader {
		r.reader.Reset(input)
		return r.reader, nil
	} else if err != nil {
		return nil, err
	}

	return r.gzip, nil
}

func fromBytes(input []byte, fn func(*profile_types.ProfileIR) error) error {
	p, err := rawFromBytes(input)
	if err != nil {
		return err
	}
	return fn(p)
}

func rawFromBytes(input []byte) (*profile_types.ProfileIR, error) {
	gzipReader := gzipReaderPool.Get().(*gzipReader)
	buf := bufPool.Get().(*bytes.Buffer)
	defer func() {
		gzipReaderPool.Put(gzipReader)
		buf.Reset()
		bufPool.Put(buf)
	}()

	r, err := gzipReader.openBytes(input)
	if err != nil {
		return nil, err
	}

	if _, err = io.Copy(buf, r); err != nil {
		return nil, fmt.Errorf("copy to buffer: %w", err)
	}
	pbp, err := pprofparser.NewPprofParser().Parse(buf, profile_types.Metadata{})
	if err != nil {
		return nil, fmt.Errorf("failed to parse profile: %w", err)
	}

	return &pbp[0], nil
}
