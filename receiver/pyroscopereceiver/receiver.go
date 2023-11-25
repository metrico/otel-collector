package pyroscopereceiver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

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

	expectedDataSizeBytes = 15e3

	nameLabel = "__name__"
)

type pyroscopeReceiver struct {
	conf     *Config
	next     consumer.Logs
	settings *receiver.CreateSettings
	host     component.Host

	httpMux      *http.ServeMux
	decompressor *decompressor
	httpServer   *http.Server
	shutdownWg   sync.WaitGroup
}

func newPyroscopeReceiver(baseCfg *Config, consumer consumer.Logs, params *receiver.CreateSettings) *pyroscopeReceiver {
	recv := &pyroscopeReceiver{
		conf:     baseCfg,
		next:     consumer,
		settings: params,
	}
	recv.decompressor = newDecompressor(recv.conf)
	recv.httpMux = http.NewServeMux()
	recv.httpMux.HandleFunc(ingestPath, func(resp http.ResponseWriter, req *http.Request) {
		handleIngest(resp, req, recv)
	})
	return recv
}

func setAttrs(params *url.Values, rec *plog.LogRecord) error {
	var (
		tmp     []string
		ok      bool
		vparams = *params
	)

	if tmp, ok = vparams["until"]; !ok {
		return fmt.Errorf("required end time is missing")
	}
	end, err := strconv.ParseInt(tmp[0], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse end time: %w", err)
	}
	rec.SetTimestamp(pcommon.Timestamp(end))

	if tmp, ok = vparams["name"]; !ok {
		return fmt.Errorf("required labels are missing")
	}
	i := strings.Index(tmp[0], "{")
	length := len(tmp[0])
	if i < 0 {
		i = length
	} else { // optional labels
		promql := tmp[0][i:length]
		labels, err := promql_parser.ParseMetric(promql)
		if err != nil {
			return fmt.Errorf("failed to parse labels: %w", err)
		}
		for _, l := range labels {
			rec.Attributes().PutStr(l.Name, l.Value)
		}
	}
	// required app name
	name := tmp[0][:i]
	rec.Attributes().PutStr(nameLabel, name)

	if tmp, ok = vparams["from"]; !ok {
		return fmt.Errorf("required end time is missing")
	}
	rec.Attributes().PutStr("start_time", tmp[0])
	return nil
}

func (recv *pyroscopeReceiver) readBytes(r io.ReadCloser) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	// small extra space to try avoid realloc where expected size fits enough and +1 like limit
	buf.Grow(expectedDataSizeBytes + bytes.MinRead + 1)

	// read max+1 to validate size via a single Read()
	lr := io.LimitReader(r, recv.conf.Protocols.Http.MaxRequestBodySize+1)

	// doesnt return EOF
	n, err := buf.ReadFrom(lr)
	if err != nil {
		return nil, err
	}
	if n < 1 {
		return nil, fmt.Errorf("empty profile")
	}
	if n > recv.conf.Protocols.Http.MaxRequestBodySize {
		return nil, fmt.Errorf("body size exceeds the limit %d bytes", recv.conf.Protocols.Http.MaxRequestBodySize)
	}
	return &buf, nil
}

func parse(req *http.Request, recv *pyroscopeReceiver) (*plog.Logs, error) {
	logs := plog.NewLogs()
	rec := logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()

	params := req.URL.Query()
	if err := setAttrs(&params, &rec); err != nil {
		return &logs, err
	}

	// support jfr only
	if tmp, ok := params["format"]; !ok || tmp[0] != "jfr" {
		return &logs, fmt.Errorf("unsupported format, supported: [jfr]")
	}

	buf, err := recv.readBytes(req.Body)
	if err != nil {
		return &logs, fmt.Errorf("failed to read body: %w", err)
	}
	// TODO: avoid realloc of []byte in buf.Bytes()
	rec.Body().SetEmptyBytes().FromRaw(buf.Bytes())

	return &logs, nil
}

func handleIngest(resp http.ResponseWriter, req *http.Request, recv *pyroscopeReceiver) {
	if req.Method != http.MethodPost {
		msg := fmt.Sprintf("method not allowed, supported: [%s]", http.MethodPost)
		log.Printf("%s\n", msg)
		writeResponse(resp, "text/plain", http.StatusMethodNotAllowed, []byte(msg))
		return
	}

	ctx := req.Context()
	err := recv.decompressor.decompress(req)
	if err != nil {
		msg := err.Error()
		log.Printf("%s\n", msg)
		writeResponse(resp, "text/plain", http.StatusBadRequest, []byte(msg))
		return
	}
	logs, err := parse(req, recv)
	if err != nil {
		msg := err.Error()
		log.Printf("%s\n", msg)
		writeResponse(resp, "text/plain", http.StatusBadRequest, []byte(msg))
		return
	}

	// delegate to next consumer in the pipeline
	recv.next.ConsumeLogs(ctx, *logs)
}

func (recv *pyroscopeReceiver) Start(_ context.Context, host component.Host) error {
	recv.host = host
	var err error

	// TODO: rm redundant interceptors applied by ToServer() like decompressor
	if recv.httpServer, err = recv.conf.Protocols.Http.ToServer(host, recv.settings.TelemetrySettings, recv.httpMux); err != nil {
		return fmt.Errorf("failed to create http server: %w", err)
	}

	recv.settings.Logger.Info("server listening on", zap.String("endpoint", recv.conf.Protocols.Http.Endpoint))
	var listener net.Listener
	if listener, err = recv.conf.Protocols.Http.ToListener(); err != nil {
		return fmt.Errorf("failed to create tcp listener: %w", err)
	}

	// TODO: rate limit clients
	recv.shutdownWg.Add(1)
	go func() {
		defer recv.shutdownWg.Done()
		if err := recv.httpServer.Serve(listener); !errors.Is(err, http.ErrServerClosed) && err != nil {
			host.ReportFatalError(err)
		}
	}()
	return nil
}

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
