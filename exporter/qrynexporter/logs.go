package qrynexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-logfmt/logfmt"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const (
	hintAttributes = "loki.attribute.labels"
	hintResources  = "loki.resource.labels"
	hintTenant     = "loki.tenant"
	hintFormat     = "loki.format"

	levelAttributeName = "level"

	formatJSON   string = "json"
	formatLogfmt string = "logfmt"
)

type logsExporter struct {
	logger *zap.Logger

	db clickhouse.Conn
}

func newLogsExporter(logger *zap.Logger, cfg *Config) (*logsExporter, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &logsExporter{
		logger: logger,
		db:     db,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *logsExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		e.db.Close()
	}
	return nil
}

func hasLokiHint(attrs pcommon.Map) bool {
	for _, hit := range []string{hintAttributes, hintResources, hintTenant, hintTenant} {
		if _, ok := attrs.Get(hit); ok {
			return true
		}
	}
	return false
}

func convertAttributesAndMerge(logAttrs pcommon.Map, resAttrs pcommon.Map) model.LabelSet {
	out := model.LabelSet{}
	if resourcesToLabel, found := resAttrs.Get(hintResources); found {
		labels := convertSelectedAttributesToLabels(resAttrs, resourcesToLabel)
		out = out.Merge(labels)
	}

	// get the hint from the log attributes, not from the resource
	// the value can be a single resource name to use as label
	// or a slice of string values
	if resourcesToLabel, found := logAttrs.Get(hintResources); found {
		labels := convertSelectedAttributesToLabels(resAttrs, resourcesToLabel)
		out = out.Merge(labels)
	}

	if attributesToLabel, found := logAttrs.Get(hintAttributes); found {
		labels := convertSelectedAttributesToLabels(logAttrs, attributesToLabel)
		out = out.Merge(labels)
	}

	// get tenant hint from resource attributes, fallback to record attributes
	// if it is not found
	if resourcesToLabel, found := resAttrs.Get(hintTenant); !found {
		if attributesToLabel, found := logAttrs.Get(hintTenant); found {
			labels := convertSelectedAttributesToLabels(logAttrs, attributesToLabel)
			out = out.Merge(labels)
		}
	} else {
		labels := convertSelectedAttributesToLabels(resAttrs, resourcesToLabel)
		out = out.Merge(labels)
	}

	// if no any hit found, just mergedLabels
	if !hasLokiHint(logAttrs) && !hasLokiHint(resAttrs) {
		out = out.Merge(convertAttributesToLabels(logAttrs))
		out = out.Merge(convertAttributesToLabels(resAttrs))
	}
	return out
}

func parseAttributeNames(attrsToSelect pcommon.Value) []string {
	var out []string

	switch attrsToSelect.Type() {
	case pcommon.ValueTypeStr:
		out = strings.Split(attrsToSelect.AsString(), ",")
	case pcommon.ValueTypeSlice:
		as := attrsToSelect.Slice().AsRaw()
		for _, a := range as {
			out = append(out, fmt.Sprintf("%v", a))
		}
	default:
		// trying to make the most of bad data
		out = append(out, attrsToSelect.AsString())
	}

	return out
}

func convertSelectedAttributesToLabels(attributes pcommon.Map, attrsToSelect pcommon.Value) model.LabelSet {
	out := model.LabelSet{}

	attrs := parseAttributeNames(attrsToSelect)
	for _, attr := range attrs {
		attr = strings.TrimSpace(attr)
		av, ok := attributes.Get(attr) // do we need to trim this?
		if ok {
			out[model.LabelName(attr)] = model.LabelValue(av.AsString())
		}
	}

	return out
}

func getFormatFromFormatHint(logAttr pcommon.Map, resourceAttr pcommon.Map) string {
	format := formatJSON
	formatVal, found := resourceAttr.Get(hintFormat)
	if !found {
		formatVal, found = logAttr.Get(hintFormat)
	}

	if found {
		format = formatVal.AsString()
	}
	return format
}

func removeAttributes(attrs pcommon.Map, labels model.LabelSet) {
	attrs.RemoveIf(func(s string, _ pcommon.Value) bool {
		if s == hintAttributes || s == hintResources || s == hintTenant || s == hintFormat {
			return true
		}
		_, exists := labels[model.LabelName(s)]
		return exists
	})
}

func timestampFromLogRecord(lr plog.LogRecord) time.Time {
	if lr.Timestamp() != 0 {
		return lr.Timestamp().AsTime()
	}
	if lr.ObservedTimestamp() != 0 {
		return lr.ObservedTimestamp().AsTime()
	}
	return time.Now()
}

// if given key:value pair already exists in keyvals, replace value. Otherwise append
func keyvalsReplaceOrAppend(keyvals []interface{}, key string, value interface{}) []interface{} {
	for i := 0; i < len(keyvals); i += 2 {
		if keyvals[i] == key {
			keyvals[i+1] = value
			return keyvals
		}
	}
	return append(keyvals, key, value)
}

func parseLogfmtLine(line string) (*[]interface{}, error) {
	var keyvals []interface{}
	decoder := logfmt.NewDecoder(strings.NewReader(line))
	decoder.ScanRecord()
	for decoder.ScanKeyval() {
		keyvals = append(keyvals, decoder.Key(), decoder.Value())
	}

	err := decoder.Err()
	if err != nil {
		return nil, err
	}
	return &keyvals, nil
}

func bodyToKeyvals(body pcommon.Value) []interface{} {
	switch body.Type() {
	case pcommon.ValueTypeEmpty:
		return nil
	case pcommon.ValueTypeStr:
		// try to parse record body as logfmt, but failing that assume it's plain text
		value := body.Str()
		keyvals, err := parseLogfmtLine(value)
		if err != nil {
			return []interface{}{"msg", body.Str()}
		}
		return *keyvals
	case pcommon.ValueTypeMap:
		return valueToKeyvals("", body)
	case pcommon.ValueTypeSlice:
		return valueToKeyvals("body", body)
	default:
		return []interface{}{"msg", body.AsRaw()}
	}
}

func valueToKeyvals(key string, value pcommon.Value) []interface{} {
	switch value.Type() {
	case pcommon.ValueTypeEmpty:
		return nil
	case pcommon.ValueTypeStr:
		return []interface{}{key, value.Str()}
	case pcommon.ValueTypeBool:
		return []interface{}{key, value.Bool()}
	case pcommon.ValueTypeInt:
		return []interface{}{key, value.Int()}
	case pcommon.ValueTypeDouble:
		return []interface{}{key, value.Double()}
	case pcommon.ValueTypeMap:
		var keyvals []interface{}
		prefix := ""
		if key != "" {
			prefix = key + "_"
		}
		value.Map().Range(func(k string, v pcommon.Value) bool {

			keyvals = append(keyvals, valueToKeyvals(prefix+k, v)...)
			return true
		})
		return keyvals
	case pcommon.ValueTypeSlice:
		prefix := ""
		if key != "" {
			prefix = key + "_"
		}
		var keyvals []interface{}
		for i := 0; i < value.Slice().Len(); i++ {
			v := value.Slice().At(i)
			keyvals = append(keyvals, valueToKeyvals(fmt.Sprintf("%s%d", prefix, i), v)...)
		}
		return keyvals
	default:
		return []interface{}{key, value.AsRaw()}
	}
}

type logRecord struct {
	Body       json.RawMessage        `json:"body,omitempty"`
	TraceID    string                 `json:"traceid,omitempty"`
	SpanID     string                 `json:"spanid,omitempty"`
	Severity   string                 `json:"severity,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
	Resources  map[string]interface{} `json:"resources,omitempty"`
}

func convertLogToLine(log plog.LogRecord, res pcommon.Resource, format string) (string, error) {
	switch format {
	case formatJSON:
		var bodyData []byte
		var err error
		body := log.Body()
		switch log.Body().Type() {
		case pcommon.ValueTypeEmpty:
		case pcommon.ValueTypeStr:
			bodyData, err = json.Marshal(body.Str())
		case pcommon.ValueTypeInt:
			bodyData, err = json.Marshal(body.Int())
		case pcommon.ValueTypeDouble:
			bodyData, err = json.Marshal(body.Double())
		case pcommon.ValueTypeBool:
			bodyData, err = json.Marshal(body.Bool())
		case pcommon.ValueTypeMap:
			bodyData, err = json.Marshal(body.Map().AsRaw())
		case pcommon.ValueTypeSlice:
			bodyData, err = json.Marshal(body.Slice().AsRaw())
		case pcommon.ValueTypeBytes:
			bodyData, err = json.Marshal(body.Bytes().AsRaw())
		default:
			err = fmt.Errorf("unsuported body type to marshal json")
		}
		if err != nil {
			return "", err
		}
		logRecord := logRecord{
			Body:       bodyData,
			TraceID:    log.TraceID().String(),
			SpanID:     log.SpanID().String(),
			Severity:   log.SeverityText(),
			Attributes: log.Attributes().AsRaw(),
			Resources:  log.Attributes().AsRaw(),
		}
		jsonRecord, err := json.Marshal(logRecord)
		if err != nil {
			return "", err
		}
		return string(jsonRecord), nil
	case formatLogfmt:
		keyvals := bodyToKeyvals(log.Body())
		if traceID := log.TraceID(); !traceID.IsEmpty() {
			keyvals = keyvalsReplaceOrAppend(keyvals, "traceID", log.TraceID().String())
		}
		if spanID := log.SpanID(); !spanID.IsEmpty() {
			keyvals = keyvalsReplaceOrAppend(keyvals, "spanID", log.SpanID().String())
		}
		severity := log.SeverityText()
		if severity != "" {
			keyvals = keyvalsReplaceOrAppend(keyvals, "severity", severity)
		}
		log.Attributes().Range(func(k string, v pcommon.Value) bool {
			keyvals = append(keyvals, valueToKeyvals(fmt.Sprintf("attribute_%s", k), v)...)
			return true
		})
		res.Attributes().Range(func(k string, v pcommon.Value) bool {
			keyvals = append(keyvals, valueToKeyvals(fmt.Sprintf("resource_%s", k), v)...)
			return true
		})
		logfmtLine, err := logfmt.MarshalKeyvals(keyvals...)
		if err != nil {
			return "", err
		}
		return string(logfmtLine), nil
	default:
		return "", fmt.Errorf("invalid format %s. Expected one of: %s, %s", format, formatJSON, formatLogfmt)
	}

}

func convertLogToSample(fingerprint model.Fingerprint, log plog.LogRecord, res pcommon.Resource, format string) (Sample, error) {
	line, err := convertLogToLine(log, res, format)
	if err != nil {
		return Sample{}, err
	}
	return Sample{
		Fingerprint: uint64(fingerprint),
		TimestampNs: timestampFromLogRecord(log).UnixNano(),
		String:      line,
	}, nil
}

func convertLogToTimeSerie(fingerprint model.Fingerprint, log plog.LogRecord, labelSet model.LabelSet) (TimeSerie, error) {
	labelsJSON, err := json.Marshal(labelSet)
	if err != nil {
		return TimeSerie{}, fmt.Errorf("marshal mergedLabels err: %w", err)
	}
	timeSerie := TimeSerie{
		Date:        timestampFromLogRecord(log),
		Fingerprint: uint64(fingerprint),
		Labels:      string(labelsJSON),
		Name:        string(labelSet[model.MetricNameLabel]),
	}
	return timeSerie, nil
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	var (
		samples    []Sample
		timeSeries []TimeSerie
	)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		logs := ld.ResourceLogs().At(i)
		for j := 0; j < logs.ScopeLogs().Len(); j++ {
			rs := logs.ScopeLogs().At(j).LogRecords()
			for k := 0; k < rs.Len(); k++ {
				resource := pcommon.NewResource()
				logs.Resource().CopyTo(resource)
				log := plog.NewLogRecord()
				rs.At(k).CopyTo(log)

				// adds level attribute from log.severityNumber
				addLogLevelAttributeAndHint(log)

				format := getFormatFromFormatHint(log.Attributes(), resource.Attributes())
				mergedLabels := convertAttributesAndMerge(log.Attributes(), resource.Attributes())
				removeAttributes(log.Attributes(), mergedLabels)
				removeAttributes(resource.Attributes(), mergedLabels)

				fingerprint := mergedLabels.Fingerprint()
				sample, err := convertLogToSample(fingerprint, log, resource, format)
				if err != nil {
					return fmt.Errorf("convertLogToSample error: %w", err)
				}
				samples = append(samples, sample)

				timeSerie, err := convertLogToTimeSerie(fingerprint, log, mergedLabels)
				if err != nil {
					return fmt.Errorf("convertLogToTimeSerie error: %w", err)
				}
				timeSeries = append(timeSeries, timeSerie)
			}
		}
	}

	return batchSamplesAndTimeSeries(ctx, e.db, samples, timeSeries)
}

func batchSamplesAndTimeSeries(ctx context.Context, db clickhouse.Conn, samples []Sample, timeSeries []TimeSerie) error {
	samplesBatch, err := db.PrepareBatch(ctx, samplesSQL)
	if err != nil {
		return err
	}
	for _, sample := range samples {
		if err := samplesBatch.AppendStruct(&sample); err != nil {
			return err
		}
	}
	if err := samplesBatch.Send(); err != nil {
		return err
	}

	timeSeriesBatch, err := db.PrepareBatch(ctx, TimeSerieSQL)
	if err != nil {
		return err
	}
	for _, timeSerie := range timeSeries {
		if err := timeSeriesBatch.AppendStruct(&timeSerie); err != nil {
			return err
		}
	}
	if err := timeSeriesBatch.Send(); err != nil {
		return err
	}

	return nil

}

func convertAttributesToLabels(attributes pcommon.Map) model.LabelSet {
	ls := model.LabelSet{}

	attributes.Range(func(k string, v pcommon.Value) bool {
		ls[model.LabelName(k)] = model.LabelValue(v.AsString())
		return true
	})

	return ls
}

func addLogLevelAttributeAndHint(log plog.LogRecord) {
	if log.SeverityNumber() == plog.SeverityNumberUnspecified {
		return
	}
	addLogLevelHit(log)
	if _, found := log.Attributes().Get(levelAttributeName); !found {
		level := severityNumberToLevel[log.SeverityNumber().String()]
		log.Attributes().PutStr(levelAttributeName, level)
	}
}

func addLogLevelHit(log plog.LogRecord) {
	if value, found := log.Attributes().Get(hintAttributes); found && !strings.Contains(value.AsString(), levelAttributeName) {
		log.Attributes().PutStr(hintAttributes, fmt.Sprintf("%s,%s", value.AsString(), levelAttributeName))
	} else {
		log.Attributes().PutStr(hintAttributes, levelAttributeName)
	}
}

var severityNumberToLevel = map[string]string{
	plog.SeverityNumberUnspecified.String(): "UNSPECIFIED",
	plog.SeverityNumberTrace.String():       "TRACE",
	plog.SeverityNumberTrace2.String():      "TRACE2",
	plog.SeverityNumberTrace3.String():      "TRACE3",
	plog.SeverityNumberTrace4.String():      "TRACE4",
	plog.SeverityNumberDebug.String():       "DEBUG",
	plog.SeverityNumberDebug2.String():      "DEBUG2",
	plog.SeverityNumberDebug3.String():      "DEBUG3",
	plog.SeverityNumberDebug4.String():      "DEBUG4",
	plog.SeverityNumberInfo.String():        "INFO",
	plog.SeverityNumberInfo2.String():       "INFO2",
	plog.SeverityNumberInfo3.String():       "INFO3",
	plog.SeverityNumberInfo4.String():       "INFO4",
	plog.SeverityNumberWarn.String():        "WARN",
	plog.SeverityNumberWarn2.String():       "WARN2",
	plog.SeverityNumberWarn3.String():       "WARN3",
	plog.SeverityNumberWarn4.String():       "WARN4",
	plog.SeverityNumberError.String():       "ERROR",
	plog.SeverityNumberError2.String():      "ERROR2",
	plog.SeverityNumberError3.String():      "ERROR3",
	plog.SeverityNumberError4.String():      "ERROR4",
	plog.SeverityNumberFatal.String():       "FATAL",
	plog.SeverityNumberFatal2.String():      "FATAL2",
	plog.SeverityNumberFatal3.String():      "FATAL3",
	plog.SeverityNumberFatal4.String():      "FATAL4",
}
