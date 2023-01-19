package qrynexporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/loki"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const (
	samplesInsertSQL = `
  INSERT INTO samples_v3 (
    fingerprint, 
    timestamp_ns, 
    value, 
    string
  ) VALUES (
    ?,
    ?,
    ?,
    ?
  )`
	timeSeriesInsertSQL = `
  INSERT INTO time_series (
    date,
    fingerprint, 
    labels,
    name
  ) VALUES (
    ?,
    ?,
    ?,
    ?
  )`
)

type logsExporter struct {
	db *sql.DB

	logger *zap.Logger
	cfg    *Config
}

func newLogsExporter(logger *zap.Logger, cfg *Config) (*logsExporter, error) {
	db, err := sql.Open("clickhouse", cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &logsExporter{
		db:     db,
		logger: logger,
		cfg:    cfg,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *logsExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

var defaultExporterLabels = model.LabelSet{"exporter": "OTLP"}

const (
	hintAttributes = "loki.attribute.labels"
	hintResources  = "loki.resource.labels"
	hintTenant     = "loki.tenant"
	hintFormat     = "loki.format"
)

const (
	formatJSON   string = "json"
	formatLogfmt string = "logfmt"
)

func convertAttributesAndMerge(logAttrs pcommon.Map, resAttrs pcommon.Map) model.LabelSet {
	out := defaultExporterLabels

	// get the hint from the log attributes, not from the resource
	// the value can be a single resource name to use as label
	// or a slice of string values
	if resourcesToLabel, found := logAttrs.Get(hintResources); found {
		labels := convertAttributesToLabels(resAttrs, resourcesToLabel)
		out = out.Merge(labels)
	}

	if attributesToLabel, found := logAttrs.Get(hintAttributes); found {
		labels := convertAttributesToLabels(logAttrs, attributesToLabel)
		out = out.Merge(labels)
	}

	// get tenant hint from resource attributes, fallback to record attributes
	// if it is not found
	if resourcesToLabel, found := resAttrs.Get(hintTenant); !found {
		if attributesToLabel, found := logAttrs.Get(hintTenant); found {
			labels := convertAttributesToLabels(logAttrs, attributesToLabel)
			out = out.Merge(labels)
		}
	} else {
		labels := convertAttributesToLabels(resAttrs, resourcesToLabel)
		out = out.Merge(labels)
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

func convertAttributesToLabels(attributes pcommon.Map, attrsToSelect pcommon.Value) model.LabelSet {
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

func timestampFromLogRecord(lr plog.LogRecord) uint64 {
	if lr.Timestamp() != 0 {
		return uint64(lr.Timestamp())
	}

	if lr.ObservedTimestamp() != 0 {
		return uint64(lr.ObservedTimestamp())
	}
	return uint64(pcommon.NewTimestampFromTime(time.Now()))
}

func convertLogToLine(log plog.LogRecord, res pcommon.Resource, format string) (string, error) {
	switch format {
	case formatJSON:
		return loki.Encode(log, res)
	case formatLogfmt:
		return loki.EncodeLogfmt(log, res)
	default:
		return "", fmt.Errorf("invalid format %s. Expected one of: %s, %s", format, formatJSON, formatLogfmt)
	}

}

func convertLogToSample(fingerprint model.Fingerprint, log plog.LogRecord, res pcommon.Resource, format string) (*Sample, error) {
	line, err := convertLogToLine(log, res, format)
	if err != nil {
		return nil, err
	}
	return &Sample{
		Fingerprint: uint64(fingerprint),
		TimestampNs: int64(timestampFromLogRecord(log)),
		String:      line,
	}, nil
}

func (e *logsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := Transaction(ctx, e.db, func(tx *sql.Tx) error {
		sampleStatement, err := tx.PrepareContext(ctx, samplesInsertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer sampleStatement.Close()
		timeSerieStatement, err := tx.PrepareContext(ctx, timeSeriesInsertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer sampleStatement.Close()
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			logs := ld.ResourceLogs().At(i)
			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				rs := logs.ScopeLogs().At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					resource := pcommon.NewResource()
					logs.Resource().CopyTo(resource)
					log := plog.NewLogRecord()
					rs.At(k).CopyTo(log)
					format := getFormatFromFormatHint(log.Attributes(), resource.Attributes())
					mergedLabels := convertAttributesAndMerge(log.Attributes(), resource.Attributes())
					// remove the attributes that were promoted to labels
					removeAttributes(log.Attributes(), mergedLabels)
					removeAttributes(resource.Attributes(), mergedLabels)

					// TODO: use same fingerprint alg
					fingerprint := mergedLabels.Fingerprint()
					sample, err := convertLogToSample(fingerprint, log, resource, format)
					if err != nil {
						return fmt.Errorf("convertLogToSample error: %w", err)
					}
					_, err = sampleStatement.ExecContext(ctx,
						sample.Fingerprint,
						sample.TimestampNs,
						sample.Value,
						sample.String,
					)
					if err != nil {
						return fmt.Errorf("ExecContext:%w", err)
					}

					date := time.Unix(0, int64(timestampFromLogRecord(log))).Format("2006-01-02")

					labelsJSON, err := json.Marshal(mergedLabels)
					if err != nil {
						return fmt.Errorf("marshal mergedLabels err: %w", err)
					}
					_, err = timeSerieStatement.ExecContext(ctx,
						date,
						fingerprint,
						string(labelsJSON),
						mergedLabels["name"],
					)
					if err != nil {
						return fmt.Errorf("ExecContext:%w", err)
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Info("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

func (e *logsExporter) convertAttributesToLabels(attributes pcommon.Map) model.LabelSet {
	ls := model.LabelSet{}

	attributes.Range(func(k string, v pcommon.Value) bool {
		if v.Type() != pcommon.ValueTypeStr {
			e.logger.Debug("Failed to convert attribute value to Loki label value, value is not a string", zap.String("attribute", k))
			return true
		}
		ls[model.LabelName(k)] = model.LabelValue(v.Str())
		return true
	})

	return ls
}
