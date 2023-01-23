// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package qrynexporter

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go/v2" // For register database driver.
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

var marshaler = &ptrace.JSONMarshaler{}

// tracesExporter for writing spans to ClickHouse
type tracesExporter struct {
	logger *zap.Logger
	db     *sql.DB
}

// newTracesExporter returns a SpanWriter for the database
func newTracesExporter(logger *zap.Logger, cfg *Config) (*tracesExporter, error) {
	db, err := sql.Open("clickhouse", cfg.DSN)
	if err != nil {
		return nil, err
	}
	return &tracesExporter{
		logger: logger,
		db:     db,
	}, nil
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	mergeAttributes(&td)
	rawTraces, err := marshaler.MarshalTraces(td)
	if err != nil {
		return err
	}
	return Transaction(ctx, e.db, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, tracesInsertSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer statement.Close()
		rss := td.ResourceSpans()
		for i := 0; i < rss.Len(); i++ {
			rs := rss.At(i)
			serviceName := serviceNameForResource(rs.Resource())
			ilss := rs.ScopeSpans()
			for j := 0; j < ilss.Len(); j++ {
				ils := ilss.At(j)
				spans := ils.Spans()
				for k := 0; k < spans.Len(); k++ {
					span := spans.At(k)
					rs.Resource().Attributes().CopyTo(span.Attributes())
					path := fmt.Sprintf("resourceSpans.%d.scopeSpans.%d.spans.%d", i, j, k)
					rawSpan := gjson.GetBytes(rawTraces, path).String()

					traceID := span.TraceID()
					if !traceID.IsEmpty() {
						rawSpan, err = sjson.Set(rawSpan, "traceId", string(base64Encode(traceID[:])))
						if err != nil {
							return err
						}
					}
					spanID := span.SpanID()
					if !spanID.IsEmpty() {
						rawSpan, err = sjson.Set(rawSpan, "spanId", string(base64Encode(spanID[:])))
						if err != nil {
							return err
						}
					}
					parentSpanID := span.ParentSpanID()
					if !parentSpanID.IsEmpty() {
						rawSpan, err = sjson.Set(rawSpan, "parentSpanId", string(base64Encode(parentSpanID[:])))
						if err != nil {
							return err
						}
					}

					tracesInput := convertTracesInput(span, serviceName, rs.Resource(), rawSpan)
					_, err := statement.ExecContext(ctx,
						tracesInput.TraceID,
						tracesInput.SpanID,
						tracesInput.ParentID,
						tracesInput.Name,
						tracesInput.TimestampNs,
						tracesInput.DurationNs,
						tracesInput.ServiceName,
						tracesInput.PayloadType,
						tracesInput.Payload,
						tracesInput.Tags,
					)
					if err != nil {
						e.logger.Sugar().Error("Error in writing traces_input to clickhouse: ", err)
					}
				}
			}
		}
		return nil
	})
}

// Shutdown will shutdown the exporter.
func (e *tracesExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}
func base64Encode(input []byte) []byte {
	eb := make([]byte, base64.StdEncoding.EncodedLen(len(input)))
	base64.StdEncoding.Encode(eb, input)
	return eb
}

// serviceNameForResource gets the service name for a specified Resource.
func serviceNameForResource(resource pcommon.Resource) string {
	tags := make(map[string]string)
	attrs := resource.Attributes()
	if attrs.Len() == 0 {
		return "OTLPResourceNoServiceName"
	}

	attrs.Range(func(k string, v pcommon.Value) bool {
		tags[k] = v.AsString()
		return true
	})

	return extractServiceName(tags)

}

func extractServiceName(tags map[string]string) string {
	var serviceName string
	if sn, ok := tags[conventions.AttributeServiceName]; ok {
		serviceName = sn
	} else if fn, ok := tags[conventions.AttributeFaaSName]; ok {
		serviceName = fn
	} else if fn, ok := tags[conventions.AttributeK8SDeploymentName]; ok {
		serviceName = fn
	} else if fn, ok := tags[conventions.AttributeProcessExecutableName]; ok {
		serviceName = fn
	} else {
		serviceName = "OTLPResourceNoServiceName"
	}
	return serviceName
}

func convertTracesInput(otelSpan ptrace.Span, serviceName string, resource pcommon.Resource, payload string) *Trace {
	durationNano := uint64(otelSpan.EndTimestamp() - otelSpan.StartTimestamp())
	attributes := otelSpan.Attributes()
	resourceAttributes := resource.Attributes()

	tags := make([][]any, 0)
	tags = append(tags, []any{"name", otelSpan.Name()})
	tags = append(tags, []any{"service.name", serviceName})

	attributes.Range(func(k string, v pcommon.Value) bool {
		tags = append(tags, []any{k, v.AsString()})
		return true
	})

	resourceAttributes.Range(func(k string, v pcommon.Value) bool {
		tags = append(tags, []any{k, v.AsString()})
		return true
	})

	span := &Trace{
		TraceID:     otelSpan.TraceID().HexString(),
		SpanID:      otelSpan.SpanID().HexString(),
		ParentID:    otelSpan.ParentSpanID().HexString(),
		Name:        otelSpan.Name(),
		TimestampNs: int64(otelSpan.StartTimestamp()),
		DurationNs:  int64(durationNano),
		ServiceName: serviceName,
		PayloadType: 2,
		Tags:        tags,
		Payload:     payload,
	}

	return span
}

func mergeAttributes(td *ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				rs.Resource().Attributes().CopyTo(span.Attributes())
			}
		}
	}
}
