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
	"encoding/base64"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

	db clickhouse.Conn
}

// newTracesExporter returns a SpanWriter for the database
func newTracesExporter(_ context.Context, logger *zap.Logger, cfg *Config) (*tracesExporter, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	return &tracesExporter{
		logger: logger,
		db:     db,
	}, nil
}

func fixRawSpan(rawSpan string, span ptrace.Span) (string, error) {
	var err error
	traceID := span.TraceID()
	if !traceID.IsEmpty() {
		rawSpan, err = sjson.Set(rawSpan, "traceId", string(base64Encode(traceID[:])))
		if err != nil {
			return "", err
		}
	}
	spanID := span.SpanID()
	if !spanID.IsEmpty() {
		rawSpan, err = sjson.Set(rawSpan, "spanId", string(base64Encode(spanID[:])))
		if err != nil {
			return "", err
		}
	}
	parentSpanID := span.ParentSpanID()
	if !parentSpanID.IsEmpty() {
		rawSpan, err = sjson.Set(rawSpan, "parentSpanId", string(base64Encode(parentSpanID[:])))
		if err != nil {
			return "", err
		}
	}
	return rawSpan, nil
}

func convertTagsToColTupleArray(tags [][]string) []proto.ColTuple {
	tupleArr := make([]proto.ColTuple, len(tags))
	for i, tag := range tags {
		first := new(proto.ColStr)
		second := new(proto.ColStr)
		t := proto.ColTuple{first, second}
		first.Append(tag[0])
		second.Append(tag[1])
		tupleArr[i] = t
	}
	return tupleArr
}

func exportScopeSpans(serviceName string, rawScopeSapns string, ilss ptrace.ScopeSpansSlice, resource pcommon.Resource, batch driver.Batch) error {
	for i := 0; i < ilss.Len(); i++ {
		spans := ilss.At(i).Spans()
		rawSpans := gjson.Get(rawScopeSapns, fmt.Sprintf("%d.spans", i)).String()
		err := exportSpans(serviceName, rawSpans, spans, resource, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func exportSpans(serviceName string, rawSapns string, spans ptrace.SpanSlice, resource pcommon.Resource, batch driver.Batch) error {
	for i := 0; i < spans.Len(); i++ {
		span := spans.At(i)
		resource.Attributes().CopyTo(span.Attributes())
		rawSpan, err := fixRawSpan(gjson.Get(rawSapns, fmt.Sprintf("%d", i)).String(), span)
		if err != nil {
			return err
		}
		tracesInput := convertTracesInput(span, serviceName, rawSpan)
		if err := batch.AppendStruct(tracesInput); err != nil {
			return err
		}
	}
	return nil

}

func (e *tracesExporter) exportResourceSapns(ctx context.Context, rawTraces string, resourceSpans ptrace.ResourceSpansSlice) error {
	batch, err := e.db.PrepareBatch(ctx, "INSERT INTO traces_input (trace_id, span_id, parent_id, name, timestamp_ns, duration_ns, service_name, payload_type, payload, tags)")
	if err != nil {
		return err
	}

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resource := rs.Resource()
		serviceName := serviceNameForResource(resource)
		ilss := rs.ScopeSpans()
		rawScopeSpans := gjson.Get(rawTraces, fmt.Sprintf("resourceSpans.%d.scopeSpans", i)).String()
		if err := exportScopeSpans(serviceName, rawScopeSpans, ilss, resource, batch); err != nil {
			batch.Abort()
			return err
		}
	}
	return batch.Send()
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	rawTraces, err := marshaler.MarshalTraces(td)
	if err != nil {
		return err
	}

	if err := e.exportResourceSapns(ctx, string(rawTraces), td.ResourceSpans()); err != nil {
		return err
	}
	return nil

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

func convertTracesInput(otelSpan ptrace.Span, serviceName string, payload string) *Trace {
	durationNano := uint64(otelSpan.EndTimestamp() - otelSpan.StartTimestamp())
	attributes := otelSpan.Attributes()

	tags := make([][]string, 0)
	tags = append(tags, []string{"name", otelSpan.Name()})
	tags = append(tags, []string{"service.name", serviceName})

	attributes.Range(func(k string, v pcommon.Value) bool {
		tags = append(tags, []string{k, v.AsString()})
		return true
	})

	trace := &Trace{
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

	return trace
}
