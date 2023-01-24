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

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
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

	client *ch.Client
}

// newTracesExporter returns a SpanWriter for the database
func newTracesExporter(ctx context.Context, logger *zap.Logger, cfg *Config) (*tracesExporter, error) {
	opts, err := parseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	client, err := ch.Dial(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &tracesExporter{
		logger: logger,
		client: client,
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

func newTracesInput(schema *TracesSchema) proto.Input {
	return proto.Input{
		{Name: "trace_id", Data: &schema.TraceID},
		{Name: "span_id", Data: &schema.SpanID},
		{Name: "parent_id", Data: &schema.ParentID},
		{Name: "name", Data: &schema.Name},
		{Name: "Timestamp_ns", Data: &schema.TimestampNs},
		{Name: "duration_ns", Data: &schema.DurationNs},
		{Name: "service_name", Data: &schema.ServiceName},
		{Name: "payload_type", Data: &schema.PayloadType},
		{Name: "payload", Data: &schema.Payload},
		{Name: "tag", Data: &schema.Tags},
	}
}

func appendTracesInput(schema *TracesSchema, tracesInput *Trace) {
	schema.TraceID.Append(tracesInput.TraceID)
	schema.SpanID.Append(tracesInput.SpanID)
	schema.ParentID.Append(tracesInput.ParentID)
	schema.Name.Append(tracesInput.Name)
	schema.TimestampNs.Append(tracesInput.TimestampNs)
	schema.ServiceName.Append(tracesInput.ServiceName)
	schema.PayloadType.Append(tracesInput.PayloadType)
	schema.Payload.Append(tracesInput.Payload)
	schema.Tags.Append(convertTagsToColTupleArray(tracesInput.Tags))
}

func appendTraces(serviceName string, rawSapns string, spans ptrace.SpanSlice, resource pcommon.Resource, schema *TracesSchema) error {
	for i := 0; i < spans.Len(); i++ {
		span := spans.At(i)
		resource.Attributes().CopyTo(span.Attributes())
		rawSpan, err := fixRawSpan(gjson.Get(rawSapns, fmt.Sprintf(".%d", i)).String(), span)
		if err != nil {
			return err
		}
		tracesInput := convertTracesInput(span, serviceName, rawSpan)
		appendTracesInput(schema, tracesInput)
	}
	return nil

}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	rawTraces, err := marshaler.MarshalTraces(td)
	if err != nil {
		return err
	}

	schema := new(TracesSchema)
	input := newTracesInput(schema)

	return e.client.Do(ctx, ch.Query{
		Body:  input.Into("traces_input"),
		Input: input,
		OnInput: func(_ context.Context) error {
			input.Reset()
			rss := td.ResourceSpans()
			for i := 0; i < rss.Len(); i++ {
				rs := rss.At(i)
				serviceName := serviceNameForResource(rs.Resource())
				ilss := rs.ScopeSpans()
				for j := 0; j < ilss.Len(); j++ {
					spans := ilss.At(j).Spans()
					rawSpans := gjson.GetBytes(rawTraces, fmt.Sprintf("resourceSpans.%d.scopeSpans.%d.spans", i, j)).String()
					err := appendTraces(serviceName, rawSpans, spans, rss.At(i).Resource(), schema)
					if err != nil {
						return err
					}
				}
			}
			return nil
		},
	})
}

// Shutdown will shutdown the exporter.
func (e *tracesExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
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
