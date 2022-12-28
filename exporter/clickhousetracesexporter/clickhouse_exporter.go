// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhousetracesexporter

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.opentelemetry.io/collector/component"
	conventions "go.opentelemetry.io/collector/model/semconv/v1.5.0"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// Crete new exporter.
func newExporter(cfg component.ExporterConfig, logger *zap.Logger) (*storage, error) {

	configClickHouse := cfg.(*Config)

	f := ClickHouseNewFactory(configClickHouse.Datasource)

	err := f.Initialize(logger)
	if err != nil {
		return nil, err
	}
	spanWriter, err := f.CreateSpanWriter()
	if err != nil {
		return nil, err
	}

	storage := storage{Writer: spanWriter}

	return &storage, nil
}

type storage struct {
	Writer Writer
}

// ServiceNameForResource gets the service name for a specified Resource.
func ServiceNameForResource(resource pcommon.Resource) string {
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

func newStructuredSpan(otelSpan ptrace.Span, serviceName string, resource pcommon.Resource, payload string) *Span {

	durationNano := uint64(otelSpan.EndTimestamp() - otelSpan.StartTimestamp())

	attributes := otelSpan.Attributes()
	resourceAttributes := resource.Attributes()

	// build tags
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

	span := &Span{
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

var marshaler = &ptrace.JSONMarshaler{}

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

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (s *storage) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	mergeAttributes(&td)
	rawTraces, err := marshaler.MarshalTraces(td)
	if err != nil {
		return err
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		serviceName := ServiceNameForResource(rs.Resource())
		ilss := rs.ScopeSpans()
		for j := 0; j < ilss.Len(); j++ {
			ils := ilss.At(j)
			spans := ils.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				rs.Resource().Attributes().CopyTo(span.Attributes())
				path := fmt.Sprintf("resourceSpans.%d.scopeSpans.%d.spans.%d", i, j, k)
				rawSpan := gjson.GetBytes(rawTraces, path).String()

				// fix json marshall error
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

				structuredSpan := newStructuredSpan(span, serviceName, rs.Resource(), rawSpan)
				err = s.Writer.WriteSpan(structuredSpan)
				if err != nil {
					zap.S().Error("Error in writing spans to clickhouse: ", err)
				}
			}
		}
	}
	return nil
}

func base64Encode(input []byte) []byte {
	eb := make([]byte, base64.StdEncoding.EncodedLen(len(input)))
	base64.StdEncoding.Encode(eb, input)
	return eb
}

// Shutdown will shutdown the exporter.
func (s *storage) Shutdown(_ context.Context) error {
	if closer, ok := s.Writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
