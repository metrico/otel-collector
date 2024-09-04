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
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.opentelemetry.io/otel/metric"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	spanLinkDataFormat = "%s|%s|%s|%s|%d"
)

var delegate = &protojson.MarshalOptions{
	// https://github.com/open-telemetry/opentelemetry-specification/pull/2758
	UseEnumNumbers: true,
}

// tracesExporter for writing spans to ClickHouse
type tracesExporter struct {
	logger *zap.Logger
	meter  metric.Meter

	db      clickhouse.Conn
	cluster bool
}

// newTracesExporter returns a SpanWriter for the database
func newTracesExporter(logger *zap.Logger, cfg *Config, set *exporter.Settings) (*tracesExporter, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	db, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	exp := &tracesExporter{
		logger:  logger,
		meter:   set.MeterProvider.Meter(typeStr),
		db:      db,
		cluster: cfg.ClusteredClickhouse,
	}
	if err := initMetrics(exp.meter); err != nil {
		exp.logger.Error(fmt.Sprintf("failed to init metrics: %s", err.Error()))
		return exp, err
	}
	return exp, nil
}

func (e *tracesExporter) exportScopeSpans(serviceName string,
	ilss ptrace.ScopeSpansSlice,
	resource pcommon.Resource,
	batch driver.Batch,
	tags map[string]string,
) error {
	for i := 0; i < ilss.Len(); i++ {
		extractScopeTags(ilss.At(i).Scope(), tags)
		spans := ilss.At(i).Spans()
		err := e.exportSpans(serviceName, spans, resource, batch, tags)
		if err != nil {
			return err
		}
	}
	return nil
}

func spanLinksToTags(links ptrace.SpanLinkSlice, tags map[string]string) error {
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		key := fmt.Sprintf("otlp.link.%d", i)
		jsonStr, err := json.Marshal(link.Attributes().AsRaw())
		if err != nil {
			return err
		}
		tags[key] = fmt.Sprintf(spanLinkDataFormat,
			link.TraceID().String(),
			link.SpanID().String(),
			link.TraceState().AsRaw(),
			jsonStr,
			link.DroppedAttributesCount())
	}
	return nil
}

func attributeMapToStringMap(attrMap pcommon.Map) map[string]string {
	rawMap := make(map[string]string)
	attrMap.Range(func(k string, v pcommon.Value) bool {
		rawMap[k] = v.AsString()
		return true
	})
	return rawMap
}

func aggregateSpanTags(span ptrace.Span, tt map[string]string) map[string]string {
	tags := make(map[string]string)
	for key, val := range tt {
		tags[key] = val
	}
	spanTags := attributeMapToStringMap(span.Attributes())
	for key, val := range spanTags {
		tags[key] = val
	}
	return tags
}

func (e *tracesExporter) exportSpans(
	localServiceName string,
	spans ptrace.SpanSlice,
	resource pcommon.Resource,
	batch driver.Batch,
	tags map[string]string,
) error {
	for i := 0; i < spans.Len(); i++ {
		span := spans.At(i)
		spanLinksToTags(span.Links(), tags)
		tracesInput, err := convertTracesInput(span, resource, localServiceName, tags)
		if err != nil {
			e.logger.Error("convertTracesInput", zap.Error(err))
			continue
		}
		if err := batch.AppendStruct(tracesInput); err != nil {
			return err
		}
	}
	return nil
}

func extractScopeTags(il pcommon.InstrumentationScope, tags map[string]string) {
	if ilName := il.Name(); ilName != "" {
		tags[conventions.OtelLibraryName] = ilName
	}
	if ilVer := il.Version(); ilVer != "" {
		tags[conventions.OtelLibraryVersion] = ilVer
	}
}

func (e *tracesExporter) exportResourceSapns(ctx context.Context, resourceSpans ptrace.ResourceSpansSlice) error {
	isCluster := ctx.Value("cluster").(bool)
	batch, err := e.db.PrepareBatch(ctx, tracesInputSQL(isCluster))
	if err != nil {
		return err
	}

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		resource := rs.Resource()
		localServiceName, tags := resourceToServiceNameAndAttributeMap(resource)
		ilss := rs.ScopeSpans()
		if err := e.exportScopeSpans(localServiceName, ilss, resource, batch, tags); err != nil {
			batch.Abort()
			return err
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	return nil
}

// traceDataPusher implements OTEL exporterhelper.traceDataPusher
func (e *tracesExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	_ctx := context.WithValue(ctx, "cluster", e.cluster)
	start := time.Now()
	if err := e.exportResourceSapns(_ctx, td.ResourceSpans()); err != nil {
		otelcolExporterQrynBatchInsertDurationMillis.Record(ctx, time.Now().UnixMilli()-start.UnixMilli(), metric.WithAttributeSet(*newOtelcolAttrSetBatch(errorCodeError, dataTypeTraces)))
		e.logger.Error(fmt.Sprintf("failed to insert batch: [%s]", err.Error()))
		return err
	}
	otelcolExporterQrynBatchInsertDurationMillis.Record(ctx, time.Now().UnixMilli()-start.UnixMilli(), metric.WithAttributeSet(*newOtelcolAttrSetBatch(errorCodeSuccess, dataTypeTraces)))
	e.logger.Debug("pushTraceData", zap.Int("spanCount", td.SpanCount()), zap.String("cost", time.Since(start).String()))
	return nil
}

// Shutdown will shutdown the exporter.
func (e *tracesExporter) Shutdown(_ context.Context) error {
	if e.db != nil {
		return e.db.Close()
	}
	return nil
}

// resourceToServiceNameAndAttributeMap gets the service name for a specified Resource.
func resourceToServiceNameAndAttributeMap(resource pcommon.Resource) (string, map[string]string) {
	tags := make(map[string]string)
	attrs := resource.Attributes()
	if attrs.Len() == 0 {
		return "OTLPResourceNoServiceName", tags
	}

	attrs.Range(func(k string, v pcommon.Value) bool {
		tags[k] = v.AsString()
		return true
	})

	serviceName := extractServiceName(tags)

	return serviceName, tags
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

func mergeAttributes(span ptrace.Span, resource pcommon.Resource) pcommon.Map {
	newAttributes := pcommon.NewMap()
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		newAttributes.PutStr(k, v.AsString())
		return true
	})
	span.Attributes().Range(func(k string, v pcommon.Value) bool {
		newAttributes.PutStr(k, v.AsString())
		return true
	})
	return newAttributes
}

func sliceToArray(vs pcommon.Slice) []*commonv1.AnyValue {
	var anyValues []*commonv1.AnyValue
	for i := 0; i < vs.Len(); i++ {
		anyValues = append(anyValues, valueToOtlpAnyVaule(vs.At(i)))
	}
	return anyValues
}

func mapToKeyValueList(m pcommon.Map) []*commonv1.KeyValue {
	var keyValues []*commonv1.KeyValue
	m.Range(func(k string, v pcommon.Value) bool {
		keyValues = append(keyValues, &commonv1.KeyValue{
			Key:   k,
			Value: valueToOtlpAnyVaule(v),
		})
		return true
	})
	return keyValues
}

func ensureUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return fmt.Sprintf("invalid utf-8: %q", s)
}

func valueToOtlpAnyVaule(v pcommon.Value) *commonv1.AnyValue {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: ensureUTF8(v.Str())}}
	case pcommon.ValueTypeBytes:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: v.Bytes().AsRaw()}}
	case pcommon.ValueTypeInt:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v.Int()}}
	case pcommon.ValueTypeDouble:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: v.Double()}}
	case pcommon.ValueTypeBool:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: v.Bool()}}
	case pcommon.ValueTypeSlice:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: sliceToArray(v.Slice())}}}
	case pcommon.ValueTypeMap:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: mapToKeyValueList(v.Map())}}}
	default:
		return nil
	}
}

func spanLinkSlickToSpanLinks(spanLinks ptrace.SpanLinkSlice) []*tracev1.Span_Link {
	links := make([]*tracev1.Span_Link, 0, spanLinks.Len())
	for i := 0; i < spanLinks.Len(); i++ {
		link := spanLinks.At(i)
		traceID := link.TraceID()
		spanID := link.SpanID()
		links = append(links, &tracev1.Span_Link{
			TraceId:                traceID[:],
			SpanId:                 spanID[:],
			TraceState:             link.TraceState().AsRaw(),
			DroppedAttributesCount: link.DroppedAttributesCount(),
			Attributes:             mapToKeyValueList(link.Attributes()),
		})
	}
	return links
}

func spanEventSliceToSpanEvents(spanEvents ptrace.SpanEventSlice) []*tracev1.Span_Event {
	events := make([]*tracev1.Span_Event, 0, spanEvents.Len())
	for i := 0; i < spanEvents.Len(); i++ {
		event := spanEvents.At(i)
		events = append(events, &tracev1.Span_Event{
			TimeUnixNano:           uint64(event.Timestamp()),
			Name:                   event.Name(),
			DroppedAttributesCount: event.DroppedAttributesCount(),
			Attributes:             mapToKeyValueList(event.Attributes()),
		})
	}
	return events
}

func marshalSpanToJSON(span ptrace.Span, mergedAttributes pcommon.Map) ([]byte, error) {
	traceID := span.TraceID()
	spanID := span.SpanID()
	parentSpanID := span.ParentSpanID()
	otlpSpan := &tracev1.Span{
		TraceId:                traceID[:],
		SpanId:                 spanID[:],
		TraceState:             span.TraceState().AsRaw(),
		ParentSpanId:           parentSpanID[:],
		Name:                   span.Name(),
		Kind:                   tracev1.Span_SpanKind(span.Kind()),
		StartTimeUnixNano:      uint64(span.StartTimestamp()),
		EndTimeUnixNano:        uint64(span.EndTimestamp()),
		Attributes:             mapToKeyValueList(mergedAttributes),
		DroppedAttributesCount: span.DroppedAttributesCount(),
		Events:                 spanEventSliceToSpanEvents(span.Events()),
		DroppedEventsCount:     span.DroppedEventsCount(),
		Links:                  spanLinkSlickToSpanLinks(span.Links()),
		DroppedLinksCount:      span.DroppedLinksCount(),
		Status: &tracev1.Status{
			Code:    tracev1.Status_StatusCode(span.Status().Code()),
			Message: span.Status().Message(),
		},
	}
	return delegate.Marshal(otlpSpan)
}

func convertTracesInput(span ptrace.Span, resource pcommon.Resource, serviceName string, tags map[string]string) (*Trace, error) {
	durationNano := uint64(span.EndTimestamp() - span.StartTimestamp())
	tags = aggregateSpanTags(span, tags)
	tags["service.name"] = serviceName
	tags["name"] = span.Name()
	tags["otel.status_code"] = span.Status().Code().String()
	tags["otel.status_description"] = span.Status().Message()

	mTags := make([][]string, 0, len(tags))
	for k, v := range tags {
		mTags = append(mTags, []string{k, v})
	}
	payload, err := marshalSpanToJSON(span, mergeAttributes(span, resource))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal span: %w", err)
	}

	trace := &Trace{
		TraceID:     span.TraceID().String(),
		SpanID:      span.SpanID().String(),
		ParentID:    span.ParentSpanID().String(),
		Name:        span.Name(),
		TimestampNs: int64(span.StartTimestamp()),
		DurationNs:  int64(durationNano),
		ServiceName: serviceName,
		PayloadType: 2,
		Tags:        mTags,
		Payload:     string(payload),
	}

	return trace, nil
}
