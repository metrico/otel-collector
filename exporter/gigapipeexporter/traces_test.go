package gigapipeexporter

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

func TestAttributeMapToStringMap(t *testing.T) {
	m := pcommon.NewMap()
	m.PutStr("s", "str")
	m.PutInt("i", 7)
	m.PutBool("b", true)

	out := attributeMapToStringMap(m)

	assert.Equal(t, "str", out["s"])
	assert.Equal(t, "7", out["i"])
	assert.Equal(t, "true", out["b"])
}

func TestAggregateSpanTags_SpanOverridesBase(t *testing.T) {
	span := ptrace.NewSpan()
	span.Attributes().PutStr("k", "span")
	span.Attributes().PutStr("only.span", "x")

	base := map[string]string{"k": "base", "only.base": "y"}

	tags := aggregateSpanTags(span, base)

	assert.Equal(t, "span", tags["k"]) // span attr wins over base
	assert.Equal(t, "x", tags["only.span"])
	assert.Equal(t, "y", tags["only.base"])
	// base map is not mutated
	assert.Equal(t, "base", base["k"])
}

func TestExtractScopeTags(t *testing.T) {
	scope := pcommon.NewInstrumentationScope()
	scope.SetName("lib")
	scope.SetVersion("2.0")

	tags := map[string]string{}
	extractScopeTags(scope, tags)

	assert.Equal(t, "lib", tags[attrOTelLibraryName])
	assert.Equal(t, "2.0", tags[attrOTelLibraryVersion])

	// empty scope adds nothing
	empty := map[string]string{}
	extractScopeTags(pcommon.NewInstrumentationScope(), empty)
	assert.Empty(t, empty)
}

func TestSpanLinksToTags(t *testing.T) {
	span := ptrace.NewSpan()
	link := span.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID([16]byte{1}))
	link.SetSpanID(pcommon.SpanID([8]byte{2}))
	link.Attributes().PutStr("lk", "lv")

	tags := map[string]string{}
	require.NoError(t, spanLinksToTags(span.Links(), tags))

	v, ok := tags["otlp.link.0"]
	require.True(t, ok)
	assert.Contains(t, v, link.TraceID().String())
	assert.Contains(t, v, `{"lk":"lv"}`)
}

func TestResourceToServiceNameAndAttributeMap(t *testing.T) {
	// no attributes -> sentinel service name
	empty := pcommon.NewResource()
	name, tags := resourceToServiceNameAndAttributeMap(empty)
	assert.Equal(t, "OTLPResourceNoServiceName", name)
	assert.Empty(t, tags)

	// service.name present
	res := pcommon.NewResource()
	res.Attributes().PutStr(attrServiceName, "svc")
	name, tags = resourceToServiceNameAndAttributeMap(res)
	assert.Equal(t, "svc", name)
	assert.Equal(t, "svc", tags[attrServiceName])
}

func TestExtractServiceName_Fallbacks(t *testing.T) {
	assert.Equal(t, "svc", extractServiceName(map[string]string{attrServiceName: "svc"}))
	assert.Equal(t, "faas", extractServiceName(map[string]string{attrFaaSName: "faas"}))
	assert.Equal(t, "dep", extractServiceName(map[string]string{attrK8SDeploymentName: "dep"}))
	assert.Equal(t, "proc", extractServiceName(map[string]string{attrProcessExecutableName: "proc"}))
	assert.Equal(t, "OTLPResourceNoServiceName", extractServiceName(map[string]string{}))
}

func TestEnsureUTF8(t *testing.T) {
	assert.Equal(t, "ok", ensureUTF8("ok"))
	assert.Contains(t, ensureUTF8("\xff\xfe"), "invalid utf-8")
}

func TestConvertTracesInput_JSONAndProto(t *testing.T) {
	span := newTraces().ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)

	for _, payloadType := range []string{"json", "proto"} {
		t.Run(payloadType, func(t *testing.T) {
			ti, err := convertTracesInput(span, pcommon.NewResource(), "svc", map[string]string{}, payloadType)
			require.NoError(t, err)
			assert.Equal(t, span.TraceID().String(), ti.TraceID)
			assert.Equal(t, span.SpanID().String(), ti.SpanID)
			assert.Equal(t, "svc", ti.ServiceName)
			assert.Equal(t, int8(2), ti.PayloadType)
			assert.NotEmpty(t, ti.Payload)
			// service.name / name are injected into tags
			foundName := false
			for _, kv := range ti.Tags {
				if kv[0] == "service.name" {
					foundName = true
					assert.Equal(t, "svc", kv[1])
				}
			}
			assert.True(t, foundName)
		})
	}
}

// TestConvertTracesInput_RichSpanPayload drives marshalSpan over span events and
// links whose attributes span every pcommon value type, covering the OTLP
// conversion helpers (valueToOtlpAnyVaule, sliceToArray, mapToKeyValueList,
// spanEventSliceToSpanEvents, spanLinkSlickToSpanLinks).
func TestConvertTracesInput_RichSpanPayload(t *testing.T) {
	span := ptrace.NewSpan()
	span.SetName("rich")
	span.SetTraceID(pcommon.TraceID([16]byte{1}))
	span.SetSpanID(pcommon.SpanID([8]byte{2}))

	ev := span.Events().AppendEmpty()
	ev.SetName("evt")
	ea := ev.Attributes()
	ea.PutStr("s", "str")
	ea.PutInt("i", 1)
	ea.PutDouble("d", 2.5)
	ea.PutBool("b", true)
	ea.PutEmptyBytes("by").Append(1, 2)
	ea.PutEmptySlice("sl").AppendEmpty().SetStr("x")
	ea.PutEmptyMap("m").PutStr("mk", "mv")

	link := span.Links().AppendEmpty()
	link.SetTraceID(pcommon.TraceID([16]byte{3}))
	link.Attributes().PutStr("lk", "lv")

	ti, err := convertTracesInput(span, pcommon.NewResource(), "svc", map[string]string{}, "json")
	require.NoError(t, err)
	assert.NotEmpty(t, ti.Payload)
	assert.Contains(t, ti.Payload, "evt")
}

func TestConvertTracesInput_UnsupportedPayloadType(t *testing.T) {
	span := newTraces().ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
	_, err := convertTracesInput(span, pcommon.NewResource(), "svc", map[string]string{}, "bogus")
	require.Error(t, err)
}

func TestCovertDBPayloadType(t *testing.T) {
	assert.Equal(t, int8(2), covertDBPayloadType("json"))
	assert.Equal(t, int8(2), covertDBPayloadType("proto"))
	assert.Equal(t, int8(2), covertDBPayloadType("anything"))
}

// --- push / orchestration paths via fakeConn -------------------------------

func TestPushTraceData_SingleNode(t *testing.T) {
	fc := &fakeConn{}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, tracePayloadType: "json"}

	require.NoError(t, e.pushTraceData(context.Background(), newTraces()))

	require.Len(t, fc.queries, 1)
	assert.Contains(t, fc.queries[0], "INSERT INTO traces_input")
	require.Len(t, fc.batches, 1)
	assert.True(t, fc.batches[0].sent)
	require.Len(t, fc.batches[0].appended, 1)
	_, ok := fc.batches[0].appended[0].(*TraceInput)
	assert.True(t, ok)
}

func TestPushTraceData_ClientSideTwoBatches(t *testing.T) {
	fc := &fakeConn{}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, cluster: true, clientSide: true, tracePayloadType: "proto"}

	require.NoError(t, e.pushTraceData(context.Background(), newTraces()))

	// client-side prepares the tempo_traces + attrs_gin batches
	require.Len(t, fc.queries, 2)
	assert.Contains(t, strings.Join(fc.queries, "\n"), "tempo_traces")
	assert.Contains(t, strings.Join(fc.queries, "\n"), "tempo_traces_attrs_gin")
	require.Len(t, fc.batches, 2)
	assert.True(t, fc.batches[0].sent)
	assert.True(t, fc.batches[1].sent)
	// span converted into a TempoTrace on the main batch
	require.NotEmpty(t, fc.batches[0].appended)
	_, ok := fc.batches[0].appended[0].(*TempoTrace)
	assert.True(t, ok)
}

func TestPushTraceData_PrepareBatchError(t *testing.T) {
	fc := &fakeConn{prepareErr: errFakePrepare}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, tracePayloadType: "json"}

	err := e.pushTraceData(context.Background(), newTraces())
	require.ErrorIs(t, err, errFakePrepare)
}

func TestPushTraceData_SendError(t *testing.T) {
	fc := &fakeConn{sendErr: errFakeSend}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, tracePayloadType: "json"}

	err := e.pushTraceData(context.Background(), newTraces())
	require.ErrorIs(t, err, errFakeSend)
}

func TestPushTraceData_AppendErrorAborts(t *testing.T) {
	fc := &fakeConn{appendErr: errFakeAppend}
	e := &tracesExporter{logger: zap.NewNop(), db: fc, tracePayloadType: "json"}

	err := e.pushTraceData(context.Background(), newTraces())
	require.ErrorIs(t, err, errFakeAppend)
	require.Len(t, fc.batches, 1)
	assert.True(t, fc.batches[0].aborted)
}

func TestExportResourceSpans_MissingClusterFlagErrors(t *testing.T) {
	e := &tracesExporter{logger: zap.NewNop(), db: &fakeConn{}}
	// bare context: the cluster flag guard must fire before touching the conn
	err := e.exportResourceSapns(context.Background(), newTraces().ResourceSpans())
	require.Error(t, err)
}

func TestTracesShutdownClosesConn(t *testing.T) {
	fc := &fakeConn{}
	e := &tracesExporter{db: fc}
	require.NoError(t, e.Shutdown(context.Background()))
	assert.True(t, fc.closed)
}
