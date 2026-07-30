package gigapipeexporter

import (
	"context"
	"errors"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// errFakePrepare / errFakeSend / errFakeAppend are sentinels the tests inject to
// exercise error-propagation paths without a real ClickHouse connection.
var (
	errFakePrepare = errors.New("fake prepare batch error")
	errFakeSend    = errors.New("fake send error")
	errFakeAppend  = errors.New("fake append error")
)

// fakeConn is an in-memory chConn. It records the queries it was asked to
// prepare and hands out fakeBatch values, optionally injecting errors so the
// exporters' failure branches can be tested.
type fakeConn struct {
	queries []string
	batches []*fakeBatch
	closed  bool

	// prepareErr, when set, makes every PrepareBatch return it.
	prepareErr error
	// prepareFailOn, when non-empty, makes PrepareBatch fail (with errFakePrepare)
	// only for queries containing this substring, so a specific batch of a
	// multi-batch push can be failed selectively.
	prepareFailOn string
	// appendErr / sendErr are propagated into every batch this conn creates.
	appendErr error
	sendErr   error
}

func (c *fakeConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.queries = append(c.queries, query)
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.prepareFailOn != "" && strings.Contains(query, c.prepareFailOn) {
		return nil, errFakePrepare
	}
	b := &fakeBatch{appendErr: c.appendErr, sendErr: c.sendErr}
	c.batches = append(c.batches, b)
	return b, nil
}

func (c *fakeConn) Close() error {
	c.closed = true
	return nil
}

// fakeBatch is an in-memory driver.Batch. It records appended structs and the
// terminal call (Send/Abort) so tests can assert what the exporter produced.
type fakeBatch struct {
	appended []any
	sent     bool
	aborted  bool
	flushed  int
	closed   bool

	appendErr error
	sendErr   error
	abortErr  error
}

func (b *fakeBatch) Abort() error {
	b.aborted = true
	return b.abortErr
}

func (b *fakeBatch) Append(v ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.appended = append(b.appended, v...)
	return nil
}

func (b *fakeBatch) AppendStruct(v any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.appended = append(b.appended, v)
	return nil
}

func (b *fakeBatch) Column(int) driver.BatchColumn { return nil }

func (b *fakeBatch) Flush() error {
	b.flushed++
	return nil
}

func (b *fakeBatch) Send() error {
	if b.sendErr != nil {
		return b.sendErr
	}
	b.sent = true
	return nil
}

func (b *fakeBatch) IsSent() bool                { return b.sent }
func (b *fakeBatch) Rows() int                   { return len(b.appended) }
func (b *fakeBatch) Columns() []column.Interface { return nil }

func (b *fakeBatch) Close() error {
	b.closed = true
	return nil
}

// --- pdata fixture helpers -------------------------------------------------

// newLogs builds a plog.Logs with a single record carrying the given body and
// severity, under one resource attribute (service.name).
func newLogs(body string, severity plog.SeverityNumber) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr(attrServiceName, "test-service")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(body)
	lr.SetSeverityNumber(severity)
	return ld
}

// newTraces builds a ptrace.Traces with a single span under one resource
// (service.name) and instrumentation scope.
func newTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr(attrServiceName, "test-service")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")
	ss.Scope().SetVersion("v1.2.3")
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span.SetParentSpanID(pcommon.SpanID([8]byte{8, 7, 6, 5, 4, 3, 2, 1}))
	span.SetStartTimestamp(pcommon.Timestamp(1_000_000_000))
	span.SetEndTimestamp(pcommon.Timestamp(2_000_000_000))
	span.Attributes().PutStr("span.attr", "v")
	return td
}

// newGaugeMetrics builds a pmetric.Metrics with a single gauge data point.
func newGaugeMetrics(name string, value float64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr(attrServiceName, "test-service")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(value)
	dp.SetTimestamp(pcommon.Timestamp(1_000_000_000))
	dp.Attributes().PutStr("dp.attr", "v")
	return md
}
