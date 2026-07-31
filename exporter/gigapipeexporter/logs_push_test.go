package gigapipeexporter

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func TestPushLogsData_SingleNode(t *testing.T) {
	fc := &fakeConn{}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: formatRaw}

	require.NoError(t, e.pushLogsData(context.Background(), newLogs("hello", plog.SeverityNumberInfo)))

	require.Len(t, fc.queries, 2)
	joined := strings.Join(fc.queries, "\n")
	assert.Contains(t, joined, "samples_v3")
	assert.Contains(t, joined, "time_series")
	require.Len(t, fc.batches, 2)
	assert.True(t, fc.batches[0].sent)
	assert.True(t, fc.batches[1].sent)
	// one sample row appended for the single log record
	require.Len(t, fc.batches[0].appended, 1)
	_, ok := fc.batches[0].appended[0].(*Sample)
	assert.True(t, ok)
}

func TestPushLogsData_PrepareBatchError(t *testing.T) {
	fc := &fakeConn{prepareErr: errFakePrepare}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: formatRaw}
	err := e.pushLogsData(context.Background(), newLogs("x", plog.SeverityNumberInfo))
	require.ErrorIs(t, err, errFakePrepare)
}

func TestPushLogsData_SendError(t *testing.T) {
	fc := &fakeConn{sendErr: errFakeSend}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: formatRaw}
	err := e.pushLogsData(context.Background(), newLogs("x", plog.SeverityNumberInfo))
	require.ErrorIs(t, err, errFakeSend)
}

func TestPushLogsData_TimeSeriesBatchError(t *testing.T) {
	fc := &fakeConn{prepareFailOn: "time_series"}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: formatRaw}
	err := e.pushLogsData(context.Background(), newLogs("x", plog.SeverityNumberInfo))
	require.ErrorIs(t, err, errFakePrepare)
}

func TestPushLogsData_InvalidFormatErrors(t *testing.T) {
	fc := &fakeConn{}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: "bogus"}
	err := e.pushLogsData(context.Background(), newLogs("x", plog.SeverityNumberInfo))
	require.Error(t, err)
	// conversion fails before any batch is prepared
	assert.Empty(t, fc.queries)
}

func TestConvertLogToLine_JSONMapBody(t *testing.T) {
	log := plog.NewLogRecord()
	log.Body().SetEmptyMap().PutStr("k", "v")
	log.SetSeverityText("INFO")
	log.Attributes().PutStr("a", "b")
	res := newLogs("x", plog.SeverityNumberInfo).ResourceLogs().At(0).Resource()

	line, err := convertLogToLine(log, res, formatJSON)
	require.NoError(t, err)
	assert.Contains(t, line, `"k":"v"`)
	assert.Contains(t, line, `"severity":"INFO"`)
}

func TestConvertLogToLine_LogfmtRichBody(t *testing.T) {
	log := plog.NewLogRecord()
	body := log.Body().SetEmptyMap()
	body.PutStr("msg", "hello")
	body.PutInt("n", 3)
	body.PutBool("ok", true)
	body.PutDouble("f", 1.5)
	body.PutEmptySlice("s").AppendEmpty().SetStr("e0")
	log.SetTraceID([16]byte{1})
	log.SetSpanID([8]byte{2})
	log.SetSeverityText("WARN")
	log.Attributes().PutStr("attr", "av")
	res := newLogs("x", plog.SeverityNumberInfo).ResourceLogs().At(0).Resource()

	line, err := convertLogToLine(log, res, formatLogfmt)
	require.NoError(t, err)
	assert.Contains(t, line, "msg=hello")
	assert.Contains(t, line, "traceID=")
	assert.Contains(t, line, "severity=WARN")
	assert.Contains(t, line, "attribute_attr=av")
}

func TestPushLogsData_LogfmtFormat(t *testing.T) {
	fc := &fakeConn{}
	e := &logsExporter{logger: zap.NewNop(), db: fc, format: formatLogfmt}
	require.NoError(t, e.pushLogsData(context.Background(), newLogs("level=info msg=hi", plog.SeverityNumberInfo)))
	require.Len(t, fc.batches, 2)
	assert.True(t, fc.batches[0].sent)
}

func TestLogsShutdownClosesConn(t *testing.T) {
	fc := &fakeConn{}
	e := &logsExporter{db: fc}
	require.NoError(t, e.Shutdown(context.Background()))
	assert.True(t, fc.closed)
}
