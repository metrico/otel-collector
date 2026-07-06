package ch

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// buildProfileRecord constructs a LogRecord shaped like what pyroscopereceiver
// emits, with just enough valid data for appendRecord to consume it.
func buildProfileRecord(t *testing.T, withValuesAgg bool, tags map[string]any) plog.LogRecord {
	t.Helper()
	lr := plog.NewLogRecord()
	a := lr.Attributes()
	a.PutStr(columnType, "process_cpu")
	a.PutStr(columnServiceName, "svc")

	st := a.PutEmptySlice("sample_types")
	st.AppendEmpty().SetStr("cpu")
	su := a.PutEmptySlice("sample_units")
	su.AppendEmpty().SetStr("nanoseconds")

	a.PutStr(columnPeriodType, "cpu")
	a.PutStr(columnPeriodUnit, "nanoseconds")

	tagMap := a.PutEmptyMap(columnTags)
	for k, v := range tags {
		switch tv := v.(type) {
		case string:
			tagMap.PutStr(k, tv)
		case int:
			tagMap.PutInt(k, int64(tv))
		}
	}

	a.PutStr(columnDurationNs, "10")
	a.PutStr(columnPayloadType, "0")

	// functions: varint size 0 -> no entries.
	a.PutEmptyBytes("functions").FromRaw([]byte{0x00})
	// tree: treeSize 1 (varint zig-zag of 1 == 0x02), one node with 0 children.
	a.PutEmptyBytes("tree").FromRaw([]byte{0x02, 0x00, 0x00, 0x00, 0x00})

	if withValuesAgg {
		va := a.PutEmptySlice(columnValuesAgg)
		inner := va.AppendEmpty().SetEmptySlice()
		inner.AppendEmpty().SetStr("cpu:nanoseconds")
		inner.AppendEmpty().SetInt(160000000)
		inner.AppendEmpty().SetInt(14)
	}

	lr.Body().SetEmptyBytes().FromRaw([]byte("payload"))
	return lr
}

// Regression test for #120: a record without values_agg must not desync the
// columns. Before the fix, values_agg was only appended when present, so a
// mixed batch produced unequal column lengths and the whole insert failed.
func TestProfileColumns_MissingValuesAggStaysAligned(t *testing.T) {
	cols := &profileColumns{}

	require.NoError(t, cols.appendRecord(buildProfileRecord(t, true, nil)))
	require.NoError(t, cols.appendRecord(buildProfileRecord(t, false, nil)))
	defer cols.release()

	assert.Equal(t, 2, cols.rowCount())
	assert.Len(t, cols.valuesAgg, 2, "values_agg must have one entry per record")
	assert.NoError(t, cols.consistent(), "all columns must stay length-aligned")
}

// Non-string tag values used to panic on tag[j] = tuple{k, v.(string)}.
func TestProfileColumns_NonStringTagDoesNotPanic(t *testing.T) {
	cols := &profileColumns{}
	rec := buildProfileRecord(t, true, map[string]any{"host": "a", "count": 5})

	assert.NotPanics(t, func() {
		require.NoError(t, cols.appendRecord(rec))
	})
	defer cols.release()

	assert.Equal(t, 1, cols.rowCount())
	assert.Len(t, cols.tags[0], 2)
	assert.NoError(t, cols.consistent())
}

// consistent() must reject a batch whose columns drifted out of alignment.
func TestProfileColumns_ConsistentDetectsMismatch(t *testing.T) {
	cols := &profileColumns{
		timestampNs: []uint64{1, 2},
		typ:         []string{"a", "b"},
		serviceName: []string{"s"}, // deliberately short
	}
	err := cols.consistent()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service_name")
}
