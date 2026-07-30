package gigapipeexporter

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnhexAndPad(t *testing.T) {
	// shorter than size: left-padded with zeros to the target width
	out, err := unhexAndPad("0102", 8)
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 1, 2}, out)

	// exact width: unchanged
	full := "0102030405060708"
	out, err = unhexAndPad(full, 8)
	require.NoError(t, err)
	want, _ := hex.DecodeString(full)
	assert.Equal(t, want, out)

	// invalid hex: error
	_, err = unhexAndPad("zz", 8)
	require.Error(t, err)
}

func TestTraceWithTagsBatch_AppendStructSplitsTraceAndTags(t *testing.T) {
	main := &fakeBatch{}
	tags := &fakeBatch{}
	b := &traceWithTagsBatch{Batch: main, tagsBatch: tags}

	ti := &TraceInput{
		TraceID:  "0102030405060708090a0b0c0d0e0f10",
		SpanID:   "0102030405060708",
		ParentID: "0807060504030201",
		Name:     "span",
		Tags:     [][]string{{"k1", "v1"}, {"k2", "v2"}},
	}
	require.NoError(t, b.AppendStruct(ti))

	require.Len(t, main.appended, 1)
	_, ok := main.appended[0].(*TempoTrace)
	assert.True(t, ok)
	// one tag row per tag pair
	require.Len(t, tags.appended, 2)
	_, ok = tags.appended[0].(*TempoTraceTag)
	assert.True(t, ok)
}

func TestTraceWithTagsBatch_AppendStructWrongType(t *testing.T) {
	b := &traceWithTagsBatch{Batch: &fakeBatch{}, tagsBatch: &fakeBatch{}}
	err := b.AppendStruct("not a trace")
	require.Error(t, err)
}

func TestTraceWithTagsBatch_SendAndAbort(t *testing.T) {
	main := &fakeBatch{}
	tags := &fakeBatch{}
	b := &traceWithTagsBatch{Batch: main, tagsBatch: tags}

	require.NoError(t, b.Send())
	assert.True(t, main.sent)
	assert.True(t, tags.sent)

	require.NoError(t, b.Abort())
	assert.True(t, main.aborted)
	assert.True(t, tags.aborted)
}
