package gigapipeexporter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSQLBuilders_ClusterSuffix verifies each INSERT builder targets the base
// table on a single node and the _dist table when clustered.
func TestSQLBuilders_ClusterSuffix(t *testing.T) {
	tests := []struct {
		name      string
		build     func(bool) string
		baseTable string
	}{
		{"samples", samplesSQL, "samples_v3"},
		{"time_series", TimeSerieSQL, "time_series"},
		{"tempo_traces", TracesV2InputSQL, "tempo_traces"},
		{"tempo_traces_attrs_gin", TracesTagsV2InputSQL, "tempo_traces_attrs_gin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			single := tt.build(false)
			clustered := tt.build(true)

			assert.Contains(t, single, "INSERT INTO "+tt.baseTable)
			assert.NotContains(t, single, tt.baseTable+"_dist")

			assert.Contains(t, clustered, "INSERT INTO "+tt.baseTable+"_dist")
		})
	}
}

// TestTracesInputSQL_NoClusterSuffix documents that the v1 traces_input builder
// ignores the cluster flag (it always targets the same Null-engine table).
func TestTracesInputSQL_NoClusterSuffix(t *testing.T) {
	assert.Equal(t, tracesInputSQL(false), tracesInputSQL(true))
	assert.Contains(t, tracesInputSQL(true), "INSERT INTO traces_input")
	assert.NotContains(t, tracesInputSQL(true), "_dist")
}

// TestSamplesSQL_Columns guards the samples_v3 column list the AppendStruct tags
// must line up with.
func TestSamplesSQL_Columns(t *testing.T) {
	sql := samplesSQL(false)
	for _, col := range []string{"fingerprint", "timestamp_ns", "value", "string", "`type`"} {
		assert.True(t, strings.Contains(sql, col), "samples SQL missing column %q", col)
	}
}
