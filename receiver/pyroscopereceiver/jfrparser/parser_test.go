package jfrparser

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"testing"

	pprof "github.com/google/pprof/profile"
	profile_types "github.com/metrico/otel-collector/receiver/pyroscopereceiver/types"
	"github.com/stretchr/testify/require"
)

// async-profiler 4.3+ includes a jdk.types.ChunkHeader constant-pool entry.
// Older jfr-parser versions misread it and reject otherwise valid recordings.
func TestParseAsyncProfiler4(t *testing.T) {
	compressed, err := os.Open("testdata/async-profiler-4.5.jfr.gz")
	require.NoError(t, err)
	defer compressed.Close()
	reader, err := gzip.NewReader(compressed)
	require.NoError(t, err)
	defer reader.Close()
	recording, err := io.ReadAll(reader)
	require.NoError(t, err)

	profiles, err := NewJfrPprofParser().Parse(bytes.NewBuffer(recording), profile_types.Metadata{SampleRateHertz: 1000})
	require.NoError(t, err)
	require.Len(t, profiles, 1)
	profile := profiles[0]
	require.Equal(t, "process_cpu", profile.Type.Type)
	require.Equal(t, []string{"cpu"}, profile.Type.SampleType)
	require.Equal(t, []string{"nanoseconds"}, profile.Type.SampleUnit)
	require.NoError(t, profile.Profile.CheckValid())
	require.NotEmpty(t, profile.Profile.Sample)

	var total int64
	var foundBusyMain bool
	for _, sample := range profile.Profile.Sample {
		require.Len(t, sample.Value, 1)
		require.Positive(t, sample.Value[0])
		total += sample.Value[0]
		for _, location := range sample.Location {
			for _, line := range location.Line {
				if line.Function.Name == "Busy.main" {
					foundBusyMain = true
				}
			}
		}
	}
	// Independently counted with the JDK jfr tool: 258 execution samples at 1 ms each.
	require.Equal(t, int64(258_000_000), total)
	require.True(t, foundBusyMain, "the recorded Java workload must survive JFR-to-pprof conversion")
	require.Equal(t, []profile_types.SampleType{{
		Key: "cpu:nanoseconds", Sum: total, Count: int32(len(profile.Profile.Sample)),
	}}, profile.ValueAggregation)

	decoded, err := pprof.ParseData(profile.Payload.Bytes())
	require.NoError(t, err)
	require.NoError(t, decoded.CheckValid())
	var encoded bytes.Buffer
	require.NoError(t, decoded.WriteUncompressed(&encoded))
	require.Equal(t, profile.Payload.Bytes(), encoded.Bytes())
}
