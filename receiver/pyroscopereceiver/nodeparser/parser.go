package nodeparser

import (
	"bytes"
	"fmt"

	pprof_proto "github.com/google/pprof/profile"
	profile_types "github.com/metrico/otel-collector/receiver/pyroscopereceiver/types"
)

type sampleType uint8

const (
	sampleTypeCpu sampleType = iota
	sampleTypeCount
)

type profileWrapper struct {
	pprof *pprof_proto.Profile
	prof  profile_types.ProfileIR
}

type nodePprofParser struct {
	proftab [sampleTypeCount]*profileWrapper                  // <sample type, (profile, pprof)>
	samptab [sampleTypeCount]map[uint32]uint32                // <extern jfr stacktrace id,matching pprof sample array index>
	loctab  [sampleTypeCount]map[uint32]*pprof_proto.Location // <extern jfr funcid, pprof location>
}

// Creates a pprof parser that parse the accepted node buffer
func NewNodePprofParser() *nodePprofParser {
	return &nodePprofParser{}
}

func (pa *nodePprofParser) Parse(data *bytes.Buffer, md profile_types.Metadata) ([]profile_types.ProfileIR, error) {
	// Parse pprof data
	pProfData, err := pprof_proto.Parse(data)
	if err != nil {
		return nil, err
	}

	// Process pprof data and create SampleType slice
	var sampleTypes []string
	var sampleUnits []string
	var valueAggregates []profile_types.SampleType

	for i, st := range pProfData.SampleType {
		sampleTypes = append(sampleTypes, pProfData.SampleType[i].Type)
		sampleUnits = append(sampleUnits, pProfData.SampleType[i].Unit)
		sum, count := calculateSumAndCount(pProfData, i)
		valueAggregates = append(valueAggregates, profile_types.SampleType{fmt.Sprintf("%s:%s", st.Type, st.Unit), sum, count})
	}

	var profiles []profile_types.ProfileIR
	var profileType string
	switch pProfData.PeriodType.Type {
	case "cpu":
		profileType = "process_cpu"
	case "wall":
		profileType = "wall"
	case "mutex", "contentions":
		profileType = "mutex"
	case "goroutine":
		profileType = "goroutines"
	case "objects", "space", "alloc_space", "alloc_objects", "inuse", "inuse_space", "inuse_objects":
		profileType = "memory"
	case "block":
		profileType = "block"
	}

	profileTypeInfo := profile_types.ProfileType{
		PeriodType: pProfData.PeriodType.Type,
		PeriodUnit: pProfData.PeriodType.Unit,
		SampleType: sampleTypes,
		SampleUnit: sampleUnits,
		Type:       profileType,
	}

	// Create a new ProfileIR instance
	profile := profile_types.ProfileIR{
		ValueAggregation: valueAggregates,
		Type:             profileTypeInfo,
		TimeStampNao:     pProfData.TimeNanos,
		DurationNano:     pProfData.DurationNanos,
		Profile:          pProfData,
	}
	profile.Payload = new(bytes.Buffer)
	pProfData.WriteUncompressed(profile.Payload)
	// Append the profile to the result
	profiles = append(profiles, profile)

	return profiles, nil
}

func calculateSumAndCount(samples *pprof_proto.Profile, sampleTypeIndex int) (int64, int32) {
	var sum int64
	count := int32(len(samples.Sample))
	for _, sample := range samples.Sample {
		// Check if the sample has a value for the specified sample type
		if sampleTypeIndex < len(sample.Value) {
			// Accumulate the value for the specified sample type
			sum += sample.Value[sampleTypeIndex]
		}
	}

	return sum, count
}
