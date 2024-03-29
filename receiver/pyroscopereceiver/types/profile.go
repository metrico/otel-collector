package profile

import (
	"bytes"

	pprof_proto "github.com/google/pprof/profile"
)

// Enumeration of low-level payload type that are supported by the pipeline
type PayloadType uint8

const (
	Pprof PayloadType = iota
)

// Auxiliary profile metadata
type Metadata struct {
	SampleRateHertz uint64
}

// Represents the high-level type of a profile
type ProfileType struct {
	Type       string
	PeriodType string
	PeriodUnit string
	SampleType []string
	SampleUnit []string
}

// Parser IR for profile processing
type ProfileIR struct {
	Type             ProfileType
	DurationNano     int64
	TimeStampNao     int64
	Payload          *bytes.Buffer
	PayloadType      PayloadType
	ValueAggregation interface{}
	Profile          *pprof_proto.Profile
}

type SampleType struct {
	Key   string
	Sum   int64
	Count int32
}
