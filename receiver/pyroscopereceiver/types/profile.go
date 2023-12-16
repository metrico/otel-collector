package profile

import (
	"bytes"
)

// Enumeration of low-level payload type that are supported by the pipeline
const (
	PayloadTypePprof = 0
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
	Type        ProfileType
	Payload     *bytes.Buffer
	PayloadType uint32
}
