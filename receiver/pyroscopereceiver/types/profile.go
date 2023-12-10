package profile

import (
	"bytes"
)

const (
	PayloadTypePprof = 0
)

type Metadata struct {
	SampleRateHertz uint64
}

type ProfileType struct {
	Type       string
	PeriodType string
	PeriodUnit string
	SampleType []string
	SampleUnit []string
}

type ProfileIR struct {
	Type        ProfileType
	Payload     *bytes.Buffer
	PayloadType uint32
}
