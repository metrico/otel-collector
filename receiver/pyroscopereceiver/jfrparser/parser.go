package jfrparser

import (
	"bytes"
	"fmt"
	"io"

	pprof_proto "github.com/google/pprof/profile"
	jfr_parser "github.com/grafana/jfr-parser/parser"
	jfr_types "github.com/grafana/jfr-parser/parser/types"
	profile_types "github.com/metrico/otel-collector/receiver/pyroscopereceiver/types"
)

type sampleType uint8

const (
	sampleTypeCpu sampleType = iota
	sampleTypeWall
	sampleTypeInNewTlab
	sampleTypeOutsideTlab
	sampleTypeLock
	sampleTypeThreadPark
	sampleTypeLiveObject

	sampleTypeCount
)

type profileWrapper struct {
	pprof *pprof_proto.Profile
	prof  profile_types.ProfileIR
}

type jfrPprofParser struct {
	jfrParser *jfr_parser.Parser

	proftab [sampleTypeCount]*profileWrapper                  // <sample type, (profile, pprof)>
	samptab [sampleTypeCount]map[uint32]uint32                // <extern jfr stacktrace id,matching pprof sample array index>
	loctab  [sampleTypeCount]map[uint32]*pprof_proto.Location // <extern jfr funcid, pprof location>
}

var typetab = []profile_types.ProfileType{
	sampleTypeCpu:         {Type: "process_cpu", PeriodType: "cpu", PeriodUnit: "nanoseconds", SampleType: []string{"cpu"}, SampleUnit: []string{"nanoseconds"}},
	sampleTypeWall:        {Type: "wall", PeriodType: "wall", PeriodUnit: "nanoseconds", SampleType: []string{"wall"}, SampleUnit: []string{"nanoseconds"}},
	sampleTypeInNewTlab:   {Type: "memory", PeriodType: "space", PeriodUnit: "bytes", SampleType: []string{"alloc_in_new_tlab_objects", "alloc_in_new_tlab_bytes"}, SampleUnit: []string{"count", "bytes"}},
	sampleTypeOutsideTlab: {Type: "memory", PeriodType: "space", PeriodUnit: "bytes", SampleType: []string{"alloc_outside_tlab_objects", "alloc_outside_tlab_bytes"}, SampleUnit: []string{"count", "bytes"}},
	sampleTypeLock:        {Type: "mutex", PeriodType: "mutex", PeriodUnit: "count", SampleType: []string{"contentions", "delay"}, SampleUnit: []string{"count", "nanoseconds"}},
	sampleTypeThreadPark:  {Type: "block", PeriodType: "block", PeriodUnit: "count", SampleType: []string{"contentions", "delay"}, SampleUnit: []string{"count", "nanoseconds"}},
	sampleTypeLiveObject:  {Type: "memory", PeriodType: "objects", PeriodUnit: "count", SampleType: []string{"live"}, SampleUnit: []string{"count"}},
}

// Creates a jfr parser that parse the accepted jfr buffer
func NewJfrPprofParser() *jfrPprofParser {
	return &jfrPprofParser{}
}

// Parses the jfr buffer into pprof
func (pa *jfrPprofParser) Parse(jfr *bytes.Buffer, md profile_types.Metadata, maxDecompressedSizeBytes int64) ([]profile_types.ProfileIR, error) {
	var (
		period int64
		event  string
		values = [2]int64{1, 0}
	)

	pa.jfrParser = jfr_parser.NewParser(jfr.Bytes(), jfr_parser.Options{SymbolProcessor: nopSymbolProcessor})

	if md.SampleRateHertz == 0 {
		period = 1
	} else {
		period = 1e9 / int64(md.SampleRateHertz)
	}

	for {
		t, err := pa.jfrParser.ParseEvent()
		if err != nil {
			if io.EOF == err {
				break
			}
			return nil, fmt.Errorf("jfr-parser ParseEvent error: %w", err)
		}

		switch t {
		case pa.jfrParser.TypeMap.T_EXECUTION_SAMPLE:
			values[0] = 1 * int64(period)
			ts := pa.jfrParser.GetThreadState(pa.jfrParser.ExecutionSample.State)
			if ts != nil && ts.Name == "STATE_RUNNABLE" {
				pa.addStacktrace(sampleTypeCpu, pa.jfrParser.ExecutionSample.StackTrace, values[:1])
			}
			// TODO: this code is from github/grafana/pyroscope, need to validate that the query simulator handles this branch as expected for wall
			if event == "wall" {
				pa.addStacktrace(sampleTypeWall, pa.jfrParser.ExecutionSample.StackTrace, values[:1])
			}
		case pa.jfrParser.TypeMap.T_ALLOC_IN_NEW_TLAB:
			values[1] = int64(pa.jfrParser.ObjectAllocationInNewTLAB.TlabSize)
			pa.addStacktrace(sampleTypeInNewTlab, pa.jfrParser.ObjectAllocationInNewTLAB.StackTrace, values[:2])
		case pa.jfrParser.TypeMap.T_ALLOC_OUTSIDE_TLAB:
			values[1] = int64(pa.jfrParser.ObjectAllocationOutsideTLAB.AllocationSize)
			pa.addStacktrace(sampleTypeOutsideTlab, pa.jfrParser.ObjectAllocationOutsideTLAB.StackTrace, values[:2])
		case pa.jfrParser.TypeMap.T_MONITOR_ENTER:
			values[1] = int64(pa.jfrParser.JavaMonitorEnter.Duration)
			pa.addStacktrace(sampleTypeLock, pa.jfrParser.JavaMonitorEnter.StackTrace, values[:2])
		case pa.jfrParser.TypeMap.T_THREAD_PARK:
			values[1] = int64(pa.jfrParser.ThreadPark.Duration)
			pa.addStacktrace(sampleTypeThreadPark, pa.jfrParser.ThreadPark.StackTrace, values[:2])
		case pa.jfrParser.TypeMap.T_LIVE_OBJECT:
			pa.addStacktrace(sampleTypeLiveObject, pa.jfrParser.LiveObject.StackTrace, values[:1])
		case pa.jfrParser.TypeMap.T_ACTIVE_SETTING:
			if pa.jfrParser.ActiveSetting.Name == "event" {
				event = pa.jfrParser.ActiveSetting.Value
			}
		}
	}

	ps := make([]profile_types.ProfileIR, 0)
	for _, pr := range pa.proftab {
		if nil != pr {
			// assuming jfr-pprof conversion should not expand memory footprint, transitively applying jfr limit on pprof
			pr.prof.Payload = &bytes.Buffer{} // TODO: consider pre-allocate a buffer sized relatively to jfr, consider event distribution for example low probability live event as part of alloc profile, something better than: compress.PrepareBuffer(pa.maxDecompressedSizeBytes)
			pr.pprof.WriteUncompressed(pr.prof.Payload)
			ps = append(ps, pr.prof)
		}
	}
	return ps, nil
}

func nopSymbolProcessor(ref *jfr_types.SymbolList) {}

func (pa *jfrPprofParser) addStacktrace(sampleType sampleType, ref jfr_types.StackTraceRef, values []int64) {
	pr := pa.getProfile(sampleType)
	if nil == pr {
		pr = pa.addProfile(sampleType)
	}

	st := pa.jfrParser.GetStacktrace(ref)
	if nil == st {
		return
	}

	addValues := func(dst []int64) {
		for i, val := range values { // value order matters
			dst[i] += val
		}
	}

	iref := uint32(ref)
	// sum values for a stacktrace that already exist
	sample := pa.getSample(sampleType, pr.pprof, iref)
	if sample != nil {
		addValues(sample.Value)
		return
	}

	// encode a new stacktrace in pprof
	ls := make([]*pprof_proto.Location, 0, len(st.Frames))
	for i := 0; i < len(st.Frames); i++ {
		f := st.Frames[i]
		imethod := uint32(f.Method)

		// append location that already exists
		// TODO: fix a bug where multiple f.Method vals are mapped to same func name but creates distinct pprof func, example:
		// function {
		//   id: 119
		//   name: 118
		// }
		// function {
		//   id: 120
		//   name: 118
		// }
		// $ cat <pb_path> | protoc --decode=perftools.profiles.Profile $HOME/go/pkg/mod/github.com/google/pprof@<version>/proto/profile.proto --proto_path $HOME/go/pkg/mod/github.com/google/pprof@<version>/proto/ >> /tmp/pprof.txt
		l := pa.getLocation(sampleType, imethod)
		if l != nil {
			ls = append(ls, l)
			continue
		}

		// append new location
		m := pa.jfrParser.GetMethod(f.Method)
		if m != nil {
			cls := pa.jfrParser.GetClass(m.Type)
			if cls != nil {
				clsName := pa.jfrParser.GetSymbolString(cls.Name)
				methodName := pa.jfrParser.GetSymbolString(m.Name)
				l = pa.appendLocation(sampleType, pr.pprof, clsName+"."+methodName, imethod)
				ls = append(ls, l)
			}
		}
	}

	newv := make([]int64, len(values))
	addValues(newv)
	pa.appendSample(sampleType, pr.pprof, ls, newv, iref)
}

func (pa *jfrPprofParser) getProfile(sampleType sampleType) *profileWrapper {
	return pa.proftab[sampleType]
}

func (pa *jfrPprofParser) addProfile(sampleType sampleType) *profileWrapper {
	pw := &profileWrapper{
		prof: profile_types.ProfileIR{
			Type:        typetab[sampleType],
			PayloadType: profile_types.Pprof,
		},
		pprof: &pprof_proto.Profile{},
	}
	pa.proftab[sampleType] = pw

	// add sample types and units to keep the pprof valid for libraries
	for i, t := range pw.prof.Type.SampleType {
		pa.appendSampleType(pw.pprof, t, pw.prof.Type.SampleUnit[i])
	}
	return pw
}

func (pa *jfrPprofParser) appendSampleType(prof *pprof_proto.Profile, typ, unit string) {
	prof.SampleType = append(prof.SampleType, &pprof_proto.ValueType{
		Type: typ,
		Unit: unit,
	})
}

func (pa *jfrPprofParser) getSample(sampleType sampleType, prof *pprof_proto.Profile, externStacktraceRef uint32) *pprof_proto.Sample {
	m := pa.samptab[sampleType]
	if nil == m {
		return nil
	}
	i, ok := m[externStacktraceRef]
	if !ok {
		return nil
	}
	return prof.Sample[i]
}

func (pa *jfrPprofParser) appendSample(sampleType sampleType, prof *pprof_proto.Profile, locations []*pprof_proto.Location, values []int64, externStacktraceRef uint32) {
	sample := &pprof_proto.Sample{
		Location: locations,
		Value:    values,
	}
	m := pa.samptab[sampleType]
	if nil == m {
		m = make(map[uint32]uint32)
		pa.samptab[sampleType] = m
	}
	m[externStacktraceRef] = uint32(len(prof.Sample))
	prof.Sample = append(prof.Sample, sample)
}

func (pa *jfrPprofParser) getLocation(sampleType sampleType, externFuncId uint32) *pprof_proto.Location {
	m := pa.loctab[sampleType]
	if nil == m {
		return nil
	}
	l, ok := m[externFuncId]
	if !ok {
		return nil
	}
	return l
}

func (pa *jfrPprofParser) appendLocation(sampleType sampleType, prof *pprof_proto.Profile, frame string, externFuncId uint32) *pprof_proto.Location {
	// append new function of the new location
	newf := &pprof_proto.Function{
		ID:   uint64(len(prof.Function)) + 1, // starts with 1 not 0
		Name: frame,
	}
	prof.Function = append(prof.Function, newf)

	// append new location with a single line referencing the new function, ignoring inlining without a line number
	newl := &pprof_proto.Location{
		ID:   uint64(len(prof.Location)) + 1, // starts with 1 not 0
		Line: []pprof_proto.Line{{Function: newf}},
	}

	prof.Location = append(prof.Location, newl)
	m := pa.loctab[sampleType]
	if nil == m {
		m = make(map[uint32]*pprof_proto.Location)
		pa.loctab[sampleType] = m
	}
	m[externFuncId] = newl
	return newl
}
