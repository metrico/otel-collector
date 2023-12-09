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

type metadata struct {
	period int64
}

const (
	sampleTypeCpu         = 0
	sampleTypeWall        = 1
	sampleTypeInNewTlab   = 2
	sampleTypeOutsideTlab = 3
	sampleTypeLock        = 4
	sampleTypeThreadPark  = 5
	sampleTypeLiveObject  = 6

	sampleTypeCount = 7
)

type pprof struct {
	prof   *profile_types.Profile
	_pprof *pprof_proto.Profile
}

type parser struct {
	md                       metadata
	_pa                      *jfr_parser.Parser
	maxDecompressedSizeBytes int64

	// TODO: try lazy allocation instead of redundant array
	proftab     [sampleTypeCount]*pprof          // <sample type, (profile, pprof)>
	sampleMap   map[uint32]uint32                // <extern jfr stacktrace id,matching pprof sample array index>
	locationMap map[uint32]*pprof_proto.Location // <extern jfr funcid, pprof location>
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

const (
	wall  = "wall"
	event = "event"
)

// Creates a jfr parser that parse the accepted jfr buffer
func NewJfrParser(jfr *bytes.Buffer, md profile_types.Metadata, maxDecompressedSizeBytes int64) *parser {
	var period int64
	if md.SampleRateHertz == 0 {
		period = 1
	} else {
		period = 1e9 / int64(md.SampleRateHertz)
	}
	return &parser{
		md:                       metadata{period: period},
		_pa:                      jfr_parser.NewParser(jfr.Bytes(), jfr_parser.Options{SymbolProcessor: nopSymbolProcessor}),
		maxDecompressedSizeBytes: maxDecompressedSizeBytes,

		sampleMap:   make(map[uint32]uint32),
		locationMap: make(map[uint32]*pprof_proto.Location),
	}
}

// Parses the jfr buffer into pprof
func (pa *parser) ParsePprof() ([]*profile_types.Profile, error) {
	var (
		event  string
		values = [2]int64{1, 0}
	)

	for {
		t, err := pa._pa.ParseEvent()
		if err != nil {
			if io.EOF == err {
				break
			}
			return nil, fmt.Errorf("jfr-parser ParseEvent error: %w", err)
		}

		switch t {
		case pa._pa.TypeMap.T_EXECUTION_SAMPLE:
			values[0] = 1 * int64(pa.md.period)
			ts := pa._pa.GetThreadState(pa._pa.ExecutionSample.State)
			if ts != nil && ts.Name == "STATE_RUNNABLE" {
				pa.addStacktrace(sampleTypeCpu, pa._pa.ExecutionSample.StackTrace, values[:1])
			}
			// TODO: this code is from github/grafana/pyroscope, need to validate that the query simulator handles this branch as expected for wall
			if wall == event {
				pa.addStacktrace(sampleTypeWall, pa._pa.ExecutionSample.StackTrace, values[:1])
			}
		case pa._pa.TypeMap.T_ALLOC_IN_NEW_TLAB:
			values[1] = int64(pa._pa.ObjectAllocationInNewTLAB.TlabSize)
			pa.addStacktrace(sampleTypeInNewTlab, pa._pa.ObjectAllocationInNewTLAB.StackTrace, values[:2])
		case pa._pa.TypeMap.T_ALLOC_OUTSIDE_TLAB:
			values[1] = int64(pa._pa.ObjectAllocationOutsideTLAB.AllocationSize)
			pa.addStacktrace(sampleTypeOutsideTlab, pa._pa.ObjectAllocationOutsideTLAB.StackTrace, values[:2])
		case pa._pa.TypeMap.T_MONITOR_ENTER:
			values[1] = int64(pa._pa.JavaMonitorEnter.Duration)
			pa.addStacktrace(sampleTypeLock, pa._pa.JavaMonitorEnter.StackTrace, values[:2])
		case pa._pa.TypeMap.T_THREAD_PARK:
			values[1] = int64(pa._pa.ThreadPark.Duration)
			pa.addStacktrace(sampleTypeThreadPark, pa._pa.ThreadPark.StackTrace, values[:2])
		case pa._pa.TypeMap.T_LIVE_OBJECT:
			pa.addStacktrace(sampleTypeLiveObject, pa._pa.LiveObject.StackTrace, values[:1])
		case pa._pa.TypeMap.T_ACTIVE_SETTING:
			if pa._pa.ActiveSetting.Name == event {
				event = pa._pa.ActiveSetting.Value
			}
		}
	}

	ps := make([]*profile_types.Profile, 0)
	for _, pp := range pa.proftab {
		if nil != pp {
			// assuming jfr-pprof conversion should not expand memory footprint, transitively applying jfr limit on pprof
			pp.prof.Payload = &bytes.Buffer{} // TODO: consider pre-allocate a buffer sized relatively to jfr, consider different event types for example low probability live event as part of allco profile, something better than: compress.PrepareBuffer(pa.maxDecompressedSizeBytes)
			pp._pprof.WriteUncompressed(pp.prof.Payload)
			ps = append(ps, pp.prof)
		}
	}
	return ps, nil
}

func nopSymbolProcessor(ref *jfr_types.SymbolList) {}

func (pa *parser) addStacktrace(sampleType int, ref jfr_types.StackTraceRef, values []int64) {
	p := pa.getProfile(sampleType)

	st := pa._pa.GetStacktrace(ref)
	if nil == st {
		return
	}

	addValues := func(dst []int64) {
		for i, val := range values { // value order matters
			dst[i] += val
		}
	}

	// sum values for a stacktrace that already exist
	sample := pa.getSample(p._pprof, uint32(ref))
	if sample != nil {
		addValues(sample.Value)
		return
	}

	// encode a new stacktrace in pprof
	locations := make([]*pprof_proto.Location, 0, len(st.Frames))
	for i := 0; i < len(st.Frames); i++ {
		f := st.Frames[i]

		// append location that already exists
		loc, found := pa.getLocation(uint32(f.Method))
		if found {
			locations = append(locations, loc)
			continue
		}

		// append new location
		m := pa._pa.GetMethod(f.Method)
		if m != nil {
			cls := pa._pa.GetClass(m.Type)
			if cls != nil {
				clsName := pa._pa.GetSymbolString(cls.Name)
				methodName := pa._pa.GetSymbolString(m.Name)
				loc = pa.appendLocation(p._pprof, clsName+"."+methodName, uint32(f.Method))
				locations = append(locations, loc)
			}
		}
	}

	v := make([]int64, len(values))
	addValues(v)
	pa.appendSample(p._pprof, locations, v, uint32(ref))
}

func (pa *parser) getProfile(sampleType int) *pprof {
	p := pa.proftab[sampleType]
	if nil == p {
		p = &pprof{
			prof: &profile_types.Profile{
				Type:        &typetab[sampleType],
				PayloadType: profile_types.PayloadTypePprof,
			},
			_pprof: &pprof_proto.Profile{},
		}
		pa.proftab[sampleType] = p

		// add sample types and units to keep the pprof valid for libraries
		for i, typ := range p.prof.Type.SampleType {
			pa.appendSampleType(p._pprof, typ, p.prof.Type.SampleUnit[i])
		}
	}
	return p
}

func (pa *parser) appendSampleType(prof *pprof_proto.Profile, typ, unit string) {
	prof.SampleType = append(prof.SampleType, &pprof_proto.ValueType{
		Type: typ,
		Unit: unit,
	})
}

func (pa *parser) getSample(prof *pprof_proto.Profile, externStacktraceRef uint32) *pprof_proto.Sample {
	idx, ok := pa.sampleMap[externStacktraceRef]
	if !ok {
		return nil
	}
	return prof.Sample[idx]
}

func (pa *parser) appendSample(prof *pprof_proto.Profile, locations []*pprof_proto.Location, values []int64, externStacktraceRef uint32) {
	sample := &pprof_proto.Sample{
		Location: locations,
		Value:    values,
	}
	pa.sampleMap[externStacktraceRef] = uint32(len(prof.Sample))
	prof.Sample = append(prof.Sample, sample)
}

func (pa *parser) getLocation(externFuncId uint32) (*pprof_proto.Location, bool) {
	loc, ok := pa.locationMap[externFuncId]
	return loc, ok
}

func (pa *parser) appendLocation(prof *pprof_proto.Profile, frame string, externFuncId uint32) *pprof_proto.Location {
	// append new function of the new location
	newFunc := &pprof_proto.Function{
		ID:   uint64(len(prof.Function)) + 1, // starts with 1 not 0
		Name: frame,
	}
	prof.Function = append(prof.Function, newFunc)

	// append new location with a single line referencing the new function, ignoring inlining without a line number
	newLoc := &pprof_proto.Location{
		ID:   uint64(len(prof.Location)) + 1, // starts with 1 not 0
		Line: []pprof_proto.Line{{Function: newFunc}},
	}
	prof.Location = append(prof.Location, newLoc)
	pa.locationMap[externFuncId] = newLoc
	return newLoc
}
