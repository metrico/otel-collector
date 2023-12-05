package jfrparser

import (
	"bytes"
	"fmt"
	"io"

	pprof_proto "github.com/google/pprof/profile"
	jfr_parser "github.com/grafana/jfr-parser/parser"
	"github.com/grafana/jfr-parser/parser/types"
	"github.com/grafana/jfr-parser/parser/types/def"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/compress"
	"github.com/metrico/otel-collector/receiver/pyroscopereceiver/profile"
)

type metadata struct {
	period int64
}

type parser struct {
	md                       metadata
	pa                       *jfr_parser.Parser
	maxDecompressedSizeBytes int64

	sampleMap   map[uint32]uint32                // <extern jfr stacktrace id,matching pprof sample array index>
	locationMap map[uint32]*pprof_proto.Location // <extern jfr funcid, pprof location>
}

const (
	sampleTypeCpu         = 0
	sampleTypeWall        = 1
	sampleTypeInNewTlab   = 2
	sampleTypeOutsideTlab = 3
	sampleTypeLock        = 4
	sampleTypeThreadPark  = 5
	sampleTypeLiveObject  = 6
)

var typetab = []profile.ProfileType{
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
func NewJfrParser(jfr *bytes.Buffer, md profile.Metadata, maxDecompressedSizeBytes int64) *parser {
	var period int64
	if md.SampleRateHertz == 0 {
		period = 1
	} else {
		period = 1e9 / int64(md.SampleRateHertz)
	}
	pa := jfr_parser.NewParser(jfr.Bytes(), jfr_parser.Options{SymbolProcessor: nopSymbolProcessor})
	return &parser{
		md:                       metadata{period: period},
		pa:                       pa,
		maxDecompressedSizeBytes: maxDecompressedSizeBytes,
		sampleMap:                make(map[uint32]uint32),
		locationMap:              make(map[uint32]*pprof_proto.Location),
	}
}

// Gets the type of the first data record (non-settings),
// which is determines the profile type because a profile should bind to a single type.
// The caller should not call ParseEvent() to process the event, as it is being called internally.
func (p *parser) parseType() (def.TypeID, int, string, error) {
	var event string
	for {
		t, err := p.pa.ParseEvent()
		if err != nil {
			if io.EOF == err {
				return def.TypeID(0), -1, "", fmt.Errorf("found no data records (non-settings)")
			}
			return def.TypeID(0), -1, "", fmt.Errorf("jfr-parser ParseEvent error while parsing type: %w", err)
		}

		switch t {
		case p.pa.TypeMap.T_EXECUTION_SAMPLE:
			if wall == event {
				return def.TypeID(0), sampleTypeWall, event, nil
			} else {
				return def.TypeID(0), sampleTypeCpu, event, nil
			}
		case p.pa.TypeMap.T_ALLOC_IN_NEW_TLAB:
			return def.TypeID(0), sampleTypeInNewTlab, event, nil
		case p.pa.TypeMap.T_ALLOC_OUTSIDE_TLAB:
			return def.TypeID(0), sampleTypeOutsideTlab, event, nil
		case p.pa.TypeMap.T_MONITOR_ENTER:
			return def.TypeID(0), sampleTypeLock, event, nil
		case p.pa.TypeMap.T_THREAD_PARK:
			return def.TypeID(0), sampleTypeThreadPark, event, nil
		case p.pa.TypeMap.T_LIVE_OBJECT:
			return def.TypeID(0), sampleTypeLiveObject, event, nil
		case p.pa.TypeMap.T_ACTIVE_SETTING:
			if p.pa.ActiveSetting.Name == event {
				event = p.pa.ActiveSetting.Value
			}
		}
	}
}

func (p *parser) ParsePprof() (*profile.Profile, error) {
	var (
		pprof = &pprof_proto.Profile{}
		prof  = &profile.Profile{
			PayloadType: profile.PayloadTypePprof,
		}
		event  string
		values = [2]int64{1, 0}
	)

	t, _type, event, err := p.parseType()
	if err != nil {
		return nil, err
	}
	prof.Type = typetab[_type]

	// add sample types and units to keep the pprof valid
	for i, typ := range prof.Type.SampleType {
		p.appendSampleType(pprof, typ, prof.Type.SampleUnit[i])
	}

	// first process the event that was parsed by parseType(), then proceed parsing
	for {
		switch t {
		case p.pa.TypeMap.T_EXECUTION_SAMPLE:
			values[0] = 1 * int64(p.md.period)
			ts := p.pa.GetThreadState(p.pa.ExecutionSample.State)
			if ts != nil && ts.Name == "STATE_RUNNABLE" {
				p.addStacktrace(pprof, sampleTypeCpu, p.pa.ExecutionSample.StackTrace, values[:1])
			}
			// TODO: this code is from github/grafana/pyroscope, need to validate that the query simulator handles this branch as expected for wall
			if wall == event {
				p.addStacktrace(pprof, sampleTypeWall, p.pa.ExecutionSample.StackTrace, values[:1])
			}
		case p.pa.TypeMap.T_ALLOC_IN_NEW_TLAB:
			values[1] = int64(p.pa.ObjectAllocationInNewTLAB.TlabSize)
			p.addStacktrace(pprof, sampleTypeInNewTlab, p.pa.ObjectAllocationInNewTLAB.StackTrace, values[:2])
		case p.pa.TypeMap.T_ALLOC_OUTSIDE_TLAB:
			values[1] = int64(p.pa.ObjectAllocationOutsideTLAB.AllocationSize)
			p.addStacktrace(pprof, sampleTypeOutsideTlab, p.pa.ObjectAllocationOutsideTLAB.StackTrace, values[:2])
		case p.pa.TypeMap.T_MONITOR_ENTER:
			values[1] = int64(p.pa.JavaMonitorEnter.Duration)
			p.addStacktrace(pprof, sampleTypeLock, p.pa.JavaMonitorEnter.StackTrace, values[:2])
		case p.pa.TypeMap.T_THREAD_PARK:
			values[1] = int64(p.pa.ThreadPark.Duration)
			p.addStacktrace(pprof, sampleTypeThreadPark, p.pa.ThreadPark.StackTrace, values[:2])
		case p.pa.TypeMap.T_LIVE_OBJECT:
			p.addStacktrace(pprof, sampleTypeLiveObject, p.pa.LiveObject.StackTrace, values[:1])
		case p.pa.TypeMap.T_ACTIVE_SETTING:
			if p.pa.ActiveSetting.Name == event {
				event = p.pa.ActiveSetting.Value
			}
		}

		t, err = p.pa.ParseEvent()
		if err != nil {
			if io.EOF == err {
				break
			}
			return nil, fmt.Errorf("jfr-parser ParseEvent error: %w", err)
		}
	}

	// assuming jfr-pprof conversion should not expand memory footprint
	prof.Payload = compress.PrepareBuffer(p.maxDecompressedSizeBytes)
	pprof.WriteUncompressed(prof.Payload)
	return prof, nil
}

func nopSymbolProcessor(ref *types.SymbolList) {}

func (p *parser) addStacktrace(prof *pprof_proto.Profile, int, ref types.StackTraceRef, values []int64) {
	st := p.pa.GetStacktrace(ref)
	if nil == st {
		return
	}

	addValues := func(dst []int64) {
		for i, val := range values { // value order matters
			dst[i] += val
		}
	}

	// sum values for a stacktrace that already exist
	sample := p.getSample(prof, uint32(ref))
	if sample != nil {
		addValues(sample.Value)
		return
	}

	// encode a new stacktrace in pprof
	locations := make([]*pprof_proto.Location, 0, len(st.Frames))
	for i := 0; i < len(st.Frames); i++ {
		f := st.Frames[i]

		// append location that already exists
		loc, found := p.getLocation(uint32(f.Method))
		if found {
			locations = append(locations, loc)
			continue
		}

		// append new location
		m := p.pa.GetMethod(f.Method)
		if m != nil {
			cls := p.pa.GetClass(m.Type)
			if cls != nil {
				clsName := p.pa.GetSymbolString(cls.Name)
				methodName := p.pa.GetSymbolString(m.Name)
				loc = p.appendLocation(prof, clsName+"."+methodName, uint32(f.Method))
				locations = append(locations, loc)
			}
		}
	}

	v := make([]int64, len(values))
	addValues(v)
	p.appendSample(prof, locations, v, uint32(ref))
}

func (p *parser) appendSampleType(prof *pprof_proto.Profile, typ, unit string) {
	prof.SampleType = append(prof.SampleType, &pprof_proto.ValueType{
		Type: typ,
		Unit: unit,
	})
}

func (p *parser) getSample(prof *pprof_proto.Profile, externStacktraceRef uint32) *pprof_proto.Sample {
	idx, ok := p.sampleMap[externStacktraceRef]
	if !ok {
		return nil
	}
	return prof.Sample[idx]
}

func (p *parser) appendSample(prof *pprof_proto.Profile, locations []*pprof_proto.Location, values []int64, externStacktraceRef uint32) {
	sample := &pprof_proto.Sample{
		Location: locations,
		Value:    values,
	}
	p.sampleMap[externStacktraceRef] = uint32(len(prof.Sample))
	prof.Sample = append(prof.Sample, sample)
}

func (p *parser) getLocation(externFuncId uint32) (*pprof_proto.Location, bool) {
	loc, ok := p.locationMap[externFuncId]
	return loc, ok
}

func (p *parser) appendLocation(prof *pprof_proto.Profile, frame string, externFuncId uint32) *pprof_proto.Location {
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
	p.locationMap[externFuncId] = newLoc
	return newLoc
}
