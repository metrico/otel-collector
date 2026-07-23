package ch

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// schema reference: https://github.com/metrico/gigapipe/blob/master/ctrl/qryn/sql/profiles.sql
const (
	columnTimestampNs      = "timestamp_ns"
	columnType             = "type"
	columnServiceName      = "service_name"
	columnSampleTypesUnits = "sample_types_units"
	columnPeriodType       = "period_type"
	columnPeriodUnit       = "period_unit"
	columnTags             = "tags"
	columnDurationNs       = "duration_ns"
	columnPayloadType      = "payload_type"
	columnPayloaf          = "payload"
	columnValuesAgg        = "values_agg"
)

type clickhouseAccessNativeColumnar struct {
	conn driver.Conn

	logger *zap.Logger
}

type tuple []any

// Connects to clickhouse and checks the connection's health, returning a new native client
func NewClickhouseAccessNativeColumnar(opts *clickhouse.Options, logger *zap.Logger) (*clickhouseAccessNativeColumnar, error) {
	c, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}
	nc := &clickhouseAccessNativeColumnar{
		conn:   c,
		logger: logger,
	}
	if err = c.Ping(context.Background()); err != nil {
		nc.logger.Warn(fmt.Sprintf("failed to ping clickhouse server: %s", err.Error()))
	}
	return nc, nil
}

func valueToStringArray(v pcommon.Value) ([]string, error) {
	raw := v.AsRaw()
	var (
		rawArray []any
		ok       bool
	)

	if rawArray, ok = raw.([]any); !ok {
		return nil, fmt.Errorf("failed to convert value to []any")
	}
	res := make([]string, len(rawArray))
	for i, v := range rawArray {
		if res[i], ok = v.(string); !ok {
			return nil, fmt.Errorf("failed to convert value [%d] to string", i)
		}
	}
	return res, nil
}

// Inserts a profile batch into the clickhouse server using columnar native protocol
func (ch *clickhouseAccessNativeColumnar) InsertBatch(ls plog.Logs) (int, error) {
	b, err := ch.conn.PrepareBatch(context.Background(), "INSERT INTO profiles_input")
	if err != nil {
		return 0, fmt.Errorf("failed to prepare batch: %w", err)
	}

	// this implementation is tightly coupled to how pyroscope-java and pyroscopereceiver work
	cols := &profileColumns{}
	// Return pooled trees to the pool on every exit path (success or error).
	defer cols.release()

	rl := ls.ResourceLogs()
	for i := 0; i < rl.Len(); i++ {
		sls := rl.At(i).ScopeLogs()
		// Iterate every scope, not just the first, so records from additional
		// instrumentation scopes are not silently dropped.
		for si := 0; si < sls.Len(); si++ {
			lr := sls.At(si).LogRecords()
			for s := 0; s < lr.Len(); s++ {
				if err := cols.appendRecord(lr.At(s)); err != nil {
					return 0, err
				}
				idx := cols.rowCount() - 1
				ch.logger.Debug(
					fmt.Sprintf("batch insert prepared row %d", idx),
					zap.Uint64(columnTimestampNs, cols.timestampNs[idx]),
					zap.String(columnType, cols.typ[idx]),
					zap.String(columnServiceName, cols.serviceName[idx]),
					zap.String(columnPeriodType, cols.periodType[idx]),
					zap.String(columnPeriodUnit, cols.periodUnit[idx]),
					zap.Any(columnSampleTypesUnits, cols.sampleTypesUnits[idx]),
					zap.String(columnPayloadType, cols.payloadType[idx]),
				)
			}
		}
	}

	// The columnar insert requires every column to have the same length. Guard
	// that invariant explicitly: a mismatch means a record skipped one of the
	// appends, so fail with a clear error instead of a cryptic driver failure
	// or silent data loss (see #120).
	if err := cols.consistent(); err != nil {
		return 0, fmt.Errorf("refusing to insert misaligned profile batch: %w", err)
	}

	// column order here should match table column order
	if err := b.Column(0).Append(cols.timestampNs); err != nil {
		return 0, err
	}
	if err := b.Column(1).Append(cols.typ); err != nil {
		return 0, err
	}
	if err := b.Column(2).Append(cols.serviceName); err != nil {
		return 0, err
	}
	if err := b.Column(3).Append(cols.sampleTypesUnits); err != nil {
		return 0, err
	}
	if err := b.Column(4).Append(cols.periodType); err != nil {
		return 0, err
	}
	if err := b.Column(5).Append(cols.periodUnit); err != nil {
		return 0, err
	}
	if err := b.Column(6).Append(cols.tags); err != nil {
		return 0, err
	}
	if err := b.Column(7).Append(cols.durationNs); err != nil {
		return 0, err
	}
	if err := b.Column(8).Append(cols.payloadType); err != nil {
		return 0, err
	}

	if err := b.Column(9).Append(cols.payload); err != nil {
		return 0, err
	}
	if err := b.Column(10).Append(cols.valuesAgg); err != nil {
		return 0, err
	}

	if err := b.Column(11).Append(cols.tree); err != nil {
		return 0, err
	}
	if err := b.Column(12).Append(cols.functions); err != nil {
		return 0, err
	}
	if err := b.Send(); err != nil {
		return 0, err
	}
	return cols.rowCount(), nil
}

// profileColumns accumulates one profiles_input row per profile record across
// all columns. The columnar ClickHouse insert requires every column slice to
// have the same length, so appendRecord always appends exactly one element to
// every column, and consistent() verifies the invariant before inserting.
type profileColumns struct {
	timestampNs      []uint64
	typ              []string
	serviceName      []string
	sampleTypesUnits [][]tuple
	periodType       []string
	periodUnit       []string
	tags             [][]tuple
	durationNs       []uint64
	payloadType      []string
	payload          [][]byte
	valuesAgg        [][]tuple
	tree             [][]tuple
	functions        [][]tuple
	pooledTrees      []*PooledTree
}

// rowCount is the number of records appended so far.
func (c *profileColumns) rowCount() int {
	return len(c.timestampNs)
}

// appendRecord extracts one profile record into every column. It appends
// exactly one element per column, tolerating optional/malformed fields rather
// than desyncing the columns or panicking.
func (c *profileColumns) appendRecord(r plog.LogRecord) error {
	m := r.Attributes()
	c.timestampNs = append(c.timestampNs, uint64(r.Timestamp()))

	tmp, _ := m.Get(columnType)
	c.typ = append(c.typ, tmp.AsString())

	tmp, _ = m.Get(columnServiceName)
	c.serviceName = append(c.serviceName, tmp.AsString())

	sampleTypes, _ := m.Get("sample_types")
	sampleUnits, _ := m.Get("sample_units")
	sampleTypesArray, err := valueToStringArray(sampleTypes)
	if err != nil {
		return err
	}
	sampleUnitsArray, err := valueToStringArray(sampleUnits)
	if err != nil {
		return err
	}
	sampleTypesUnitsItem := make([]tuple, len(sampleTypesArray))
	for n, v := range sampleTypesArray {
		sampleTypesUnitsItem[n] = tuple{v, sampleUnitsArray[n]}
	}
	c.sampleTypesUnits = append(c.sampleTypesUnits, sampleTypesUnitsItem)

	tmp, _ = m.Get(columnPeriodType)
	c.periodType = append(c.periodType, tmp.AsString())

	tmp, _ = m.Get(columnPeriodUnit)
	c.periodUnit = append(c.periodUnit, tmp.AsString())

	tmp, _ = m.Get(columnTags)
	tm := tmp.Map().AsRaw()
	tag, ti := make([]tuple, len(tm)), 0
	for k, v := range tm {
		// Tolerate non-string tag values instead of panicking on a failed
		// type assertion.
		sv, _ := v.(string)
		tag[ti] = tuple{k, sv}
		ti++
	}
	c.tags = append(c.tags, tag)

	tmp, _ = m.Get(columnDurationNs)
	dur, _ := strconv.ParseUint(tmp.Str(), 10, 64)
	c.durationNs = append(c.durationNs, dur)

	tmp, _ = m.Get(columnPayloadType)
	c.payloadType = append(c.payloadType, tmp.AsString())

	c.payload = append(c.payload, r.Body().Bytes().AsRaw())

	// values_agg is optional on the record; append an empty entry when it is
	// absent so the column stays aligned with the rest (this missing append was
	// the root cause of #120).
	if valuesAggRaw, ok := m.Get(columnValuesAgg); ok {
		valuesAggTuple, err := valueAggToTuple(&valuesAggRaw)
		if err != nil {
			return err
		}
		c.valuesAgg = append(c.valuesAgg, valuesAggTuple)
	} else {
		c.valuesAgg = append(c.valuesAgg, []tuple{})
	}

	functions, err := readFunctionsFromMap(m)
	if err != nil {
		return err
	}
	c.functions = append(c.functions, functions)

	t, err := readTreeFromMap(m)
	if err != nil {
		return err
	}
	c.pooledTrees = append(c.pooledTrees, t)
	c.tree = append(c.tree, t.data)

	return nil
}

// consistent verifies every column has exactly rowCount elements.
func (c *profileColumns) consistent() error {
	n := c.rowCount()
	for _, col := range []struct {
		name string
		got  int
	}{
		{"type", len(c.typ)},
		{"service_name", len(c.serviceName)},
		{"sample_types_units", len(c.sampleTypesUnits)},
		{"period_type", len(c.periodType)},
		{"period_unit", len(c.periodUnit)},
		{"tags", len(c.tags)},
		{"duration_ns", len(c.durationNs)},
		{"payload_type", len(c.payloadType)},
		{"payload", len(c.payload)},
		{"values_agg", len(c.valuesAgg)},
		{"tree", len(c.tree)},
		{"functions", len(c.functions)},
	} {
		if col.got != n {
			return fmt.Errorf("column %q has length %d, expected %d", col.name, col.got, n)
		}
	}
	return nil
}

// release returns the borrowed pooled trees to the pool so they can be reused.
func (c *profileColumns) release() {
	for _, t := range c.pooledTrees {
		if t != nil {
			trees.put(t)
		}
	}
	c.pooledTrees = nil
}

// Closes the clickhouse connection pool
func (ch *clickhouseAccessNativeColumnar) Shutdown() error {
	return ch.conn.Close()
}

func valueAggToTuple(value *pcommon.Value) ([]tuple, error) {
	var res []tuple
	for _, value_agg_any := range value.AsRaw().([]any) {
		value_agg_any_array, ok := value_agg_any.([]any)
		if !ok || len(value_agg_any_array) != 3 {
			return nil, fmt.Errorf("failed to convert value_agg to tuples")
		}
		res = append(res, tuple{
			value_agg_any_array[0],
			value_agg_any_array[1],
			int32(value_agg_any_array[2].(int64)),
		})
	}
	return res, nil
}

func readFunctionsFromMap(m pcommon.Map) ([]tuple, error) {
	raw, _ := m.Get("functions")
	bRaw := bytes.NewReader(raw.Bytes().AsRaw())
	size, err := binary.ReadVarint(bRaw)
	if err != nil {
		return nil, err
	}

	res := make([]tuple, size)

	for i := int64(0); i < size; i++ {
		id, err := binary.ReadUvarint(bRaw)
		if err != nil {
			return nil, err
		}
		size, err := binary.ReadVarint(bRaw)
		if err != nil {
			return nil, err
		}

		name := make([]byte, size)
		_, err = bRaw.Read(name)
		if err != nil {
			return nil, err
		}
		res[i] = tuple{id, string(name)}
	}
	return res, nil
}

type LimitedPool struct {
	m          sync.RWMutex
	pool       [20]*sync.Pool
	createPool func() *sync.Pool
}

type PooledTree struct {
	time         time.Time
	triplesCount int
	data         []tuple
	triples      []tuple
}

func (l *LimitedPool) get(quadruples int, triples int) *PooledTree {
	l.m.Lock()
	defer l.m.Unlock()
	var pool *sync.Pool
	if triples >= 20 {
		pool = l.createPool()
	} else if l.pool[triples] == nil {
		l.pool[triples] = l.createPool()
		pool = l.pool[triples]
	} else {
		pool = l.pool[triples]
	}
	tree := pool.Get().(*PooledTree)
	var redo bool
	if cap(tree.triples) < quadruples*triples {
		tree.triples = make([]tuple, quadruples*triples)
		for i := range tree.triples {
			tree.triples[i] = tuple{nil, nil, nil}
		}
		redo = true
	}
	tree.triples = tree.triples[:quadruples*triples]
	if cap(tree.data) < quadruples {
		tree.data = make([]tuple, quadruples)
		redo = true
	}
	tree.data = tree.data[:quadruples]
	if redo || tree.triplesCount != triples {
		j := 0
		for i := range tree.data {
			_triples := tree.triples[j : j+triples]
			j += triples
			tree.data[i] = tuple{nil, nil, nil, _triples}
		}
	}
	tree.triplesCount = triples
	return tree
}

func (l *LimitedPool) put(t *PooledTree) {
	l.m.Lock()
	defer l.m.Unlock()
	if t.triplesCount >= 20 {
		return
	}
	pool := l.pool[t.triplesCount]
	if time.Now().Sub(t.time) < time.Minute {
		pool.Put(t)
	}
}

var trees = LimitedPool{
	createPool: func() *sync.Pool {
		return &sync.Pool{
			New: func() interface{} {
				return &PooledTree{time: time.Now()}
			},
		}
	},
}

func readTreeFromMap(m pcommon.Map) (*PooledTree, error) {
	raw, _ := m.Get("tree")
	bRaw := bytes.NewReader(raw.Bytes().AsRaw())
	treeSize, err := binary.ReadVarint(bRaw)
	if err != nil {
		return nil, err
	}

	var res *PooledTree

	for i := int64(0); i < treeSize; i++ {
		parentId, err := binary.ReadUvarint(bRaw)
		if err != nil {
			return nil, err
		}

		fnId, err := binary.ReadUvarint(bRaw)
		if err != nil {
			return nil, err
		}

		nodeId, err := binary.ReadUvarint(bRaw)
		if err != nil {
			return nil, err
		}

		size, err := binary.ReadVarint(bRaw)
		if err != nil {
			return nil, err
		}

		if res == nil {
			res = trees.get(int(treeSize), int(size))
		}

		for j := int64(0); j < size; j++ {
			size, err := binary.ReadVarint(bRaw)
			if err != nil {
				return nil, err
			}
			name := make([]byte, size)
			_, err = bRaw.Read(name)
			if err != nil {
				return nil, err
			}

			self, err := binary.ReadVarint(bRaw)
			if err != nil {
				return nil, err
			}

			total, err := binary.ReadVarint(bRaw)
			if err != nil {
				return nil, err
			}
			res.data[i][3].([]tuple)[j][0] = name
			res.data[i][3].([]tuple)[j][1] = self
			res.data[i][3].([]tuple)[j][2] = total
		}
		res.data[i][0] = parentId
		res.data[i][1] = fnId
		res.data[i][2] = nodeId
	}

	// An empty tree (treeSize <= 0) leaves res nil. Return a valid pooled tree
	// with no rows instead, so callers can append res.data and release res via
	// trees.put without a nil-pointer dereference (see #142).
	if res == nil {
		res = trees.get(0, 0)
	}
	return res, nil
}
