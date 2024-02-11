package ch

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// schema reference: https://github.com/metrico/qryn/blob/master/lib/db/maintain/scripts.js
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
	timestamp_ns := make([]uint64, 0)
	typ := make([]string, 0)
	service_name := make([]string, 0)
	values_agg := make([][]tuple, 0)
	sample_types_units := make([][]tuple, 0)
	period_type := make([]string, 0)
	period_unit := make([]string, 0)
	tags := make([][]tuple, 0)
	duration_ns := make([]uint64, 0)
	payload_type := make([]string, 0)
	payload := make([][]byte, 0)

	rl := ls.ResourceLogs()
	var (
		lr     plog.LogRecordSlice
		r      plog.LogRecord
		m      pcommon.Map
		tmp    pcommon.Value
		tm     map[string]any
		offset int
		s      int
		idx    int
	)
	for i := 0; i < rl.Len(); i++ {
		lr = rl.At(i).ScopeLogs().At(0).LogRecords()
		for s = 0; s < lr.Len(); s++ {
			r = lr.At(s)
			m = r.Attributes()
			timestamp_ns = append(timestamp_ns, uint64(r.Timestamp()))

			tmp, _ = m.Get(columnType)
			typ = append(typ, tmp.AsString())

			tmp, _ = m.Get(columnServiceName)
			service_name = append(service_name, tmp.AsString())

			sample_types, _ := m.Get("sample_types")
			sample_units, _ := m.Get("sample_units")
			sample_types_array, err := valueToStringArray(sample_types)
			if err != nil {
				return 0, err
			}
			sample_units_array, err := valueToStringArray(sample_units)
			if err != nil {
				return 0, err
			}
			values_agg_raw, ok := m.Get(columnValuesAgg)
			if ok {
				values_agg_tuple, err := valueAggToTuple(&values_agg_raw)
				if err != nil {
					return 0, err
				}
				values_agg = append(values_agg, values_agg_tuple)
			}
			sample_types_units_item := make([]tuple, len(sample_types_array))
			for i, v := range sample_types_array {
				sample_types_units_item[i] = tuple{v, sample_units_array[i]}
			}
			sample_types_units = append(sample_types_units, sample_types_units_item)

			tmp, _ = m.Get(columnPeriodType)
			period_type = append(period_type, tmp.AsString())

			tmp, _ = m.Get(columnPeriodUnit)
			period_unit = append(period_unit, tmp.AsString())

			tmp, _ = m.Get(columnTags)
			tm = tmp.Map().AsRaw()
			tag, j := make([]tuple, len(tm)), 0
			for k, v := range tm {
				tag[j] = tuple{k, v.(string)}
				j++
			}
			tags = append(tags, tag)

			tmp, _ = m.Get(columnDurationNs)
			dur, _ := strconv.ParseUint(tmp.Str(), 10, 64)
			duration_ns = append(duration_ns, dur)

			tmp, _ = m.Get(columnPayloadType)
			payload_type = append(payload_type, tmp.AsString())

			payload = append(payload, r.Body().Bytes().AsRaw())

			idx = offset + s
			ch.logger.Debug(
				fmt.Sprintf("batch insert prepared row %d", idx),
				zap.Uint64(columnTimestampNs, timestamp_ns[idx]),
				zap.String(columnType, typ[idx]),
				zap.String(columnServiceName, service_name[idx]),
				zap.String(columnPeriodType, period_type[idx]),
				zap.String(columnPeriodUnit, period_unit[idx]),
				zap.String(columnPayloadType, payload_type[idx]),
			)
		}
		offset += s
	}

	// column order here should match table column order
	if err := b.Column(0).Append(timestamp_ns); err != nil {
		return 0, err
	}
	if err := b.Column(1).Append(typ); err != nil {
		return 0, err
	}
	if err := b.Column(2).Append(service_name); err != nil {
		return 0, err
	}
	if err := b.Column(3).Append(sample_types_units); err != nil {
		return 0, err
	}
	if err := b.Column(4).Append(period_type); err != nil {
		return 0, err
	}
	if err := b.Column(5).Append(period_unit); err != nil {
		return 0, err
	}
	if err := b.Column(6).Append(tags); err != nil {
		return 0, err
	}
	if err := b.Column(7).Append(duration_ns); err != nil {
		return 0, err
	}
	if err := b.Column(8).Append(payload_type); err != nil {
		return 0, err
	}

	if err := b.Column(9).Append(payload); err != nil {
		return 0, err
	}
	if err := b.Column(10).Append(values_agg); err != nil {
		return 0, err
	}
	return offset, b.Send()
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
