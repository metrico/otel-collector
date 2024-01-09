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
func (ch *clickhouseAccessNativeColumnar) InsertBatch(ls plog.Logs) error {
	b, err := ch.conn.PrepareBatch(context.Background(), "INSERT INTO profiles_input")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// this implementation is tightly coupled to how pyroscope-java and pyroscopereciver work,
	// specifically receiving a single profile at a time from the agent,
	// and thus each batched resource logs slice contains a single log record
	rl := ls.ResourceLogs()
	sz := rl.Len()

	timestamp_ns := make([]uint64, sz)
	typ := make([]string, sz)
	service_name := make([]string, sz)
	sample_types_units := make([][]tuple, sz)
	period_type := make([]string, sz)
	period_unit := make([]string, sz)
	tags := make([][]tuple, sz)
	duration_ns := make([]uint64, sz)
	payload_type := make([]string, sz)
	payload := make([][]byte, sz)

	var (
		r   plog.LogRecord
		m   pcommon.Map
		tmp pcommon.Value
		tm  map[string]any
	)
	for i := 0; i < sz; i++ {
		r = rl.At(i).ScopeLogs().At(0).LogRecords().At(0)
		m = r.Attributes()

		timestamp_ns[i] = uint64(r.Timestamp())

		tmp, _ = m.Get("type")
		typ[i] = tmp.AsString()

		tmp, _ = m.Get("service_name")
		service_name[i] = tmp.AsString()

		sample_types, _ := m.Get("sample_types")
		sample_units, _ := m.Get("sample_units")

		sample_types_array, err := valueToStringArray(sample_types)
		if err != nil {
			return err
		}

		sample_units_array, err := valueToStringArray(sample_units)
		if err != nil {
			return err
		}

		sample_types_units_item := make([]tuple, len(sample_types_array))
		for i, v := range sample_types_array {
			sample_types_units_item[i] = tuple{v, sample_units_array[i]}
		}
		sample_types_units[i] = sample_types_units_item

		tmp, _ = m.Get("period_type")
		period_type[i] = tmp.AsString()

		tmp, _ = m.Get("period_unit")
		period_unit[i] = tmp.AsString()

		tmp, _ = m.Get("tags")
		tm = tmp.Map().AsRaw()
		tag, j := make([]tuple, len(tm)), 0
		for k, v := range tm {
			tag[j] = tuple{k, v.(string)}
			j++
		}
		tags[i] = tag

		tmp, _ = m.Get("duration_ns")
		duration_ns[i], _ = strconv.ParseUint(tmp.Str(), 10, 64)

		tmp, _ = m.Get("payload_type")
		payload_type[i] = tmp.AsString()

		payload[i] = r.Body().Bytes().AsRaw()
	}

	// column order here should match table column order
	if err := b.Column(0).Append(timestamp_ns); err != nil {
		return err
	}
	if err := b.Column(1).Append(typ); err != nil {
		return err
	}
	if err := b.Column(2).Append(service_name); err != nil {
		return err
	}
	if err := b.Column(3).Append(sample_types_units); err != nil {
		return err
	}
	if err := b.Column(4).Append(period_type); err != nil {
		return err
	}
	if err := b.Column(5).Append(period_unit); err != nil {
		return err
	}
	if err := b.Column(6).Append(tags); err != nil {
		return err
	}
	if err := b.Column(7).Append(duration_ns); err != nil {
		return err
	}
	if err := b.Column(8).Append(payload_type); err != nil {
		return err
	}
	if err := b.Column(9).Append(payload); err != nil {
		return err
	}
	return b.Send()
}

// Closes the clickhouse connection pool
func (ch *clickhouseAccessNativeColumnar) Shutdown() error {
	return ch.conn.Close()
}
