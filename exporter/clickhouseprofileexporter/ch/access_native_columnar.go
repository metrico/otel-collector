package ch

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// schema reference: https://github.com/metrico/qryn/blob/master/lib/db/maintain/scripts.js
type clickhouseAccessNativeColumnar struct {
	conn driver.Conn
}

type kv struct {
	k string
	v string
}

// Connects to clickhouse and checks the connection's health, returning a new native client
func NewClickhouseAccessNativeColumnar(opts *clickhouse.Options) (*clickhouseAccessNativeColumnar, error) {
	c, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}
	if err = c.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse server: %w", err)
	}
	return &clickhouseAccessNativeColumnar{
		conn: c,
	}, nil
}

// Inserts a profile batch into the clickhouse server using columnar native protocol
func (ch *clickhouseAccessNativeColumnar) InsertBatch(ls plog.Logs) error {
	b, err := ch.conn.PrepareBatch(context.Background(), "INSERT INTO profiles_input")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	rs := ls.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	sz := rs.Len()

	// send efficient native columnar protocol
	timestamp_ns := make([]uint64, sz)
	profile_id := make([]string, sz)
	typ := make([]string, sz)
	service_name := make([]string, sz)
	period_type := make([]string, sz)
	period_unit := make([]string, sz)
	tags := make([][]kv, sz)
	duration_ns := make([]uint64, sz)
	payload_type := make([]string, sz)
	payload := make([][]byte, sz)

	var (
		r   plog.LogRecord
		m   pcommon.Map
		tmp pcommon.Value
		tm  map[string]any
	)
	for i := 0; i < rs.Len(); i++ {
		r = rs.At(i)
		m = r.Attributes()

		timestamp_ns = append(timestamp_ns, uint64(r.Timestamp()))

		profile_id = append(profile_id, "")

		tmp, _ = m.Get("type")
		typ = append(typ, tmp.AsString())

		tmp, _ = m.Get("service_name")
		service_name = append(service_name, tmp.AsString())

		tmp, _ = m.Get("period_type")
		period_type = append(period_type, tmp.AsString())

		tmp, _ = m.Get("period_unit")
		period_unit = append(period_unit, tmp.AsString())

		tmp, _ = m.Get("tags")
		tm = tmp.Map().AsRaw()
		tag := make([]kv, len(tm))
		for k, v := range tm {
			tag = append(tag, kv{k, v.(string)})
		}
		tags = append(tags, tag)

		tmp, _ = m.Get("duration_ns")
		duration_ns = append(duration_ns, uint64(tmp.Int()))

		tmp, _ = m.Get("payload_type")
		payload_type = append(payload_type, tmp.AsString())

		payload = append(payload, r.Body().Bytes().AsRaw())
	}

	if err := b.Column(0).Append(timestamp_ns); err != nil {
		return err
	}
	if err := b.Column(1).Append(profile_id); err != nil {
		return err
	}
	if err := b.Column(2).Append(typ); err != nil {
		return err
	}
	if err := b.Column(3).Append(service_name); err != nil {
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
