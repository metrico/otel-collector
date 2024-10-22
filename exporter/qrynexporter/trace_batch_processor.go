package qrynexporter

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type traceWithTagsBatch struct {
	driver.Batch
	tagsBatch driver.Batch
}

func (b *traceWithTagsBatch) AppendStruct(v any) error {
	ti, ok := v.(*TraceInput)
	if !ok {
		return fmt.Errorf("invalid data type, expected *Trace, got %T", v)
	}
	binTraceId, err := unhexAndPad(ti.TraceID, 16)
	if err != nil {
		return err
	}
	binParentID, err := unhexAndPad(ti.ParentID, 8)
	if err != nil {
		return err
	}
	binSpanID, err := unhexAndPad(ti.SpanID, 8)
	if err != nil {
		return err
	}
	trace := &TempoTrace{
		OID:         "0",
		TraceID:     binTraceId,
		SpanID:      binSpanID,
		ParentID:    binParentID,
		Name:        ti.Name,
		TimestampNs: ti.TimestampNs,
		DurationNs:  ti.DurationNs,
		ServiceName: ti.ServiceName,
		PayloadType: ti.PayloadType,
		Payload:     ti.Payload,
	}
	err = b.Batch.AppendStruct(trace)
	if err != nil {
		return err
	}
	for _, tag := range ti.Tags {
		attr := &TempoTraceTag{
			OID:         "0",
			Date:        time.Unix(0, trace.TimestampNs).Truncate(time.Hour * 24),
			Key:         tag[0],
			Val:         tag[1],
			TraceID:     binTraceId,
			SpanID:      binSpanID,
			TimestampNs: ti.TimestampNs,
			DurationNs:  ti.DurationNs,
		}
		err = b.tagsBatch.AppendStruct(attr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *traceWithTagsBatch) Abort() error {
	var errs [2]error
	errs[0] = b.Batch.Abort()
	errs[1] = b.tagsBatch.Abort()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *traceWithTagsBatch) Send() error {
	var errs [2]error
	errs[0] = b.Batch.Send()
	errs[1] = b.tagsBatch.Send()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func unhexAndPad(s string, size int) ([]byte, error) {
	bStr, err := hex.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(bStr) < size {
		res := make([]byte, size)
		copy(res[size-len(bStr):], bStr)
		return res, nil
	}
	return bStr[size-len(bStr):], nil
}
