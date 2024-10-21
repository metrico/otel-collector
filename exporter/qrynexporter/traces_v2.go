package qrynexporter

import (
	"encoding/hex"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"time"
)

type batchV2 struct {
	driver.Batch
	subBatch driver.Batch
}

func (b *batchV2) AppendStruct(data any) error {
	_data, ok := data.(*Trace)
	if !ok {
		return fmt.Errorf("invalid data type, expected *Trace, got %T", data)
	}
	binTraceId, err := unhexAndPad(_data.TraceID, 16)
	if err != nil {
		return err
	}
	binParentID, err := unhexAndPad(_data.ParentID, 8)
	if err != nil {
		return err
	}
	binSpanID, err := unhexAndPad(_data.SpanID, 8)
	if err != nil {
		return err
	}
	trace := &TraceV2{
		OID:         "0",
		TraceID:     binTraceId,
		SpanID:      binSpanID,
		ParentID:    binParentID,
		Name:        _data.Name,
		TimestampNs: _data.TimestampNs,
		DurationNs:  _data.DurationNs,
		ServiceName: _data.ServiceName,
		PayloadType: _data.PayloadType,
		Payload:     _data.Payload,
	}
	err = b.Batch.AppendStruct(trace)
	if err != nil {
		return err
	}
	for _, tag := range _data.Tags {
		attr := &TraceTagsV2{
			OID:         "0",
			Date:        time.Unix(0, trace.TimestampNs).Truncate(time.Hour * 24),
			Key:         tag[0],
			Val:         tag[1],
			TraceID:     binTraceId,
			SpanID:      binSpanID,
			TimestampNs: _data.TimestampNs,
			DurationNs:  _data.DurationNs,
		}
		err = b.subBatch.AppendStruct(attr)
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *batchV2) Abort() error {
	var errs [2]error
	errs[0] = b.Batch.Abort()
	errs[1] = b.subBatch.Abort()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *batchV2) Send() error {
	var errs [2]error
	errs[0] = b.Batch.Send()
	errs[1] = b.subBatch.Send()
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
