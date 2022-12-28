// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhousetracesexporter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for spans encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)

// SpanWriter for writing spans to ClickHouse
type SpanWriter struct {
	logger        *zap.Logger
	db            clickhouse.Conn
	traceDatabase string
	encoding      Encoding
	delay         time.Duration
	size          int
	inputs        chan *Span
	finish        chan struct{}
	done          sync.WaitGroup
}

// NewSpanWriter returns a SpanWriter for the database
func NewSpanWriter(logger *zap.Logger, db clickhouse.Conn, traceDatabase string, encoding Encoding, delay time.Duration, size int) *SpanWriter {
	writer := &SpanWriter{
		logger:        logger,
		db:            db,
		traceDatabase: traceDatabase,
		encoding:      encoding,
		delay:         delay,
		size:          size,
		inputs:        make(chan *Span, size),
		finish:        make(chan struct{}),
	}

	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) backgroundWriter() {
	batch := make([]*Span, 0, w.size)

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.inputs:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
		}

		if flush {
			if err := w.writeBatch(batch); err != nil {
				w.logger.Error("Could not write a batch of spans", zap.Error(err))
			}

			batch = make([]*Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*Span) error {
	ctx := context.Background()
	statement, err := w.db.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s", w.traceDatabase, "traces_input"))
	if err != nil {
		return err
	}

	for _, span := range batch {
		err = statement.Append(
			"0",
			span.TraceID,
			span.SpanID,
			span.ParentID,
			span.Name,
			span.TimestampNs,
			span.DurationNs,
			span.ServiceName,
			span.PayloadType,
			span.Payload,
			span.Tags,
		)
		if err != nil {
			return err
		}
	}

	return statement.Send()

	return nil
}

// WriteSpan writes the encoded span
func (w *SpanWriter) WriteSpan(span *Span) error {
	w.inputs <- span
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	close(w.finish)
	w.done.Wait()
	return nil
}
