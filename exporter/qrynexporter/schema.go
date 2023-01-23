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

package qrynexporter

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Trace represent trace model
type Trace struct {
	TraceID     string
	SpanID      string
	ParentID    string
	Name        string
	TimestampNs int64
	DurationNs  int64
	ServiceName string
	PayloadType int8
	Payload     string
	Tags        [][]string
}

// Note: https://github.com/metrico/qryn/blob/master/lib/db/maintain/scripts.js
// We need to align with the schema here.
//
// CREATE TABLE IF NOT EXISTS traces_input (
//
//	oid String DEFAULT '0',
//	trace_id String,
//	span_id String,
//	parent_id String,
//	name String,
//	timestamp_ns Int64 CODEC(DoubleDelta),
//	duration_ns Int64,
//	service_name String,
//	payload_type Int8,
//	payload String,
//	tags Array(Tuple(String, String))
//
// ) Engine=Null

// TracesSchema
type TracesSchema struct {
	TraceID     proto.ColStr
	SpanID      proto.ColStr
	ParentID    proto.ColStr
	Name        proto.ColStr
	TimestampNs proto.ColInt64
	DurationNs  proto.ColInt64
	ServiceName proto.ColStr
	PayloadType proto.ColInt8
	Payload     proto.ColStr
	Tags        proto.ColArr[proto.ColTuple]
}

// Sample represent sample data
type Sample struct {
	Fingerprint uint64
	TimestampNs int64
	Value       float64
	String      string
}

// SampleSchema samples_v3 schema
//
// `CREATE TABLE IF NOT EXISTS samples_v3
// (
//
//	fingerprint UInt64,
//	timestamp_ns Int64 CODEC(DoubleDelta),
//	value Float64 CODEC(Gorilla),
//	string String
//
// ) ENGINE = MergeTree
// PARTITION BY toStartOfDay(toDateTime(timestamp_ns / 1000000000))
// ORDER BY ({{SAMPLES_ORDER_RUL}})`
type SampleSchema struct {
	Fingerprint proto.ColUInt64
	TimestampNs proto.ColInt64
	Value       proto.ColFloat64
	String      proto.ColStr
}

// TimeSerie represent TimeSerie
type TimeSerie struct {
	Date        time.Time
	Fingerprint uint64
	Labels      string
	Name        string
}

// TimeSerieSchema time_series schema
// `CREATE TABLE IF NOT EXISTS time_series
// (
//
//	date Date,
//	fingerprint UInt64,
//	labels String,
//	name String
//
// ) ENGINE = ReplacingMergeTree(date)
// PARTITION BY date
// ORDER BY fingerprint
type TimeSerieSchema struct {
	Date        proto.ColDate
	Fingerprint proto.ColUInt64
	Labels      proto.ColStr
	Name        proto.ColStr
}

// Transaction wrap func under Transaction
func Transaction(ctx context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
