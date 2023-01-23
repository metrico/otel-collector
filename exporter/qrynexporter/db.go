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
)

const (
	tracesInsertSQL = `
  INSERT INTO traces_input (
    trace_id,
    span_id, 
    parent_id, 
    name,
    timestamp_ns, 
    duration_ns, 
    service_name, 
    payload_type, 
    payload, 
    tags
  ) VALUES (
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?,
    ?
  )`
	samplesInsertSQL = `
  INSERT INTO samples_v3 (
    fingerprint, 
    timestamp_ns, 
    value, 
    string
  ) VALUES (
    ?,
    ?,
    ?,
    ?
  )`
	timeSeriesInsertSQL = `
  INSERT INTO time_series (
    date,
    fingerprint, 
    labels,
    name
  ) VALUES (
    ?,
    ?,
    ?,
    ?
  )`
)

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
	Tags        [][]any
}

// Sample represent sample data
type Sample struct {
	Fingerprint uint64
	TimestampNs int64
	Value       float64
	String      string
}

// TimeSerie represent TimeSerie
type TimeSerie struct {
	Date        string
	Fingerprint uint64
	Labels      string
	Name        string
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
