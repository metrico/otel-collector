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
	"time"
)

const (
	tracesInputSQL = `INSERT INTO traces_input (
  trace_id, 
  span_id, 
  parent_id, 
  name, 
  timestamp_ns, 
  duration_ns, 
  service_name,
  payload_type, 
  payload, 
  tags)`
	samplesSQL = `INSERT INTO samples_v3 (
  fingerprint,
  timestamp_ns,
  value, 
  string)`
	TimeSerieSQL = `INSERT INTO time_series (
  date, 
  fingerprint,
  labels,
  name)`
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

// Trace represent trace model
type Trace struct {
	TraceID     string     `ch:"trace_id"`
	SpanID      string     `ch:"span_id"`
	ParentID    string     `ch:"parent_id"`
	Name        string     `ch:"name"`
	TimestampNs int64      `ch:"timestamp_ns"`
	DurationNs  int64      `ch:"duration_ns"`
	ServiceName string     `ch:"service_name"`
	PayloadType int8       `ch:"payload_type"`
	Payload     string     `ch:"payload"`
	Tags        [][]string `ch:"tags"`
}

// Sample represent sample data
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
type Sample struct {
	Fingerprint uint64  `ch:"fingerprint"`
	TimestampNs int64   `ch:"timestamp_ns"`
	Value       float64 `ch:"value"`
	String      string  `ch:"string"`
}

// TimeSerie represent TimeSerie
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
type TimeSerie struct {
	Date        time.Time `ch:"date"`
	Fingerprint uint64    `ch:"fingerprint"`
	Labels      string    `ch:"labels"`
	Name        string    `ch:"name"`
}
