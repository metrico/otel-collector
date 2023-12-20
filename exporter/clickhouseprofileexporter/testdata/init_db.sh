#!/bin/bash
set -e

clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DB" \
  --multiquery "CREATE TABLE IF NOT EXISTS profiles_input (
    timestamp_ns UInt64,
    profile_id FixedString(16),
    type LowCardinality(String),
    service_name LowCardinality(String),
    period_type LowCardinality(String),
    period_unit LowCardinality(String),
    tags Array(Tuple(String, String)),
    duration_ns UInt64,
    payload_type LowCardinality(String),
    payload String
  ) Engine=Null;
  CREATE TABLE IF NOT EXISTS profiles (
    timestamp_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    profile_id FixedString(16) CODEC(ZSTD(1)),
    duration_ns UInt64 CODEC(DoubleDelta, ZSTD(1)),
    payload_type LowCardinality(String) CODEC(ZSTD(1)),
    payload String CODEC(ZSTD(1))
  ) Engine MergeTree() 
  ORDER BY (type_id, service_name, timestamp_ns)
  PARTITION BY toDate(FROM_UNIXTIME(intDiv(timestamp_ns, 1000000000)));
  CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_mv TO profiles AS
    SELECT 
      timestamp_ns, 
      cityHash64(arraySort(arrayConcat(
        profiles_input.tags, [('__type__', concatWithSeparator(':', type, period_type, period_unit) as _type_id), ('service_name', service_name)]
      )) as _tags) as fingerprint,
      _type_id as type_id,
      service_name,
      unhex(profile_id)::FixedString(16) as profile_id,
      duration_ns,
      payload_type,
      payload 
    FROM profiles_input;
  CREATE TABLE IF NOT EXISTS profiles_series (
    date Date CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),    
    tags Array(Tuple(String, String)) CODEC(ZSTD(1)),
  ) Engine ReplacingMergeTree() 
  ORDER BY (date, type_id, fingerprint)
  PARTITION BY date;
  CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_series_mv TO profiles_series AS
    SELECT 
      toDate(intDiv(timestamp_ns, 1000000000)) as date,
      concatWithSeparator(':', type, period_type, period_unit) as type_id,
      service_name,
      cityHash64(arraySort(arrayConcat(
        profiles_input.tags, [('__type__', type_id), ('service_name', service_name)]
      )) as _tags) as fingerprint,
      tags
    FROM profiles_input;
  CREATE TABLE IF NOT EXISTS profiles_series_gin (
    date Date CODEC(ZSTD(1)),
    key String CODEC(ZSTD(1)),
    val String CODEC(ZSTD(1)),
    type_id LowCardinality(String) CODEC(ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint UInt64 CODEC(DoubleDelta, ZSTD(1)),
  ) Engine ReplacingMergeTree()
  ORDER BY (date, key, val, type_id, fingerprint)
  PARTITION BY date;
  CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_series_gin_mv TO profiles_series_gin AS
    SELECT 
      date,
      kv.1 as key,
      kv.2 as val,
      type_id,
      service_name,
      fingerprint,
    FROM profiles_series ARRAY JOIN tags as kv;
  CREATE TABLE IF NOT EXISTS profiles_series_keys (
    date Date,
    key String,
    val String,
    val_id UInt64,
  ) Engine ReplacingMergeTree()
  ORDER BY (date, key, val_id)
  PARTITION BY date;
  CREATE MATERIALIZED VIEW IF NOT EXISTS profiles_series_keys_mv TO profiles_series_keys AS
    SELECT 
      date,
      key,
      val,
      cityHash64(val) % 50000 as val_id
    FROM profiles_series_gin"
