# Clickhouse Statistics Receiver

| Status                   |                |
| ------------------------ |----------------|
| Stability                | [beta]         |
| Supported pipeline types | metrics, logs  |

The chstatsreceiver module is a component of the OpenTelemetry collector that collects and exports 
metrics and logs from ClickHouse databases.

It uses the ClickHouse Go client library to connect to the database and execute SQL queries 
to retrieve metrics and log data.

The module is designed to be highly configurable, allowing users to specify the database connection details, 
the SQL queries to execute, and the metrics and logs to export.


## Configuration

- `dsn`: sets the Data Source Name (DSN) for the ClickHouse database. 
The DSN is a string that contains the necessary information to connect to the database, 
such as the host, port, and database name
- `type`: specifies the type of data to collect. Valid values are:
  - `"metrics"`: collect metrics data (default if not specified)
  - `"logs"`: collect log data
- `queries`: list of the SQL queries that the receiver will execute against the database to retrieve metrics data. 
The queries are specified as a list of strings.
- `timeout`: amount of time between two consecutive stats requests iterations. 
The timeout is specified as the duration value like `20s`, `1m`, etc. 

## Clickhouse Queries

### For Metrics (type: "metrics")
Each clickhouse query should return two fields:
- labels as array of Tuple(String, String)
- value Float64

Labels should have the `__name__` label with the name of the metric.

For example
```sql
SELECT 
    [('__name__', 'some_metric'), ('label2', 'val2')]::Array(Tuple(String,String)), 
    2::Float64
```

### For Logs (type: "logs")
Queries for logs should return two fields:
- labels as array of Tuple(String, String)
- message String

The receiver will automatically convert the query results into log records.

```sql
SELECT 
    [('level', 'debug'), ('label2', 'val2')]::Array(Tuple(String,String)), 
    'log line to send'
```

## Example

```yaml
receivers:
  chstatsreceiver/metrics:
    dsn: clickhouse://localhost:9000
    type: metrics
    timeout: 30s
    queries:
      - |
        SELECT [
          ('__name__', 'clickhouse_bytes_on_disk'), ('db', database), ('disk', disk_name), ('host', hostname())
        ],
        sum(bytes_on_disk)::Float64
        FROM system.parts
        WHERE (active = 1) AND (database NOT IN ('system', '_system'))
        GROUP BY database, disk_name
  chstatsreceiver/logs:
    dsn: clickhouse://localhost:9000
    type: logs
    timeout: 1m
    queries:
      - |
        SELECT 
          [('job', 'clickhouse_query_logs')],
          format('id={} query={}', query_id, query)
        FROM system.query_id
        WHERE event_time > now() - INTERVAL 1 MINUTE
exporters:
  prometheusremotewrite:
    endpoint: http://localhost:3100/prom/remote/write
    timeout: 30s
  loki:
    endpoint: http://localhost:3100/loki/api/v1/push
service:
  pipelines:
    metrics:
      receivers: [chstatsreceiver/metrics]
      exporters: [prometheusremotewrite]
    logs:
      receivers: [chstatsreceiver/logs]
      exporters: [loki]
```
