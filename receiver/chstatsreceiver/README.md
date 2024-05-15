# Pyroscope Receiver

| Status                   |         |
| ------------------------ |---------|
| Stability                | [beta]  |
| Supported pipeline types | metrics |

The chstatsreceiver module is a component of the OpenTelemetry collector that collects and exports metrics from ClickHouse databases. It uses the ClickHouse Go client library to connect to the database and execute SQL queries to retrieve metrics data. The module is designed to be highly configurable, allowing users to specify the database connection details, the SQL queries to execute, and the metrics to export.

## Configuration

- `dsn`: sets the Data Source Name (DSN) for the ClickHouse database. 
The DSN is a string that contains the necessary information to connect to the database, 
such as the host, port, and database name
- `queries`: list of the SQL queries that the receiver will execute against the database to retrieve metrics data. 
The queries are specified as a list of strings.
- `timeout`: amount of time between two consecutive stats requests iterations. 
The timeout is specified as the duration value like `20s`, `1m`, etc. 

## Clickhouse Queries

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

## Example

```yaml
receivers:
  chstatsreceiver:
    dsn: clickhouse://localhost:9000
    queries:
      - |
        SELECT [
          ('__name__', 'clickhouse_bytes_on_disk'), ('db', database), ('disk', disk_name), ('host', hostname())
        ],
        sum(bytes_on_disk)::Float64
        FROM system.parts
        WHERE (active = 1) AND (database NOT IN ('system', '_system'))
        GROUP BY database, disk_name
exporters:
  prometheusremotewrite:
    endpoint: http://localhost:3100/prom/remote/write
    timeout: 30s
service:
  pipelines:
    metrics:
      receivers: [chstatsreceiver]
      exporters: [prometheusremotewrite]
```
