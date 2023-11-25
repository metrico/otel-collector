# pyroscope receiver

| Status                   |                       |
| ------------------------ |-----------------------|
| Stability                | [alpha]               |
| Supported pipeline types | logs                  |

The pyroscope receiver implements the pyroscope ingest http api.

## Getting Started

Example:
```yaml
receivers:
  pyroscopereceiver:
    protocols:
      http:
        endpoint: 0.0.0.0:8062
```
