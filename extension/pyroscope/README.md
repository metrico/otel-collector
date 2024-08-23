# Pyroscope client

| Status                    |        |
|---------------------------|--------|
| Stability                 | [beta] |


The Extension provides the functionality to export the opentelementry collector profiles into a pyroscope compatible 
server. 


The following settings can be configured:

- application_name (default = "opentelemetry-collector"): The name of the application as it will appear in Pyroscope.
- tags (default = empty map): A map of key-value pairs for additional tags to be associated with the profiles.
- server_address (default = "http://localhost:8062"): The URL of the Pyroscope server to which the profiles will be sent.
- basic_auth (default = empty): Basic authentication configuration for connecting to the Pyroscope server.
  - username: The username for basic authentication.
  - password: The password for basic authentication.
- profile_types (default = ["cpu", "alloc_objects", "alloc_space", "inuse_objects", "inuse_space"]):
  An array of profile types to collect and send to Pyroscope.
- tenant_id (default = ""): The tenant ID to use when sending profiles to Pyroscope, for multi-tenancy support.

Example:
```yaml
extensions:
  pyroscope:
    application_name: "my-collector"
    tags:
      environment: "production"
      region: "us-west-2"
    server_address: "http://localhost:8062"
    basic_auth:
      username: "user"
      password: "secret"
    profile_types:
      - "cpu"
      - "alloc_objects"
      - "inuse_space"
service:
  extensions: [pyroscope]
```
