# Arrow Flight SQL

Kyuubi can expose an experimental Arrow Flight SQL frontend in addition to its
HiveServer2-compatible frontend. Flight SQL is disabled by default.

Enable it with:

```properties
kyuubi.frontend.protocols=FLIGHT_SQL
kyuubi.frontend.flight.sql.bind.host=0.0.0.0
kyuubi.frontend.flight.sql.bind.port=10299
```

The endpoint uses gRPC and is available at:

```text
grpc://<host>:10299
```

The result page size defaults to 1000 rows and can be changed with:

```properties
kyuubi.frontend.flight.sql.fetch.max.rows=1000
```

## Authentication

When Kyuubi authentication is disabled or configured as `NOSASL`/`NONE`, the
endpoint accepts requests without an authorization header. Clients may provide
`x-user-name`; otherwise the session user is `anonymous`.

For plain authentication methods, clients can use HTTP Basic credentials in the
Flight authorization header. The credentials are validated through Kyuubi's
configured authentication provider before a Kyuubi SQL session is opened.

TLS is not currently enabled by the Flight frontend because Arrow Flight 16
expects PEM certificate/key input while Kyuubi's shared settings use a Java
keystore. Keep the endpoint behind a trusted network boundary until PEM-based
TLS support is added.

## Supported MVP operations

The initial endpoint supports:

- SQL statement queries;
- Arrow IPC result streaming with bounded `FETCH_NEXT` pages;
- catalog, schema, table, table-type, and XDBC type metadata;
- query cancellation and operation cleanup.

Prepared statements, ingestion, Substrait plans, transactions, and key
metadata are not supported yet and return `UNIMPLEMENTED` where applicable.

Query results reuse Kyuubi's existing Arrow result format. Arrow IPC is still
carried inside the Kyuubi backend's Thrift `TRowSet`, so this implementation
does not provide end-to-end zero-copy transport.
