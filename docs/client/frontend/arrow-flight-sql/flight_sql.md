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

With TLS enabled:

```text
grpc+tls://<host>:10299
```

The result page size defaults to 1000 rows and can be changed with:

```properties
kyuubi.frontend.flight.sql.fetch.max.rows=1000
```

## Authentication

When Kyuubi authentication is disabled or configured as `NOSASL`/`NONE`, the
endpoint accepts requests without an authorization header. Clients may provide
`x-user-name`; otherwise the session user is `anonymous`.

For plain authentication methods (LDAP/JDBC/CUSTOM), clients can use HTTP Basic
credentials in the Flight authorization header. Credentials are validated
through Kyuubi's configured authentication provider before a Kyuubi SQL session
is opened.

When `kyuubi.authentication` includes `KERBEROS`, clients may send
`Authorization: Negotiate <spnego-token>`. Successful Basic or Negotiate
authentication issues a short-lived Flight bearer token
(`kyuubi.frontend.flight.sql.token.ttl`, default 2 hours). Subsequent calls
should use `Authorization: Bearer <token>`.

Kerberos uses the shared SPNEGO settings:

```properties
kyuubi.spnego.principal=HTTP/<host>@REALM
kyuubi.spnego.keytab=/path/to/spnego.keytab
```

Python and some BI clients may need custom middleware for Negotiate/Bearer
headers. See [compatibility matrix](flight_sql_compatibility.md).

## TLS

Enable TLS with PEM certificate and private key files:

```properties
kyuubi.frontend.flight.sql.ssl.enabled=true
kyuubi.frontend.flight.sql.ssl.cert.file=/path/to/cert.pem
kyuubi.frontend.flight.sql.ssl.key.file=/path/to/key.pem
```

If PEM files are unset, Kyuubi can materialize temporary PEM files from the
shared Java keystore settings (`kyuubi.frontend.ssl.keystore.*`) at startup.

## High availability

When `kyuubi.ha.addresses` is configured, the Flight frontend registers under a
dedicated namespace:

```properties
kyuubi.ha.flight.sql.namespace=kyuubi_flight
```

Flight tickets are node-local. `FlightInfo` advertises the owning endpoint and
clients must use that endpoint for `do_get`, cancel, and close. Transparent
mid-query failover across Kyuubi nodes is not supported.

Load balancers must use sticky routing or direct clients to the advertised owner
endpoint for active streams.

## Large results and types

Results are streamed through a bounded page iterator. Only the current backend
page / Arrow batch is retained. Spark engines prefer Arrow IPC batches; other
engines convert columnar thrift pages into Arrow vectors without collecting the
entire result as a Scala `Seq`.

See [type compatibility](flight_sql_types.md) for the supported type matrix.

## Metrics and monitoring

Flight-specific metrics are published through the shared Kyuubi metrics system.
See [ADM monitoring](../deployment/adm/flight_sql_monitoring.md).

## Supported operations

The endpoint supports:

- SQL statement queries;
- Arrow IPC / page-oriented result streaming with bounded `FETCH_NEXT` pages;
- catalog, schema, table, table-type, and XDBC type metadata;
- query cancellation and operation cleanup;
- Basic/LDAP, Kerberos Negotiate bootstrap + bearer tokens, and TLS.

Prepared statements, ingestion, Substrait plans, transactions, and key
metadata are not supported yet and return `UNIMPLEMENTED` where applicable.

Query results reuse Kyuubi's existing Arrow/thrift result formats. Arrow IPC for
Spark is still carried inside the Kyuubi backend's Thrift `TRowSet`, so this
implementation does not provide end-to-end zero-copy transport past the backend
boundary.
