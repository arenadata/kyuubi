# Flight SQL Compatibility Matrix and Architecture Decisions

This document records the Milestone 0 compatibility spike for the production
Flight SQL frontend. It is the contract for later milestones.

## Baseline

| Component | Value |
|-----------|-------|
| Java target | 17 |
| Arrow | 16.0.0 (`flight-core`, `flight-sql`) |
| gRPC / Netty / Protobuf | Kyuubi-managed (gRPC 1.76.x, Netty 4.2.x, protobuf 3.25.x) |
| Enable switch | `kyuubi.frontend.protocols` contains `FLIGHT_SQL` |
| Default bind port | `10299` |
| Backend contract | `BackendService.openSession` / `executeStatement` / `getOperationStatus` / `getResultSetMetadata` / `fetchResults` / cancel / close |

## Client capability matrix

| Client | Basic/LDAP | TLS | Kerberos/SPNEGO | Bearer token after SPNEGO | Cancellation |
|--------|------------|-----|-----------------|---------------------------|--------------|
| Java `FlightSqlClient` (Arrow 16) | Yes | Yes (`useTls`) | Yes via custom `Authorization: Negotiate` header | Yes (`GeneratedBearerTokenAuthenticator`) | Yes |
| Python `pyarrow.flight` | Yes (Basic middleware) | Yes | Limited; requires custom middleware | Yes if middleware sets Bearer | Yes |
| BI / DBeaver / Power BI | Depends on driver | Depends on driver | Not assumed | Prefer Basic or TLS+Basic | Depends on driver |

Decision: support Basic/LDAP and SPNEGO bootstrap plus short-lived Bearer tokens.
Do not claim universal native multi-step GSSAPI Flight auth for every language
client. Document Python/BI Kerberos as requiring custom header middleware.

## TLS material format

Arrow Flight 16 `FlightServer.Builder.useTls` accepts PEM certificate chain and
private key (`File` or `InputStream`). It does not consume a Java keystore
directly.

Decision:
- Canonical configs: `kyuubi.frontend.flight.sql.ssl.cert.file` and
  `kyuubi.frontend.flight.sql.ssl.key.file`.
- Optional fallback: materialize PEM from the shared
  `kyuubi.frontend.ssl.keystore.*` settings at startup with secure temporary
  files and cleanup on stop.
- Advertised location uses `Location.forGrpcTls` when TLS is enabled.

## Iterator / BackendService contract

`BackendService.fetchResults(operation, FETCH_NEXT, maxRows, fetchLog=false)` is
sufficient for a page-oriented Flight iterator. No new BackendService cursor API
is required for the first production milestone.

Decision:
- Wrap `fetchResults` in a closeable `FlightResultIterator`.
- Keep one page / one `ArrowRecordBatch` / one `VectorSchemaRoot` in memory.
- Preserve the existing BackendService API for Thrift/REST/MySQL/Trino frontends.

## Engine result paths

| Engine | Preferred path | Fallback |
|--------|----------------|----------|
| Spark SQL | Arrow IPC bytes inside `TRowSet` binary column | Columnar thrift conversion |
| Trino / Flink / Hive / JDBC | Page-oriented columnar thrift → Arrow vectors | Deterministic unsupported-type error |
| Metadata RPCs | Ordinary thrift columns → Flight SQL fixed schemas | N/A |

Decision: keep the producer engine-agnostic. Conversion lives in shared Flight
Arrow utilities / common result helpers. Silent `Utf8` fallback is not allowed
for production type compatibility.

## HA semantics

Decision for the first production HA milestone:
- Register Flight endpoints under `kyuubi.ha.flight.sql.namespace`.
- Tickets remain node-local.
- `FlightInfo` advertises the owning node endpoint.
- Clients must use the owner endpoint for `do_get` / cancel / close.
- Transparent mid-query failover across Kyuubi nodes is explicitly out of scope
  until a distributed operation store exists.

## ADCM / ADM repositories

No ADCM bundle or ADM monitoring repository was found under
`/Volumes/ADATA/arenadata`. Flight SQL deployment artifacts are therefore
delivered as:

- Kyuubi configuration keys and generated docs in this repository;
- ADCM property mapping templates under `docs/deployment/adcm/`;
- ADM Prometheus scrape / dashboard / alert templates under
  `docs/deployment/adm/`.

External Arenadata ADCM/ADM repositories should import those templates rather
than inventing a second configuration model.

## Non-goals retained from the MVP

- Prepared statements, ingestion, Substrait, transactions/savepoints;
- End-to-end zero-copy past the Thrift `TRowSet` backend boundary;
- Transparent active-query migration between Kyuubi nodes.
