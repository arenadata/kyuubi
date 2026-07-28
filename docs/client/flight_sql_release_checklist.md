# Flight SQL Production Release Checklist

## Build and dependency checks

- [ ] `mvn -pl :kyuubi-server_2.12 -am -DskipTests -Dwebui.skip=true package`
- [ ] Dependency tree contains Arrow 16.0.0 Flight artifacts without unmanaged gRPC/Netty majors
- [ ] Java 17 compile target unchanged

## Functional validation

- [ ] Lifecycle suite: start/stop, advertised host, deprecated aliases, TLS missing-material failure
- [ ] Auth suite: anonymous, missing credentials, malformed Basic
- [ ] Arrow utils suite: columnar thrift page conversion, Arrow rowset helpers
- [ ] Query suite: `SELECT 1` over insecure Flight SQL
- [ ] Multi-page / cancellation coverage with reduced `fetch.max.rows`
- [ ] Kerberos Negotiate + bearer token path (when keytab available)
- [ ] TLS path with PEM cert/key (or keystore fallback)

## HA / ops

- [ ] Flight registration under `kyuubi.ha.flight.sql.namespace`
- [ ] Owner endpoint routing documented and verified
- [ ] Metrics appear in Prometheus/JMX for Flight connection/operation/stream counters
- [ ] ADM templates imported from `docs/deployment/adm/`
- [ ] ADCM mapping imported from `docs/deployment/adcm/`

## Known limitations to restate in release notes

- Prepared statements / ingest / Substrait / transactions remain unimplemented
- Tickets are node-local; no transparent active-query failover
- Spark Arrow IPC still crosses the Thrift `TRowSet` backend boundary
- Python/BI Kerberos may require custom Flight middleware
