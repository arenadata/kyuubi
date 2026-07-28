# ADM Monitoring Integration for Flight SQL

## Scrape targets

Reuse the existing Kyuubi Prometheus reporter:

```properties
kyuubi.metrics.enabled=true
kyuubi.metrics.reporters=PROMETHEUS
kyuubi.metrics.prometheus.port=10019
```

ADM scrape configuration should target:

```text
http://<kyuubi-host>:10019/metrics
```

## Flight-specific metric names

| Metric | Type | Meaning |
|--------|------|---------|
| `kyuubi.flight.sql.connection.opened` | gauge/counter pair | Active Flight authenticated connections/sessions |
| `kyuubi.flight.sql.connection.total` | counter | Accepted Flight auth results |
| `kyuubi.flight.sql.connection.failed` | counter | Authentication / TLS handshake failures |
| `kyuubi.flight.sql.operation.opened` | gauge | Active Flight statement/metadata streams |
| `kyuubi.flight.sql.operation.total` | counter | Started Flight operations |
| `kyuubi.flight.sql.operation.failed` | counter | Failed Flight operations |
| `kyuubi.flight.sql.operation.cancelled` | counter | Cancelled Flight operations |
| `kyuubi.flight.sql.stream.batches` | meter | Emitted Arrow batches |
| `kyuubi.flight.sql.stream.rows` | meter | Emitted rows |
| `kyuubi.flight.sql.stream.bytes` | meter | Approximate streamed payload bytes |
| `kyuubi.backend_service.fetch_result_rows_rate` | meter | Backend fetch row accounting (Arrow-aware) |

Do not label metrics by SQL text, username, ticket id, session UUID, or remote
address.

## Suggested dashboard panels

1. Flight endpoint up / HA membership.
2. Active Flight connections and operations.
3. Auth failure rate.
4. Streamed batches / rows / bytes rate.
5. Cancel and decode/error rate.
6. Backend `fetchResults` latency.

## Suggested alerts

- Flight frontend down while `FLIGHT_SQL` is enabled.
- Auth failure rate above threshold for 5 minutes.
- Active streams stuck above threshold with zero batch progress.
- HA registration missing for a configured Flight instance.

## Health checks

Prefer existing Kyuubi admin REST endpoints for server/engine/session/operation
state. Flight HA membership is visible through discovery under
`kyuubi.ha.flight.sql.namespace`.
