# ADCM Configuration Mapping for Flight SQL

Canonical Kyuubi properties are the source of truth. ADCM should render these
properties into `kyuubi-defaults.conf`. Optional ticket aliases are accepted as
deprecated input names only and must map to the canonical keys below.

## Enablement

| ADCM / ticket name | Canonical Kyuubi key | Notes |
|--------------------|----------------------|-------|
| `kyuubi.flight.sql.enabled` | `kyuubi.frontend.protocols` includes `FLIGHT_SQL` | Do not invent a second Boolean enable switch in Kyuubi runtime. ADCM may expose a Boolean UI that appends `FLIGHT_SQL` to the protocol list. |

## Networking

| ADCM / ticket name | Canonical Kyuubi key | Default |
|--------------------|----------------------|---------|
| `kyuubi.flight.sql.bind.host` | `kyuubi.frontend.flight.sql.bind.host` | fallback to `kyuubi.frontend.bind.host` |
| `kyuubi.flight.sql.bind.port` | `kyuubi.frontend.flight.sql.bind.port` | `10299` |

## TLS

| ADCM / ticket name | Canonical Kyuubi key | Default |
|--------------------|----------------------|---------|
| `kyuubi.flight.sql.tls.enabled` | `kyuubi.frontend.flight.sql.ssl.enabled` | `false` |
| _(new)_ | `kyuubi.frontend.flight.sql.ssl.cert.file` | unset |
| _(new)_ | `kyuubi.frontend.flight.sql.ssl.key.file` | unset |

Secret handling:
- Certificate and key paths are file references, never inline PEM bodies in ADCM logs.
- If keystore fallback is used, keystore password must be stored as an ADCM secret.

## Kerberos

| ADCM / ticket name | Canonical Kyuubi key | Notes |
|--------------------|----------------------|-------|
| `kyuubi.flight.sql.kerberos.principal` | `kyuubi.spnego.principal` | Shared SPNEGO principal used by Flight Negotiate auth |
| _(shared)_ | `kyuubi.spnego.keytab` | Shared SPNEGO keytab |

## HA and paging

| ADCM name | Canonical Kyuubi key | Default |
|-----------|----------------------|---------|
| Flight HA namespace | `kyuubi.ha.flight.sql.namespace` | `kyuubi_flight` |
| Fetch page size | `kyuubi.frontend.flight.sql.fetch.max.rows` | `1000` |

## Validation rules for ADCM

1. Flight remains disabled unless `FLIGHT_SQL` is selected.
2. Port must be `0` or `1025..65534`.
3. When TLS is enabled, either PEM cert+key files or a complete shared keystore
   configuration must be present.
4. When Kerberos is selected, SPNEGO principal and keytab must be present.
5. Changing bind/TLS/Kerberos/HA settings is restart-required.
6. Upgrade from MVP configs preserves bind host/port and fetch max rows.
